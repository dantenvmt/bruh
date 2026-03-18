"""
Sitemap.xml parser for career pages.

Probes well-known sitemap paths, filters <loc> entries that look like job
URLs, and returns RawScrapedJob objects — no JavaScript required.

Sitemap data is structured and authoritative: when present it beats
link-graph extraction because the site explicitly lists every URL.
"""
from __future__ import annotations

import logging
import re
from typing import List, Optional
from urllib.parse import unquote, urlparse
from xml.etree import ElementTree

import httpx

from .css import _dedupe_jobs, _is_valid_job
from ..types import RawScrapedJob

logger = logging.getLogger(__name__)

# Sitemap paths to probe, in order.
_SITEMAP_PROBES = [
    "/sitemap.xml",
    "/sitemap_index.xml",
    "/careers/sitemap.xml",
    "/jobs/sitemap.xml",
    "/sitemap-careers.xml",
]

# URL patterns that suggest a job detail page.
_JOB_URL_RE = re.compile(
    r"/(jobs?|careers?|positions?|openings?|opportunities|listings?|postings?|vacancies|reqs?|requisitions?)"
    r"(/[^/]+)+/?$",
    re.IGNORECASE,
)

# Looser pattern: does the URL mention jobs/careers *anywhere* (for filtering
# child sitemaps in a sitemap index, e.g. "sitemap-careers.xml")?
_JOB_MENTION_RE = re.compile(
    r"(jobs?|careers?|positions?|openings?|opportunities|postings?)",
    re.IGNORECASE,
)

# Sitemap XML namespaces
_SM_NS = "http://www.sitemaps.org/schemas/sitemap/0.9"

# Paths with these markers are likely job detail pages.
_JOB_DETAIL_PATH_RE = re.compile(
    r"/(jobs?|positions?|openings?|opportunities|listings?|postings?|vacancies|reqs?|requisitions?|apply)(/|$)",
    re.IGNORECASE,
)

# Segment-level heuristics for sitemap-only URLs where the path is sparse
# (e.g. /careers/software-engineer).
_ROLE_WORDS = {
    "analyst",
    "architect",
    "assistant",
    "associate",
    "consultant",
    "coordinator",
    "designer",
    "developer",
    "director",
    "engineer",
    "intern",
    "lead",
    "manager",
    "officer",
    "operator",
    "pharmacist",
    "planner",
    "principal",
    "scientist",
    "specialist",
    "supervisor",
    "technician",
}
_NUMERIC_SEGMENT_RE = re.compile(r"^\d+$")
_REQ_ID_SEGMENT_RE = re.compile(r"^\d{4,}$")
_LOCALE_SEGMENT_RE = re.compile(r"^[a-z]{2}(?:-[a-z]{2})?$", re.IGNORECASE)
_TITLE_STOPWORDS = {
    "about",
    "apply",
    "career",
    "careers",
    "global",
    "job",
    "jobs",
    "node",
    "opening",
    "openings",
    "opportunities",
    "opportunity",
    "position",
    "positions",
    "posting",
    "postings",
    "req",
    "reqs",
    "requisition",
    "requisitions",
    "vacancies",
    "vacancy",
    "work-at-apple",
    "life-at-apple",
    "locations",
    "location",
    "teams",
    "team",
}


def _title_from_url(url: str) -> str:
    """Infer a human-readable job title from the URL path slug.

    Example: /jobs/senior-software-engineer-remote → 'Senior Software Engineer Remote'
    """
    path = urlparse(url).path.strip("/")
    if not path:
        return ""

    segments = [seg for seg in path.split("/") if seg]
    candidate: Optional[str] = None

    # Prefer a meaningful non-numeric segment near the end of the path.
    for segment in reversed(segments):
        low = segment.lower()
        if low in _TITLE_STOPWORDS:
            continue
        if _LOCALE_SEGMENT_RE.fullmatch(low):
            continue
        if _NUMERIC_SEGMENT_RE.fullmatch(low):
            continue
        if len(re.sub(r"[-_]+", "", low)) < 3:
            continue
        candidate = segment
        break

    if candidate is None:
        candidate = segments[-1]

    candidate = unquote(candidate)
    candidate = re.sub(r"\.(html?|php|aspx?)$", "", candidate, flags=re.IGNORECASE)
    candidate = re.sub(r"[-_]+", " ", candidate)
    candidate = re.sub(r"\s+", " ", candidate).strip()
    return candidate.title()


def _looks_like_job_detail_url(url: str) -> bool:
    """Conservative guard to reject sitemap URLs that are likely marketing pages."""
    path = urlparse(url).path.lower()
    if not path or path == "/":
        return False

    if _JOB_DETAIL_PATH_RE.search(path):
        return True

    # Some sites use /careers/{role-slug} without an explicit /job token.
    if "/careers/" not in path and not path.startswith("/careers"):
        return False

    segments = [seg for seg in path.strip("/").split("/") if seg]
    if not segments:
        return False
    last = re.sub(r"\.(html?|php|aspx?)$", "", segments[-1], flags=re.IGNORECASE)

    if _REQ_ID_SEGMENT_RE.fullmatch(last):
        return True

    if "-" in last:
        words = [w for w in last.split("-") if w]
        if len(words) >= 2 and any(w in _ROLE_WORDS for w in words):
            return True

    return False


def _jobs_from_urls(urls: List[str], base_url: str, company_name: str) -> List[RawScrapedJob]:
    """Convert sitemap URLs into validated, deduplicated jobs."""
    raw: List[RawScrapedJob] = []
    for url in urls:
        if not _looks_like_job_detail_url(url):
            continue
        title = _title_from_url(url)
        if not title:
            continue
        job = RawScrapedJob(title=title, url=url, company=company_name)
        if _is_valid_job(job, base_url):
            raw.append(job)
    return _dedupe_jobs(raw)


def _parse_sitemap_xml(xml_text: str, base_url: str) -> List[str]:
    """
    Parse a sitemap or sitemap index XML and return matching job URLs.

    Handles:
    - Standard sitemap: <urlset><url><loc>…</loc></url></urlset>
    - Sitemap index: <sitemapindex><sitemap><loc>…</loc></sitemap></sitemapindex>
      (we return the child sitemap URLs as-is; the caller re-probes them)
    """
    try:
        root = ElementTree.fromstring(xml_text)
    except ElementTree.ParseError as exc:
        logger.debug("Sitemap XML parse error for %s: %s", base_url, exc)
        return []

    # Strip namespace for tag comparison
    tag = root.tag.split("}")[-1] if "}" in root.tag else root.tag
    ns = _SM_NS if _SM_NS in (root.tag or "") else ""

    def _loc(elem):
        loc_tag = f"{{{ns}}}loc" if ns else "loc"
        node = elem.find(loc_tag)
        return (node.text or "").strip() if node is not None else ""

    urls: List[str] = []

    if tag == "sitemapindex":
        # Return child sitemap URLs so the caller can probe them
        child_tag = f"{{{ns}}}sitemap" if ns else "sitemap"
        for child in root.findall(child_tag):
            loc = _loc(child)
            if loc:
                urls.append(loc)
    else:
        # Standard urlset
        url_tag = f"{{{ns}}}url" if ns else "url"
        for url_elem in root.findall(url_tag):
            loc = _loc(url_elem)
            if loc and _JOB_URL_RE.search(urlparse(loc).path):
                urls.append(loc)

    return urls


async def fetch_and_parse_sitemap(
    base_url: str, company_name: Optional[str] = None, timeout: float = 15.0
) -> List[RawScrapedJob]:
    """
    Probe well-known sitemap paths for *base_url*, parse job URLs, and
    return a list of :class:`RawScrapedJob` objects.

    Args:
        base_url: The careers page URL (used to derive the site root).
        company_name: Company name to attach to each result. Falls back to
            the site's hostname when not supplied.
        timeout: HTTP timeout in seconds.

    Returns:
        List of RawScrapedJob. Empty if no sitemap found or no job URLs match.
    """
    parsed = urlparse(base_url)
    site_root = f"{parsed.scheme}://{parsed.netloc}"

    if not company_name:
        company_name = parsed.netloc

    async with httpx.AsyncClient(
        timeout=timeout,
        follow_redirects=True,
        headers={"User-Agent": "Mozilla/5.0 (compatible; JobScraper/1.0)"},
    ) as client:
        for path in _SITEMAP_PROBES:
            probe_url = site_root + path
            try:
                resp = await client.get(probe_url)
            except Exception as exc:
                logger.debug("Sitemap probe failed for %s: %s", probe_url, exc)
                continue

            if resp.status_code >= 400:
                continue

            ct = resp.headers.get("content-type", "").lower()
            if not any(t in ct for t in ("xml", "text")):
                # Check body prefix anyway — some servers send wrong content-type
                if not resp.text.lstrip().startswith("<"):
                    continue

            urls = _parse_sitemap_xml(resp.text, probe_url)
            if not urls:
                continue

            # If this was a sitemap index, recursively probe the first child sitemap
            # (don't probe all — they can be huge; take job-sounding ones first)
            if all(not _JOB_URL_RE.search(urlparse(u).path) for u in urls):
                # These are child sitemap URLs — pick job-flavoured ones
                child_urls = [u for u in urls if _JOB_MENTION_RE.search(u)]
                if not child_urls:
                    # Take the first child regardless and hope it has jobs
                    child_urls = urls[:1]

                job_urls: List[str] = []
                for child_sitemap_url in child_urls[:3]:  # probe at most 3 children
                    try:
                        child_resp = await client.get(child_sitemap_url)
                        if child_resp.status_code < 400:
                            job_urls.extend(_parse_sitemap_xml(child_resp.text, child_sitemap_url))
                    except Exception as exc:
                        logger.debug("Child sitemap fetch failed %s: %s", child_sitemap_url, exc)
                urls = job_urls

            if not urls:
                continue

            logger.info("Sitemap at %s yielded %d candidate job URLs", probe_url, len(urls))
            jobs = _jobs_from_urls(urls, base_url, company_name)
            if jobs:
                logger.info(
                    "Sitemap parser kept %d/%d validated jobs for %s",
                    len(jobs),
                    len(urls),
                    base_url,
                )
                return jobs
            logger.debug("Sitemap yielded 0 validated jobs for %s", base_url)

    return []
