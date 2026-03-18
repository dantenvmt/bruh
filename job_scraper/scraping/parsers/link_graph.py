"""
Link-graph extractor for job listings.

Instead of relying on CSS selectors to find "job card" containers, this parser
extracts ALL <a> tags from the page and filters by URL pattern.  Job-related
links (e.g. /jobs/*, /positions/*, /careers/*/apply) are kept; everything else
is discarded.

Advantages over CSS parsing:
  - No per-site selector configuration needed
  - Links are the most stable part of any page (href cannot change without
    breaking the site's own navigation)
  - Deterministic, instant, zero cost

Limitations:
  - Only extracts title (link text) and URL — no location data
  - SPA-heavy sites using onClick routing instead of <a> tags will not work
"""
import logging
import re
from typing import List, Set
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from .css import _is_valid_job, _dedupe_jobs
from ..types import RawScrapedJob

logger = logging.getLogger(__name__)

# URL path patterns that strongly indicate an individual job posting.
# These are fragments that appear in the path component of the URL.
_JOB_URL_PATTERNS = re.compile(
    r"/(jobs?|positions?|openings?|vacancies|opportunities|postings?|roles?|requisitions?|reqs?)"
    r"/[^/]+",  # Must have at least one more path segment (the specific job)
    re.IGNORECASE,
)

# Additional patterns for ATS-hosted job pages
_ATS_JOB_PATTERNS = re.compile(
    r"("
    r"boards\.greenhouse\.io/[^/]+/jobs/\d+"
    r"|jobs\.lever\.co/[^/]+/[a-f0-9-]+"
    r"|jobs\.ashbyhq\.com/[^/]+/[a-f0-9-]+"
    r"|apply\.workable\.com/[^/]+/j/[A-Z0-9]+"
    r"|[^/]+\.myworkdayjobs\.com/.+/job/"
    r"|[^/]+\.icims\.com/jobs/\d+"
    r"|smartrecruiters\.com/[^/]+/[0-9]+"
    r")",
    re.IGNORECASE,
)

# Patterns that indicate a listing/index page rather than a specific job
_INDEX_PAGE_PATTERNS = re.compile(
    r"/(jobs?|careers?|openings?|positions?|requisitions?|reqs?)/?(\?|#|$)",
    re.IGNORECASE,
)

# Marketing/content subpaths that are not job detail pages.
_CAREERS_MARKETING_SEGMENTS = {
    "about",
    "accommodation",
    "accessibility",
    "benefits",
    "choose-country-region",
    "culture",
    "diversity",
    "diversity-inclusion",
    "early-careers",
    "events",
    "faq",
    "home",
    "index",
    "join-us",
    "life-at",
    "life-at-apple",
    "life-at-lm",
    "locations",
    "students",
    "teams",
    "who-we-are",
    "work-at",
    "work-at-apple",
}

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

_NUMERIC_SEGMENT_RE = re.compile(r"^\d{4,}$")

# Minimum link text length to be considered a job title
_MIN_TEXT_LENGTH = 5
# Maximum — avoids grabbing paragraph-length link text
_MAX_TEXT_LENGTH = 200


def _is_job_url(url: str, base_url: str) -> bool:
    """Check if a URL looks like an individual job posting (not an index page)."""
    parsed = urlparse(url)
    base_parsed = urlparse(base_url)

    # Skip same-page links
    if parsed.netloc == base_parsed.netloc and parsed.path == base_parsed.path:
        return False

    full_url = parsed.geturl()

    # Check ATS patterns first (highest confidence)
    if _ATS_JOB_PATTERNS.search(full_url):
        return True

    # For non-ATS URLs, stay on-domain to avoid pulling careers-related
    # external content links (rankings, blog posts, partner sites, etc.).
    if parsed.netloc and parsed.netloc.lower() != (base_parsed.netloc or "").lower():
        return False

    # Check generic job URL patterns
    if _JOB_URL_PATTERNS.search(parsed.path):
        # Make sure it's not just an index page
        if not _INDEX_PAGE_PATTERNS.search(parsed.path):
            return True

    # Separate, stricter heuristic for /careers/* URLs.
    path = parsed.path.lower()
    if "/careers/" in path or path.startswith("/careers"):
        segments = [s for s in path.strip("/").split("/") if s]
        if not segments:
            return False
        norm_segments = [
            re.sub(r"\.(html?|php|aspx?)$", "", seg, flags=re.IGNORECASE)
            for seg in segments
        ]
        if any(seg in _CAREERS_MARKETING_SEGMENTS for seg in norm_segments):
            return False

        last = norm_segments[-1]
        last = re.sub(r"\.(html?|php|aspx?)$", "", last, flags=re.IGNORECASE)

        if _NUMERIC_SEGMENT_RE.fullmatch(last):
            return True

        words = [w for w in re.split(r"[-_]+", last) if w]
        if len(words) >= 2 and any(w in _ROLE_WORDS for w in words):
            return True

    return False


def parse_link_graph(html: str, base_url: str) -> List[RawScrapedJob]:
    """Extract job listings by analyzing all links on the page.

    Args:
        html: Raw HTML content.
        base_url: The URL the HTML was fetched from.

    Returns:
        List of RawScrapedJob objects. Empty list if no job links found.
    """
    soup = BeautifulSoup(html, "html.parser")

    jobs: List[RawScrapedJob] = []
    seen_urls: Set[str] = set()

    for a_tag in soup.find_all("a", href=True):
        href = a_tag.get("href", "").strip()
        if not href or href.startswith(("javascript:", "mailto:", "tel:", "#")):
            continue

        url = urljoin(base_url, href)

        # Skip duplicates early
        if url in seen_urls:
            continue

        if not _is_job_url(url, base_url):
            continue

        # Extract link text as title
        text = a_tag.get_text(strip=True)
        if not text or len(text) < _MIN_TEXT_LENGTH or len(text) > _MAX_TEXT_LENGTH:
            continue

        seen_urls.add(url)
        job = RawScrapedJob(title=text, url=url)
        if _is_valid_job(job, base_url):
            jobs.append(job)

    jobs = _dedupe_jobs(jobs)

    if jobs:
        logger.info(
            "Link-graph extractor found %d job links on %s", len(jobs), base_url
        )

    return jobs
