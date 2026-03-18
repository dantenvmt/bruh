"""
RSS/Atom feed detector and parser for career pages.

Some company career pages expose RSS or Atom feeds (e.g. /careers/feed,
/jobs.rss).  When present, these are zero-risk, structured data sources
that require no HTML parsing at all.

Detection strategy:
  1. Check for <link rel="alternate" type="application/rss+xml"> in HTML
  2. Check for <link rel="alternate" type="application/atom+xml"> in HTML
  3. Probe common feed URL patterns as a fallback

Feed parsing uses xml.etree.ElementTree (stdlib, no dependencies).
"""
import logging
import re
from html import unescape
from typing import List, Optional
from urllib.parse import urljoin
from xml.etree import ElementTree

import httpx
from bs4 import BeautifulSoup

from .css import _is_valid_job, _dedupe_jobs
from ..types import RawScrapedJob

logger = logging.getLogger(__name__)

# Common feed URL suffixes to probe when no <link> tag is found.
_FEED_PROBES = [
    "/careers/feed",
    "/careers/feed.xml",
    "/careers/rss",
    "/careers.rss",
    "/jobs/feed",
    "/jobs/feed.xml",
    "/jobs/rss",
    "/jobs.rss",
    "/feed/careers",
    "/feed/jobs",
]

# Atom namespace
_ATOM_NS = "http://www.w3.org/2005/Atom"


def detect_feed_url(html: str, base_url: str) -> Optional[str]:
    """Detect an RSS or Atom feed URL from HTML <link> tags.

    Args:
        html: Raw HTML content.
        base_url: The URL the page was fetched from.

    Returns:
        Absolute feed URL if found, None otherwise.
    """
    soup = BeautifulSoup(html, "html.parser")

    for link in soup.find_all("link", rel="alternate"):
        link_type = (link.get("type") or "").lower()
        href = link.get("href", "").strip()
        if not href:
            continue
        if link_type in ("application/rss+xml", "application/atom+xml", "text/xml"):
            return urljoin(base_url, href)

    return None


async def probe_feed_urls(base_url: str, timeout: float = 10.0) -> Optional[str]:
    """Probe common feed URL patterns to find a valid feed.

    Args:
        base_url: The careers page URL.
        timeout: HTTP timeout in seconds.

    Returns:
        The first responding feed URL, or None.
    """
    # Derive the site root for probing
    from urllib.parse import urlparse
    parsed = urlparse(base_url)
    site_root = f"{parsed.scheme}://{parsed.netloc}"

    async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
        for suffix in _FEED_PROBES:
            probe_url = site_root + suffix
            try:
                resp = await client.head(probe_url)
                if resp.status_code < 400:
                    ct = resp.headers.get("content-type", "").lower()
                    if any(t in ct for t in ("xml", "rss", "atom")):
                        return probe_url
            except Exception:
                continue
    return None


def _parse_rss(xml_text: str, base_url: str) -> List[RawScrapedJob]:
    """Parse RSS 2.0 feed XML into job listings."""
    try:
        root = ElementTree.fromstring(xml_text)
    except ElementTree.ParseError:
        return []

    channel = root.find("channel")
    if channel is None:
        return []

    jobs: List[RawScrapedJob] = []
    for item in channel.findall("item"):
        title = unescape(item.findtext("title") or "").strip()
        link = (item.findtext("link") or "").strip()
        if not title or not link:
            continue
        if not link.startswith("http"):
            link = urljoin(base_url, link)

        # Try to extract location from category tags or description
        location = None
        categories = [c.text.strip() for c in item.findall("category") if c.text]
        for cat in categories:
            if _looks_like_location(cat):
                location = cat
                break

        jobs.append(RawScrapedJob(title=title, url=link, location=location))

    return jobs


def _parse_atom(xml_text: str, base_url: str) -> List[RawScrapedJob]:
    """Parse Atom feed XML into job listings."""
    try:
        root = ElementTree.fromstring(xml_text)
    except ElementTree.ParseError:
        return []

    jobs: List[RawScrapedJob] = []
    for entry in root.findall(f"{{{_ATOM_NS}}}entry"):
        title_elem = entry.find(f"{{{_ATOM_NS}}}title")
        title = unescape((title_elem.text or "").strip()) if title_elem is not None else ""
        if not title:
            continue

        # Atom links are in <link> elements with href attribute
        link = ""
        for link_elem in entry.findall(f"{{{_ATOM_NS}}}link"):
            rel = link_elem.get("rel", "alternate")
            if rel == "alternate":
                link = link_elem.get("href", "").strip()
                break
        if not link:
            # Try first link regardless of rel
            first_link = entry.find(f"{{{_ATOM_NS}}}link")
            if first_link is not None:
                link = first_link.get("href", "").strip()
        if not link:
            continue
        if not link.startswith("http"):
            link = urljoin(base_url, link)

        jobs.append(RawScrapedJob(title=title, url=link))

    return jobs


def _looks_like_location(text: str) -> bool:
    """Heuristic: does this category text look like a location?"""
    location_patterns = re.compile(
        r"(remote|hybrid|on-?site|new york|san francisco|london|berlin|"
        r"[A-Z]{2},\s*[A-Z]{2}|"  # e.g. "Austin, TX"
        r"\b[A-Z][a-z]+,\s*[A-Z]{2}\b)",  # e.g. "Denver, CO"
        re.IGNORECASE,
    )
    return bool(location_patterns.search(text))


def parse_feed(xml_text: str, base_url: str) -> List[RawScrapedJob]:
    """Parse an RSS or Atom feed into job listings.

    Automatically detects feed format.

    Args:
        xml_text: Raw XML content of the feed.
        base_url: The URL the feed was fetched from.

    Returns:
        List of RawScrapedJob objects.
    """
    # Detect format
    if "<feed" in xml_text[:500] and "xmlns" in xml_text[:500]:
        jobs = _parse_atom(xml_text, base_url)
    else:
        jobs = _parse_rss(xml_text, base_url)

    # Validate and dedupe
    valid = [j for j in jobs if _is_valid_job(j, base_url)]
    valid = _dedupe_jobs(valid)

    if valid:
        logger.info("RSS/Atom feed extracted %d jobs from %s", len(valid), base_url)

    return valid


async def fetch_and_parse_feed(
    feed_url: str, base_url: str, timeout: float = 15.0
) -> List[RawScrapedJob]:
    """Fetch a feed URL and parse it.

    Args:
        feed_url: The RSS/Atom feed URL.
        base_url: The original careers page URL (for context).
        timeout: HTTP timeout.

    Returns:
        List of RawScrapedJob objects. Empty on any error.
    """
    try:
        async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
            resp = await client.get(feed_url)
            if resp.status_code >= 400:
                return []
            return parse_feed(resp.text, base_url)
    except Exception as exc:
        logger.debug("Failed to fetch/parse feed %s: %s", feed_url, exc)
        return []
