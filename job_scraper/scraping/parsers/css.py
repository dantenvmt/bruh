"""
CSS selector-based parser for extracting job listings from HTML.

Uses BeautifulSoup to parse HTML with user-defined CSS selectors.
Includes validation to filter garbage results (nav items, footer links, etc).
"""
import re
from typing import List, Set
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from ..types import RawScrapedJob


class ParseError(Exception):
    """Raised when parsing fails or no jobs are found."""
    pass


# Validation thresholds
MAX_JOBS_PER_PAGE = 100  # More than this = likely nav items
MIN_TITLE_LENGTH = 3  # Shorter = likely garbage
MAX_TITLE_LENGTH = 200  # Longer = likely scraped wrong element

# Patterns that indicate garbage titles (nav, footer, generic links)
GARBAGE_TITLE_PATTERNS = [
    r"^(home|about|contact|career|careers|job|jobs|login|sign in|sign up|register)$",
    r"^(privacy|terms|cookie|legal|help|faq|support|blog|news)$",
    r"^(facebook|twitter|linkedin|instagram|youtube|tiktok)$",
    r"^(menu|navigation|skip to|go to|back to|view all|see all|load more)$",
    r"^(copyright|all rights reserved|\d{4})$",
    r"^\d+$",  # Pure numbers
    r"^[a-z]$",  # Single letters
    r"^(yes|no|ok|cancel|submit|apply|search)$",
    r"^(join us|who we are|early careers|all other roles)$",
    r"^(diversity inclusion|research and development|opportunities pre college)$",
    r"^(locations?|teams?|benefits?|accessibility|accommodation|students?|shared values)$",
    r"^.+\.(html?|php|aspx?)$",
    # UI buttons / CTA text scraped as job titles
    r"^(apply now|learn more|read more|view details|view more|click here|get started)$",
    r"^(show more|see details|explore|discover|find out more|view job|view jobs)$",
    r"^learn more about.+",   # "Learn more about our accommodations..."
    r"^apply now.+",          # "Apply nowabout Oracle NetSuite"
]
GARBAGE_REGEX = re.compile("|".join(GARBAGE_TITLE_PATTERNS), re.IGNORECASE)

# Titles ending in a country-code pair like "Engineer Fr Fr" or "Manager De De"
# are locale variants scraped from multinational ATS pages — not US jobs.
_COUNTRY_CODE_SUFFIX_RE = re.compile(r"\s+[A-Za-z]{2}\s+[A-Za-z]{2}$")

# Titles with a high ratio of non-ASCII characters are likely non-English.
_NON_ASCII_RE = re.compile(r"[^\x00-\x7F]")

# URL patterns that indicate non-job links
GARBAGE_URL_PATTERNS = [
    r"/(login|signin|signup|register|auth)/",
    r"/(about|contact|privacy|terms|legal|help|faq)/",
    r"/(blog|news|press|media)/",
    r"/#$",  # Anchor-only links
    r"/\?.*=",  # Query params without path (likely filters)
    r"^mailto:",
    r"^tel:",
    r"^javascript:",
]
GARBAGE_URL_REGEX = re.compile("|".join(GARBAGE_URL_PATTERNS), re.IGNORECASE)

# URL must look like a job detail or known ATS posting endpoint.
JOB_URL_SIGNAL_REGEX = re.compile(
    r"/(jobs?|positions?|openings?|opportunities|postings?|vacancies|requisitions?|reqs?|apply|careers?)(/|$|-)",
    re.IGNORECASE,
)
ATS_URL_SIGNAL_REGEX = re.compile(
    r"("
    r"boards\\.greenhouse\\.io/[^/]+/jobs/\\d+"
    r"|jobs\\.lever\\.co/[^/]+/[a-f0-9-]+"
    r"|jobs\\.ashbyhq\\.com/[^/]+/[a-f0-9-]+"
    r"|apply\\.workable\\.com/[^/]+/j/[A-Z0-9]+"
    r"|[^/]+\\.myworkdayjobs\\.com/.+/job/"
    r"|[^/]+\\.icims\\.com/jobs?/\\d+"
    r"|smartrecruiters\\.com/[^/]+/[0-9]+"
    r"|careers\\.(amd|arm)\\.com/job/"
    r"|jobs\\.boeing\\.com/job/"
    r")",
    re.IGNORECASE,
)
MARKETING_PATH_REGEX = re.compile(
    r"/("
    r"about(-us)?"
    r"|benefits?"
    r"|culture"
    r"|diversity(-inclusion)?"
    r"|events?"
    r"|insights?"
    r"|investors?"
    r"|join-us"
    r"|life-at(-[a-z0-9-]+)?"
    r"|locations?"
    r"|news(room)?"
    r"|products?"
    r"|science"
    r"|stories"
    r"|students?"
    r"|team(s)?"
    r"|who-we-are"
    r"|work-at(-[a-z0-9-]+)?"
    r"|choose-country-region"
    r")(?:/|$|\.)",
    re.IGNORECASE,
)
NON_HTML_RESOURCE_REGEX = re.compile(r"\.(pdf|docx?|pptx?|xlsx?)$", re.IGNORECASE)


def _is_valid_job(job: RawScrapedJob, base_url: str) -> bool:
    """
    Validate that a scraped item looks like a real job posting.

    Returns False for nav items, footer links, and other garbage.
    """
    title = job.title.strip()

    # Title length check
    if len(title) < MIN_TITLE_LENGTH or len(title) > MAX_TITLE_LENGTH:
        return False

    # Garbage title pattern check
    if GARBAGE_REGEX.match(title):
        return False

    # Drop locale variants — titles ending in "XX XX" country code pairs
    if _COUNTRY_CODE_SUFFIX_RE.search(title):
        return False

    # Drop titles where >30% of characters are non-ASCII (likely non-English)
    non_ascii_count = len(_NON_ASCII_RE.findall(title))
    if non_ascii_count / max(len(title), 1) > 0.30:
        return False

    # URL validation
    if not job.url:
        return False

    # Skip same-page anchors
    parsed = urlparse(job.url)
    if not parsed.path or parsed.path == "/":
        return False

    # Skip links to documents/resources, not job detail pages.
    if NON_HTML_RESOURCE_REGEX.search(parsed.path):
        return False

    # Skip garbage URL patterns
    if GARBAGE_URL_REGEX.search(job.url):
        return False

    # Skip if URL is identical to base_url (self-link)
    base_parsed = urlparse(base_url)
    if parsed.netloc == base_parsed.netloc and parsed.path == base_parsed.path:
        return False

    # URL must look job-like.
    full_url = job.url
    if not ATS_URL_SIGNAL_REGEX.search(full_url):
        if not JOB_URL_SIGNAL_REGEX.search(parsed.path):
            return False
        if MARKETING_PATH_REGEX.search(parsed.path):
            return False

    return True


def _dedupe_jobs(jobs: List[RawScrapedJob]) -> List[RawScrapedJob]:
    """Remove duplicate jobs by URL."""
    seen_urls: Set[str] = set()
    unique = []
    for job in jobs:
        if job.url not in seen_urls:
            seen_urls.add(job.url)
            unique.append(job)
    return unique


def parse(html: str, selector_hints: dict, base_url: str) -> List[RawScrapedJob]:
    """
    Parse HTML using CSS selectors to extract job listings.

    Args:
        html: Raw HTML content to parse
        selector_hints: Dict with keys:
            - job_container: CSS selector for each job listing container (required)
            - title: CSS selector for job title within container (required)
            - link: CSS selector for job URL within container (required)
            - location: CSS selector for location within container (optional)
        base_url: Base URL for resolving relative links

    Returns:
        List of RawScrapedJob objects

    Raises:
        ParseError: If no jobs found or required selectors missing

    Example:
        >>> selector_hints = {
        ...     "job_container": ".job-listing",
        ...     "title": "h3",
        ...     "link": "a",
        ...     "location": ".location"
        ... }
        >>> jobs = parse(html, selector_hints, "https://example.com/careers")
    """
    # Validate required selectors
    required = ["job_container", "title", "link"]
    missing = [k for k in required if k not in selector_hints]
    if missing:
        raise ParseError(f"Missing required selectors: {', '.join(missing)}")

    soup = BeautifulSoup(html, "html.parser")

    # Find all job containers
    containers = soup.select(selector_hints["job_container"])
    if not containers:
        raise ParseError(
            f"No job containers found with selector: {selector_hints['job_container']}"
        )

    jobs = []
    for container in containers:
        try:
            # Extract title
            title_elem = container.select_one(selector_hints["title"])
            if not title_elem:
                continue  # Skip if no title found
            title = title_elem.get_text(strip=True)
            if not title:
                continue

            # Extract link
            link_elem = container.select_one(selector_hints["link"])
            if not link_elem:
                continue  # Skip if no link found

            # Get href and resolve relative URLs
            href = link_elem.get("href")
            if not href:
                continue
            url = urljoin(base_url, href)

            # Extract location (optional)
            location = None
            if "location" in selector_hints and selector_hints["location"]:
                location_elem = container.select_one(selector_hints["location"])
                if location_elem:
                    location = location_elem.get_text(strip=True) or None

            job = RawScrapedJob(
                title=title,
                url=url,
                location=location
            )

            # Validate job before adding
            if _is_valid_job(job, base_url):
                jobs.append(job)

        except Exception as e:
            # Skip individual jobs that fail to parse
            continue

    # Dedupe by URL
    jobs = _dedupe_jobs(jobs)

    # Check for garbage selector (too many results = nav items)
    if len(jobs) > MAX_JOBS_PER_PAGE:
        raise ParseError(
            f"Selector returned {len(jobs)} jobs (max {MAX_JOBS_PER_PAGE}). "
            "Likely selecting nav items or non-job elements. "
            "Refine the job_container selector."
        )

    if not jobs:
        raise ParseError(
            f"No valid jobs extracted from {len(containers)} containers after filtering. "
            "Selectors may be targeting non-job elements."
        )

    return jobs


def parse_with_selectors(html: str, selector_hints: dict, base_url: str) -> List[RawScrapedJob]:
    """
    Alias for parse() to match the import in __init__.py.

    This maintains backward compatibility with the existing import:
    `from .css import parse_with_selectors`
    """
    return parse(html, selector_hints, base_url)
