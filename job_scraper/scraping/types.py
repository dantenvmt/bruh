"""
Type definitions for Phase A scraper.
"""
import re
from dataclasses import dataclass
from typing import Optional, List
from uuid import UUID

from ..models import Job


@dataclass
class RawScrapedJob:
    """Parser output - minimal extracted fields."""
    title: str
    url: str
    location: Optional[str] = None
    company: Optional[str] = None
    description: Optional[str] = None
    salary: Optional[str] = None


@dataclass
class SiteResult:
    """Per-site scrape outcome for accounting."""
    site_id: UUID
    success: bool
    jobs_found: int
    error: Optional[str] = None
    # Set True when the stored api_endpoint has gone stale (auth expiry, URL
    # change, etc.) so the orchestrator can reset fetch_mode and re-probe.
    needs_reprobe: bool = False


_COUNTRY_CODE_SUFFIX_RE = re.compile(r"\s+[A-Za-z]{2}\s+[A-Za-z]{2}$")
_NON_ASCII_RE = re.compile(r"[^\x00-\x7F]")

# Final-gate garbage title patterns (mirrors css.py GARBAGE_TITLE_PATTERNS).
# Kept here to avoid circular import (css.py imports RawScrapedJob from types.py).
_GARBAGE_TITLE_RE = re.compile(
    r"|".join([
        r"^(home|about|contact|career|careers|job|jobs|login|sign in|sign up|register)$",
        r"^(privacy|terms|cookie|legal|help|faq|support|blog|news)$",
        r"^(menu|navigation|skip to|go to|back to|view all|see all|load more)$",
        r"^(yes|no|ok|cancel|submit|apply|search)$",
        r"^(join us|who we are|early careers|all other roles)$",
        r"^(locations?|teams?|benefits?|accessibility|accommodation|students?|shared values)$",
        r"^(apply now|learn more|read more|view details|view more|click here|get started)$",
        r"^(show more|see details|explore|discover|find out more|view job|view jobs)$",
        r"^learn more about.+",
        r"^apply now.+",
    ]),
    re.IGNORECASE,
)


def _truncate(value: Optional[str], max_len: int) -> Optional[str]:
    """Truncate string to max length, preserving None."""
    if value is None:
        return None
    return value[:max_len] if len(value) > max_len else value


def _clean_title(title: Optional[str]) -> Optional[str]:
    """Strip trailing locale suffixes like 'Engineer Fr Fr' → 'Engineer'."""
    if not title:
        return title
    return _COUNTRY_CODE_SUFFIX_RE.sub("", title).strip()


def _is_garbage_title(title: Optional[str]) -> bool:
    """Return True for titles that are clearly non-English or locale variants."""
    if not title:
        return True
    non_ascii = len(_NON_ASCII_RE.findall(title))
    return non_ascii / max(len(title), 1) > 0.30


def convert_to_job_models(raw_jobs: List[RawScrapedJob], site) -> List[Job]:
    """Convert parser output to Job models for upsert_jobs().

    Applies quality filters:
    - Drops non-English titles (>30% non-ASCII characters)
    - Strips trailing locale/country-code suffixes from titles
    """
    jobs = []
    for raw in raw_jobs:
        # Drop non-English titles
        if _is_garbage_title(raw.title):
            continue

        cleaned_title = _clean_title(raw.title)

        # Drop titles matching known garbage patterns (UI text, CTA buttons)
        if cleaned_title and _GARBAGE_TITLE_RE.match(cleaned_title):
            continue

        # Drop single-word titles ≤15 chars — too generic ("Associate", "Job", "Sealer")
        if cleaned_title and " " not in cleaned_title.strip() and len(cleaned_title.strip()) <= 15:
            continue

        jobs.append(Job(
            title=_truncate(cleaned_title, 255),
            company=_truncate(raw.company or site.company_name, 255),
            location=_truncate(raw.location, 255),
            url=raw.url[:512] if raw.url else raw.url,
            description=_truncate(raw.description, 2000),
            salary=_truncate(raw.salary, 255),
            source="custom_scraper",
            job_id=_truncate(f"{site.id}:{raw.url}", 128),  # source_job_id is varchar(128)
        ))

    return jobs
