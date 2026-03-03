"""
Type definitions for Phase A scraper.
"""
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


def _truncate(value: Optional[str], max_len: int) -> Optional[str]:
    """Truncate string to max length, preserving None."""
    if value is None:
        return None
    return value[:max_len] if len(value) > max_len else value


def convert_to_job_models(raw_jobs: List[RawScrapedJob], site) -> List[Job]:
    """Convert parser output to Job models for upsert_jobs()."""
    return [
        Job(
            title=_truncate(raw.title, 255),
            company=_truncate(raw.company or site.company_name, 255),
            location=_truncate(raw.location, 255),
            url=raw.url[:512] if raw.url else raw.url,
            source="custom_scraper",
            job_id=_truncate(f"{site.id}:{raw.url}", 128),  # source_job_id is varchar(128)
        )
        for raw in raw_jobs
    ]
