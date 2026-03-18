"""
Unified Job data model for all API sources
"""
from dataclasses import dataclass, asdict
from typing import Optional, List
from datetime import datetime


@dataclass
class Job:
    """Unified job posting model"""

    # Core fields
    title: str
    company: str
    location: Optional[str] = None
    url: Optional[str] = None

    # Details
    description: Optional[str] = None
    salary: Optional[str] = None
    employment_type: Optional[str] = None  # full-time, part-time, contract, etc.

    # Metadata
    posted_date: Optional[str] = None
    source: Optional[str] = None  # API source name
    job_id: Optional[str] = None

    # Categorization
    category: Optional[str] = None
    tags: Optional[List[str]] = None
    skills: Optional[List[str]] = None
    experience_level: Optional[str] = None
    experience_min_years: Optional[int] = None
    experience_max_years: Optional[int] = None
    required_skills: Optional[List[str]] = None
    industry: Optional[str] = None
    industry_confidence: Optional[float] = None
    work_mode: Optional[str] = None
    role_pop_reasons: Optional[List[str]] = None
    enrichment_version: Optional[int] = None
    enrichment_updated_at: Optional[str] = None

    # Remote work
    remote: Optional[bool] = None
    # Raw source payload (best-effort)
    raw_payload: Optional[dict] = None

    # Normalization output (populated by normalize.py)
    salary_min: Optional[int] = None
    salary_max: Optional[int] = None
    seniority: Optional[str] = None
    visa_sponsorship: Optional[bool] = None
    ai_summary_card: Optional[str] = None
    ai_summary_bullets: Optional[list] = None
    normalized_at: Optional[str] = None

    def to_dict(self) -> dict:
        """Convert to dictionary"""
        return asdict(self)

    @property
    def unique_key(self) -> str:
        """Generate unique key for deduplication"""
        title = (self.title or "").strip().lower()
        company = (self.company or "").strip().lower()
        url = (self.url or "").strip().lower()
        if url:
            return f"{url}|{title}|{company}"
        return f"{title}|{company}"
