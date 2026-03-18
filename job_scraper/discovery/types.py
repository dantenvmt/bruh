"""
Type definitions for the discovery module.

These dataclasses define the contracts between discovery components.
"""
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional


class DiscoverySource(str, Enum):
    """Source of company discovery."""
    SEED_CSV = "seed_csv"
    HARDCODED = "hardcoded"
    FORTUNE500 = "fortune500"  # Deferred
    YC = "yc"  # Deferred


class ATSType(str, Enum):
    """Detected ATS platform type."""
    GREENHOUSE = "greenhouse"
    LEVER = "lever"
    ASHBY = "ashby"
    SMARTRECRUITERS = "smartrecruiters"
    WORKDAY = "workday"
    ICIMS = "icims"
    WORKABLE = "workable"
    TALEO = "taleo"
    CUSTOM = "custom"
    UNKNOWN = "unknown"

    @property
    def has_existing_adapter(self) -> bool:
        """Check if we have an existing API adapter for this ATS."""
        return self in {
            ATSType.GREENHOUSE,
            ATSType.LEVER,
            ATSType.ASHBY,
            ATSType.SMARTRECRUITERS,
            ATSType.WORKDAY,
            ATSType.ICIMS,
            ATSType.WORKABLE,
        }

    @property
    def is_deferred(self) -> bool:
        """Check if this ATS type is deferred (no adapter yet)."""
        return self in {
            ATSType.TALEO,
        }


@dataclass(frozen=True)
class DiscoveredCompany:
    """A company discovered from a source.

    This is the output of the sources module and input to the resolver.
    """
    name: str
    source: DiscoverySource
    priority: Optional[int] = None  # 1 = highest priority
    careers_url: Optional[str] = None  # May be pre-populated from source
    category: Optional[str] = None  # e.g., "big_tech", "consulting"

    def __post_init__(self):
        if not self.name or not self.name.strip():
            raise ValueError("Company name cannot be empty")


@dataclass(frozen=True)
class ProbeResult:
    """Result of probing a careers URL for ATS type.

    This is the output of the probe module.
    """
    careers_url: str
    final_url: str  # After redirects
    detected_ats: ATSType
    confidence: float  # 0.0 to 1.0
    fetch_mode: str  # 'static', 'browser', or 'api_spy'
    robots_allowed: bool
    ats_token: Optional[str] = None  # e.g., greenhouse board ID
    detection_method: Optional[str] = None  # 'url', 'dom', 'iframe', 'network_spy'
    error: Optional[str] = None
    # Populated when fetch_mode == 'api_spy': serialisable endpoint config
    # (url, method, replay_headers, request_post_data, pagination, confidence)
    api_endpoint: Optional[dict] = None

    def __post_init__(self):
        if not 0.0 <= self.confidence <= 1.0:
            raise ValueError(f"Confidence must be between 0 and 1, got {self.confidence}")
        if self.fetch_mode not in ("static", "browser", "api_spy"):
            raise ValueError(f"fetch_mode must be 'static', 'browser', or 'api_spy', got {self.fetch_mode}")


@dataclass
class SelectorHint:
    """Auto-detected CSS selector hints for a custom site.

    These are NOT production-ready selectors - they require human validation.
    """
    job_container: Optional[str] = None
    title: Optional[str] = None
    link: Optional[str] = None
    location: Optional[str] = None
    description_snippet: Optional[str] = None
    confidence: float = 0.0
    sample_count: int = 0  # Number of job items found with these selectors
    notes: str = ""

    def to_dict(self) -> dict:
        """Convert to JSONB-compatible dict."""
        return {
            "job_container": self.job_container,
            "title": self.title,
            "link": self.link,
            "location": self.location,
            "description_snippet": self.description_snippet,
            "confidence": self.confidence,
            "sample_count": self.sample_count,
            "notes": self.notes,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "SelectorHint":
        """Create from JSONB dict."""
        return cls(
            job_container=data.get("job_container"),
            title=data.get("title"),
            link=data.get("link"),
            location=data.get("location"),
            description_snippet=data.get("description_snippet"),
            confidence=data.get("confidence", 0.0),
            sample_count=data.get("sample_count", 0),
            notes=data.get("notes", ""),
        )

    def is_valid(self) -> bool:
        """Check if hints have the minimum required selectors."""
        return bool(self.job_container and self.title and self.link)


@dataclass
class DiscoveryStats:
    """Statistics from a discovery run."""
    total_companies: int = 0
    urls_resolved: int = 0
    urls_failed: int = 0
    ats_probed: int = 0
    by_ats_type: dict = field(default_factory=dict)
    robots_blocked: int = 0
    selectors_generated: int = 0
    duration_seconds: float = 0.0
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None

    def summary(self) -> str:
        """Generate human-readable summary."""
        lines = [
            f"Total companies: {self.total_companies}",
            f"URLs resolved: {self.urls_resolved} ({self.urls_failed} failed)",
            f"ATS probed: {self.ats_probed}",
        ]
        if self.by_ats_type:
            lines.append("By ATS type:")
            for ats, count in sorted(self.by_ats_type.items(), key=lambda x: -x[1]):
                lines.append(f"  {ats}: {count}")
        if self.robots_blocked:
            lines.append(f"Robots.txt blocked: {self.robots_blocked}")
        if self.selectors_generated:
            lines.append(f"Selector hints generated: {self.selectors_generated}")
        if self.duration_seconds:
            lines.append(f"Duration: {self.duration_seconds:.1f}s")
        return "\n".join(lines)
