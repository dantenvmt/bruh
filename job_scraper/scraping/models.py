"""
SQLAlchemy model for scrape_sites table.

Matches schema from migrations 005 + 006.
"""
import uuid

from sqlalchemy import Column, String, Text, Boolean, Integer, DateTime, Float, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.sql import func

from ..storage import Base


class ScrapeSite(Base):
    """Configuration for a career site to scrape directly.

    This model supports both discovery (Phase 1) and scraping (Phase 2) workflows.

    Discovery fields (Phase 1):
        source: where company was discovered (seed_csv, hardcoded, etc.)
        detected_ats: ATS platform classification
        detection_probed_at: when ATS probe ran
        selector_hints: auto-detected hints (not production)
        selector_confidence: confidence score 0-1
        discovery_notes: human review notes
        robots_allowed: robots.txt compliance check result
        priority: company priority for weighted calculations

    Phase 2 fields:
        fetch_mode: 'static' or 'browser'
        next_scrape_at: explicit scheduling timestamp
        max_failures: auto-disable threshold
        last_error_code: typed error code

    Legacy fields (kept for backwards compatibility):
        site_type, requires_js, anti_bot_level
    """

    __tablename__ = "scrape_sites"

    # Primary key
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)

    # Core fields
    company_name = Column(String(256), nullable=False)
    careers_url = Column(Text, nullable=False, unique=True)

    # Legacy fields (kept for backwards compatibility, to be dropped in 007)
    site_type = Column(String(32), default="custom")
    requires_js = Column(Boolean, default=False)
    anti_bot_level = Column(String(16), default="none")

    # Scraping configuration
    selectors = Column(JSONB, default=dict)
    scrape_interval_hours = Column(Integer, default=24)
    enabled = Column(Boolean, default=True)

    # Discovery fields (Phase 1)
    source = Column(String(32), nullable=True)  # 'seed_csv', 'hardcoded', etc.
    detected_ats = Column(String(32), nullable=True)  # 'greenhouse', 'lever', 'custom', etc.
    detection_probed_at = Column(DateTime, nullable=True)
    selector_hints = Column(JSONB, nullable=True)
    selector_confidence = Column(Float, nullable=True)
    discovery_notes = Column(Text, nullable=True)
    robots_allowed = Column(Boolean, nullable=True)
    priority = Column(Integer, nullable=True)

    # Phase 2 fields
    fetch_mode = Column(String(16), default="static")  # 'static', 'browser', or 'api_spy'
    # Populated when fetch_mode='api_spy': discovered endpoint config from NetworkSpy.
    # Keys: url, method, replay_headers, request_post_data, pagination, confidence.
    api_endpoint = Column(JSONB, nullable=True)
    next_scrape_at = Column(DateTime, nullable=True)
    max_failures = Column(Integer, default=5)
    last_error_code = Column(String(32), nullable=True)

    # Runtime state
    last_scraped_at = Column(DateTime, nullable=True)
    last_success_at = Column(DateTime, nullable=True)
    consecutive_failures = Column(Integer, default=0)
    last_error = Column(Text, nullable=True)
    notes = Column(Text, nullable=True)

    # Timestamps
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())

    # Constraints
    __table_args__ = (
        UniqueConstraint('company_name', 'source', name='uq_scrape_sites_company_source'),
    )

    def __repr__(self) -> str:
        return f"<ScrapeSite(company={self.company_name!r}, url={self.careers_url!r}, ats={self.detected_ats}, enabled={self.enabled})>"
