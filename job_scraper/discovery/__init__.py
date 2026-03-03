"""
Career site discovery module.

Phase 1 of the career site scraping pipeline. Discovers companies,
resolves career page URLs, probes for ATS platforms, and generates
selector hints for custom sites.

CLI: python -m job_scraper.cli discover <subcommand>
"""
from .types import (
    DiscoveredCompany,
    ProbeResult,
    SelectorHint,
    ATSType,
    DiscoverySource,
)

__all__ = [
    "DiscoveredCompany",
    "ProbeResult",
    "SelectorHint",
    "ATSType",
    "DiscoverySource",
]
