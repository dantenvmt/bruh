"""
URL canonicalization and deduplication utilities.

Used to prevent duplicate entries when building company lists from
multiple sources.
"""
import re
from urllib.parse import urlparse
from typing import Set, Optional

from ..config import Config


def canonicalize_domain(url: str) -> str:
    """Canonicalize a URL to its domain for deduplication.

    Examples:
        "https://WWW.Stripe.com/jobs/" -> "stripe.com"
        "http://careers.google.com" -> "careers.google.com"
        "https://boards.greenhouse.io/airbnb" -> "boards.greenhouse.io"

    Args:
        url: URL to canonicalize

    Returns:
        Canonical domain string (lowercase, no www prefix, no trailing slash)
    """
    url = url.lower().strip()

    # Handle URLs without scheme
    if not url.startswith(("http://", "https://")):
        url = "https://" + url

    parsed = urlparse(url)
    domain = parsed.netloc

    # Remove www. prefix
    if domain.startswith("www."):
        domain = domain[4:]

    # Remove port if present
    if ":" in domain:
        domain = domain.split(":")[0]

    return domain


def canonicalize_company_name(name: str) -> str:
    """Canonicalize a company name for matching.

    Examples:
        "Google, Inc." -> "google"
        "JPMorgan Chase & Co." -> "jpmorgan chase"
        "  Amazon  " -> "amazon"

    Args:
        name: Company name to canonicalize

    Returns:
        Canonical name (lowercase, stripped, common suffixes removed)
    """
    name = name.lower().strip()

    # Remove common suffixes
    suffixes = [
        r",?\s*inc\.?$",
        r",?\s*llc\.?$",
        r",?\s*ltd\.?$",
        r",?\s*corp\.?$",
        r",?\s*corporation$",
        r",?\s*& co\.?$",
        r",?\s*co\.?$",
        r",?\s*plc$",
    ]

    for suffix in suffixes:
        name = re.sub(suffix, "", name, flags=re.IGNORECASE)

    # Remove extra whitespace
    name = " ".join(name.split())

    return name


class DeduplicationChecker:
    """Check for duplicates against existing data sources.

    Maintains sets of known companies from:
    - known_tokens.yaml (ATS overrides)
    - Existing scrape_sites entries
    - ATS adapter configs (greenhouse boards, lever sites, etc.)
    """

    def __init__(self, config: Optional[Config] = None):
        """Initialize with optional config for loading known data.

        Args:
            config: Config instance for loading known_tokens and ATS configs.
                   If None, only manual additions via add_* methods work.
        """
        self._known_domains: Set[str] = set()
        self._known_companies: Set[str] = set()
        self._known_ats_tokens: dict[str, Set[str]] = {
            "greenhouse": set(),
            "lever": set(),
            "ashby": set(),
            "smartrecruiters": set(),
        }

        if config:
            self._load_from_config(config)

    def _load_from_config(self, config: Config) -> None:
        """Load known companies from config sources."""
        # Load from known_tokens.yaml
        known_tokens = config.known_tokens
        for company_name in known_tokens.keys():
            self._known_companies.add(canonicalize_company_name(company_name))

        # Load from ATS adapter configs
        gh = config.greenhouse
        if gh and gh.get("boards"):
            self._known_ats_tokens["greenhouse"].update(
                t.lower() for t in gh["boards"]
            )

        lever = config.lever
        if lever and lever.get("sites"):
            self._known_ats_tokens["lever"].update(
                t.lower() for t in lever["sites"]
            )

        ashby = config.ashby
        if ashby and ashby.get("companies"):
            self._known_ats_tokens["ashby"].update(
                t.lower() for t in ashby["companies"]
            )

        sr = config.smartrecruiters
        if sr and sr.get("companies"):
            self._known_ats_tokens["smartrecruiters"].update(
                t.lower() for t in sr["companies"]
            )

    def add_domain(self, url: str) -> None:
        """Add a domain to the known set."""
        self._known_domains.add(canonicalize_domain(url))

    def add_company(self, name: str) -> None:
        """Add a company name to the known set."""
        self._known_companies.add(canonicalize_company_name(name))

    def add_ats_token(self, ats: str, token: str) -> None:
        """Add an ATS token to the known set."""
        ats = ats.lower()
        if ats in self._known_ats_tokens:
            self._known_ats_tokens[ats].add(token.lower())

    def is_duplicate_domain(self, url: str) -> bool:
        """Check if a URL's domain is already known."""
        return canonicalize_domain(url) in self._known_domains

    def is_duplicate_company(self, name: str) -> bool:
        """Check if a company name is already known."""
        return canonicalize_company_name(name) in self._known_companies

    def is_duplicate_ats_token(self, ats: str, token: str) -> bool:
        """Check if an ATS token is already known."""
        ats = ats.lower()
        if ats not in self._known_ats_tokens:
            return False
        return token.lower() in self._known_ats_tokens[ats]

    def check_duplicate(
        self,
        company_name: Optional[str] = None,
        careers_url: Optional[str] = None,
        ats: Optional[str] = None,
        ats_token: Optional[str] = None,
    ) -> tuple[bool, str]:
        """Check if any of the provided identifiers indicate a duplicate.

        Args:
            company_name: Company name to check
            careers_url: Careers URL to check
            ats: ATS type (e.g., "greenhouse")
            ats_token: ATS-specific token (e.g., board ID)

        Returns:
            Tuple of (is_duplicate, reason)
        """
        if company_name and self.is_duplicate_company(company_name):
            return True, f"company '{company_name}' already in known_tokens"

        if careers_url and self.is_duplicate_domain(careers_url):
            return True, f"domain '{canonicalize_domain(careers_url)}' already known"

        if ats and ats_token and self.is_duplicate_ats_token(ats, ats_token):
            return True, f"{ats} token '{ats_token}' already in adapter config"

        return False, ""

    @property
    def known_domain_count(self) -> int:
        """Number of known domains."""
        return len(self._known_domains)

    @property
    def known_company_count(self) -> int:
        """Number of known companies."""
        return len(self._known_companies)
