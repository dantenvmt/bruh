"""
URL resolution for career pages.

Resolves company names to their careers page URLs using:
1. Pattern guessing (company.com/careers, careers.company.com, etc.)
2. Homepage crawling to find careers links
"""
import asyncio
import logging
import re
from typing import Optional, List
from urllib.parse import urljoin, urlparse

import httpx
from bs4 import BeautifulSoup

from .compliance import ComplianceGate, DISCOVERY_USER_AGENT
from .dedup import canonicalize_company_name


logger = logging.getLogger(__name__)


# Common careers page URL patterns
CAREERS_PATTERNS = [
    "{domain}/careers",
    "{domain}/jobs",
    "{domain}/careers/",
    "{domain}/jobs/",
    "careers.{domain}",
    "jobs.{domain}",
    "{domain}/en/careers",
    "{domain}/company/careers",
    "{domain}/about/careers",
]

# Link text patterns that indicate a careers page
CAREERS_LINK_PATTERNS = [
    r"careers?",
    r"jobs?",
    r"work\s*(with|at|for)?\s*us",
    r"join\s*(us|our\s*team)",
    r"hiring",
    r"openings?",
    r"opportunities",
]


def normalize_company_to_domain(company_name: str) -> str:
    """Convert a company name to a likely domain.

    Examples:
        "Google" -> "google.com"
        "JPMorgan Chase" -> "jpmorganchase.com"
        "Tata Consultancy Services" -> "tcs.com" (special case)

    Args:
        company_name: Company name to convert

    Returns:
        Likely domain name
    """
    # Special cases for known companies
    special_cases = {
        "tata consultancy services": "tcs.com",
        "hcltech": "hcltech.com",
        "tech mahindra": "techmahindra.com",
        "jpmorgan chase": "jpmorgan.com",
        "bytedance": "bytedance.com",
    }

    canonical = canonicalize_company_name(company_name)
    if canonical in special_cases:
        return special_cases[canonical]

    # Remove spaces and special characters
    domain = re.sub(r"[^a-z0-9]", "", canonical)
    return f"{domain}.com"


async def try_url(
    client: httpx.AsyncClient,
    url: str,
    compliance: ComplianceGate,
) -> Optional[str]:
    """Try to access a URL and return final URL if successful.

    Args:
        client: HTTP client
        url: URL to try
        compliance: Compliance gate for robots.txt and rate limiting

    Returns:
        Final URL after redirects if successful, None otherwise
    """
    # Check compliance first
    allowed, reason = await compliance.check_and_wait(url)
    if not allowed:
        logger.debug(f"Skipped {url}: {reason}")
        return None

    try:
        response = await client.head(url, follow_redirects=True)
        if response.status_code < 400:
            return str(response.url)
    except Exception as e:
        logger.debug(f"Failed to access {url}: {e}")

    return None


async def resolve_by_patterns(
    company_name: str,
    client: httpx.AsyncClient,
    compliance: ComplianceGate,
) -> Optional[str]:
    """Try to resolve careers URL using common patterns.

    Args:
        company_name: Company name
        client: HTTP client
        compliance: Compliance gate

    Returns:
        Resolved careers URL or None
    """
    domain = normalize_company_to_domain(company_name)

    for pattern in CAREERS_PATTERNS:
        # Build URL from pattern
        if pattern.startswith("{domain}"):
            url = f"https://{pattern.format(domain=domain)}"
        else:
            url = f"https://{pattern.format(domain=domain)}"

        result = await try_url(client, url, compliance)
        if result:
            logger.info(f"Resolved {company_name} via pattern: {result}")
            return result

    return None


def find_careers_links(html: str, base_url: str) -> List[str]:
    """Find careers-related links in HTML.

    Args:
        html: HTML content
        base_url: Base URL for resolving relative links

    Returns:
        List of potential careers page URLs
    """
    soup = BeautifulSoup(html, "html.parser")
    links = []

    for anchor in soup.find_all("a", href=True):
        href = anchor.get("href", "")
        text = anchor.get_text(strip=True).lower()

        # Check link text
        for pattern in CAREERS_LINK_PATTERNS:
            if re.search(pattern, text, re.IGNORECASE):
                full_url = urljoin(base_url, href)
                if full_url not in links:
                    links.append(full_url)
                break

        # Also check href itself
        href_lower = href.lower()
        if any(kw in href_lower for kw in ["career", "job", "hiring"]):
            full_url = urljoin(base_url, href)
            if full_url not in links:
                links.append(full_url)

    return links


async def resolve_by_homepage(
    company_name: str,
    client: httpx.AsyncClient,
    compliance: ComplianceGate,
) -> Optional[str]:
    """Try to resolve careers URL by crawling homepage.

    Args:
        company_name: Company name
        client: HTTP client
        compliance: Compliance gate

    Returns:
        Resolved careers URL or None
    """
    domain = normalize_company_to_domain(company_name)
    homepage = f"https://{domain}"

    # Check compliance
    allowed, reason = await compliance.check_and_wait(homepage)
    if not allowed:
        logger.debug(f"Skipped homepage {homepage}: {reason}")
        return None

    try:
        response = await client.get(homepage, follow_redirects=True)
        if response.status_code >= 400:
            return None

        # Find careers links
        links = find_careers_links(response.text, str(response.url))
        if not links:
            return None

        # Try the first few links
        for link in links[:3]:
            result = await try_url(client, link, compliance)
            if result:
                logger.info(f"Resolved {company_name} via homepage crawl: {result}")
                return result

    except Exception as e:
        logger.debug(f"Failed to crawl homepage for {company_name}: {e}")

    return None


class URLResolver:
    """Resolves company names to careers page URLs."""

    def __init__(
        self,
        compliance: Optional[ComplianceGate] = None,
        timeout: float = 15.0,
    ):
        """Initialize URL resolver.

        Args:
            compliance: ComplianceGate instance, or None to create new
            timeout: HTTP timeout in seconds
        """
        self.compliance = compliance or ComplianceGate()
        self.timeout = timeout
        self._client: Optional[httpx.AsyncClient] = None

    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create HTTP client."""
        if self._client is None:
            self._client = httpx.AsyncClient(
                timeout=self.timeout,
                follow_redirects=True,
                headers={"User-Agent": DISCOVERY_USER_AGENT},
            )
        return self._client

    async def close(self) -> None:
        """Close resources."""
        if self._client:
            await self._client.aclose()
            self._client = None
        await self.compliance.close()

    async def resolve(
        self,
        company_name: str,
        known_url: Optional[str] = None,
    ) -> Optional[str]:
        """Resolve a company's careers page URL.

        Resolution order:
        1. Return known_url if provided and valid
        2. Try common URL patterns
        3. Crawl homepage for careers links

        Args:
            company_name: Company name to resolve
            known_url: Pre-known careers URL to validate

        Returns:
            Resolved careers URL or None
        """
        client = await self._get_client()

        # If we have a known URL, just validate it
        if known_url:
            result = await try_url(client, known_url, self.compliance)
            if result:
                return result
            logger.debug(f"Known URL invalid for {company_name}: {known_url}")

        # Try pattern-based resolution
        result = await resolve_by_patterns(company_name, client, self.compliance)
        if result:
            return result

        # Try homepage crawling
        result = await resolve_by_homepage(company_name, client, self.compliance)
        if result:
            return result

        logger.warning(f"Could not resolve careers URL for: {company_name}")
        return None

    async def __aenter__(self) -> "URLResolver":
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close()
