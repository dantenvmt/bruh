"""
Compliance utilities for discovery crawling.

Implements robots.txt checking and rate limiting per the plan's hard gates:
1. Check robots.txt before crawling - skip disallowed paths
2. Rate limit: max 1 request/second per domain
3. User-Agent: JobScraperDiscovery/1.0
"""
import asyncio
import logging
import time
from collections import defaultdict
from typing import Optional
from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser

import httpx

from .dedup import canonicalize_domain


logger = logging.getLogger(__name__)

# Discovery user agent per plan specification
DISCOVERY_USER_AGENT = "JobScraperDiscovery/1.0 (+https://github.com/job-scraper)"


class RobotsChecker:
    """Check robots.txt compliance for URLs.

    Caches robots.txt files per domain to avoid repeated fetches.
    """

    def __init__(self, timeout: float = 10.0):
        """Initialize robots checker.

        Args:
            timeout: Timeout in seconds for fetching robots.txt
        """
        self._cache: dict[str, RobotFileParser] = {}
        self._timeout = timeout
        self._client: Optional[httpx.AsyncClient] = None

    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create HTTP client."""
        if self._client is None:
            self._client = httpx.AsyncClient(
                timeout=self._timeout,
                follow_redirects=True,
                headers={"User-Agent": DISCOVERY_USER_AGENT},
            )
        return self._client

    async def close(self) -> None:
        """Close HTTP client."""
        if self._client:
            await self._client.aclose()
            self._client = None

    async def _fetch_robots(self, domain: str, scheme: str = "https") -> RobotFileParser:
        """Fetch and parse robots.txt for a domain.

        Args:
            domain: Domain to fetch robots.txt for
            scheme: URL scheme (http or https)

        Returns:
            Parsed RobotFileParser (may be empty if fetch failed)
        """
        robots_url = f"{scheme}://{domain}/robots.txt"
        parser = RobotFileParser()

        try:
            client = await self._get_client()
            response = await client.get(robots_url)

            if response.status_code == 200:
                # Parse the robots.txt content
                parser.parse(response.text.splitlines())
                logger.debug(f"Loaded robots.txt from {robots_url}")
            else:
                # No robots.txt or error - allow all
                logger.debug(f"No robots.txt at {robots_url} (status {response.status_code})")

        except Exception as e:
            # Network error - allow all (fail open)
            logger.warning(f"Failed to fetch {robots_url}: {e}")

        return parser

    async def is_allowed(self, url: str) -> bool:
        """Check if URL is allowed by robots.txt.

        Args:
            url: Full URL to check

        Returns:
            True if crawling is allowed, False if disallowed
        """
        parsed = urlparse(url)
        domain = parsed.netloc.lower()
        if domain.startswith("www."):
            domain = domain[4:]

        scheme = parsed.scheme or "https"
        path = parsed.path or "/"

        # Check cache first
        if domain not in self._cache:
            self._cache[domain] = await self._fetch_robots(domain, scheme)

        parser = self._cache[domain]

        # Check if our user agent is allowed
        # RobotFileParser.can_fetch expects a user agent string and URL
        try:
            return parser.can_fetch(DISCOVERY_USER_AGENT, url)
        except Exception:
            # If parsing fails, allow (fail open)
            return True

    def clear_cache(self) -> None:
        """Clear the robots.txt cache."""
        self._cache.clear()


class RateLimiter:
    """Rate limiter for per-domain request throttling.

    Enforces max 1 request/second per domain as per plan specification.
    """

    def __init__(self, requests_per_second: float = 1.0):
        """Initialize rate limiter.

        Args:
            requests_per_second: Maximum requests per second per domain
        """
        self._min_interval = 1.0 / requests_per_second
        self._last_request: dict[str, float] = defaultdict(float)
        self._locks: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)

    async def acquire(self, url: str) -> None:
        """Wait until a request to this domain is allowed.

        Args:
            url: URL being requested (domain is extracted)
        """
        domain = canonicalize_domain(url)

        async with self._locks[domain]:
            now = time.monotonic()
            elapsed = now - self._last_request[domain]

            if elapsed < self._min_interval:
                wait_time = self._min_interval - elapsed
                logger.debug(f"Rate limiting {domain}: waiting {wait_time:.2f}s")
                await asyncio.sleep(wait_time)

            self._last_request[domain] = time.monotonic()

    def reset(self, domain: Optional[str] = None) -> None:
        """Reset rate limit tracking.

        Args:
            domain: Specific domain to reset, or None for all
        """
        if domain:
            self._last_request.pop(domain, None)
        else:
            self._last_request.clear()


class ComplianceGate:
    """Combined compliance checking for discovery crawls.

    Coordinates robots.txt checking and rate limiting. Use this as the
    single entry point for compliance in discovery operations.
    """

    def __init__(
        self,
        robots_checker: Optional[RobotsChecker] = None,
        rate_limiter: Optional[RateLimiter] = None,
    ):
        """Initialize compliance gate.

        Args:
            robots_checker: RobotsChecker instance, or None to create new
            rate_limiter: RateLimiter instance, or None to create new
        """
        self.robots = robots_checker or RobotsChecker()
        self.rate_limiter = rate_limiter or RateLimiter()
        self._blocked_count = 0

    async def check_and_wait(self, url: str) -> tuple[bool, str]:
        """Check compliance and wait for rate limit.

        This is the main entry point for compliance. It:
        1. Checks robots.txt - returns (False, reason) if disallowed
        2. Waits for rate limit if allowed
        3. Returns (True, "") when ready to proceed

        Args:
            url: URL to check and prepare for request

        Returns:
            Tuple of (allowed, reason). If allowed is False, do not proceed.
        """
        # Check robots.txt first
        if not await self.robots.is_allowed(url):
            self._blocked_count += 1
            reason = f"robots.txt disallows crawling {url}"
            logger.info(f"Blocked by robots.txt: {url}")
            return False, reason

        # Wait for rate limit
        await self.rate_limiter.acquire(url)

        return True, ""

    async def close(self) -> None:
        """Clean up resources."""
        await self.robots.close()

    @property
    def blocked_count(self) -> int:
        """Number of URLs blocked by robots.txt."""
        return self._blocked_count

    async def __aenter__(self) -> "ComplianceGate":
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close()
