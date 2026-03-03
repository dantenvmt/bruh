"""
ATS detection probe for career pages.

Probes career pages to detect which ATS platform they use.
Detection precedence (first match wins):
1. URL-based (high confidence)
2. DOM-based (lower confidence)
3. iframe/script detection (lowest confidence)
"""
import logging
import re
from typing import Optional, Tuple
from urllib.parse import urlparse

import httpx
from bs4 import BeautifulSoup

from .compliance import ComplianceGate, DISCOVERY_USER_AGENT
from .types import ATSType, ProbeResult


logger = logging.getLogger(__name__)


# ATS detection patterns with confidence scores
# Priority order matters - first match wins
ATS_URL_PATTERNS: list[tuple[ATSType, str, float]] = [
    (ATSType.GREENHOUSE, r"boards\.greenhouse\.io", 1.0),
    (ATSType.LEVER, r"jobs\.lever\.co", 1.0),
    (ATSType.ASHBY, r"jobs\.ashbyhq\.com", 1.0),
    (ATSType.SMARTRECRUITERS, r"smartrecruiters\.com", 1.0),
    (ATSType.WORKDAY, r"myworkdayjobs\.com", 1.0),
    (ATSType.ICIMS, r"icims\.com", 1.0),
    (ATSType.TALEO, r"taleo\.net", 1.0),
]

ATS_DOM_PATTERNS: list[tuple[ATSType, str, float]] = [
    (ATSType.LEVER, r'class=["\']lever-jobs', 0.7),
    (ATSType.GREENHOUSE, r'greenhouse\.io/embed', 0.7),
    (ATSType.WORKDAY, r'wd5\.myworkday', 0.5),
    (ATSType.ICIMS, r'<script[^>]+icims', 0.5),
]


def detect_ats_from_url(url: str) -> Optional[Tuple[ATSType, float, str]]:
    """Detect ATS type from URL.

    Args:
        url: URL to check

    Returns:
        Tuple of (ATS type, confidence, detection method) or None
    """
    url_lower = url.lower()

    for ats, pattern, confidence in ATS_URL_PATTERNS:
        if re.search(pattern, url_lower):
            return ats, confidence, "url"

    return None


def detect_ats_from_html(html: str) -> Optional[Tuple[ATSType, float, str]]:
    """Detect ATS type from HTML content.

    Args:
        html: HTML content to check

    Returns:
        Tuple of (ATS type, confidence, detection method) or None
    """
    html_lower = html.lower()

    for ats, pattern, confidence in ATS_DOM_PATTERNS:
        if re.search(pattern, html_lower, re.IGNORECASE):
            method = "iframe" if "iframe" in pattern or "src" in pattern else "dom"
            return ats, confidence, method

    return None


def extract_ats_token(url: str, ats: ATSType) -> Optional[str]:
    """Extract the ATS-specific token from a URL.

    Examples:
        boards.greenhouse.io/airbnb -> "airbnb"
        jobs.lever.co/netflix -> "netflix"

    Args:
        url: Careers URL
        ats: Detected ATS type

    Returns:
        Token string or None
    """
    parsed = urlparse(url)

    if ats == ATSType.GREENHOUSE:
        # boards.greenhouse.io/{company}
        path_parts = [p for p in parsed.path.split("/") if p]
        if path_parts:
            return path_parts[0]

    elif ats == ATSType.LEVER:
        # jobs.lever.co/{company}
        path_parts = [p for p in parsed.path.split("/") if p]
        if path_parts:
            return path_parts[0]

    elif ats == ATSType.ASHBY:
        # jobs.ashbyhq.com/{company}
        path_parts = [p for p in parsed.path.split("/") if p]
        if path_parts:
            return path_parts[0]

    elif ats == ATSType.SMARTRECRUITERS:
        # careers.smartrecruiters.com/{company}
        path_parts = [p for p in parsed.path.split("/") if p]
        if path_parts:
            return path_parts[0]

    return None


def detect_requires_js(html: str) -> bool:
    """Detect if a page likely requires JavaScript rendering.

    Heuristics:
    - Very little text content
    - React/Vue/Angular app markers
    - "JavaScript required" messages

    Args:
        html: HTML content

    Returns:
        True if page likely requires JS rendering
    """
    soup = BeautifulSoup(html, "html.parser")

    # Remove script and style tags
    for tag in soup(["script", "style"]):
        tag.decompose()

    text = soup.get_text(strip=True)

    # If very little text, probably needs JS
    if len(text) < 200:
        return True

    # Check for common SPA markers
    html_lower = html.lower()
    spa_markers = [
        "id=\"root\"",
        "id=\"app\"",
        "ng-app=",
        "data-reactroot",
        "__next",
        "please enable javascript",
        "javascript is required",
    ]

    for marker in spa_markers:
        if marker in html_lower:
            return True

    return False


class ATSProbe:
    """Probes career pages to detect ATS platforms."""

    def __init__(
        self,
        compliance: Optional[ComplianceGate] = None,
        timeout: float = 15.0,
        try_api_spy: bool = False,
        api_spy_min_confidence: float = 0.5,
    ):
        """Initialize ATS probe.

        Args:
            compliance: ComplianceGate instance, or None to create new
            timeout: HTTP timeout in seconds
            try_api_spy: If True, run NetworkSpy on CUSTOM+JS sites to discover
                         hidden JSON APIs. Sets fetch_mode='api_spy' when a
                         high-confidence endpoint is found.
            api_spy_min_confidence: Minimum NetworkSpy confidence to accept an
                                    endpoint as the scrape target (default 0.5).
        """
        self.compliance = compliance or ComplianceGate()
        self.timeout = timeout
        self.try_api_spy = try_api_spy
        self.api_spy_min_confidence = api_spy_min_confidence
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

    async def probe(self, careers_url: str) -> ProbeResult:
        """Probe a careers URL for ATS detection.

        Args:
            careers_url: URL to probe

        Returns:
            ProbeResult with detection results
        """
        # Check robots.txt compliance
        robots_allowed, reason = await self.compliance.check_and_wait(careers_url)
        if not robots_allowed:
            return ProbeResult(
                careers_url=careers_url,
                final_url=careers_url,
                detected_ats=ATSType.UNKNOWN,
                confidence=0.0,
                fetch_mode="static",
                robots_allowed=False,
                error=reason,
            )

        client = await self._get_client()

        try:
            response = await client.get(careers_url)
            final_url = str(response.url)

            if response.status_code >= 400:
                return ProbeResult(
                    careers_url=careers_url,
                    final_url=final_url,
                    detected_ats=ATSType.UNKNOWN,
                    confidence=0.0,
                    fetch_mode="static",
                    robots_allowed=True,
                    error=f"HTTP {response.status_code}",
                )

            html = response.text

            # Try URL-based detection first (highest confidence)
            result = detect_ats_from_url(final_url)
            if result:
                ats, confidence, method = result
                token = extract_ats_token(final_url, ats)
                return ProbeResult(
                    careers_url=careers_url,
                    final_url=final_url,
                    detected_ats=ats,
                    confidence=confidence,
                    fetch_mode="static",
                    robots_allowed=True,
                    ats_token=token,
                    detection_method=method,
                )

            # Try DOM-based detection
            result = detect_ats_from_html(html)
            if result:
                ats, confidence, method = result
                return ProbeResult(
                    careers_url=careers_url,
                    final_url=final_url,
                    detected_ats=ats,
                    confidence=confidence,
                    fetch_mode="browser" if detect_requires_js(html) else "static",
                    robots_allowed=True,
                    detection_method=method,
                )

            # No ATS detected - mark as custom
            requires_js = detect_requires_js(html)

            # For JS-heavy custom sites, optionally try NetworkSpy to find a
            # hidden JSON API instead of relying on fragile CSS selectors.
            if requires_js and self.try_api_spy:
                api_endpoint = await self._try_network_spy(final_url)
                if api_endpoint is not None:
                    return ProbeResult(
                        careers_url=careers_url,
                        final_url=final_url,
                        detected_ats=ATSType.CUSTOM,
                        confidence=1.0,
                        fetch_mode="api_spy",
                        robots_allowed=True,
                        detection_method="network_spy",
                        api_endpoint=api_endpoint,
                    )

            return ProbeResult(
                careers_url=careers_url,
                final_url=final_url,
                detected_ats=ATSType.CUSTOM,
                confidence=1.0,  # High confidence it's custom if no ATS found
                fetch_mode="browser" if requires_js else "static",
                robots_allowed=True,
                detection_method="fallback",
            )

        except Exception as e:
            logger.error(f"Error probing {careers_url}: {e}")
            return ProbeResult(
                careers_url=careers_url,
                final_url=careers_url,
                detected_ats=ATSType.UNKNOWN,
                confidence=0.0,
                fetch_mode="static",
                robots_allowed=True,
                error=str(e),
            )

    async def _try_network_spy(self, url: str) -> Optional[dict]:
        """Run NetworkSpy on *url* and return the best endpoint as a plain dict.

        Returns None if no endpoint meets self.api_spy_min_confidence.
        """
        try:
            from ..scraping.fetchers.network_spy import NetworkSpy
        except ImportError:
            logger.debug("NetworkSpy not available (playwright not installed)")
            return None

        try:
            spy = NetworkSpy(
                headless=True,
                scroll=True,
                click_load_more=True,
                min_confidence=self.api_spy_min_confidence,
                timeout=30_000,
            )
            endpoints = await spy.spy(url)
        except Exception as exc:
            logger.warning("NetworkSpy failed for %s: %s", url, exc)
            return None

        if not endpoints:
            logger.debug("NetworkSpy found no qualifying endpoints on %s", url)
            return None

        best = endpoints[0]
        logger.info(
            "NetworkSpy discovered API endpoint for %s: %s (confidence=%.2f, jobs~%d)",
            url, best.url, best.confidence, best.job_count_estimate,
        )

        endpoint_dict: dict = {
            "url": best.url,
            "method": best.method,
            "replay_headers": best.replay_headers,
            "request_post_data": best.request_post_data,
            "confidence": best.confidence,
            "job_count_estimate": best.job_count_estimate,
        }
        if best.pagination:
            endpoint_dict["pagination"] = {
                "style": best.pagination.style,
                "param_name": best.pagination.param_name,
                "current_value": best.pagination.current_value,
                "in_body": best.pagination.in_body,
            }
        return endpoint_dict

    async def __aenter__(self) -> "ATSProbe":
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close()
