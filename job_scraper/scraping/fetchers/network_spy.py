"""
Playwright-based network spy for API discovery.

Navigates to a page in a headless browser and captures every XHR / Fetch
request+response pair — exactly like opening F12 > Network > XHR/Fetch in
Chrome DevTools.  Each captured JSON response is scored for job-listing
relevance so the most useful endpoints bubble to the top.

Typical workflow
----------------
    spy = NetworkSpy()
    endpoints = await spy.spy("https://example.com/jobs")
    for ep in endpoints:
        if ep.looks_like_jobs:
            print(ep.url, ep.confidence, ep.job_count_estimate)

Then replay the best endpoint directly (no browser) via ReplayClient in
replay.py, or iterate pages by tweaking query-string/body params.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
from dataclasses import dataclass, field
from typing import Any, Optional
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass
class CapturedCall:
    """A single XHR/Fetch request+response pair captured from the browser."""

    method: str
    url: str
    resource_type: str  # "xhr" | "fetch" | "document" | etc.
    request_headers: dict[str, str] = field(default_factory=dict)
    request_post_data: Optional[str] = None
    response_status: int = 0
    response_headers: dict[str, str] = field(default_factory=dict)
    response_body: Optional[bytes] = None
    response_text: Optional[str] = None
    response_json: Any = None
    error: Optional[str] = None

    @property
    def is_json(self) -> bool:
        ct = self.response_headers.get("content-type", "")
        if "json" in ct:
            return True
        # Fallback: peek at the body
        text = self.response_text or ""
        stripped = text.lstrip()
        return stripped.startswith("{") or stripped.startswith("[")


@dataclass
class PaginationHint:
    """Detected pagination parameters for a discovered endpoint."""

    style: str  # "page", "offset", "cursor", "graphql_offset", "unknown"
    param_name: str  # query-string key or JSON body key
    current_value: Any
    in_body: bool = False  # True → POST body param; False → query-string param

    def next_url(self, url: str, step: int = 1) -> str:
        """Return a URL with the page/offset incremented by *step*."""
        if self.in_body or self.style == "cursor":
            return url  # caller must handle body / cursor pagination
        parsed = urlparse(url)
        qs = parse_qs(parsed.query, keep_blank_values=True)
        try:
            new_val = int(self.current_value) + step
        except (TypeError, ValueError):
            return url
        qs[self.param_name] = [str(new_val)]
        new_query = urlencode(qs, doseq=True)
        return urlunparse(parsed._replace(query=new_query))


@dataclass
class DiscoveredEndpoint:
    """
    An API endpoint discovered during network spying, scored for job relevance.
    """

    url: str
    method: str
    request_headers: dict[str, str]
    request_post_data: Optional[str]
    response_status: int
    response_headers: dict[str, str]
    response_json: Any
    response_text: Optional[str]

    # Scoring
    looks_like_jobs: bool = False
    job_count_estimate: int = 0
    confidence: float = 0.0
    score_notes: list[str] = field(default_factory=list)

    # Pagination
    pagination: Optional[PaginationHint] = None

    # Headers safe to use when replaying without a browser
    replay_headers: dict[str, str] = field(default_factory=dict)

    def next_page_url(self, step: int = 1) -> Optional[str]:
        """Return URL for the next page if simple query-string pagination was detected."""
        if self.pagination and not self.pagination.in_body:
            return self.pagination.next_url(self.url, step)
        return None


# ---------------------------------------------------------------------------
# Scoring helpers
# ---------------------------------------------------------------------------

# URL fragment patterns that suggest a job-related endpoint
_JOB_URL_RE = re.compile(
    r"(jobs?|careers?|listings?|positions?|openings?|vacancies|"
    r"search|requisitions?|reqs?|postings?|opportunities)",
    re.IGNORECASE,
)

# JSON field names that suggest job data
_JOB_FIELDS = frozenset(
    {
        "title",
        "job_title",
        "jobTitle",
        "position",
        "role",
        "company",
        "employer",
        "organization",
        "companyName",
        "location",
        "city",
        "country",
        "description",
        "summary",
        "overview",
        "body",
        "salary",
        "compensation",
        "pay",
        "url",
        "apply_url",
        "applyUrl",
        "link",
        "href",
        "applicationUrl",
        "posted_at",
        "postedAt",
        "created_at",
        "createdAt",
        "date",
        "publishedAt",
        "id",
        "job_id",
        "jobId",
        "req_id",
        "reqId",
        "requisitionId",
        "department",
        "team",
        "category",
        "employment_type",
        "employmentType",
        "type",
        "remote",
        "workplaceType",
    }
)

# Common JSON envelope keys that wrap job arrays
_ENVELOPE_KEYS = (
    "jobs",
    "results",
    "data",
    "listings",
    "items",
    "positions",
    "openings",
    "postings",
    "hits",
    "records",
    "vacancies",
    "opportunities",
    "edges",       # GraphQL
    "nodes",       # GraphQL
)

# Query-string / body keys used for pagination
_PAGE_PARAMS = {"page", "p", "pg"}
_OFFSET_PARAMS = {"offset", "from", "start", "skip"}
_CURSOR_PARAMS = {"cursor", "after", "next_token", "nextCursor"}

# Headers safe to include when replaying (no session cookies or browser internals)
_SAFE_REPLAY_HEADERS = frozenset(
    {
        "accept",
        "accept-language",
        "accept-encoding",
        "content-type",
        "x-requested-with",
        "authorization",
        "x-api-key",
        "x-auth-token",
        "x-client-id",
        "origin",
        "referer",
    }
)


def _score_json_for_jobs(data: Any) -> tuple[float, int, list[str]]:
    """
    Score a parsed JSON value for likelihood of being a job-listing payload.
    Returns (confidence 0..1, estimated job count, explanatory notes).
    """
    notes: list[str] = []
    score = 0.0
    count = 0

    # 1. Unwrap common envelope patterns: {jobs:[…]} {data:[…]} {results:[…]}
    candidates: list[Any] = [data]
    if isinstance(data, dict):
        for key in _ENVELOPE_KEYS:
            val = data.get(key)
            if isinstance(val, list):
                candidates.append(val)
                notes.append(f"Found envelope key '{key}'")
                break
            # GraphQL: {data: {jobs: {edges: […]}}}
            if isinstance(val, dict):
                for inner_key in _ENVELOPE_KEYS:
                    inner_val = val.get(inner_key)
                    if isinstance(inner_val, list):
                        candidates.append(inner_val)
                        notes.append(f"Found nested envelope '{key}.{inner_key}'")
                        break

    # 2. Pick the longest list candidate
    best_list: Optional[list] = None
    for c in candidates:
        if isinstance(c, list) and (best_list is None or len(c) > len(best_list)):
            best_list = c

    if best_list is None:
        return 0.0, 0, ["No list found in response"]

    count = len(best_list)
    notes.append(f"List has {count} item(s)")

    if count == 0:
        return 0.05, 0, notes

    # 3. Check field overlap against job schema
    samples = best_list[:5]
    all_keys: set[str] = set()
    for item in samples:
        if isinstance(item, dict):
            all_keys.update(item.keys())
        # GraphQL edge nodes: {node: {…}}
        if isinstance(item, dict) and "node" in item and isinstance(item["node"], dict):
            all_keys.update(item["node"].keys())

    if not all_keys:
        return 0.1, count, notes + ["Items are not dicts"]

    matched = _JOB_FIELDS & all_keys
    match_ratio = len(matched) / max(len(all_keys), 1)
    notes.append(f"Matched job fields: {sorted(matched)}")

    # Field-name overlap (up to 0.55)
    score += min(0.55, match_ratio * 1.4)
    # Volume bonus (up to 0.25)
    score += min(0.25, count / 40 * 0.25)
    # At least 2 items (avoids single-item false positives)
    if count >= 2:
        score += 0.1
    # Minimum required fields (title + at least one of url/company/location)
    has_title = bool({"title", "job_title", "jobTitle", "position"} & all_keys)
    has_anchor = bool({"url", "apply_url", "applyUrl", "link", "company", "location"} & all_keys)
    if has_title and has_anchor:
        score += 0.1
        notes.append("Has title + anchor fields")

    return min(score, 1.0), count, notes


def _detect_pagination(url: str, post_data: Optional[str]) -> Optional[PaginationHint]:
    """
    Detect pagination style and current value from a URL or POST body.
    """
    parsed = urlparse(url)
    qs = parse_qs(parsed.query, keep_blank_values=True)

    for key in _PAGE_PARAMS:
        if key in qs:
            try:
                val = int(qs[key][0])
            except (ValueError, IndexError):
                val = 0
            return PaginationHint("page", key, val, in_body=False)

    for key in _OFFSET_PARAMS:
        if key in qs:
            try:
                val = int(qs[key][0])
            except (ValueError, IndexError):
                val = 0
            return PaginationHint("offset", key, val, in_body=False)

    for key in _CURSOR_PARAMS:
        if key in qs:
            return PaginationHint("cursor", key, qs[key][0], in_body=False)

    # POST body pagination
    if post_data:
        try:
            body = json.loads(post_data)
            if isinstance(body, dict):
                for key in _PAGE_PARAMS:
                    if key in body:
                        return PaginationHint("page", key, body[key], in_body=True)
                for key in _OFFSET_PARAMS:
                    if key in body:
                        return PaginationHint("offset", key, body[key], in_body=True)
                # GraphQL: variables.offset / variables.page
                variables = body.get("variables", {})
                if isinstance(variables, dict):
                    for key in list(_PAGE_PARAMS) + list(_OFFSET_PARAMS):
                        if key in variables:
                            return PaginationHint(
                                "graphql_offset", key, variables[key], in_body=True
                            )
        except (json.JSONDecodeError, TypeError):
            pass

    return None


# ---------------------------------------------------------------------------
# NetworkSpy
# ---------------------------------------------------------------------------


class NetworkSpy:
    """
    Playwright-based network traffic recorder.

    Opens a headless Chromium browser, navigates to a URL, and captures every
    XHR / Fetch request and response – the same calls you'd see in
    F12 > Network > XHR/Fetch.  Optionally scrolls the page and clicks
    "Load More" buttons to trigger lazy-loaded API calls.

    Parameters
    ----------
    headless:
        Run browser in headless mode (default True).  Set False for debugging.
    timeout:
        Navigation timeout in milliseconds (default 30 000).
    scroll:
        Scroll the page after load to trigger lazy-loaded content.
    click_load_more:
        Attempt to click common "Load more" / "Next" buttons once.
    capture_resource_types:
        Which Playwright resource types to capture. Default ``("xhr", "fetch")``.
    min_confidence:
        Filter returned endpoints below this threshold (0.0 = return all).
    extra_wait_ms:
        Extra milliseconds to wait after networkidle, for slow SPAs.
    """

    def __init__(
        self,
        *,
        headless: bool = True,
        timeout: int = 30_000,
        scroll: bool = True,
        click_load_more: bool = True,
        capture_resource_types: tuple[str, ...] = ("xhr", "fetch"),
        min_confidence: float = 0.0,
        extra_wait_ms: int = 1_500,
    ) -> None:
        self.headless = headless
        self.timeout = timeout
        self.scroll = scroll
        self.click_load_more = click_load_more
        self.capture_resource_types = capture_resource_types
        self.min_confidence = min_confidence
        self.extra_wait_ms = extra_wait_ms

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def spy(self, url: str) -> list[DiscoveredEndpoint]:
        """
        Navigate to *url*, intercept all XHR/Fetch calls, and return
        a list of :class:`DiscoveredEndpoint` ranked by confidence
        (highest first).
        """
        calls = await self._capture(url)
        endpoints: list[DiscoveredEndpoint] = []
        for call in calls:
            if not call.is_json:
                continue
            ep = self._score(call)
            if ep.confidence >= self.min_confidence:
                endpoints.append(ep)

        endpoints.sort(key=lambda e: e.confidence, reverse=True)
        logger.info(
            "NetworkSpy captured %d XHR/Fetch calls from %s; "
            "%d are JSON, %d pass min_confidence=%.2f",
            len(calls),
            url,
            sum(1 for c in calls if c.is_json),
            len(endpoints),
            self.min_confidence,
        )
        return endpoints

    def score_captured(self, calls: list[CapturedCall]) -> list[DiscoveredEndpoint]:
        """Score pre-captured network calls without launching a browser.

        Accepts :class:`CapturedCall` objects already collected (e.g. by the
        browser fetcher with ``capture_network=True``) and runs only the
        scoring / filtering logic — no Playwright, no navigation.
        """
        endpoints: list[DiscoveredEndpoint] = []
        for call in calls:
            if not call.is_json:
                continue
            ep = self._score(call)
            if ep.confidence >= self.min_confidence:
                endpoints.append(ep)
        endpoints.sort(key=lambda e: e.confidence, reverse=True)
        logger.info(
            "NetworkSpy scored %d pre-captured calls; "
            "%d are JSON, %d pass min_confidence=%.2f",
            len(calls),
            sum(1 for c in calls if c.is_json),
            len(endpoints),
            self.min_confidence,
        )
        return endpoints

    async def record_raw(self, url: str) -> list[CapturedCall]:
        """Low-level: return every :class:`CapturedCall` without scoring."""
        return await self._capture(url)

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    async def _capture(self, url: str) -> list[CapturedCall]:
        try:
            from playwright.async_api import async_playwright, Request, Response
        except ImportError as exc:
            raise ImportError(
                "Playwright is required for network spying. "
                "Install with: pip install playwright && playwright install chromium"
            ) from exc

        calls: list[CapturedCall] = []
        # url → in-flight call (request fired, response not yet received)
        pending: dict[str, CapturedCall] = {}

        async with async_playwright() as pw:
            browser = await pw.chromium.launch(
                headless=self.headless,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--disable-dev-shm-usage",
                    "--no-sandbox",
                    "--disable-setuid-sandbox",
                    "--disable-gpu",
                ],
            )
            context = await browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                ),
                viewport={"width": 1280, "height": 900},
                java_script_enabled=True,
                locale="en-US",
                timezone_id="America/New_York",
                ignore_https_errors=True,
            )
            page = await context.new_page()

            # ---- request listener ----
            async def on_request(req: Request) -> None:  # type: ignore[type-arg]
                if req.resource_type not in self.capture_resource_types:
                    return
                call = CapturedCall(
                    method=req.method,
                    url=req.url,
                    resource_type=req.resource_type,
                    request_headers=dict(req.headers),
                    request_post_data=req.post_data,
                )
                # Multiple requests to the same URL can occur (rare); last wins
                pending[req.url] = call

            # ---- response listener ----
            async def on_response(resp: Response) -> None:  # type: ignore[type-arg]
                call = pending.pop(resp.url, None)
                if call is None:
                    return
                call.response_status = resp.status
                call.response_headers = dict(resp.headers)
                try:
                    body = await resp.body()
                    call.response_body = body
                    call.response_text = body.decode("utf-8", errors="replace")
                    if call.is_json:
                        call.response_json = json.loads(call.response_text)
                except Exception as exc:
                    call.error = str(exc)
                    logger.debug("Could not read response body for %s: %s", resp.url, exc)
                calls.append(call)

            page.on("request", on_request)
            page.on("response", on_response)

            # ---- navigate ----
            try:
                await page.goto(url, timeout=self.timeout, wait_until="domcontentloaded")
                await page.wait_for_load_state("networkidle", timeout=10_000)
            except Exception as exc:
                logger.debug("Navigation/idle wait raised (non-fatal): %s", exc)

            if self.scroll:
                await self._scroll_page(page)

            if self.click_load_more:
                await self._click_load_more(page)

            # Final settle after interactions
            if self.extra_wait_ms > 0:
                await asyncio.sleep(self.extra_wait_ms / 1000)
            try:
                await page.wait_for_load_state("networkidle", timeout=5_000)
            except Exception:
                pass

            await browser.close()

        return calls

    async def _scroll_page(self, page: Any) -> None:
        from ._interactions import scroll_page
        await scroll_page(page)

    async def _click_load_more(self, page: Any) -> None:
        from ._interactions import click_load_more
        await click_load_more(page)

    def _score(self, call: CapturedCall) -> DiscoveredEndpoint:
        """Convert a :class:`CapturedCall` into a scored :class:`DiscoveredEndpoint`."""
        confidence = 0.0
        notes: list[str] = []

        # URL-pattern bonus (up to 0.30)
        parsed = urlparse(call.url)
        path_and_query = parsed.path
        if _JOB_URL_RE.search(path_and_query) or _JOB_URL_RE.search(parsed.netloc):
            confidence += 0.30
            notes.append("URL path matches job pattern")

        # JSON content scoring (weighted 70 %)
        if call.response_json is not None:
            json_score, count, json_notes = _score_json_for_jobs(call.response_json)
            confidence += json_score * 0.70
            notes.extend(json_notes)
        else:
            count = 0

        confidence = round(min(confidence, 1.0), 3)

        # Pagination detection
        pagination = _detect_pagination(call.url, call.request_post_data)

        # Replay-safe headers (drop cookies and browser internals)
        replay_headers = {
            k: v
            for k, v in call.request_headers.items()
            if k.lower() in _SAFE_REPLAY_HEADERS
        }

        return DiscoveredEndpoint(
            url=call.url,
            method=call.method,
            request_headers=call.request_headers,
            request_post_data=call.request_post_data,
            response_status=call.response_status,
            response_headers=call.response_headers,
            response_json=call.response_json,
            response_text=call.response_text,
            looks_like_jobs=confidence >= 0.4,
            job_count_estimate=count,
            confidence=confidence,
            score_notes=notes,
            pagination=pagination,
            replay_headers=replay_headers,
        )
