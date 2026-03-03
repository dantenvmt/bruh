"""
Direct HTTP replay of endpoints discovered by NetworkSpy.

Once NetworkSpy has identified a job API endpoint, ReplayClient lets you
call it directly with httpx — no browser required — and optionally page
through all results automatically.

Usage
-----
    from job_scraper.scraping.fetchers.network_spy import NetworkSpy
    from job_scraper.scraping.fetchers.replay import ReplayClient

    # 1. Discover the API
    spy = NetworkSpy(min_confidence=0.4)
    endpoints = await spy.spy("https://example.com/jobs")
    best = endpoints[0]

    # 2. Replay it directly
    client = ReplayClient()
    response = await client.fetch(best)
    print(response.json_body)

    # 3. Paginate (if simple query-string pagination was detected)
    all_pages = await client.paginate(best, max_pages=10)
    for page in all_pages:
        jobs = page.json_body  # list or dict with jobs list
"""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass, field
from typing import Any, Optional
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

logger = logging.getLogger(__name__)

# Default browser-like headers added to every replay request
_DEFAULT_HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}


@dataclass
class ReplayResponse:
    """Result of a single replayed request."""

    url: str
    method: str
    status: int
    headers: dict[str, str]
    body: Optional[bytes]
    text: Optional[str]
    json_body: Any = None
    error: Optional[str] = None

    @property
    def ok(self) -> bool:
        return 200 <= self.status < 300

    @property
    def is_json(self) -> bool:
        ct = self.headers.get("content-type", "")
        return "json" in ct or bool(self.text and self.text.lstrip().startswith(("{", "[")))

    def extract_jobs(self, envelope_keys: tuple[str, ...] = ()) -> list[Any]:
        """
        Try to extract a job list from the response JSON.

        Checks *envelope_keys* first, then a set of common wrapper keys.
        Returns an empty list if no job array is found.
        """
        data = self.json_body
        if data is None:
            return []

        _envelope = list(envelope_keys) + [
            "jobs", "results", "data", "listings", "items",
            "positions", "openings", "hits", "records",
            "vacancies", "postings", "opportunities", "edges", "nodes",
        ]

        if isinstance(data, list):
            return data

        if isinstance(data, dict):
            for key in _envelope:
                val = data.get(key)
                if isinstance(val, list):
                    # Unwrap GraphQL edges: [{node: {…}}, …]
                    if val and isinstance(val[0], dict) and "node" in val[0]:
                        return [item["node"] for item in val]
                    return val
                # Two-level nesting: {data: {jobs: […]}}
                if isinstance(val, dict):
                    for inner_key in _envelope:
                        inner_val = val.get(inner_key)
                        if isinstance(inner_val, list):
                            return inner_val

        return []


@dataclass
class ReplayClient:
    """
    Thin httpx wrapper for replaying :class:`~network_spy.DiscoveredEndpoint`
    requests without spinning up a browser.

    Parameters
    ----------
    extra_headers:
        Additional headers merged into every request (override defaults).
    timeout:
        Request timeout in seconds.
    follow_redirects:
        Follow HTTP redirects (default True).
    cookies:
        Optional dict of cookies to include (e.g. session cookies captured
        from the browser during spying).
    """

    extra_headers: dict[str, str] = field(default_factory=dict)
    timeout: float = 20.0
    follow_redirects: bool = True
    cookies: dict[str, str] = field(default_factory=dict)

    async def fetch(self, endpoint: Any, *, override_url: Optional[str] = None) -> ReplayResponse:
        """
        Replay *endpoint* (a :class:`~network_spy.DiscoveredEndpoint`) once.

        Pass *override_url* to call a different URL with the same headers/body
        (useful for manually constructed page URLs).
        """
        try:
            import httpx
        except ImportError as exc:
            raise ImportError("httpx is required. pip install httpx") from exc

        url = override_url or endpoint.url
        method = endpoint.method.upper()
        headers = {**_DEFAULT_HEADERS, **endpoint.replay_headers, **self.extra_headers}

        # Determine request body / content-type
        content: Optional[bytes] = None
        if method == "POST" and endpoint.request_post_data:
            content = endpoint.request_post_data.encode()
            headers.setdefault("Content-Type", "application/json")

        async with httpx.AsyncClient(
            timeout=self.timeout,
            follow_redirects=self.follow_redirects,
            cookies=self.cookies,
        ) as client:
            try:
                resp = await client.request(
                    method,
                    url,
                    headers=headers,
                    content=content,
                )
                body = resp.content
                text = resp.text
                try:
                    json_body = resp.json() if resp.headers.get("content-type", "") else None
                    if json_body is None and text.lstrip().startswith(("{", "[")):
                        json_body = json.loads(text)
                except Exception:
                    json_body = None

                return ReplayResponse(
                    url=url,
                    method=method,
                    status=resp.status_code,
                    headers=dict(resp.headers),
                    body=body,
                    text=text,
                    json_body=json_body,
                )
            except Exception as exc:
                logger.warning("Replay request failed for %s: %s", url, exc)
                return ReplayResponse(
                    url=url,
                    method=method,
                    status=0,
                    headers={},
                    body=None,
                    text=None,
                    error=str(exc),
                )

    async def paginate(
        self,
        endpoint: Any,
        *,
        max_pages: int = 20,
        page_step: int = 1,
        stop_on_empty: bool = True,
        delay: float = 0.5,
    ) -> list[ReplayResponse]:
        """
        Paginate through results by repeatedly calling *endpoint* with an
        incremented page / offset parameter.

        Works only when :attr:`~network_spy.DiscoveredEndpoint.pagination`
        was detected and its style is ``"page"`` or ``"offset"`` on the
        query-string.  For cursor or body-based pagination, use :meth:`fetch`
        manually.

        Parameters
        ----------
        endpoint:
            A :class:`~network_spy.DiscoveredEndpoint` with pagination info.
        max_pages:
            Maximum number of pages to fetch (safety cap).
        page_step:
            Increment applied to the page/offset parameter each iteration.
        stop_on_empty:
            Stop paging when a page returns an empty job list.
        delay:
            Seconds to wait between page requests (be polite).
        """
        pagination = getattr(endpoint, "pagination", None)
        if pagination is None or pagination.in_body or pagination.style == "cursor":
            logger.warning(
                "paginate() only supports query-string page/offset pagination. "
                "Fetching page 1 only."
            )
            return [await self.fetch(endpoint)]

        pages: list[ReplayResponse] = []
        current_url = endpoint.url

        for page_num in range(max_pages):
            resp = await self.fetch(endpoint, override_url=current_url)
            pages.append(resp)

            if not resp.ok:
                logger.warning(
                    "Pagination stopped at page %d: HTTP %d", page_num + 1, resp.status
                )
                break

            if stop_on_empty:
                jobs = resp.extract_jobs()
                if not jobs:
                    logger.debug("Empty page at page %d, stopping pagination", page_num + 1)
                    break

            # Advance to next page
            next_url = pagination.next_url(current_url, step=page_step)
            if next_url == current_url:
                logger.debug("next_url unchanged, stopping pagination")
                break
            current_url = next_url

            if page_num < max_pages - 1 and delay > 0:
                await asyncio.sleep(delay)

        logger.info("Pagination fetched %d page(s)", len(pages))
        return pages

    async def fetch_with_body_page(
        self,
        endpoint: Any,
        *,
        page_key: str,
        page_values: list[Any],
        delay: float = 0.5,
    ) -> list[ReplayResponse]:
        """
        Paginate a POST endpoint by replacing a key in the JSON request body.

        Example::
            pages = await client.fetch_with_body_page(
                endpoint,
                page_key="page",
                page_values=[1, 2, 3, 4, 5],
            )
        """
        pages: list[ReplayResponse] = []
        for val in page_values:
            body_dict = {}
            if endpoint.request_post_data:
                try:
                    body_dict = json.loads(endpoint.request_post_data)
                except json.JSONDecodeError:
                    pass
            body_dict[page_key] = val

            # Temporarily swap the post_data
            class _TempEndpoint:
                url = endpoint.url
                method = endpoint.method
                replay_headers = endpoint.replay_headers
                request_post_data = json.dumps(body_dict)

            resp = await self.fetch(_TempEndpoint())  # type: ignore[arg-type]
            pages.append(resp)

            if not resp.ok:
                break
            if not resp.extract_jobs():
                break
            if val != page_values[-1] and delay > 0:
                await asyncio.sleep(delay)

        return pages
