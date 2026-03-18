"""
Workday public job search integration.

Workday career sites follow a predictable URL pattern:
  https://{host}/wday/cxs/{tenant}/{site}/jobs

The JSON API requires no auth and returns paginated results.
Pagination uses offset-based approach with a POST body.

Phase 1: list-only ingestion with locationsText location parsing.
Phase 2 (future): inline detail enrichment + US-authoritative filtering.
"""
from __future__ import annotations

import asyncio
import logging
import re
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import httpx

from . import BaseJobAPI, BoardResult, TrackedJob
from ..models import Job
from ..utils import ExponentialBackoff

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# URL parsing
# ---------------------------------------------------------------------------

_WORKDAY_URL_RE = re.compile(
    r"https?://(?P<host>(?P<tenant>[^.]+)(?:\.wd\d+)?\.myworkdayjobs\.com)"
    r"(?:/[a-z]{2}(?:-[A-Z]{2})?)?"  # optional locale e.g. en-US, fr-FR
    r"/(?P<site>[^/?#]+)",
    re.IGNORECASE,
)

_FILTERED_PATHS = frozenset({"wday", "cxs", "api", "js", "css", "static"})


@dataclass
class WorkdaySite:
    """Three-field Workday host model."""

    host: str    # e.g. "nvidia.wd5.myworkdayjobs.com"
    tenant: str  # e.g. "nvidia"
    site: str    # e.g. "NVIDIAExternalCareerSite"

    @property
    def api_base(self) -> str:
        return f"https://{self.host}/wday/cxs/{self.tenant}/{self.site}"

    @property
    def careers_url(self) -> str:
        return f"https://{self.host}/{self.site}"


def parse_workday_url(url: str) -> Optional[WorkdaySite]:
    """Extract a WorkdaySite from a Workday careers URL.

    Examples:
        https://nvidia.wd5.myworkdayjobs.com/en-US/NVIDIAExternalCareerSite
        -> WorkdaySite(host="nvidia.wd5.myworkdayjobs.com", tenant="nvidia",
                       site="NVIDIAExternalCareerSite")

        https://amazon.myworkdayjobs.com/AmazonJobs
        -> WorkdaySite(host="amazon.myworkdayjobs.com", tenant="amazon",
                       site="AmazonJobs")
    """
    match = _WORKDAY_URL_RE.match(url)
    if not match:
        return None
    host = match.group("host")
    tenant = match.group("tenant")
    site = match.group("site")
    if site.lower() in _FILTERED_PATHS:
        return None
    return WorkdaySite(host=host, tenant=tenant, site=site)


# ---------------------------------------------------------------------------
# API client
# ---------------------------------------------------------------------------

class WorkdayAPI(BaseJobAPI):
    """Workday job search API client.

    Each Workday instance is identified by a (host, tenant, site) triple.
    """

    def __init__(
        self,
        sites: Optional[List[dict]] = None,
        requests_per_minute: int = 30,
        include_details: bool = False,
        max_details_per_site: int = 50,
        detail_concurrency: int = 3,
        detail_timeout: float = 10.0,
    ):
        super().__init__(name="Workday")
        self.sites = sites or []
        self.requests_per_minute = max(1, int(requests_per_minute or 30))
        self.include_details = include_details
        self.max_details_per_site = max_details_per_site
        self.detail_concurrency = detail_concurrency
        self.detail_timeout = detail_timeout
        self._rate_lock = asyncio.Lock()
        self._last_request_time = 0.0
        self._csrf_cache: Dict[str, str] = {}

    def is_configured(self) -> bool:
        return bool(self.sites)

    async def search_jobs(
        self,
        query: Optional[str] = None,
        location: Optional[str] = None,
        max_results: int = 100,
        **kwargs,
    ) -> List[Job]:
        tracked_jobs, _ = await self.search_jobs_with_tracking(
            query=query, location=location, max_results=max_results, **kwargs
        )
        return [tracked.job for tracked in tracked_jobs]

    async def search_jobs_with_tracking(
        self,
        query: Optional[str] = None,
        location: Optional[str] = None,
        max_results: int = 100,
        **kwargs,
    ) -> Tuple[List[TrackedJob], List[BoardResult]]:
        if not self.is_configured():
            logger.warning("Workday sites not configured, skipping")
            return [], []

        all_jobs: List[TrackedJob] = []
        board_results: List[BoardResult] = []
        backoff = ExponentialBackoff(base_seconds=3.0, max_seconds=60.0)

        async with httpx.AsyncClient(timeout=30.0) as client:
            for site_cfg in self.sites:
                host = site_cfg.get("host", "")
                tenant = site_cfg.get("tenant", "")
                site = site_cfg.get("site", "")
                if not host or not tenant or not site:
                    # Backwards compat: derive host from tenant if missing
                    if tenant and site and not host:
                        host = f"{tenant}.myworkdayjobs.com"
                    else:
                        continue

                board_token = f"{tenant}/{site}"
                started = time.monotonic()
                site_jobs: List[TrackedJob] = []
                error_msg: Optional[str] = None
                error_code: Optional[str] = None

                try:
                    offset = 0
                    page_size = 20  # Workday default

                    while len(all_jobs) + len(site_jobs) < max_results:
                        await self._wait_for_slot()
                        api_url = f"https://{host}/wday/cxs/{tenant}/{site}/jobs"
                        body = {
                            "appliedFacets": {},
                            "limit": page_size,
                            "offset": offset,
                            "searchText": query or "",
                        }

                        resp = await self._post_with_csrf(
                            client, api_url, body, host, backoff
                        )

                        if resp is None:
                            error_code = "timeout"
                            error_msg = "Request timeout after retries"
                            break

                        if resp.status_code == 422:
                            error_code = "invalid_site"
                            error_msg = f"HTTP 422 — bad tenant/site: {board_token}"
                            logger.warning("Workday 422 for %s — invalid site", board_token)
                            break

                        if resp.status_code == 429:
                            error_code = "rate_limited"
                            error_msg = "HTTP 429"
                            break

                        if resp.status_code >= 400:
                            error_code = "http_error"
                            error_msg = f"HTTP {resp.status_code}"
                            break

                        data = resp.json() if resp.content else {}
                        postings = data.get("jobPostings") or []
                        total = data.get("total", 0)

                        if not postings:
                            break

                        for posting in postings:
                            job = await self._parse_job(client, posting, host, tenant, site)
                            if not job:
                                continue
                            if location and job.location and location.lower() not in job.location.lower():
                                continue
                            site_jobs.append(TrackedJob(job=job, board_token=board_token))

                        offset += page_size
                        if offset >= total:
                            break

                except httpx.TimeoutException:
                    error_code = "timeout"
                    error_msg = "Request timeout"
                except Exception as exc:
                    error_code = "unknown_error"
                    error_msg = str(exc)
                    logger.error("Workday error for %s: %s", board_token, exc)

                duration_ms = int((time.monotonic() - started) * 1000)
                board_results.append(
                    BoardResult(
                        source=self.name.lower(),
                        board_token=board_token,
                        jobs_fetched=len(site_jobs),
                        error=error_msg,
                        error_code=error_code,
                        duration_ms=duration_ms,
                    )
                )
                all_jobs.extend(site_jobs)
                if len(all_jobs) >= max_results:
                    break

        logger.info("Workday returned %d jobs from %d sites", len(all_jobs), len(board_results))
        return all_jobs, board_results

    # ------------------------------------------------------------------
    # CSRF handling
    # ------------------------------------------------------------------

    async def _post_with_csrf(
        self,
        client: httpx.AsyncClient,
        url: str,
        body: dict,
        host: str,
        backoff: ExponentialBackoff,
    ) -> Optional[httpx.Response]:
        """POST with lazy CSRF fallback.

        - Try plain POST first.
        - On 401/403 only: GET careers page, read CALYPSO_CSRF_TOKEN cookie, retry.
        - 422 = bad tenant/site — return immediately (no CSRF retry).
        - Cache acquired CSRF tokens per host for session duration.
        """
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Origin": f"https://{host}",
            "Referer": f"https://{host}/",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
        }

        # Use cached CSRF token if available
        csrf_token = self._csrf_cache.get(host)
        if csrf_token:
            headers["CALYPSO_CSRF_TOKEN"] = csrf_token

        resp = None
        for attempt in range(3):
            try:
                resp = await client.post(url, json=body, headers=headers)

                # 401/403 without CSRF → acquire and retry once
                if resp.status_code in (401, 403) and host not in self._csrf_cache:
                    token = await self._acquire_csrf(client, host)
                    if token:
                        self._csrf_cache[host] = token
                        headers["CALYPSO_CSRF_TOKEN"] = token
                        resp = await client.post(url, json=body, headers=headers)
                        if resp.status_code in (401, 403):
                            logger.warning(
                                "Workday CSRF retry failed for %s (HTTP %d)",
                                host, resp.status_code,
                            )
                    else:
                        logger.warning("Workday CSRF acquisition failed for %s", host)
                    break  # Don't retry further after CSRF attempt

                # 422 = bad site, don't retry
                if resp.status_code == 422:
                    break

                # 429 = rate limited, backoff and retry
                if resp.status_code == 429 and attempt < 2:
                    await asyncio.sleep(backoff.get_delay(attempt))
                    continue

                # Success or non-retryable error
                break

            except httpx.TimeoutException:
                if attempt < 2:
                    await asyncio.sleep(backoff.get_delay(attempt))
                    continue
                raise

        return resp

    async def _acquire_csrf(self, client: httpx.AsyncClient, host: str) -> Optional[str]:
        """GET the careers page and extract CALYPSO_CSRF_TOKEN cookie."""
        try:
            careers_url = f"https://{host}"
            resp = await client.get(
                careers_url,
                follow_redirects=True,
                headers={"User-Agent": "Mozilla/5.0"},
            )
            for cookie in resp.cookies.jar:
                if cookie.name == "CALYPSO_CSRF_TOKEN":
                    logger.debug("Acquired CSRF token for %s", host)
                    return cookie.value
        except Exception as exc:
            logger.debug("CSRF acquisition error for %s: %s", host, exc)
        return None

    # ------------------------------------------------------------------
    # Job parsing
    # ------------------------------------------------------------------

    async def _parse_job(
        self, client: httpx.AsyncClient, posting: dict, host: str, tenant: str, site: str,
    ) -> Optional[Job]:
        if not isinstance(posting, dict):
            return None

        title = posting.get("title") or (posting.get("bulletFields") or [None])[0]
        if not title:
            return None

        external_path = posting.get("externalPath", "")
        url = f"https://{host}/en-US/{site}{external_path}" if external_path else None

        # Location: prefer locationsText (always present in Workday list responses)
        location = None
        locations_text = posting.get("locationsText")
        if locations_text and isinstance(locations_text, str):
            location = locations_text.strip()
        if not location:
            bullet_fields = posting.get("bulletFields") or []
            if isinstance(bullet_fields, list) and len(bullet_fields) > 1:
                location = bullet_fields[1] if isinstance(bullet_fields[1], str) else None

        posted_on = posting.get("postedOn")

        remote = False
        loc_str = (location or "") + " " + (title or "")
        if "remote" in loc_str.lower():
            remote = True

        description = None
        if self.include_details and url:
            try:
                resp = await client.get(
                    url,
                    headers={
                        "User-Agent": (
                            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                            "AppleWebKit/537.36 (KHTML, like Gecko) "
                            "Chrome/124.0.0.0 Safari/537.36"
                        ),
                        "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
                        "Accept-Language": "en-US,en;q=0.9",
                    },
                    timeout=self.detail_timeout,
                    follow_redirects=True,
                )
                if resp.status_code == 200:
                    import json as _json
                    ld_blocks = re.findall(
                        r'<script[^>]+application/ld\+json[^>]*>(.*?)</script>',
                        resp.text, re.DOTALL,
                    )
                    for block in ld_blocks:
                        try:
                            data = _json.loads(block.strip())
                            if data.get("@type") == "JobPosting":
                                raw = data.get("description", "")
                                if raw:
                                    description = re.sub(r"<[^>]+>", " ", raw).strip()
                                    description = re.sub(r"\s{2,}", " ", description)
                                    break
                        except Exception:
                            continue
            except Exception as exc:
                logger.debug("Workday detail fetch failed for %s: %s", url, exc)

        return Job(
            title=str(title),
            company=tenant,
            location=location,
            url=url,
            description=description,
            salary=None,
            employment_type=None,
            posted_date=posted_on,
            source="Workday",
            job_id=external_path or None,
            category=None,
            tags=None,
            skills=None,
            remote=remote,
            raw_payload=posting,
        )

    # ------------------------------------------------------------------
    # Rate limiting
    # ------------------------------------------------------------------

    async def _wait_for_slot(self) -> None:
        min_interval = 60.0 / self.requests_per_minute
        async with self._rate_lock:
            now = time.monotonic()
            elapsed = now - self._last_request_time
            wait_seconds = max(0.0, min_interval - elapsed)
            if wait_seconds > 0:
                await asyncio.sleep(wait_seconds)
            self._last_request_time = time.monotonic()
