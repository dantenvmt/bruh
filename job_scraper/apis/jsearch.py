"""
JSearch (RapidAPI) integration.
"""
import asyncio
import logging
import random
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import List, Optional

import httpx

from . import BaseJobAPI
from ..models import Job

logger = logging.getLogger(__name__)


class JSearchAPI(BaseJobAPI):
    """RapidAPI JSearch client"""

    BASE_URL = "https://jsearch.p.rapidapi.com/search"
    DEFAULT_PAGE_SIZE = 10
    BLOCKED_STATUSES = {401, 403}
    RETRY_STATUSES = {429, 500, 502, 503, 504}

    def __init__(
        self,
        api_key: Optional[str] = None,
        host: str = "jsearch.p.rapidapi.com",
        safe_mode: bool = True,
        min_interval_seconds: float = 1.5,
        jitter_seconds: float = 0.7,
        requests_per_minute: int = 25,
        max_pages: int = 10,
        max_retries: int = 4,
        backoff_base_seconds: float = 2.0,
        backoff_cap_seconds: float = 45.0,
        cooldown_every_n_requests: int = 5,
        cooldown_seconds: float = 8.0,
        respect_retry_after: bool = True,
        user_agent: Optional[str] = None,
        timeout_seconds: float = 30.0,
        rate_limit_remaining_floor: int = 1,
    ):
        super().__init__(name="JSearch")
        self.api_key = api_key
        self.host = host
        self.safe_mode = bool(safe_mode)
        self.min_interval_seconds = max(0.0, float(min_interval_seconds))
        self.jitter_seconds = max(0.0, float(jitter_seconds))
        self.requests_per_minute = max(0, int(requests_per_minute))
        self.max_pages = int(max_pages) if max_pages is not None else 0
        self.max_retries = max(0, int(max_retries))
        self.backoff_base_seconds = max(0.0, float(backoff_base_seconds))
        self.backoff_cap_seconds = max(0.0, float(backoff_cap_seconds))
        self.cooldown_every_n_requests = max(0, int(cooldown_every_n_requests))
        self.cooldown_seconds = max(0.0, float(cooldown_seconds))
        self.respect_retry_after = bool(respect_retry_after)
        self.user_agent = user_agent or "multi-api-aggregator/1.0"
        self.timeout_seconds = max(1.0, float(timeout_seconds))
        self.rate_limit_remaining_floor = max(0, int(rate_limit_remaining_floor))
        self._requests_made = 0
        self._rate_lock = asyncio.Lock()
        self._last_request_time = 0.0

        if not self.safe_mode:
            self.min_interval_seconds = 0.0
            self.jitter_seconds = 0.0
            self.requests_per_minute = 0
            self.cooldown_every_n_requests = 0

    def is_configured(self) -> bool:
        return bool(self.api_key)

    async def search_jobs(
        self,
        query: Optional[str] = None,
        location: Optional[str] = None,
        max_results: int = 100,
        **kwargs,
    ) -> List[Job]:
        if not self.is_configured():
            logger.warning("JSearch API not configured, skipping")
            return []

        base_query = query or "software"
        if location:
            base_query = f"{base_query} in {location}" if base_query else location

        headers = {
            "X-RapidAPI-Key": self.api_key,
            "X-RapidAPI-Host": self.host,
            "User-Agent": self.user_agent,
        }

        jobs: List[Job] = []
        page = 1
        page_limit = self._page_limit(max_results)
        try:
            async with httpx.AsyncClient(
                timeout=self.timeout_seconds, limits=self._client_limits()
            ) as client:
                while len(jobs) < max_results and page <= page_limit:
                    params = {
                        "query": base_query,
                        "page": page,
                        "num_pages": 1,
                    }
                    resp = await self._fetch_page(client, headers, params)
                    if resp is None:
                        break

                    data = resp.json() if resp.content else {}
                    items = data.get("data") or []
                    if not items:
                        break

                    for item in items:
                        job = self._parse_job(item)
                        if job:
                            jobs.append(job)
                        if len(jobs) >= max_results:
                            break

                    await self._apply_rate_limit_headers(resp.headers)
                    page += 1

            logger.info(f"JSearch returned {len(jobs)} jobs")
            return jobs
        except Exception as exc:
            logger.error(f"JSearch API error: {exc}")
            return jobs

    def _parse_job(self, item: dict) -> Optional[Job]:
        city = item.get("job_city")
        state = item.get("job_state")
        country = item.get("job_country")
        location_parts = [p for p in [city, state, country] if p]
        location = ", ".join(location_parts) if location_parts else None

        employment_type = item.get("job_employment_type")
        if isinstance(employment_type, list):
            employment_type = ", ".join([t for t in employment_type if t])

        posted = item.get("job_posted_at_datetime_utc") or item.get("job_posted_at")
        if posted and isinstance(posted, (int, float)):
            try:
                posted = datetime.utcfromtimestamp(posted).isoformat()
            except Exception:
                posted = str(posted)

        return Job(
            title=item.get("job_title", ""),
            company=item.get("employer_name", ""),
            location=location,
            url=item.get("job_apply_link") or item.get("job_apply_link") or item.get("job_google_link"),
            description=item.get("job_description"),
            salary=item.get("job_min_salary") or item.get("job_max_salary"),
            employment_type=employment_type,
            posted_date=posted,
            source="JSearch",
            job_id=item.get("job_id"),
            category=item.get("job_category"),
            tags=item.get("job_required_skills"),
            skills=item.get("job_required_skills"),
            remote=item.get("job_is_remote"),
            raw_payload=item,
        )

    def _client_limits(self) -> httpx.Limits:
        return httpx.Limits(max_connections=2, max_keepalive_connections=1)

    def _page_limit(self, max_results: int) -> int:
        if max_results <= 0:
            return 1
        estimated_pages = (max_results + self.DEFAULT_PAGE_SIZE - 1) // self.DEFAULT_PAGE_SIZE
        if self.max_pages and self.max_pages > 0:
            return min(estimated_pages, self.max_pages)
        return estimated_pages

    def _base_interval(self) -> float:
        interval = self.min_interval_seconds
        if self.requests_per_minute > 0:
            interval = max(interval, 60.0 / self.requests_per_minute)
        return interval

    async def _wait_for_slot(self) -> None:
        target_interval = self._base_interval()
        jitter = random.uniform(0.0, self.jitter_seconds) if self.jitter_seconds else 0.0
        target_interval += jitter

        async with self._rate_lock:
            now = time.monotonic()
            elapsed = now - self._last_request_time
            wait_seconds = max(0.0, target_interval - elapsed)
            if wait_seconds > 0:
                await asyncio.sleep(wait_seconds)

            self._requests_made += 1
            if self.cooldown_every_n_requests and self._requests_made % self.cooldown_every_n_requests == 0:
                cooldown = self.cooldown_seconds
                if self.jitter_seconds:
                    cooldown += random.uniform(0.0, self.jitter_seconds)
                if cooldown > 0:
                    await asyncio.sleep(cooldown)

            self._last_request_time = time.monotonic()

    async def _fetch_page(self, client: httpx.AsyncClient, headers: dict, params: dict) -> Optional[httpx.Response]:
        attempt = 0
        while True:
            await self._wait_for_slot()
            try:
                resp = await client.get(self.BASE_URL, headers=headers, params=params)
            except (httpx.TimeoutException, httpx.TransportError) as exc:
                if attempt >= self.max_retries:
                    logger.error(f"JSearch network error after retries: {exc}")
                    return None
                delay = self._compute_backoff_delay(None, attempt)
                logger.warning(f"JSearch network error, backing off for {delay:.1f}s")
                await asyncio.sleep(delay)
                attempt += 1
                continue

            status = resp.status_code
            if status in self.BLOCKED_STATUSES:
                logger.error(f"JSearch returned {status}, stopping to avoid blocking")
                return None

            if status in self.RETRY_STATUSES:
                if attempt >= self.max_retries:
                    logger.error(f"JSearch repeated {status}, giving up")
                    return None
                delay = self._compute_backoff_delay(resp, attempt)
                logger.warning(f"JSearch status {status}, backing off for {delay:.1f}s")
                await asyncio.sleep(delay)
                attempt += 1
                continue

            if status >= 400:
                logger.error(f"JSearch error {status}: {resp.text}")
                return None

            return resp

    def _compute_backoff_delay(self, response: Optional[httpx.Response], attempt: int) -> float:
        if response is not None and self.respect_retry_after:
            retry_after = self._parse_retry_after(response.headers.get("Retry-After"))
            if retry_after is not None:
                return min(self.backoff_cap_seconds, max(retry_after, self.backoff_base_seconds))

        base = self.backoff_base_seconds * (2 ** attempt)
        jitter = random.uniform(0.0, self.jitter_seconds) if self.jitter_seconds else 0.0
        return min(self.backoff_cap_seconds, base + jitter)

    def _parse_retry_after(self, value: Optional[str]) -> Optional[float]:
        if not value:
            return None
        try:
            return max(0.0, float(value))
        except ValueError:
            try:
                dt = parsedate_to_datetime(value)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return max(0.0, (dt - datetime.now(timezone.utc)).total_seconds())
            except Exception:
                return None

    async def _apply_rate_limit_headers(self, headers: httpx.Headers) -> None:
        remaining = self._parse_int_header(headers, "X-RateLimit-Remaining")
        if remaining is None:
            return
        if remaining <= self.rate_limit_remaining_floor:
            reset_seconds = self._parse_reset_seconds(headers)
            if reset_seconds is not None and reset_seconds > 0:
                delay = reset_seconds
                if self.jitter_seconds:
                    delay += random.uniform(0.0, self.jitter_seconds)
                logger.warning(
                    f"JSearch rate limit low (remaining={remaining}). Cooling down for {delay:.1f}s"
                )
                await asyncio.sleep(delay)

    def _parse_int_header(self, headers: httpx.Headers, key: str) -> Optional[int]:
        value = headers.get(key) or headers.get(key.lower())
        if value is None:
            return None
        try:
            return int(float(value))
        except (TypeError, ValueError):
            return None

    def _parse_reset_seconds(self, headers: httpx.Headers) -> Optional[float]:
        for key in ("X-RateLimit-Reset", "X-RateLimit-Reset-Seconds"):
            value = headers.get(key) or headers.get(key.lower())
            if not value:
                continue
            try:
                seconds = float(value)
            except ValueError:
                continue
            if seconds > 1_000_000_000:
                return max(0.0, seconds - time.time())
            return max(0.0, seconds)
        return None
