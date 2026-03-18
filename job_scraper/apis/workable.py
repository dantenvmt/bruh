"""
Workable public API integration.

Workable-powered job boards expose postings via:
  https://apply.workable.com/{company}/api/v1/jobs

No authentication required.  Returns paginated JSON with job postings.
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import List, Optional, Tuple

import httpx

from . import BaseJobAPI, BoardResult, TrackedJob
from ..models import Job
from ..utils import ExponentialBackoff

logger = logging.getLogger(__name__)


class WorkableAPI(BaseJobAPI):
    """Workable public job board API client."""

    BASE_URL = "https://apply.workable.com"

    def __init__(
        self,
        companies: Optional[List[str]] = None,
        requests_per_minute: int = 60,
    ):
        super().__init__(name="Workable")
        self.companies = [c.strip() for c in (companies or []) if c and str(c).strip()]
        self.requests_per_minute = max(1, int(requests_per_minute or 60))
        self._rate_lock = asyncio.Lock()
        self._last_request_time = 0.0

    def is_configured(self) -> bool:
        return bool(self.companies)

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
            logger.warning("Workable companies not configured, skipping")
            return [], []

        jobs: List[TrackedJob] = []
        board_results: List[BoardResult] = []
        needle_q = (query or "").strip().lower()
        needle_loc = (location or "").strip().lower()
        backoff = ExponentialBackoff(base_seconds=2.0, max_seconds=60.0)

        async with httpx.AsyncClient(timeout=30.0) as client:
            for company in self.companies:
                started = time.monotonic()
                company_jobs: List[TrackedJob] = []
                error_msg: Optional[str] = None
                error_code: Optional[str] = None

                for attempt in range(3):
                    try:
                        await self._wait_for_slot()
                        url = f"{self.BASE_URL}/{company}/api/v1/jobs"
                        resp = await client.get(url)

                        if resp.status_code == 429:
                            if attempt < 2:
                                delay = backoff.get_delay(attempt)
                                logger.warning(
                                    "Workable rate limited for '%s', retrying in %.1fs",
                                    company, delay,
                                )
                                await asyncio.sleep(delay)
                                continue
                            error_code = "rate_limited"
                            error_msg = "Rate limited after 3 attempts"
                            break

                        if resp.status_code == 404:
                            error_code = "not_found"
                            error_msg = f"Company not found: {company}"
                            break

                        if resp.status_code >= 400:
                            error_code = "http_error"
                            error_msg = f"HTTP {resp.status_code}"
                            break

                        data = resp.json() if resp.content else {}
                        items = data.get("results") or data.get("jobs") or []
                        if isinstance(data, list):
                            items = data

                        for item in items:
                            job = self._parse_job(item, company)
                            if not job:
                                continue
                            if needle_q and needle_q not in (job.title or "").lower():
                                continue
                            if needle_loc and job.location and needle_loc not in job.location.lower():
                                continue
                            company_jobs.append(TrackedJob(job=job, board_token=company))
                            if len(jobs) + len(company_jobs) >= max_results:
                                break
                        break  # success

                    except httpx.TimeoutException:
                        error_code = "timeout"
                        error_msg = "Request timeout"
                        break
                    except Exception as exc:
                        error_code = "unknown_error"
                        error_msg = str(exc)
                        logger.error("Workable error for '%s': %s", company, exc)
                        break

                duration_ms = int((time.monotonic() - started) * 1000)
                board_results.append(
                    BoardResult(
                        source=self.name.lower(),
                        board_token=company,
                        jobs_fetched=len(company_jobs),
                        error=error_msg,
                        error_code=error_code,
                        duration_ms=duration_ms,
                    )
                )
                jobs.extend(company_jobs)
                if len(jobs) >= max_results:
                    break

        logger.info("Workable returned %d jobs from %d companies", len(jobs), len(board_results))
        return jobs, board_results

    def _parse_job(self, item: dict, company: str) -> Optional[Job]:
        if not isinstance(item, dict):
            return None
        title = item.get("title") or item.get("name") or ""
        if not title:
            return None

        shortcode = item.get("shortcode") or item.get("id") or ""
        url = item.get("url") or item.get("application_url")
        if not url and shortcode:
            url = f"{self.BASE_URL}/{company}/j/{shortcode}"

        location = item.get("location") or item.get("city")
        if isinstance(location, dict):
            location = location.get("name") or location.get("city")

        department = item.get("department")
        employment_type = item.get("employment_type") or item.get("type")
        remote = item.get("telecommuting") or False

        return Job(
            title=str(title),
            company=company,
            location=str(location) if location else None,
            url=url,
            description=item.get("description"),
            salary=None,
            employment_type=str(employment_type) if employment_type else None,
            posted_date=item.get("published_on") or item.get("created_at"),
            source="Workable",
            job_id=str(shortcode) if shortcode else None,
            category=str(department) if department else None,
            tags=None,
            skills=None,
            remote=bool(remote),
            raw_payload=item,
        )

    async def _wait_for_slot(self) -> None:
        min_interval = 60.0 / self.requests_per_minute
        async with self._rate_lock:
            now = time.monotonic()
            elapsed = now - self._last_request_time
            wait_seconds = max(0.0, min_interval - elapsed)
            if wait_seconds > 0:
                await asyncio.sleep(wait_seconds)
            self._last_request_time = time.monotonic()
