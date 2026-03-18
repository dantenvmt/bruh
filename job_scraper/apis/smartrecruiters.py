"""
SmartRecruiters public API integration.

Many SmartRecruiters-powered job boards expose postings via:
  - https://api.smartrecruiters.com/v1/companies/{company}/postings

Company slugs come from the public board URL:
  - https://jobs.smartrecruiters.com/{company}
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


class SmartRecruitersAPI(BaseJobAPI):
    """SmartRecruiters postings client."""

    BASE_URL = "https://api.smartrecruiters.com/v1/companies"

    def __init__(
        self,
        companies: Optional[List[str]] = None,
        include_content: bool = True,
        requests_per_minute: int = 60,
    ):
        super().__init__(name="SmartRecruiters")
        self.companies = [c.strip() for c in (companies or []) if c and str(c).strip()]
        self.include_content = bool(include_content)
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
        """Backward-compatible search_jobs that returns only jobs."""
        tracked_jobs, _ = await self.search_jobs_with_tracking(query, location, max_results, **kwargs)
        return [tracked.job for tracked in tracked_jobs]

    async def search_jobs_with_tracking(
        self,
        query: Optional[str] = None,
        location: Optional[str] = None,
        max_results: int = 100,
        **kwargs,
    ) -> Tuple[List[TrackedJob], List[BoardResult]]:
        """
        Search jobs across all configured companies with per-board tracking.

        Returns:
            Tuple of (tracked_jobs, board_results) where board_results contains metadata for each company.
        """
        if not self.is_configured():
            logger.warning("SmartRecruiters companies not configured, skipping")
            return [], []

        jobs: List[TrackedJob] = []
        board_results: List[BoardResult] = []
        needle_q = (query or "").strip().lower()
        needle_loc = (location or "").strip().lower()

        page_size = 100

        _headers = {
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
        }
        async with httpx.AsyncClient(timeout=30.0, headers=_headers) as client:
            for company in self.companies:
                start_time = time.monotonic()
                company_jobs: List[TrackedJob] = []
                error = None
                error_code = None

                try:
                    offset = 0
                    while len(jobs) < max_results:
                        # Fetch page with retry logic on 429
                        payload = await self._fetch_postings_page_with_retry(
                            client, company=company, limit=page_size, offset=offset
                        )

                        if payload is None:
                            # Error already logged in _fetch_postings_page_with_retry
                            error = f"Failed to fetch postings after retries"
                            error_code = "rate_limited"
                            break

                        items = payload.get("content") or []
                        if not isinstance(items, list) or not items:
                            break

                        for item in items:
                            job = await self._parse_job(client, company, item)
                            if not job:
                                continue

                            if needle_q and not self._matches_query(job, item, needle_q):
                                continue
                            if needle_loc and job.location and needle_loc not in job.location.lower():
                                continue

                            tracked_job = TrackedJob(job=job, board_token=company)
                            company_jobs.append(tracked_job)
                            jobs.append(tracked_job)
                            if len(jobs) >= max_results:
                                break

                        if len(jobs) >= max_results:
                            break

                        offset += page_size

                except httpx.TimeoutException as exc:
                    error = f"Timeout fetching jobs: {exc}"
                    error_code = "timeout"
                    logger.warning(f"SmartRecruiters timeout for {company}: {exc}")
                except httpx.HTTPStatusError as exc:
                    if exc.response.status_code == 404:
                        error = f"Company not found: {company}"
                        error_code = "not_found"
                        logger.warning(f"SmartRecruiters company not found: {company}")
                    else:
                        error = f"HTTP {exc.response.status_code}: {exc}"
                        error_code = "http_error"
                        logger.error(f"SmartRecruiters HTTP error for {company}: {exc}")
                except Exception as exc:
                    error = f"Parse error: {exc}"
                    error_code = "parse_error"
                    logger.error(f"SmartRecruiters error for {company}: {exc}")

                # Record result for this company
                duration_ms = int((time.monotonic() - start_time) * 1000)
                board_results.append(
                    BoardResult(
                        source=self.name.lower(),
                        board_token=company,
                        jobs_fetched=len(company_jobs),
                        error=error,
                        error_code=error_code,
                        duration_ms=duration_ms,
                    )
                )

                if len(jobs) >= max_results:
                    break

        logger.info(f"SmartRecruiters returned {len(jobs)} jobs from {len(board_results)} companies")
        return jobs, board_results

    async def _fetch_postings_page(
        self,
        client: httpx.AsyncClient,
        company: str,
        limit: int,
        offset: int,
    ) -> dict:
        await self._wait_for_slot()
        url = f"{self.BASE_URL}/{company}/postings"
        resp = await client.get(url, params={"limit": int(limit), "offset": int(offset)})
        if resp.status_code >= 400:
            logger.warning(f"SmartRecruiters list failed for {company}: {resp.status_code}")
            return {}
        data = resp.json() if resp.content else {}
        return data if isinstance(data, dict) else {}

    async def _fetch_postings_page_with_retry(
        self,
        client: httpx.AsyncClient,
        company: str,
        limit: int,
        offset: int,
        max_retries: int = 3,
    ) -> Optional[dict]:
        """
        Fetch postings page with exponential backoff retry on 429 rate limit.

        Args:
            client: HTTP client
            company: Company slug
            limit: Page size
            offset: Pagination offset
            max_retries: Maximum retry attempts (default: 3)

        Returns:
            Response dict or None if all retries failed
        """
        backoff = ExponentialBackoff(base_seconds=2.0, max_seconds=60.0)

        for attempt in range(max_retries):
            await self._wait_for_slot()
            url = f"{self.BASE_URL}/{company}/postings"

            try:
                resp = await client.get(url, params={"limit": int(limit), "offset": int(offset)})

                # Handle rate limiting with retry
                if resp.status_code == 429:
                    if attempt < max_retries - 1:
                        # Parse Retry-After header if present
                        retry_after = backoff.parse_retry_after(dict(resp.headers))
                        if retry_after is None:
                            retry_after = backoff.get_delay(attempt)

                        logger.warning(
                            f"SmartRecruiters rate limited for {company} "
                            f"(attempt {attempt + 1}/{max_retries}). "
                            f"Retrying in {retry_after:.1f}s..."
                        )
                        await asyncio.sleep(retry_after)
                        continue
                    else:
                        logger.error(f"SmartRecruiters rate limited for {company} after {max_retries} attempts")
                        return None

                # Handle other errors
                if resp.status_code >= 400:
                    logger.warning(f"SmartRecruiters list failed for {company}: {resp.status_code}")
                    return {}

                # Success
                data = resp.json() if resp.content else {}
                return data if isinstance(data, dict) else {}

            except httpx.HTTPStatusError as exc:
                if exc.response.status_code == 429 and attempt < max_retries - 1:
                    retry_after = backoff.get_delay(attempt)
                    logger.warning(
                        f"SmartRecruiters rate limited for {company} "
                        f"(attempt {attempt + 1}/{max_retries}). "
                        f"Retrying in {retry_after:.1f}s..."
                    )
                    await asyncio.sleep(retry_after)
                    continue
                raise
            except Exception as exc:
                logger.error(f"SmartRecruiters fetch error for {company}: {exc}")
                raise

        return None

    async def _fetch_posting_detail(self, client: httpx.AsyncClient, company: str, posting_id: str) -> Optional[dict]:
        await self._wait_for_slot()
        url = f"{self.BASE_URL}/{company}/postings/{posting_id}"
        resp = await client.get(url)
        if resp.status_code >= 400:
            return None
        data = resp.json() if resp.content else None
        return data if isinstance(data, dict) else None

    async def _parse_job(self, client: httpx.AsyncClient, company: str, item: dict) -> Optional[Job]:
        if not isinstance(item, dict):
            return None

        posting_id = item.get("id")
        title = item.get("name") or item.get("title") or ""
        if not posting_id or not title:
            return None

        loc = item.get("location") or {}
        full_location = None
        if isinstance(loc, dict):
            full_location = loc.get("fullLocation")
            if not full_location:
                parts = [loc.get("city"), loc.get("region"), loc.get("country")]
                full_location = ", ".join([p for p in parts if p])

        remote = None
        if isinstance(loc, dict):
            remote = loc.get("remote")
            if remote is None and loc.get("hybrid") is True:
                remote = True

        url = f"https://jobs.smartrecruiters.com/{company}/{posting_id}"
        posted = item.get("releasedDate")
        employment_type = item.get("typeOfEmployment")

        description = None
        raw_payload = item
        if self.include_content:
            detail = await self._fetch_posting_detail(client, company, str(posting_id))
            if isinstance(detail, dict):
                raw_payload = {"list_item": item, "detail": detail}
                description = self._extract_description(detail) or description
                apply_url = detail.get("applyUrl") or detail.get("postingUrl")
                if apply_url:
                    url = apply_url

        return Job(
            title=str(title),
            company=company,
            location=full_location,
            url=url,
            description=description,
            salary=None,
            employment_type=str(employment_type) if employment_type else None,
            posted_date=str(posted) if posted else None,
            source="SmartRecruiters",
            job_id=str(posting_id),
            category=item.get("department") if isinstance(item.get("department"), str) else None,
            tags=None,
            skills=None,
            remote=remote,
            raw_payload=raw_payload,
        )

    def _extract_description(self, detail: dict) -> Optional[str]:
        job_ad = detail.get("jobAd") or {}
        sections = job_ad.get("sections") if isinstance(job_ad, dict) else None
        if not isinstance(sections, dict):
            return None
        parts = []
        for key in ("jobDescription", "qualifications", "additionalInformation", "companyDescription"):
            block = sections.get(key)
            if isinstance(block, dict):
                text = block.get("text")
                if isinstance(text, str) and text.strip():
                    parts.append(text.strip())
        if parts:
            return "\n\n".join(parts)
        return None

    def _matches_query(self, job: Job, item: dict, needle: str) -> bool:
        haystack = " ".join(
            [
                job.title or "",
                job.company or "",
                job.location or "",
                job.description or "",
                str(item.get("department") or ""),
                str(item.get("function") or ""),
            ]
        ).lower()
        return needle in haystack

    async def _wait_for_slot(self) -> None:
        min_interval = 60.0 / self.requests_per_minute if self.requests_per_minute > 0 else 0.0
        if min_interval <= 0:
            return
        async with self._rate_lock:
            now = time.monotonic()
            elapsed = now - self._last_request_time
            wait_seconds = max(0.0, min_interval - elapsed)
            if wait_seconds > 0:
                await asyncio.sleep(wait_seconds)
            self._last_request_time = time.monotonic()

