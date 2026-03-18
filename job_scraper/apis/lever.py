"""
Lever Postings API integration.
"""
import asyncio
import logging
import time
from datetime import datetime
from typing import List, Optional, Tuple

import httpx

from . import BaseJobAPI, BoardResult, TrackedJob
from ..models import Job
from ..utils import ExponentialBackoff

logger = logging.getLogger(__name__)


class LeverAPI(BaseJobAPI):
    """Lever postings API client"""

    BASE_URL = "https://api.lever.co/v0/postings"

    def __init__(self, sites: Optional[List[str]] = None, concurrency: int = 20):
        super().__init__(name="Lever")
        self.sites = [s.strip() for s in (sites or []) if s and str(s).strip()]
        self.concurrency = max(1, int(concurrency or 20))

    def is_configured(self) -> bool:
        return bool(self.sites)

    async def search_jobs_with_tracking(
        self,
        query: Optional[str] = None,
        location: Optional[str] = None,
        max_results: int = 100,
        **kwargs,
    ) -> Tuple[List[TrackedJob], List[BoardResult]]:
        """
        Search jobs with per-board tracking.

        Returns:
            Tuple of (tracked_jobs_list, board_results_list)
        """
        if not self.is_configured():
            logger.warning("Lever sites not configured, skipping")
            return [], []

        params = {"mode": "json"}
        semaphore = asyncio.Semaphore(self.concurrency)

        async def fetch_site(site: str, client: httpx.AsyncClient) -> Tuple[str, List[TrackedJob], BoardResult]:
            backoff = ExponentialBackoff(base_seconds=2.0, max_seconds=300.0)
            async with semaphore:
                start_time_ms = int(time.time() * 1000)
                url = f"{self.BASE_URL}/{site}"
                error_msg: Optional[str] = None
                error_code: Optional[str] = None
                site_jobs: List[TrackedJob] = []

                # Retry logic with exponential backoff for 429 responses
                max_retries = 3
                for attempt in range(max_retries):
                    try:
                        resp = await client.get(url, params=params)

                        if resp.status_code == 429:
                            # Rate limited
                            if attempt < max_retries - 1:
                                delay = backoff.get_delay(attempt)
                                retry_after = backoff.parse_retry_after(dict(resp.headers))
                                if retry_after:
                                    delay = retry_after
                                logger.warning(
                                    f"Lever rate limited for site '{site}', "
                                    f"retrying in {delay:.1f}s (attempt {attempt + 1}/{max_retries})"
                                )
                                await asyncio.sleep(delay)
                                continue
                            else:
                                error_code = "rate_limited"
                                error_msg = "Rate limited after 3 retries"
                                logger.error(f"Lever rate limited for site '{site}' after {max_retries} retries")
                                break

                        resp.raise_for_status()
                        items = resp.json() if resp.content else []

                        for item in items:
                            job = self._parse_job(item, site)
                            if not job:
                                continue
                            if query and query.lower() not in (job.title or "").lower() and query.lower() not in (job.description or "").lower():
                                continue
                            if location and job.location and location.lower() not in job.location.lower():
                                continue
                            site_jobs.append(TrackedJob(job=job, board_token=site))

                        # Success - break retry loop
                        break

                    except httpx.HTTPStatusError as exc:
                        if exc.response.status_code == 404:
                            error_code = "not_found"
                            error_msg = f"Site not found: {site}"
                        elif exc.response.status_code >= 500:
                            error_code = "server_error"
                            error_msg = f"Server error: {exc.response.status_code}"
                        else:
                            error_code = "http_error"
                            error_msg = str(exc)
                        logger.warning(f"Lever HTTP error for site '{site}': {error_msg}")
                        break
                    except httpx.TimeoutException:
                        error_code = "timeout"
                        error_msg = "Request timeout"
                        logger.warning(f"Lever timeout for site '{site}'")
                        break
                    except Exception as exc:
                        error_code = "parse_error"
                        error_msg = str(exc)
                        logger.error(f"Lever error for site '{site}': {exc}")
                        break

                duration_ms = int(time.time() * 1000) - start_time_ms
                board_result = BoardResult(
                    source=self.name.lower(),
                    board_token=site,
                    jobs_fetched=len(site_jobs),
                    error=error_msg,
                    error_code=error_code,
                    duration_ms=duration_ms,
                )
                return site, site_jobs, board_result

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
            raw_results: List[Tuple[str, List[TrackedJob], BoardResult]] = await asyncio.gather(
                *[fetch_site(site, client) for site in self.sites]
            )

        # Sort by site name for deterministic ordering, then apply max_results
        raw_results.sort(key=lambda t: t[0])

        jobs: List[TrackedJob] = []
        board_results: List[BoardResult] = []
        for _site_name, site_jobs, board_result in raw_results:
            board_results.append(board_result)
            if len(jobs) < max_results:
                remaining = max_results - len(jobs)
                jobs.extend(site_jobs[:remaining])
                if len(jobs) >= max_results:
                    logger.info(f"Lever reached max_results ({max_results}), stopping")

        logger.info(f"Lever returned {len(jobs)} jobs from {len(self.sites)} sites")
        return jobs, board_results

    async def search_jobs(
        self,
        query: Optional[str] = None,
        location: Optional[str] = None,
        max_results: int = 100,
        **kwargs,
    ) -> List[Job]:
        """
        Search jobs (backwards compatible wrapper).

        Returns only jobs list, discarding tracking data.
        For tracking data, use search_jobs_with_tracking().
        """
        tracked_jobs, _ = await self.search_jobs_with_tracking(
            query=query,
            location=location,
            max_results=max_results,
            **kwargs,
        )
        return [tracked.job for tracked in tracked_jobs]

    def _parse_job(self, item: dict, site: str) -> Optional[Job]:
        categories = item.get("categories") or {}
        location = categories.get("location")
        employment_type = categories.get("commitment")
        team = categories.get("team")
        posted_date = None
        created_at = item.get("createdAt")
        if isinstance(created_at, (int, float)):
            try:
                posted_date = datetime.utcfromtimestamp(created_at / 1000).isoformat()
            except Exception:
                posted_date = str(created_at)

        remote = False
        if location and "remote" in location.lower():
            remote = True

        return Job(
            title=item.get("text", ""),
            company=site,
            location=location,
            url=item.get("hostedUrl") or item.get("applyUrl"),
            description=item.get("descriptionPlain") or item.get("description"),
            salary=None,
            employment_type=employment_type,
            posted_date=posted_date,
            source="Lever",
            job_id=item.get("id"),
            category=team,
            tags=item.get("tags"),
            skills=item.get("tags"),
            remote=remote,
            raw_payload=item,
        )
