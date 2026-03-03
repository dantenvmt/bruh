"""
Greenhouse Job Board API integration.
"""
import asyncio
import logging
import time
from typing import List, Optional, Tuple

import httpx

from . import BaseJobAPI, BoardResult, TrackedJob
from ..models import Job
from ..utils import ExponentialBackoff

logger = logging.getLogger(__name__)


class GreenhouseAPI(BaseJobAPI):
    """Greenhouse job board API client"""

    BASE_URL = "https://boards-api.greenhouse.io/v1/boards"

    def __init__(self, boards: Optional[List[str]] = None, include_content: bool = True):
        super().__init__(name="Greenhouse")
        self.boards = [b.strip() for b in (boards or []) if b and str(b).strip()]
        self.include_content = include_content

    def is_configured(self) -> bool:
        return bool(self.boards)

    async def search_jobs_with_tracking(
        self,
        query: Optional[str] = None,
        location: Optional[str] = None,
        max_results: int = 100,
        **kwargs,
    ) -> Tuple[List[TrackedJob], List[BoardResult]]:
        """
        Search for jobs across all configured boards with per-board tracking.

        Returns:
            Tuple of (tracked_jobs, board_results) where board_results contains metadata
            for each board including errors, duration, and job counts.
        """
        if not self.is_configured():
            logger.warning("Greenhouse boards not configured, skipping")
            return [], []

        jobs: List[TrackedJob] = []
        board_results: List[BoardResult] = []
        params = {"content": "true"} if self.include_content else None
        backoff = ExponentialBackoff(base_seconds=2.0, max_seconds=60.0)

        async with httpx.AsyncClient(timeout=30.0) as client:
            for board in self.boards:
                start_time = time.time()
                board_jobs: List[TrackedJob] = []
                error_msg: Optional[str] = None
                error_code: Optional[str] = None

                # Retry logic for 429 rate limiting
                for attempt in range(3):
                    try:
                        url = f"{self.BASE_URL}/{board}/jobs"
                        resp = await client.get(url, params=params)

                        # Handle 429 rate limiting
                        if resp.status_code == 429:
                            if attempt < 2:  # Retry up to 3 times total
                                delay = backoff.parse_retry_after(dict(resp.headers)) or backoff.get_delay(attempt)
                                logger.warning(f"Rate limited on board '{board}', retrying in {delay:.1f}s (attempt {attempt + 1}/3)")
                                await asyncio.sleep(delay)
                                continue
                            else:
                                error_code = "rate_limited"
                                error_msg = f"Rate limited after {attempt + 1} attempts"
                                logger.error(f"Board '{board}': {error_msg}")
                                break

                        # Handle other error status codes
                        if resp.status_code == 404:
                            error_code = "not_found"
                            error_msg = f"Board not found (404)"
                            logger.warning(f"Board '{board}': {error_msg}")
                            break
                        elif resp.status_code >= 500:
                            error_code = "server_error"
                            error_msg = f"Server error ({resp.status_code})"
                            logger.error(f"Board '{board}': {error_msg}")
                            break
                        elif resp.status_code != 200:
                            error_code = "http_error"
                            error_msg = f"HTTP {resp.status_code}"
                            logger.error(f"Board '{board}': {error_msg}")
                            break

                        # Parse response
                        try:
                            data = resp.json() if resp.content else {}
                            items = data.get("jobs") or []

                            for item in items:
                                job = self._parse_job(item, board)
                                if not job:
                                    continue

                                # Apply filters
                                if query:
                                    query_lower = query.lower()
                                    if query_lower not in (job.title or "").lower() and query_lower not in (job.description or "").lower():
                                        continue
                                if location and job.location and location.lower() not in job.location.lower():
                                    continue

                                board_jobs.append(TrackedJob(job=job, board_token=board))

                                # Check max_results across all boards
                                if len(jobs) + len(board_jobs) >= max_results:
                                    break

                        except Exception as e:
                            error_code = "parse_error"
                            error_msg = f"Failed to parse response: {str(e)}"
                            logger.error(f"Board '{board}': {error_msg}")
                            break

                        # Success - break retry loop
                        break

                    except httpx.TimeoutException:
                        error_code = "timeout"
                        error_msg = "Request timeout"
                        logger.error(f"Board '{board}': {error_msg}")
                        break
                    except Exception as e:
                        error_code = "unknown_error"
                        error_msg = f"Unexpected error: {str(e)}"
                        logger.error(f"Board '{board}': {error_msg}")
                        break

                # Calculate duration
                duration_ms = int((time.time() - start_time) * 1000)

                # Create BoardResult
                board_result = BoardResult(
                    source=self.name.lower(),
                    board_token=board,
                    jobs_fetched=len(board_jobs),
                    error=error_msg,
                    error_code=error_code,
                    duration_ms=duration_ms,
                )
                board_results.append(board_result)

                # Add successful jobs to total
                jobs.extend(board_jobs)

                # Log result
                if error_msg:
                    logger.info(f"Board '{board}': {error_code} - {error_msg} (took {duration_ms}ms)")
                else:
                    logger.info(f"Board '{board}': fetched {len(board_jobs)} jobs (took {duration_ms}ms)")

                # Check if we've hit max_results
                if len(jobs) >= max_results:
                    logger.info(f"Reached max_results ({max_results}), stopping")
                    break

        logger.info(f"Greenhouse returned {len(jobs)} jobs from {len(self.boards)} boards")
        return jobs, board_results

    async def search_jobs(
        self,
        query: Optional[str] = None,
        location: Optional[str] = None,
        max_results: int = 100,
        **kwargs,
    ) -> List[Job]:
        """
        Search for jobs across all configured boards (backwards compatible wrapper).

        This method maintains backwards compatibility by calling search_jobs_with_tracking()
        and returning only the jobs list.

        Returns:
            List of Job objects
        """
        tracked_jobs, _ = await self.search_jobs_with_tracking(
            query=query,
            location=location,
            max_results=max_results,
            **kwargs,
        )
        return [tracked.job for tracked in tracked_jobs]

    def _parse_job(self, item: dict, board: str) -> Optional[Job]:
        location = None
        loc = item.get("location") or {}
        if isinstance(loc, dict):
            location = loc.get("name")
        description = item.get("content") or item.get("internal_content")
        category = None
        dept = item.get("departments")
        if isinstance(dept, list) and dept:
            first = dept[0]
            if isinstance(first, dict):
                category = first.get("name")

        remote = False
        if location and "remote" in location.lower():
            remote = True

        return Job(
            title=item.get("title", ""),
            company=board,
            location=location,
            url=item.get("absolute_url"),
            description=description,
            salary=None,
            employment_type=None,
            posted_date=item.get("updated_at") or item.get("created_at"),
            source="Greenhouse",
            job_id=str(item.get("id")) if item.get("id") is not None else None,
            category=category,
            tags=None,
            skills=None,
            remote=remote,
            raw_payload=item,
        )
