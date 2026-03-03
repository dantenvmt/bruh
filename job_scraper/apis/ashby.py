"""
Ashby job board integration.

Uses Ashby's public non-user GraphQL endpoint:
  https://jobs.ashbyhq.com/api/non-user-graphql
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


class AshbyAPI(BaseJobAPI):
    """Ashby hosted jobs API client."""

    BASE_URL = "https://jobs.ashbyhq.com/api/non-user-graphql"

    LIST_QUERY = """
query ApiJobBoardWithTeams($organizationHostedJobsPageName: String!) {
  jobBoard: jobBoardWithTeams(organizationHostedJobsPageName: $organizationHostedJobsPageName) {
    jobPostings {
      id
      title
      locationName
      locationAddress
      workplaceType
      employmentType
      secondaryLocations {
        locationName
      }
      compensationTierSummary
    }
  }
}
""".strip()

    DETAIL_QUERY = """
query ApiJobPosting($organizationHostedJobsPageName: String!, $jobPostingId: String!) {
  jobPosting(
    organizationHostedJobsPageName: $organizationHostedJobsPageName
    jobPostingId: $jobPostingId
  ) {
    publishedDate
    descriptionHtml
    scrapeableCompensationSalarySummary
  }
}
""".strip()

    def __init__(
        self,
        companies: Optional[List[str]] = None,
        include_content: bool = False,
        requests_per_minute: int = 60,
    ):
        super().__init__(name="Ashby")
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
        tracked_jobs, _ = await self.search_jobs_with_tracking(
            query=query,
            location=location,
            max_results=max_results,
            **kwargs,
        )
        return [tracked.job for tracked in tracked_jobs]

    async def search_jobs_with_tracking(
        self,
        query: Optional[str] = None,
        location: Optional[str] = None,
        max_results: int = 100,
        **kwargs,
    ) -> Tuple[List[TrackedJob], List[BoardResult]]:
        """
        Search jobs across configured Ashby companies with per-company tracking.
        """
        if not self.is_configured():
            logger.warning("Ashby companies not configured, skipping")
            return [], []

        needle_query = (query or "").strip().lower()
        needle_location = (location or "").strip().lower()

        all_jobs: List[TrackedJob] = []
        board_results: List[BoardResult] = []

        async with httpx.AsyncClient(timeout=30.0) as client:
            for company in self.companies:
                started = time.monotonic()
                jobs_fetched_raw = 0
                board_jobs: List[TrackedJob] = []
                error_message: Optional[str] = None
                error_code: Optional[str] = None

                try:
                    postings = await self._fetch_job_board(client, company)
                    jobs_fetched_raw = len(postings)

                    for posting in postings:
                        job = await self._parse_job(client, company, posting)
                        if not job:
                            continue
                        if needle_query and not self._matches_query(job, needle_query):
                            continue
                        if needle_location and job.location and needle_location not in job.location.lower():
                            continue

                        board_jobs.append(TrackedJob(job=job, board_token=company))
                        all_jobs.append(board_jobs[-1])
                        if len(all_jobs) >= max_results:
                            break

                except httpx.TimeoutException:
                    error_code = "timeout"
                    error_message = "Request timeout"
                    logger.warning(f"Ashby timeout for '{company}'")
                except httpx.HTTPStatusError as exc:
                    if exc.response.status_code == 404:
                        error_code = "not_found"
                    elif exc.response.status_code == 429:
                        error_code = "rate_limited"
                    elif exc.response.status_code >= 500:
                        error_code = "server_error"
                    else:
                        error_code = "http_error"
                    error_message = f"HTTP {exc.response.status_code}"
                    logger.warning(f"Ashby HTTP error for '{company}': {error_message}")
                except ValueError as exc:
                    error_code = "parse_error"
                    error_message = str(exc)
                    logger.warning(f"Ashby parse error for '{company}': {exc}")
                except Exception as exc:
                    error_code = "unknown_error"
                    error_message = str(exc)
                    logger.error(f"Ashby unexpected error for '{company}': {exc}")

                board_results.append(
                    BoardResult(
                        source=self.name.lower(),
                        board_token=company,
                        jobs_fetched=jobs_fetched_raw,
                        error=error_message,
                        error_code=error_code,
                        duration_ms=int((time.monotonic() - started) * 1000),
                    )
                )

                if len(all_jobs) >= max_results:
                    break

        logger.info(f"Ashby returned {len(all_jobs)} jobs from {len(board_results)} companies")
        return all_jobs, board_results

    async def _fetch_job_board(self, client: httpx.AsyncClient, company: str) -> List[dict]:
        payload = {
            "operationName": "ApiJobBoardWithTeams",
            "variables": {"organizationHostedJobsPageName": company},
            "query": self.LIST_QUERY,
        }
        response = await self._post_graphql_with_backoff(client, payload)
        data = response.json() if response.content else {}
        if data.get("errors"):
            raise ValueError(str(data["errors"][0].get("message", "GraphQL error")))
        job_board = (data.get("data") or {}).get("jobBoard") or {}
        postings = job_board.get("jobPostings") or []
        return postings if isinstance(postings, list) else []

    async def _fetch_job_detail(
        self,
        client: httpx.AsyncClient,
        company: str,
        job_posting_id: str,
    ) -> Optional[dict]:
        payload = {
            "operationName": "ApiJobPosting",
            "variables": {
                "organizationHostedJobsPageName": company,
                "jobPostingId": job_posting_id,
            },
            "query": self.DETAIL_QUERY,
        }
        response = await self._post_graphql_with_backoff(client, payload)
        data = response.json() if response.content else {}
        if data.get("errors"):
            return None
        posting = (data.get("data") or {}).get("jobPosting")
        return posting if isinstance(posting, dict) else None

    async def _post_graphql_with_backoff(
        self,
        client: httpx.AsyncClient,
        payload: dict,
        max_retries: int = 3,
    ) -> httpx.Response:
        backoff = ExponentialBackoff(base_seconds=2.0, max_seconds=45.0)

        for attempt in range(max_retries):
            await self._wait_for_slot()
            response = await client.post(self.BASE_URL, json=payload)

            if response.status_code == 429:
                if attempt < max_retries - 1:
                    retry_after = backoff.parse_retry_after(dict(response.headers))
                    await asyncio.sleep(retry_after or backoff.get_delay(attempt))
                    continue
                response.raise_for_status()

            if response.status_code >= 500 and attempt < max_retries - 1:
                await asyncio.sleep(backoff.get_delay(attempt))
                continue

            response.raise_for_status()
            return response

        raise RuntimeError("Ashby request retries exhausted")

    async def _parse_job(self, client: httpx.AsyncClient, company: str, posting: dict) -> Optional[Job]:
        if not isinstance(posting, dict):
            return None

        job_id = str(posting.get("id") or "").strip()
        title = str(posting.get("title") or "").strip()
        if not job_id or not title:
            return None

        location = posting.get("locationName") or posting.get("locationAddress")
        secondary_locations = posting.get("secondaryLocations") or []
        if not location and secondary_locations:
            first = secondary_locations[0]
            if isinstance(first, dict):
                location = first.get("locationName")

        workplace_type = str(posting.get("workplaceType") or "")
        remote = "remote" in workplace_type.lower()
        if not remote and workplace_type.lower() == "hybrid":
            remote = True

        salary = self._parse_compensation(posting.get("compensationTierSummary"))
        description = None
        posted_date = None

        if self.include_content:
            detail = await self._fetch_job_detail(client, company, job_id)
            if detail:
                description = detail.get("descriptionHtml")
                posted_date = detail.get("publishedDate")
                salary = detail.get("scrapeableCompensationSalarySummary") or salary

        return Job(
            title=title,
            company=company,
            location=str(location) if location else None,
            url=f"https://jobs.ashbyhq.com/{company}/{job_id}",
            description=description,
            salary=salary,
            employment_type=posting.get("employmentType"),
            posted_date=str(posted_date) if posted_date else None,
            source="Ashby",
            job_id=job_id,
            category=None,
            tags=None,
            skills=None,
            remote=remote,
            raw_payload=posting,
        )

    def _matches_query(self, job: Job, needle: str) -> bool:
        haystack = " ".join(
            [
                job.title or "",
                job.company or "",
                job.location or "",
                job.description or "",
            ]
        ).lower()
        return needle in haystack

    def _parse_compensation(self, value: object) -> Optional[str]:
        if value is None:
            return None
        if isinstance(value, str):
            return value
        if isinstance(value, dict):
            for key in ("summary", "displayText", "text"):
                raw = value.get(key)
                if isinstance(raw, str) and raw.strip():
                    return raw.strip()
            return str(value)
        return str(value)

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
