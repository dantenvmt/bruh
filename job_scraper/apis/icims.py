"""
iCIMS public job search integration.

iCIMS career portals expose a JSON search endpoint:
  https://{company}.icims.com/jobs/search?pr=0&schemaVersion=&o=

The portal renders via JavaScript but the underlying API returns JSON.
Pagination is offset-based (?pr=N where N = page number starting at 0).
"""
from __future__ import annotations

import asyncio
import logging
import re
import time
from typing import List, Optional, Tuple
from urllib.parse import urlparse

import httpx

from . import BaseJobAPI, BoardResult, TrackedJob
from ..models import Job
from ..utils import ExponentialBackoff

logger = logging.getLogger(__name__)

# Extract the iCIMS portal hostname from a careers URL
_ICIMS_HOST_RE = re.compile(
    r"(https?://[^/]*\.icims\.com)",
    re.IGNORECASE,
)


class ICIMSApi(BaseJobAPI):
    """iCIMS public job portal API client."""

    def __init__(
        self,
        portals: Optional[List[str]] = None,
        requests_per_minute: int = 30,
    ):
        """
        Args:
            portals: List of iCIMS portal base URLs, e.g.
                     ["https://careers-acme.icims.com"]
        """
        super().__init__(name="iCIMS")
        self.portals = [p.rstrip("/") for p in (portals or []) if p and p.strip()]
        self.requests_per_minute = max(1, int(requests_per_minute or 30))
        self._rate_lock = asyncio.Lock()
        self._last_request_time = 0.0

    def is_configured(self) -> bool:
        return bool(self.portals)

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
            logger.warning("iCIMS portals not configured, skipping")
            return [], []

        all_jobs: List[TrackedJob] = []
        board_results: List[BoardResult] = []
        backoff = ExponentialBackoff(base_seconds=2.0, max_seconds=60.0)

        async with httpx.AsyncClient(
            timeout=30.0,
            follow_redirects=True,
            headers={
                "Accept": "application/json",
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                ),
            },
        ) as client:
            for portal_base in self.portals:
                started = time.monotonic()
                portal_jobs: List[TrackedJob] = []
                error_msg: Optional[str] = None
                error_code: Optional[str] = None
                board_token = urlparse(portal_base).netloc

                try:
                    page = 0
                    page_size = 20

                    while len(all_jobs) + len(portal_jobs) < max_results:
                        await self._wait_for_slot()
                        search_url = f"{portal_base}/jobs/search"
                        params = {
                            "pr": page,
                            "o": "",
                            "schemaVersion": "",
                        }
                        if query:
                            params["ss"] = query
                        if location:
                            params["sl"] = location

                        resp = None
                        for attempt in range(3):
                            try:
                                resp = await client.get(search_url, params=params)
                                if resp.status_code == 429 and attempt < 2:
                                    await asyncio.sleep(backoff.get_delay(attempt))
                                    continue
                                break
                            except httpx.TimeoutException:
                                if attempt < 2:
                                    await asyncio.sleep(backoff.get_delay(attempt))
                                    continue
                                raise

                        if resp is None or resp.status_code >= 400:
                            status = resp.status_code if resp else 0
                            if status == 404:
                                error_code = "not_found"
                                error_msg = "Portal not found"
                            else:
                                error_code = "http_error"
                                error_msg = f"HTTP {status}"
                            break

                        # iCIMS can return HTML or JSON depending on headers
                        ct = resp.headers.get("content-type", "")
                        if "json" not in ct:
                            # Try to extract job data from HTML
                            jobs_from_html = self._extract_from_html(resp.text, portal_base)
                            for job in jobs_from_html:
                                portal_jobs.append(TrackedJob(job=job, board_token=board_token))
                            break

                        data = resp.json() if resp.content else {}
                        items = data.get("jobs") or data.get("jobPostings") or data.get("items") or []
                        total = data.get("totalCount") or data.get("total") or 0

                        if not items:
                            break

                        for item in items:
                            job = self._parse_job(item, portal_base)
                            if not job:
                                continue
                            portal_jobs.append(TrackedJob(job=job, board_token=board_token))

                        page += 1
                        if page * page_size >= total:
                            break

                except httpx.TimeoutException:
                    error_code = "timeout"
                    error_msg = "Request timeout"
                except Exception as exc:
                    error_code = "unknown_error"
                    error_msg = str(exc)
                    logger.error("iCIMS error for %s: %s", portal_base, exc)

                duration_ms = int((time.monotonic() - started) * 1000)
                board_results.append(
                    BoardResult(
                        source=self.name.lower(),
                        board_token=board_token,
                        jobs_fetched=len(portal_jobs),
                        error=error_msg,
                        error_code=error_code,
                        duration_ms=duration_ms,
                    )
                )
                all_jobs.extend(portal_jobs)
                if len(all_jobs) >= max_results:
                    break

        logger.info("iCIMS returned %d jobs from %d portals", len(all_jobs), len(board_results))
        return all_jobs, board_results

    def _parse_job(self, item: dict, portal_base: str) -> Optional[Job]:
        if not isinstance(item, dict):
            return None

        title = item.get("title") or item.get("name") or ""
        if not title:
            return None

        job_id = item.get("id") or item.get("jobId")
        url = item.get("url") or item.get("applyUrl")
        if not url and job_id:
            url = f"{portal_base}/jobs/{job_id}/job"

        location = item.get("location") or item.get("city")
        if isinstance(location, dict):
            parts = [location.get("city"), location.get("state"), location.get("country")]
            location = ", ".join(p for p in parts if p)

        remote = False
        if isinstance(location, str) and "remote" in location.lower():
            remote = True

        return Job(
            title=str(title),
            company=urlparse(portal_base).netloc.split(".")[0].replace("careers-", ""),
            location=str(location) if location else None,
            url=url,
            description=item.get("description"),
            salary=None,
            employment_type=item.get("type") or item.get("employmentType"),
            posted_date=item.get("postedDate") or item.get("datePosted"),
            source="iCIMS",
            job_id=str(job_id) if job_id else None,
            category=item.get("category") or item.get("department"),
            tags=None,
            skills=None,
            remote=remote,
            raw_payload=item,
        )

    def _extract_from_html(self, html: str, portal_base: str) -> List[Job]:
        """Fallback: extract job links from iCIMS HTML when JSON is not available."""
        from bs4 import BeautifulSoup

        soup = BeautifulSoup(html, "html.parser")
        jobs: List[Job] = []

        for a_tag in soup.find_all("a", href=True):
            href = a_tag.get("href", "")
            if "/jobs/" not in href or "/job" not in href:
                continue
            title = a_tag.get_text(strip=True)
            if not title or len(title) < 3:
                continue

            from urllib.parse import urljoin
            url = urljoin(portal_base, href)

            job_id_match = re.search(r"/jobs/(\d+)/", href)
            job_id = job_id_match.group(1) if job_id_match else None

            jobs.append(Job(
                title=title,
                company=urlparse(portal_base).netloc.split(".")[0].replace("careers-", ""),
                location=None,
                url=url,
                source="iCIMS",
                job_id=job_id,
            ))

        return jobs

    async def _wait_for_slot(self) -> None:
        min_interval = 60.0 / self.requests_per_minute
        async with self._rate_lock:
            now = time.monotonic()
            elapsed = now - self._last_request_time
            wait_seconds = max(0.0, min_interval - elapsed)
            if wait_seconds > 0:
                await asyncio.sleep(wait_seconds)
            self._last_request_time = time.monotonic()
