"""
Findwork API integration.

Docs: https://findwork.dev/
Auth: API key required.
"""
import asyncio
import logging
import time
from typing import List, Optional
from urllib.parse import urljoin

import httpx

from . import BaseJobAPI
from ..models import Job

logger = logging.getLogger(__name__)


class FindworkAPI(BaseJobAPI):
    """Findwork job board API client."""

    DEFAULT_BASE_URL = "https://findwork.dev/api/jobs/"

    def __init__(
        self,
        api_key: Optional[str] = None,
        base_url: Optional[str] = None,
        requests_per_minute: int = 60,
    ):
        super().__init__(name="Findwork")
        self.api_key = api_key
        self.base_url = (base_url or self.DEFAULT_BASE_URL).rstrip("/") + "/"
        self.requests_per_minute = max(1, int(requests_per_minute))
        self._rate_lock = asyncio.Lock()
        self._last_request_time = 0.0

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
            logger.warning("Findwork API not configured, skipping")
            return []

        headers = {
            "Authorization": f"Token {self.api_key}",
            "X-Api-Key": self.api_key,
        }

        params = {}
        if query:
            params["search"] = query
        if location:
            params["location"] = location

        jobs: List[Job] = []
        next_url = self.base_url
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                while next_url and len(jobs) < max_results:
                    await self._wait_for_slot()
                    response = await client.get(next_url, headers=headers, params=params)
                    response.raise_for_status()

                    data = response.json() if response.content else {}
                    items = self._extract_items(data)
                    if not items:
                        break

                    for item in items:
                        job = self._parse_job(item)
                        if job:
                            jobs.append(job)
                        if len(jobs) >= max_results:
                            break

                    next_url = data.get("next")
                    if next_url:
                        next_url = urljoin(self.base_url, next_url)
                        params = None

            logger.info(f"Findwork returned {len(jobs)} jobs")
            return jobs
        except Exception as exc:
            logger.error(f"Findwork API error: {exc}")
            return jobs

    def _extract_items(self, data) -> List[dict]:
        if isinstance(data, list):
            return data
        if not isinstance(data, dict):
            return []
        return data.get("results") or data.get("jobs") or []

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

    def _parse_job(self, item: dict) -> Optional[Job]:
        title = (
            item.get("role")
            or item.get("title")
            or item.get("position")
            or item.get("job_title")
            or ""
        )
        company = (
            item.get("company_name")
            or item.get("company")
            or item.get("employer_name")
            or ""
        )
        if not title and not company:
            return None

        location = item.get("location") or item.get("candidate_required_location")
        url = item.get("url") or item.get("apply_url") or item.get("link")
        description = item.get("text") or item.get("description")
        posted = item.get("date_posted") or item.get("published_at") or item.get("publication_date")
        employment_type = item.get("employment_type") or item.get("job_type")
        job_id = item.get("id") or item.get("uuid") or item.get("slug")
        category = item.get("category")
        tags = item.get("tags") if isinstance(item.get("tags"), list) else None
        skills = item.get("skills") if isinstance(item.get("skills"), list) else None

        remote = item.get("remote")
        if remote is None and location:
            remote = "remote" in str(location).lower()

        return Job(
            title=title,
            company=company,
            location=location,
            url=url,
            description=description,
            salary=item.get("salary"),
            employment_type=employment_type,
            posted_date=posted,
            source="Findwork",
            job_id=str(job_id) if job_id is not None else None,
            category=category,
            tags=tags,
            skills=skills,
            remote=remote,
            raw_payload=item,
        )
