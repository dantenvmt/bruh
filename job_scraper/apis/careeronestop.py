"""
CareerOneStop API integration (US Department of Labor).

Docs: https://api.careeronestop.org/
"""
import logging
from typing import List, Optional
from urllib.parse import quote

import httpx

from . import BaseJobAPI
from ..models import Job

logger = logging.getLogger(__name__)


class CareerOneStopAPI(BaseJobAPI):
    """CareerOneStop job search API client"""

    BASE_URL = "https://api.careeronestop.org/v1/jobsearch"

    def __init__(self, api_key: Optional[str] = None, user_id: Optional[str] = None):
        super().__init__(name="CareerOneStop")
        self.api_key = api_key
        self.user_id = user_id

    def is_configured(self) -> bool:
        return bool(self.api_key and self.user_id)

    async def search_jobs(
        self,
        query: Optional[str] = None,
        location: Optional[str] = None,
        max_results: int = 100,
        **kwargs,
    ) -> List[Job]:
        if not self.is_configured():
            logger.warning("CareerOneStop API not configured, skipping")
            return []

        keyword = query or "software"
        loc = location or "US"
        radius = int(kwargs.get("radius", 25))

        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Accept": "application/json",
        }

        jobs: List[Job] = []
        page = 1

        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                while len(jobs) < max_results:
                    url = (
                        f"{self.BASE_URL}/"
                        f"{quote(str(self.user_id))}/"
                        f"{quote(str(keyword))}/"
                        f"{quote(str(loc))}/"
                        f"{radius}/"
                        f"{page}"
                    )
                    resp = await client.get(url, headers=headers)
                    resp.raise_for_status()

                    data = resp.json() if resp.content else {}
                    items = data.get("Jobs") or data.get("jobs") or []
                    if not items:
                        break

                    for item in items:
                        job = self._parse_job(item)
                        if job:
                            jobs.append(job)
                        if len(jobs) >= max_results:
                            break

                    total_pages = (
                        data.get("TotalPages")
                        or data.get("total_pages")
                        or data.get("Pages")
                        or data.get("TotalPagesAvailable")
                    )
                    if total_pages and page >= int(total_pages):
                        break
                    page += 1

            logger.info(f"CareerOneStop returned {len(jobs)} jobs")
            return jobs
        except Exception as exc:
            logger.error(f"CareerOneStop API error: {exc}")
            return jobs

    def _parse_job(self, item: dict) -> Optional[Job]:
        title = item.get("JobTitle") or item.get("Title") or ""
        company = item.get("Company") or item.get("CompanyName") or ""
        location = item.get("Location") or item.get("JobLocation")
        url = item.get("URL") or item.get("ApplyURL") or item.get("JobURL")
        description = item.get("JobDescription") or item.get("Snippet") or item.get("JobSummary")
        posted_date = item.get("PostedDate") or item.get("DatePosted") or item.get("AcquisitionDate")
        job_id = item.get("JobId") or item.get("JobID") or item.get("id")
        employment_type = item.get("EmploymentType") or item.get("JobType")

        return Job(
            title=title,
            company=company,
            location=location,
            url=url,
            description=description,
            salary=None,
            employment_type=employment_type,
            posted_date=posted_date,
            source="CareerOneStop",
            job_id=str(job_id) if job_id is not None else None,
            category=item.get("JobCategory"),
            tags=None,
            skills=None,
            remote=None,
            raw_payload=item,
        )
