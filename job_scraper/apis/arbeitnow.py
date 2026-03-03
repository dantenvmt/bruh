"""
Arbeitnow API integration

Arbeitnow provides free job listings focused on Europe and Remote positions.

API Docs: https://documenter.getpostman.com/view/18545278/UVJbJdKh
Also available on RapidAPI

May require API key depending on access method.
"""
import logging
from typing import List, Optional
import httpx
from . import BaseJobAPI
from ..models import Job

logger = logging.getLogger(__name__)


class ArbeitnowAPI(BaseJobAPI):
    """Arbeitnow job board API client"""

    BASE_URL = "https://www.arbeitnow.com/api/job-board-api"

    def __init__(self, api_key: Optional[str] = None):
        super().__init__(name="Arbeitnow")
        self.api_key = api_key

    def is_configured(self) -> bool:
        """Arbeitnow may work without API key"""
        return True  # Works without key for basic access

    async def search_jobs(
        self,
        query: Optional[str] = None,
        location: Optional[str] = None,
        max_results: int = 100,
        **kwargs,
    ) -> List[Job]:
        """
        Search Arbeitnow for jobs (Europe/Remote focus)

        Args:
            query: Search query (job title, keywords)
            location: Location filter
            max_results: Maximum results to return

        Returns:
            List of Job objects
        """
        logger.info(f"Fetching jobs from Arbeitnow (query='{query}', location='{location}')")

        jobs = []
        page = 1

        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                while len(jobs) < max_results:
                    params = {}
                    if query:
                        params["search"] = query
                    if page > 1:
                        params["page"] = page

                    headers = {}
                    if self.api_key:
                        headers["Authorization"] = f"Bearer {self.api_key}"

                    response = await client.get(self.BASE_URL, params=params, headers=headers)
                    response.raise_for_status()

                    data = response.json()
                    results = data.get("data", [])

                    if not results:
                        break

                    for item in results:
                        # Filter by location if provided
                        if location:
                            job_location = item.get("location", "").lower()
                            if location.lower() not in job_location:
                                continue

                        job = self._parse_job(item)
                        jobs.append(job)

                        if len(jobs) >= max_results:
                            break

                    # Check if there are more pages
                    links = data.get("links", {})
                    if not links.get("next"):
                        break

                    page += 1

                logger.info(f"Arbeitnow returned {len(jobs)} jobs")
                return jobs

        except Exception as e:
            logger.error(f"Arbeitnow API error: {e}")
            return jobs

    def _parse_job(self, item: dict) -> Job:
        """Parse Arbeitnow job item to Job model"""
        return Job(
            title=item.get("title", ""),
            company=item.get("company_name", ""),
            location=item.get("location"),
            url=item.get("url"),
            description=item.get("description"),
            salary=None,  # Not provided
            employment_type=item.get("job_types", [None])[0] if item.get("job_types") else None,
            posted_date=item.get("created_at"),
            source="Arbeitnow",
            job_id=item.get("slug"),
            category=None,
            tags=item.get("tags", []),
            skills=None,
            remote=item.get("remote", False),
            raw_payload=item,
        )
