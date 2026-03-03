"""
The Muse API integration

The Muse provides curated job listings with company culture information.

API Docs: https://www.themuse.com/developers/api/v2
Register for API key (if required)
"""
import logging
from typing import List, Optional
import httpx
from . import BaseJobAPI
from ..models import Job

logger = logging.getLogger(__name__)


class TheMuseAPI(BaseJobAPI):
    """The Muse job board API client"""

    BASE_URL = "https://www.themuse.com/api/public/jobs"

    def __init__(self, api_key: Optional[str] = None):
        super().__init__(name="TheMuse")
        self.api_key = api_key

    def is_configured(self) -> bool:
        """The Muse API may work without key for basic access"""
        return True

    async def search_jobs(
        self,
        query: Optional[str] = None,
        location: Optional[str] = None,
        max_results: int = 100,
        **kwargs,
    ) -> List[Job]:
        """
        Search The Muse for jobs

        Args:
            query: Search query (job title, keywords)
            location: Location filter
            max_results: Maximum results to return

        Returns:
            List of Job objects
        """
        logger.info(f"Fetching jobs from The Muse (query='{query}', location='{location}')")

        jobs = []
        page = 0
        page_size = 20

        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                while len(jobs) < max_results:
                    params = {
                        "page": page,
                        "descending": "true",
                    }

                    if query:
                        params["category"] = query

                    if location:
                        params["location"] = location

                    if self.api_key:
                        params["api_key"] = self.api_key

                    response = await client.get(self.BASE_URL, params=params)
                    response.raise_for_status()

                    data = response.json()
                    results = data.get("results", [])

                    if not results:
                        break

                    for item in results:
                        job = self._parse_job(item)
                        jobs.append(job)

                        if len(jobs) >= max_results:
                            break

                    # Check pagination
                    page_count = data.get("page_count", 0)
                    if page >= page_count - 1:
                        break

                    page += 1

                logger.info(f"The Muse returned {len(jobs)} jobs")
                return jobs

        except Exception as e:
            status = getattr(getattr(e, 'response', None), 'status_code', 'N/A')
            logger.error(f"The Muse API error: type={type(e).__name__}, status={status}")
            return jobs

    def _parse_job(self, item: dict) -> Job:
        """Parse The Muse job item to Job model"""
        company = item.get("company", {})
        locations = item.get("locations", [])
        location_names = [loc.get("name") for loc in locations if loc.get("name")]

        return Job(
            title=item.get("name", ""),
            company=company.get("name", ""),
            location=", ".join(location_names) if location_names else None,
            url=item.get("refs", {}).get("landing_page"),
            description=item.get("contents"),
            salary=None,  # Not provided
            employment_type=item.get("type"),
            posted_date=item.get("publication_date"),
            source="TheMuse",
            job_id=str(item.get("id")),
            category=None,
            tags=item.get("tags", []),
            skills=None,
            remote=any("remote" in loc.get("name", "").lower() for loc in locations),
            raw_payload=item,
        )
