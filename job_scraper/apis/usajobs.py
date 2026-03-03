"""
USAJobs API integration

USAJobs provides access to all U.S. federal government job listings.

API Docs: https://developer.usajobs.gov/
Apply for API key: https://developer.usajobs.gov/apirequest/

Requires:
- API key (Authorization-Key header)
- User-Agent (email address)
"""
import logging
from typing import List, Optional
import httpx
from . import BaseJobAPI
from ..models import Job

logger = logging.getLogger(__name__)


class USAJobsAPI(BaseJobAPI):
    """USAJobs government job board API client"""

    BASE_URL = "https://data.usajobs.gov/api/search"

    def __init__(self, api_key: Optional[str] = None, user_agent: Optional[str] = None):
        super().__init__(name="USAJobs")
        self.api_key = api_key
        self.user_agent = user_agent or "your-email@example.com"

    def is_configured(self) -> bool:
        """Check if API credentials are set"""
        return bool(self.api_key)

    async def search_jobs(
        self,
        query: Optional[str] = None,
        location: Optional[str] = None,
        max_results: int = 100,
        **kwargs,
    ) -> List[Job]:
        """
        Search USAJobs for federal government jobs

        Args:
            query: Search query (job title, keywords)
            location: Location filter
            max_results: Maximum results to return

        Returns:
            List of Job objects
        """
        if not self.is_configured():
            logger.warning("USAJobs API not configured, skipping")
            return []

        logger.info(f"Fetching jobs from USAJobs (query='{query}', location='{location}')")

        jobs = []
        page = 1
        results_per_page = 500  # USAJobs max

        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                while len(jobs) < max_results:
                    params = {
                        "Keyword": query or "",
                        "LocationName": location or "",
                        "ResultsPerPage": min(results_per_page, max_results - len(jobs)),
                        "Page": page,
                    }

                    headers = {
                        "Authorization-Key": self.api_key,
                        "User-Agent": self.user_agent,
                    }

                    response = await client.get(self.BASE_URL, params=params, headers=headers)
                    response.raise_for_status()

                    data = response.json()
                    results = data.get("SearchResult", {}).get("SearchResultItems", [])

                    if not results:
                        break

                    for item in results:
                        job_data = item.get("MatchedObjectDescriptor", {})
                        job = self._parse_job(job_data)
                        jobs.append(job)

                        if len(jobs) >= max_results:
                            break

                    # Check if there are more pages
                    total_results = data.get("SearchResult", {}).get("SearchResultCount", 0)
                    if len(jobs) >= total_results:
                        break

                    page += 1

                logger.info(f"USAJobs returned {len(jobs)} jobs")
                return jobs

        except Exception as e:
            logger.error(f"USAJobs API error: {e}")
            return jobs

    def _parse_job(self, item: dict) -> Job:
        """Parse USAJobs item to Job model"""
        return Job(
            title=item.get("PositionTitle", ""),
            company=item.get("OrganizationName", "U.S. Government"),
            location=self._parse_location(item.get("PositionLocationDisplay")),
            url=item.get("PositionURI"),
            description=item.get("UserArea", {}).get("Details", {}).get("JobSummary"),
            salary=self._parse_salary(item),
            employment_type=item.get("PositionSchedule", [{}])[0].get("Name"),
            posted_date=item.get("PublicationStartDate"),
            source="USAJobs",
            job_id=item.get("PositionID"),
            category=item.get("JobCategory", [{}])[0].get("Name"),
            tags=None,
            skills=None,
            remote=item.get("PositionRemoteIndicator", False),
            raw_payload=item,
        )

    def _parse_location(self, location_display: Optional[str]) -> Optional[str]:
        """Parse location string"""
        if not location_display:
            return None
        # USAJobs returns comma-separated locations
        locations = location_display.split(";")
        return locations[0].strip() if locations else location_display

    def _parse_salary(self, item: dict) -> Optional[str]:
        """Parse salary information"""
        salary_min = item.get("PositionRemuneration", [{}])[0].get("MinimumRange")
        salary_max = item.get("PositionRemuneration", [{}])[0].get("MaximumRange")

        try:
            if salary_min and salary_max:
                return f"${float(salary_min):,.0f} - ${float(salary_max):,.0f}"
            elif salary_min:
                return f"${float(salary_min):,.0f}+"
        except (TypeError, ValueError) as e:
            logger.warning(f"Failed to parse salary values (min={salary_min}, max={salary_max}): {e}")
            return None

        return None
