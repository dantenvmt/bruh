"""
Adzuna API integration

Adzuna provides access to 3+ million job listings worldwide
with generous free tier.

API Docs: https://developer.adzuna.com/
Register: https://developer.adzuna.com/signup

Requires:
- app_id
- app_key
"""
import logging
from typing import List, Optional
import httpx
from . import BaseJobAPI
from ..models import Job

logger = logging.getLogger(__name__)


class AdzunaAPI(BaseJobAPI):
    """Adzuna job board API client"""

    BASE_URL = "https://api.adzuna.com/v1/api/jobs"
    SUPPORTED_COUNTRIES = ["us", "gb", "au", "ca", "de", "fr", "nl", "br", "in", "sg"]

    def __init__(self, app_id: Optional[str] = None, app_key: Optional[str] = None):
        super().__init__(name="Adzuna")
        self.app_id = app_id
        self.app_key = app_key

    def is_configured(self) -> bool:
        """Check if API credentials are set"""
        return bool(self.app_id and self.app_key)

    async def search_jobs(
        self,
        query: Optional[str] = None,
        location: Optional[str] = None,
        max_results: int = 100,
        country: str = "us",
        **kwargs,
    ) -> List[Job]:
        """
        Search Adzuna for jobs

        Args:
            query: Search query (job title, keywords)
            location: Location filter
            max_results: Maximum results to return
            country: Country code (us, gb, au, etc.)

        Returns:
            List of Job objects
        """
        if not self.is_configured():
            logger.warning("Adzuna API not configured, skipping")
            return []

        if country not in self.SUPPORTED_COUNTRIES:
            logger.warning(f"Country '{country}' not supported, using 'us'")
            country = "us"

        logger.info(
            f"Fetching jobs from Adzuna (query='{query}', location='{location}', country='{country}')"
        )

        jobs = []
        page = 1
        results_per_page = 50

        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                while len(jobs) < max_results:
                    url = f"{self.BASE_URL}/{country}/search/{page}"

                    params = {
                        "app_id": self.app_id,
                        "app_key": self.app_key,
                        "results_per_page": min(results_per_page, max_results - len(jobs)),
                        "what": query or "",
                        "where": location or "",
                    }

                    response = await client.get(url, params=params)
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

                    # Check if there are more pages
                    total_results = data.get("count", 0)
                    if len(jobs) >= total_results:
                        break

                    page += 1

                logger.info(f"Adzuna returned {len(jobs)} jobs")
                return jobs

        except Exception as e:
            status = getattr(getattr(e, 'response', None), 'status_code', 'N/A')
            logger.error(f"Adzuna API error: type={type(e).__name__}, status={status}")
            return jobs  # Return what we got so far

    def _parse_job(self, item: dict) -> Job:
        """Parse Adzuna job item to Job model"""
        return Job(
            title=item.get("title", ""),
            company=item.get("company", {}).get("display_name", ""),
            location=item.get("location", {}).get("display_name"),
            url=item.get("redirect_url"),
            description=item.get("description"),
            salary=self._parse_salary(item),
            employment_type=item.get("contract_time"),
            posted_date=item.get("created"),
            source="Adzuna",
            job_id=str(item.get("id")),
            category=item.get("category", {}).get("label"),
            tags=None,
            skills=None,
            remote=None,  # Not explicitly provided
            raw_payload=item,
        )

    def _parse_salary(self, item: dict) -> Optional[str]:
        """Parse salary information"""
        salary_min = item.get("salary_min")
        salary_max = item.get("salary_max")

        if salary_min and salary_max:
            return f"${salary_min:,.0f} - ${salary_max:,.0f}"
        elif salary_min:
            return f"${salary_min:,.0f}+"
        elif salary_max:
            return f"Up to ${salary_max:,.0f}"

        return None
