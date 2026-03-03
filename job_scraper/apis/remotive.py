"""
Remotive API integration

Remotive is a remote job board with curated remote positions across various categories.

API: https://remotive.com/api/remote-jobs
- No authentication required
- Free access
- Supports category filtering
- Returns JSON with job listings
"""
import logging
from typing import List, Optional
import httpx
from . import BaseJobAPI
from ..models import Job
from ..utils import is_us_job

logger = logging.getLogger(__name__)


class RemotiveAPI(BaseJobAPI):
    """Remotive job board API client"""

    BASE_URL = "https://remotive.com/api/remote-jobs"

    def __init__(self):
        super().__init__(name="Remotive")

    def is_configured(self) -> bool:
        """Remotive doesn't require configuration"""
        return True

    async def search_jobs(
        self,
        query: Optional[str] = None,
        location: Optional[str] = None,
        max_results: int = 100,
        **kwargs,
    ) -> List[Job]:
        """
        Search Remotive for jobs

        Args:
            query: Search query (used for category filtering if applicable)
            location: Location filter (filtered client-side for US jobs)
            max_results: Maximum results to return

        Returns:
            List of Job objects
        """
        logger.info(f"Fetching jobs from Remotive (query='{query}', location='{location}')")

        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                params = {}

                # Remotive supports category parameter
                if query:
                    # Map common queries to Remotive categories
                    # Categories: software-dev, customer-service, design, marketing, sales, etc.
                    params["category"] = query.lower()

                response = await client.get(self.BASE_URL, params=params)
                response.raise_for_status()

                data = response.json()

                # Jobs are in the 'jobs' array
                jobs_data = data.get("jobs", [])

                jobs = []
                for item in jobs_data:
                    # Client-side filtering by query if not using category
                    if query and "category" not in params:
                        title = item.get("title", "").lower()
                        company = item.get("company_name", "").lower()
                        description = item.get("description", "").lower()
                        category = item.get("category", "").lower()

                        if (
                            query.lower() not in title
                            and query.lower() not in company
                            and query.lower() not in description
                            and query.lower() not in category
                        ):
                            continue

                    job = self._parse_job(item)

                    # Filter by location (US jobs only if location specified)
                    if location:
                        if not is_us_job(job):
                            continue

                    jobs.append(job)

                    if len(jobs) >= max_results:
                        break

                logger.info(f"Remotive returned {len(jobs)} jobs")
                return jobs

        except Exception as e:
            logger.error(f"Remotive API error: {e}")
            return []

    def _parse_job(self, item: dict) -> Job:
        """Parse Remotive job item to Job model"""
        # Extract candidate required location if available
        candidate_location = item.get("candidate_required_location", "")

        # Determine if job is remote
        job_type = item.get("job_type", "").lower()
        is_remote = "remote" in job_type or item.get("remote_ok", False)

        # Parse location - use candidate_required_location or default to "Remote"
        location = candidate_location if candidate_location else "Remote"

        return Job(
            title=item.get("title", ""),
            company=item.get("company_name", ""),
            location=location,
            url=item.get("url"),
            description=item.get("description"),
            salary=self._parse_salary(item),
            employment_type=item.get("job_type"),
            posted_date=item.get("publication_date"),
            source="Remotive",
            job_id=str(item.get("id")),
            category=item.get("category"),
            tags=item.get("tags", []) if item.get("tags") else [],
            skills=None,  # Not directly provided
            remote=is_remote,
            raw_payload=item,
        )

    def _parse_salary(self, item: dict) -> Optional[str]:
        """Parse salary information"""
        salary = item.get("salary")

        if salary:
            return str(salary)

        return None
