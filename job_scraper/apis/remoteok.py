"""
RemoteOK API integration

RemoteOK is the #1 remote job board with 30,000+ listings
covering 80% of remote jobs on the web.

API: https://remoteok.com/api
- No authentication required
- Free tier available
- Jobs available 24hrs after posting
- Must link back to source
"""
import logging
from typing import List, Optional
import httpx
from . import BaseJobAPI
from ..models import Job

logger = logging.getLogger(__name__)


class RemoteOKAPI(BaseJobAPI):
    """RemoteOK job board API client"""

    BASE_URL = "https://remoteok.com/api"

    def __init__(self):
        super().__init__(name="RemoteOK")

    def is_configured(self) -> bool:
        """RemoteOK doesn't require configuration"""
        return True

    async def search_jobs(
        self,
        query: Optional[str] = None,
        location: Optional[str] = None,
        max_results: int = 100,
        **kwargs,
    ) -> List[Job]:
        """
        Search RemoteOK for jobs

        Args:
            query: Search query (filtered client-side)
            location: Location filter (filtered client-side)
            max_results: Maximum results to return

        Returns:
            List of Job objects
        """
        logger.info(f"Fetching jobs from RemoteOK (query='{query}', location='{location}')")

        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.get(self.BASE_URL)
                response.raise_for_status()

                data = response.json()

                # First item is metadata, skip it
                jobs_data = data[1:] if len(data) > 1 else []

                jobs = []
                for item in jobs_data:
                    # Filter by query if provided
                    if query:
                        title = item.get("position", "").lower()
                        company = item.get("company", "").lower()
                        tags_str = " ".join(item.get("tags", [])).lower()

                        if (
                            query.lower() not in title
                            and query.lower() not in company
                            and query.lower() not in tags_str
                        ):
                            continue

                    # Filter by location if provided
                    if location:
                        job_location = item.get("location", "").lower()
                        if location.lower() not in job_location:
                            continue

                    job = self._parse_job(item)
                    jobs.append(job)

                    if len(jobs) >= max_results:
                        break

                logger.info(f"RemoteOK returned {len(jobs)} jobs")
                return jobs

        except Exception as e:
            logger.error(f"RemoteOK API error: {e}")
            return []

    def _parse_job(self, item: dict) -> Job:
        """Parse RemoteOK job item to Job model"""
        return Job(
            title=item.get("position", ""),
            company=item.get("company", ""),
            location=item.get("location") or "Remote",
            url=item.get("url"),
            description=item.get("description"),
            salary=self._parse_salary(item),
            employment_type=None,  # Not provided by RemoteOK
            posted_date=item.get("date"),
            source="RemoteOK",
            job_id=str(item.get("id")),
            category=None,
            tags=item.get("tags", []),
            skills=item.get("tags", []),  # Use tags as skills
            remote=True,  # All RemoteOK jobs are remote
            raw_payload=item,
        )

    def _parse_salary(self, item: dict) -> Optional[str]:
        """Parse salary information"""
        salary_min = item.get("salary_min")
        salary_max = item.get("salary_max")

        if salary_min and salary_max:
            return f"${salary_min:,} - ${salary_max:,}"
        elif salary_min:
            return f"${salary_min:,}+"
        elif salary_max:
            return f"Up to ${salary_max:,}"

        return None
