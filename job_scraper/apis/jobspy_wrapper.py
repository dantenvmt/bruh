"""
JobSpy wrapper

JobSpy is an open-source library that scrapes LinkedIn, Indeed,
Glassdoor, Google Jobs, and ZipRecruiter.

GitHub: https://github.com/speedyapply/JobSpy
Install: pip install python-jobspy

Note: This is a scraper, not an API. It may be subject to rate limiting.
"""
import asyncio
import logging
from typing import List, Optional
from . import BaseJobAPI
from ..models import Job

logger = logging.getLogger(__name__)


class JobSpyWrapper(BaseJobAPI):
    """Wrapper for JobSpy scraping library"""

    SUPPORTED_SITES = ["indeed", "linkedin", "glassdoor", "google", "zip_recruiter"]

    def __init__(self):
        super().__init__(name="JobSpy")
        self._jobspy_available = self._check_jobspy()

    def _check_jobspy(self) -> bool:
        """Check if python-jobspy is installed"""
        try:
            import jobspy

            return True
        except ImportError:
            logger.warning(
                "python-jobspy not installed. Install with: pip install python-jobspy"
            )
            return False

    def is_configured(self) -> bool:
        """JobSpy doesn't require configuration, just installation"""
        return self._jobspy_available

    async def search_jobs(
        self,
        query: Optional[str] = None,
        location: Optional[str] = None,
        max_results: int = 100,
        sites: Optional[List[str]] = None,
        **kwargs,
    ) -> List[Job]:
        """
        Search using JobSpy scraper

        Args:
            query: Search query (job title, keywords)
            location: Location filter
            max_results: Maximum results to return
            sites: List of sites to scrape (indeed, linkedin, glassdoor, google, zip_recruiter)

        Returns:
            List of Job objects
        """
        if not self.is_configured():
            logger.warning("JobSpy not available, skipping")
            return []

        try:
            from jobspy import scrape_jobs
        except ImportError:
            logger.error("Failed to import jobspy")
            return []

        sites = sites or ["indeed"]  # Default to Indeed (least restrictive)

        # Validate sites
        sites = [s for s in sites if s in self.SUPPORTED_SITES]
        if not sites:
            logger.warning("No valid sites specified for JobSpy")
            return []

        logger.info(
            f"Scraping jobs with JobSpy (query='{query}', location='{location}', sites={sites})"
        )

        try:
            # JobSpy is synchronous - run in thread pool to avoid blocking event loop
            loop = asyncio.get_running_loop()
            df = await loop.run_in_executor(
                None,
                lambda: scrape_jobs(
                    site_name=sites,
                    search_term=query or "",
                    location=location or "",
                    results_wanted=max_results,
                    hours_old=72,  # Jobs posted in last 72 hours
                    country_indeed="USA",  # Default country
                ),
            )

            if df is None or df.empty:
                logger.info("JobSpy returned no results")
                return []

            jobs = []
            for _, row in df.iterrows():
                job = self._parse_job(row)
                jobs.append(job)

            logger.info(f"JobSpy returned {len(jobs)} jobs")
            return jobs

        except Exception as e:
            logger.error(f"JobSpy error: {e}")
            return []

    def _parse_job(self, row) -> Job:
        """Parse JobSpy DataFrame row to Job model"""
        raw_payload = None
        try:
            raw_payload = row.to_dict()
        except Exception:
            raw_payload = None
        return Job(
            title=row.get("title", ""),
            company=row.get("company", ""),
            location=row.get("location"),
            url=row.get("job_url"),
            description=row.get("description"),
            salary=self._parse_salary(row),
            employment_type=row.get("job_type"),
            posted_date=str(row.get("date_posted")) if row.get("date_posted") else None,
            source=f"JobSpy-{row.get('site', 'unknown')}",
            job_id=row.get("job_url"),  # Use URL as ID
            category=None,
            tags=None,
            skills=None,
            remote=row.get("is_remote", False),
            raw_payload=raw_payload,
        )

    def _parse_salary(self, row) -> Optional[str]:
        """Parse salary information from JobSpy row"""
        min_sal = row.get("min_amount")
        max_sal = row.get("max_amount")
        currency = row.get("currency", "$")
        interval = row.get("interval", "")

        if min_sal and max_sal:
            return f"{currency}{min_sal:,.0f} - {currency}{max_sal:,.0f}{' ' + interval if interval else ''}"
        elif min_sal:
            return f"{currency}{min_sal:,.0f}+{' ' + interval if interval else ''}"
        elif max_sal:
            return f"Up to {currency}{max_sal:,.0f}{' ' + interval if interval else ''}"

        return None
