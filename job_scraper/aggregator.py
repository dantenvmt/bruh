"""
Job aggregator - combines results from multiple job board APIs
"""
import asyncio
import logging
from typing import Dict, List, Optional, Tuple
from .models import Job
from .utils import build_dedupe_key, is_us_job_for_source
from .config import Config
from .apis import BoardResult, TrackedJob
from .apis.remoteok import RemoteOKAPI
from .apis.adzuna import AdzunaAPI
from .apis.usajobs import USAJobsAPI
from .apis.themuse import TheMuseAPI
from .apis.jobspy_wrapper import JobSpyWrapper
from .apis.careeronestop import CareerOneStopAPI
from .apis.jsearch import JSearchAPI
from .apis.greenhouse import GreenhouseAPI
from .apis.lever import LeverAPI
from .apis.smartrecruiters import SmartRecruitersAPI
from .apis.ashby import AshbyAPI
from .apis.remotive import RemotiveAPI
from .apis.findwork import FindworkAPI
from .apis.hn_rss import HNRSSAPI
from .apis.weworkremotely import WeWorkRemotelyAPI
from .apis.builtin import BuiltInAPI
# Note: Arbeitnow removed - Europe-focused, excluded per US-only scope

logger = logging.getLogger(__name__)


class JobAggregator:
    """Aggregates jobs from multiple sources"""

    ALL_SOURCES = [
        "remoteok",
        "adzuna",
        "usajobs",
        "careeronestop",
        "jsearch",
        "greenhouse",
        "lever",
        "smartrecruiters",
        "themuse",
        "jobspy",
        "remotive",
        "findwork",
        "hnrss",
        "weworkremotely",
        "builtin",
        "ashby",
        # Note: arbeitnow removed - Europe-focused, excluded per US-only scope
    ]

    def __init__(self, config: Optional[Config] = None):
        """
        Initialize aggregator with API configurations

        Args:
            config: Configuration object with API credentials
        """
        self.config = config or Config()
        self.apis = self._initialize_apis()

    def _initialize_apis(self) -> dict:
        """Initialize all API clients"""
        return {
            "remoteok": RemoteOKAPI(),
            "adzuna": AdzunaAPI(
                app_id=self.config.adzuna.get("app_id"),
                app_key=self.config.adzuna.get("app_key"),
            ),
            "usajobs": USAJobsAPI(
                api_key=self.config.usajobs.get("api_key"),
                user_agent=self.config.usajobs.get("user_agent"),
            ),
            "careeronestop": CareerOneStopAPI(
                api_key=self.config.careeronestop.get("api_key"),
                user_id=self.config.careeronestop.get("user_id"),
            ),
            "jsearch": JSearchAPI(
                api_key=self.config.jsearch.get("api_key"),
                host=self.config.jsearch.get("host", "jsearch.p.rapidapi.com"),
                safe_mode=self.config.jsearch.get("safe_mode", True),
                min_interval_seconds=self.config.jsearch.get("min_interval_seconds", 1.5),
                jitter_seconds=self.config.jsearch.get("jitter_seconds", 0.7),
                requests_per_minute=self.config.jsearch.get("requests_per_minute", 25),
                max_pages=self.config.jsearch.get("max_pages", 10),
                max_retries=self.config.jsearch.get("max_retries", 4),
                backoff_base_seconds=self.config.jsearch.get("backoff_base_seconds", 2.0),
                backoff_cap_seconds=self.config.jsearch.get("backoff_cap_seconds", 45.0),
                cooldown_every_n_requests=self.config.jsearch.get("cooldown_every_n_requests", 5),
                cooldown_seconds=self.config.jsearch.get("cooldown_seconds", 8.0),
                respect_retry_after=self.config.jsearch.get("respect_retry_after", True),
                user_agent=self.config.jsearch.get("user_agent"),
                timeout_seconds=self.config.jsearch.get("timeout_seconds", 30.0),
                rate_limit_remaining_floor=self.config.jsearch.get("rate_limit_remaining_floor", 1),
            ),
            "greenhouse": GreenhouseAPI(
                boards=self.config.greenhouse.get("boards"),
                include_content=self.config.greenhouse.get("include_content", True),
            ),
            "lever": LeverAPI(
                sites=self.config.lever.get("sites"),
            ),
            "smartrecruiters": SmartRecruitersAPI(
                companies=self.config.smartrecruiters.get("companies"),
                include_content=self.config.smartrecruiters.get("include_content", False),
                requests_per_minute=self.config.smartrecruiters.get("requests_per_minute", 60),
            ),
            "ashby": AshbyAPI(
                companies=self.config.ashby.get("companies"),
                include_content=self.config.ashby.get("include_content", False),
                requests_per_minute=self.config.ashby.get("requests_per_minute", 60),
            ),
            # arbeitnow removed - Europe-focused, excluded per US-only scope
            "themuse": TheMuseAPI(
                api_key=self.config.themuse.get("api_key")
            ),
            "jobspy": JobSpyWrapper(),
            "remotive": RemotiveAPI(),
            "findwork": FindworkAPI(
                api_key=self.config.findwork.get("api_key"),
                base_url=self.config.findwork.get("base_url"),
                requests_per_minute=self.config.findwork.get("requests_per_minute", 60),
            ),
            "hnrss": HNRSSAPI(
                base_url=self.config.hnrss.get("base_url")
            ),
            "weworkremotely": WeWorkRemotelyAPI(
                base_url=self.config.weworkremotely.get("base_url")
            ),
            "builtin": BuiltInAPI(
                domains=self.config.builtin.get("domains"),
                max_pages=self.config.builtin.get("max_pages", 5),
                requests_per_minute=self.config.builtin.get("requests_per_minute", 60),
            ),
        }

    async def search(
        self,
        query: Optional[str] = None,
        location: Optional[str] = None,
        max_per_source: int = 100,
        sources: Optional[List[str]] = None,
        **kwargs,
    ) -> List[Job]:
        """Backwards-compatible wrapper that returns jobs only."""
        jobs, _, _ = await self.search_with_tracking(
            query=query,
            location=location,
            max_per_source=max_per_source,
            sources=sources,
            **kwargs,
        )
        return jobs

    async def search_with_tracking(
        self,
        query: Optional[str] = None,
        location: Optional[str] = None,
        max_per_source: int = 100,
        sources: Optional[List[str]] = None,
        max_concurrency: int = 15,
        **kwargs,
    ) -> Tuple[List[Job], List[BoardResult], Dict[str, Tuple[str, str]]]:
        """
        Search with per-board tracking and deterministic attribution.

        Returns:
            jobs: Deduplicated and filtered jobs
            board_results: Per-board metadata for run_sources
            lineage: dedupe_key -> (source, board_token)
        """
        available = [name for name, api in self.apis.items() if api.is_configured()]
        selected = sorted(s.lower() for s in (sources or available) if s.lower() in available)

        if not selected:
            logger.warning("No valid sources configured or specified")
            return [], [], {}

        logger.info(f"Searching jobs from {len(selected)} sources: {', '.join(selected)}")

        uncapped = {source.lower() for source in self.config.uncapped_sources}
        semaphore = asyncio.Semaphore(max_concurrency)

        async def fetch_source(source_name: str):
            async with semaphore:
                api = self.apis[source_name]
                max_for_source = 999999 if source_name in uncapped else max_per_source
                try:
                    tracked_jobs, board_results = await api.search_jobs_with_tracking(
                        query=query,
                        location=location,
                        max_results=max_for_source,
                        **kwargs,
                    )
                    return source_name, tracked_jobs, board_results
                except Exception as exc:
                    logger.error(f"Source {source_name} failed: {exc}")
                    return source_name, [], [
                        BoardResult(
                            source=source_name,
                            board_token=source_name,
                            jobs_fetched=0,
                            error=str(exc),
                            error_code="fetch_failed",
                        )
                    ]

        results = await asyncio.gather(*(fetch_source(source_name) for source_name in selected))

        indexed_tracked: List[Tuple[TrackedJob, str, int]] = []
        all_board_results: List[BoardResult] = []

        for source_name, tracked_jobs, board_results in results:
            all_board_results.extend(board_results)
            for idx, tracked_job in enumerate(tracked_jobs):
                indexed_tracked.append((tracked_job, source_name, idx))

        indexed_tracked.sort(key=lambda item: (item[1], item[0].board_token, item[2]))

        lineage: Dict[str, Tuple[str, str]] = {}
        unique_jobs: List[Job] = []
        for tracked_job, source_name, _ in indexed_tracked:
            dedupe_key = build_dedupe_key(tracked_job.job)
            if dedupe_key in lineage:
                continue
            lineage[dedupe_key] = (source_name, tracked_job.board_token)
            unique_jobs.append(tracked_job.job)

        if self.config.us_only:
            us_scoped = {"usajobs", "adzuna", "careeronestop", "jsearch"}
            unique_jobs = [
                job for job in unique_jobs
                if (job.source or "").lower() not in us_scoped or is_us_job_for_source(job, us_scoped)
            ]
            kept_keys = {build_dedupe_key(job) for job in unique_jobs}
            lineage = {key: value for key, value in lineage.items() if key in kept_keys}

        logger.info(f"Final job count after deduplication: {len(unique_jobs)}")
        return unique_jobs, all_board_results, lineage

    async def _search_source(
        self, api, query: Optional[str], location: Optional[str], max_results: int, **kwargs
    ) -> List[Job]:
        """Search a single source with error handling"""
        try:
            return await api.search_jobs(
                query=query, location=location, max_results=max_results, **kwargs
            )
        except Exception as e:
            logger.error(f"{api.name} search failed: {e}")
            return []

    def get_available_sources(self) -> List[str]:
        """Get list of configured and available sources"""
        return [name for name, api in self.apis.items() if api.is_configured()]

    def get_source_status(self) -> dict:
        """Get configuration status of all sources"""
        return {
            name: {
                "configured": api.is_configured(),
                "name": api.name,
            }
            for name, api in self.apis.items()
        }
