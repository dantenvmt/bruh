"""
Job board API integrations

Base class and implementations for various job board APIs
"""
from abc import ABC, abstractmethod
from dataclasses import dataclass
import time
from typing import List, Optional, Tuple
from ..models import Job


@dataclass
class BoardResult:
    """Result metadata for a single ATS board fetch"""
    source: str
    board_token: str
    jobs_fetched: int
    error: Optional[str] = None
    error_code: Optional[str] = None  # "rate_limited", "not_found", "timeout", "parse_error"
    duration_ms: int = 0


@dataclass
class TrackedJob:
    """Job with ingestion provenance for attribution."""
    job: Job
    board_token: str


class BaseJobAPI(ABC):
    """Abstract base class for job board APIs"""

    def __init__(self, name: str):
        self.name = name

    @abstractmethod
    async def search_jobs(
        self,
        query: Optional[str] = None,
        location: Optional[str] = None,
        max_results: int = 100,
        **kwargs,
    ) -> List[Job]:
        """
        Search for jobs

        Args:
            query: Search query (job title, keywords, etc.)
            location: Location filter
            max_results: Maximum number of results to return
            **kwargs: Additional API-specific parameters

        Returns:
            List of Job objects
        """
        pass

    @abstractmethod
    def is_configured(self) -> bool:
        """Check if API is properly configured with credentials"""
        pass

    async def search_jobs_with_tracking(
        self,
        query: Optional[str] = None,
        location: Optional[str] = None,
        max_results: int = 100,
        **kwargs,
    ) -> Tuple[List[TrackedJob], List[BoardResult]]:
        """Default implementation for sources without board-level tracking."""
        start_mono = time.monotonic()
        source_lower = self.name.lower()
        try:
            jobs = await self.search_jobs(query=query, location=location, max_results=max_results, **kwargs)
            tracked = [TrackedJob(job=job, board_token=source_lower) for job in jobs]
            return tracked, [
                BoardResult(
                    source=source_lower,
                    board_token=source_lower,
                    jobs_fetched=len(jobs),
                    duration_ms=int((time.monotonic() - start_mono) * 1000),
                )
            ]
        except Exception as exc:
            return [], [
                BoardResult(
                    source=source_lower,
                    board_token=source_lower,
                    jobs_fetched=0,
                    error=str(exc),
                    error_code="unknown_error",
                    duration_ms=int((time.monotonic() - start_mono) * 1000),
                )
            ]

    def __repr__(self):
        return f"<{self.__class__.__name__}(name='{self.name}')>"
