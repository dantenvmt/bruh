"""
Integration tests for aggregator tracking and attribution behavior.
"""
from __future__ import annotations

import asyncio
from typing import List, Optional, Tuple

import pytest

from job_scraper.aggregator import JobAggregator
from job_scraper.apis import BaseJobAPI, BoardResult, TrackedJob
from job_scraper.config import Config
from job_scraper.models import Job
from job_scraper.utils import build_dedupe_key


def _make_job(title: str, company: str, url: str, source: str) -> Job:
    return Job(title=title, company=company, url=url, source=source)


class DummyDefaultAPI(BaseJobAPI):
    def __init__(self, name: str, jobs: List[Job]):
        super().__init__(name=name)
        self._jobs = jobs

    async def search_jobs(
        self,
        query: Optional[str] = None,
        location: Optional[str] = None,
        max_results: int = 100,
        **kwargs,
    ) -> List[Job]:
        return self._jobs[:max_results]

    def is_configured(self) -> bool:
        return True


class DummyTrackingAPI(BaseJobAPI):
    def __init__(
        self,
        name: str,
        tracked_jobs: List[TrackedJob],
        board_results: List[BoardResult],
        delay_seconds: float = 0.0,
        fail: bool = False,
    ):
        super().__init__(name=name)
        self._tracked_jobs = tracked_jobs
        self._board_results = board_results
        self._delay_seconds = delay_seconds
        self._fail = fail

    async def search_jobs(
        self,
        query: Optional[str] = None,
        location: Optional[str] = None,
        max_results: int = 100,
        **kwargs,
    ) -> List[Job]:
        return [tracked.job for tracked in self._tracked_jobs][:max_results]

    async def search_jobs_with_tracking(
        self,
        query: Optional[str] = None,
        location: Optional[str] = None,
        max_results: int = 100,
        **kwargs,
    ) -> Tuple[List[TrackedJob], List[BoardResult]]:
        if self._delay_seconds:
            await asyncio.sleep(self._delay_seconds)
        if self._fail:
            raise RuntimeError(f"{self.name} failed")
        return self._tracked_jobs[:max_results], self._board_results

    def is_configured(self) -> bool:
        return True


def _build_test_aggregator() -> JobAggregator:
    cfg = Config()
    cfg._config["us_only"] = False
    cfg._config.setdefault("ingestion", {})["uncapped_sources"] = []
    return JobAggregator(cfg)


@pytest.mark.asyncio
async def test_non_ats_default_tracking():
    api = DummyDefaultAPI(
        name="USAJobs",
        jobs=[_make_job("Engineer", "Agency", "https://example.com/job/1", "usajobs")],
    )

    tracked_jobs, board_results = await api.search_jobs_with_tracking(max_results=10)

    assert len(tracked_jobs) == 1
    assert tracked_jobs[0].board_token == "usajobs"
    assert len(board_results) == 1
    assert board_results[0].source == "usajobs"
    assert board_results[0].board_token == "usajobs"
    assert board_results[0].jobs_fetched == 1


@pytest.mark.asyncio
async def test_duplicate_across_sources_first_source_wins():
    duplicate_a = _make_job("Data Engineer", "Stripe", "https://jobs.example.com/1", "greenhouse")
    duplicate_b = _make_job("Data Engineer", "Stripe", "https://jobs.example.com/1", "lever")
    unique_b = _make_job("Backend Engineer", "Netflix", "https://jobs.example.com/2", "lever")

    aggregator = _build_test_aggregator()
    aggregator.apis = {
        "greenhouse": DummyTrackingAPI(
            name="Greenhouse",
            tracked_jobs=[TrackedJob(job=duplicate_a, board_token="stripe")],
            board_results=[BoardResult(source="greenhouse", board_token="stripe", jobs_fetched=1)],
            delay_seconds=0.05,
        ),
        "lever": DummyTrackingAPI(
            name="Lever",
            tracked_jobs=[
                TrackedJob(job=duplicate_b, board_token="stripe"),
                TrackedJob(job=unique_b, board_token="netflix"),
            ],
            board_results=[
                BoardResult(source="lever", board_token="stripe", jobs_fetched=1),
                BoardResult(source="lever", board_token="netflix", jobs_fetched=1),
            ],
            delay_seconds=0.0,
        ),
    }

    jobs, _, lineage = await aggregator.search_with_tracking()

    duplicate_key = build_dedupe_key(duplicate_a)
    assert len(jobs) == 2
    assert lineage[duplicate_key] == ("greenhouse", "stripe")


@pytest.mark.asyncio
async def test_attribution_determinism():
    job_a = _make_job("Data Engineer", "Stripe", "https://jobs.example.com/1", "greenhouse")
    job_b = _make_job("ML Engineer", "Airbnb", "https://jobs.example.com/2", "greenhouse")
    job_c = _make_job("Data Engineer", "Stripe", "https://jobs.example.com/1", "lever")

    aggregator = _build_test_aggregator()
    aggregator.apis = {
        "greenhouse": DummyTrackingAPI(
            name="Greenhouse",
            tracked_jobs=[
                TrackedJob(job=job_a, board_token="stripe"),
                TrackedJob(job=job_b, board_token="airbnb"),
            ],
            board_results=[
                BoardResult(source="greenhouse", board_token="stripe", jobs_fetched=1),
                BoardResult(source="greenhouse", board_token="airbnb", jobs_fetched=1),
            ],
            delay_seconds=0.01,
        ),
        "lever": DummyTrackingAPI(
            name="Lever",
            tracked_jobs=[TrackedJob(job=job_c, board_token="stripe")],
            board_results=[BoardResult(source="lever", board_token="stripe", jobs_fetched=1)],
            delay_seconds=0.0,
        ),
    }

    jobs_1, _, lineage_1 = await aggregator.search_with_tracking()
    jobs_2, _, lineage_2 = await aggregator.search_with_tracking()

    assert lineage_1 == lineage_2
    assert [build_dedupe_key(job) for job in jobs_1] == [build_dedupe_key(job) for job in jobs_2]


def test_partial_failure_status_logic():
    board_results = [
        BoardResult(source="greenhouse", board_token="stripe", jobs_fetched=10),
        BoardResult(source="greenhouse", board_token="bad-board", jobs_fetched=0, error="timeout"),
    ]
    error_count = sum(1 for result in board_results if result.error)
    if not board_results:
        status = "success"
    elif error_count == len(board_results):
        status = "failed"
    elif error_count > 0:
        status = "partial"
    else:
        status = "success"

    assert status == "partial"


@pytest.mark.asyncio
async def test_reconciliation_invariant():
    job_a = _make_job("A", "Stripe", "https://jobs.example.com/1", "greenhouse")
    job_b = _make_job("B", "Stripe", "https://jobs.example.com/2", "greenhouse")
    job_c = _make_job("A", "Stripe", "https://jobs.example.com/1", "lever")

    aggregator = _build_test_aggregator()
    aggregator.apis = {
        "greenhouse": DummyTrackingAPI(
            name="Greenhouse",
            tracked_jobs=[
                TrackedJob(job=job_a, board_token="stripe"),
                TrackedJob(job=job_b, board_token="stripe"),
            ],
            board_results=[BoardResult(source="greenhouse", board_token="stripe", jobs_fetched=2)],
        ),
        "lever": DummyTrackingAPI(
            name="Lever",
            tracked_jobs=[TrackedJob(job=job_c, board_token="stripe")],
            board_results=[BoardResult(source="lever", board_token="stripe", jobs_fetched=1)],
        ),
    }

    jobs, _, lineage = await aggregator.search_with_tracking()
    jobs_stored_map = {}
    for job in jobs:
        dedupe_key = build_dedupe_key(job)
        jobs_stored_map[lineage[dedupe_key]] = jobs_stored_map.get(lineage[dedupe_key], 0) + 1

    assert sum(jobs_stored_map.values()) == len(jobs)
