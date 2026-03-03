"""
Ingestion runner: fetch jobs and persist to Postgres.
"""
from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from typing import List, Optional

from .aggregator import JobAggregator
from .config import Config
from .enrichment import enrich_job
from .storage import (
    finish_run,
    purge_old_runs,
    record_source_results_bulk,
    start_run,
    upsert_jobs,
)
from .utils import build_dedupe_key, filter_recent_jobs
from .visa import enrich_jobs_with_visa_tags

logger = logging.getLogger(__name__)


def run_ingest(
    query: Optional[str] = None,
    location: Optional[str] = None,
    max_per_source: int = 100,
    sources: Optional[List[str]] = None,
    dry_run: bool = False,
) -> str:
    cfg = Config()
    if not cfg.db_dsn and not dry_run:
        raise RuntimeError("DB DSN not configured. Set JOB_SCRAPER_DB_DSN or config.yaml db.dsn")

    aggregator = JobAggregator(cfg)

    if dry_run:
        jobs, board_results, _ = asyncio.run(
            aggregator.search_with_tracking(
                query=query,
                location=location,
                max_per_source=max_per_source,
                sources=sources,
            )
        )
        jobs, stale_count = filter_recent_jobs(jobs, cfg.max_posting_age_days)
        board_count = len([result for result in board_results if result.board_token != result.source])
        logger.info(
            f"[DRY RUN] {len(board_results)} sources, {board_count} boards, "
            f"{len(jobs)} jobs (dropped stale: {stale_count})"
        )
        return "dry-run"

    run_id = start_run(cfg.db_dsn, sources or [])
    try:
        jobs, board_results, lineage = asyncio.run(
            aggregator.search_with_tracking(
                query=query,
                location=location,
                max_per_source=max_per_source,
                sources=sources,
            )
        )

        jobs, stale_count = filter_recent_jobs(jobs, cfg.max_posting_age_days)
        if stale_count > 0:
            logger.info(f"Dropped {stale_count} stale jobs older than {cfg.max_posting_age_days} days")

        jobs_stored_map = defaultdict(int)
        for job in jobs:
            dedupe_key = build_dedupe_key(job)
            if dedupe_key in lineage:
                jobs_stored_map[lineage[dedupe_key]] += 1

        jobs = enrich_jobs_with_visa_tags(jobs, cfg)
        enrichment_version = int(cfg.enrichment.get("version", 1) or 1)
        jobs = [enrich_job(job, enrichment_version=enrichment_version) for job in jobs]
        record_source_results_bulk(cfg.db_dsn, run_id, board_results, jobs_stored_map)

        error_count = sum(1 for result in board_results if result.error)
        if not board_results:
            status = "success"
        elif error_count == len(board_results):
            status = "failed"
        elif error_count > 0:
            status = "partial"
        else:
            status = "success"

        stored = upsert_jobs(cfg.db_dsn, run_id, jobs)
        finish_run(cfg.db_dsn, run_id, stored, status=status)
    except Exception as exc:
        finish_run(cfg.db_dsn, run_id, 0, status="error")
        raise
    finally:
        purge_old_runs(cfg.db_dsn, cfg.retention_days)

    return str(run_id)
