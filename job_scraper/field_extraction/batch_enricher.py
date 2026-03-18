"""
Batch field enrichment pipeline.

Queries jobs where salary_min is not yet extracted (checked via raw_payload),
calls field_extractor per job (concurrency=5, Groq rate-limited to 25/min),
and writes results back to the DB:

  Existing columns updated directly:
    - remote           (bool)
    - employment_type  (text)
    - experience_min_years (int)

  All extracted fields stored in raw_payload["extracted_fields"]:
    - salary_min, salary_max, remote, experience_years_min,
      visa_sponsorship, employment_type, seniority

CLI usage:
    python -m job_scraper.enrichment.batch_enricher [--limit N] [--dry-run]

Environment / config:
    GROQ_API_KEY  (or config.yaml llm_parser.groq_api_key)
    JOB_SCRAPER_DB_DSN / DATABASE_URL
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from datetime import datetime
from typing import List, Optional
from uuid import UUID

from sqlalchemy import text

from ..config import Config
from ..storage import JobRecord, get_session, session_scope
from ..scraping.parsers.detail import _LLMRateLimiter
from .field_extractor import extract_fields

logger = logging.getLogger(__name__)

_BATCH_SIZE = 50
_CONCURRENCY = 5
_GROQ_CALLS_PER_MINUTE = 25


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def _fetch_unenriched_jobs(dsn: str, limit: int) -> List[JobRecord]:
    """
    Return jobs that have a description but have not yet had field extraction
    run (i.e. raw_payload->'extracted_fields' is NULL).

    We use a raw SQL expression for the JSONB path check so we don't need a
    generated column or a new migration.
    """
    session = get_session(dsn)
    try:
        query = (
            session.query(JobRecord)
            .filter(JobRecord.description.isnot(None))
            .filter(
                text(
                    "raw_payload -> 'extracted_fields' IS NULL"
                )
            )
            .order_by(JobRecord.created_at.desc())
            .limit(limit)
        )
        results = query.all()
        session.expunge_all()
        return results
    finally:
        session.close()


def _write_extracted_fields(
    dsn: str,
    job_id: UUID,
    fields: dict,
    *,
    dry_run: bool,
) -> None:
    """
    Persist extracted fields to the DB for a single job.

    - Writes all fields into raw_payload["extracted_fields"].
    - Also promotes remote / employment_type / experience_min_years
      directly onto their native columns (only when currently NULL,
      so a human override is never stomped).
    """
    if dry_run:
        return

    with session_scope(dsn) as session:
        job = session.query(JobRecord).filter(JobRecord.id == job_id).one_or_none()
        if job is None:
            return

        # Merge into raw_payload["extracted_fields"]
        payload = dict(job.raw_payload or {})
        payload["extracted_fields"] = fields
        job.raw_payload = payload

        # Promote native columns only when they are currently NULL
        if job.remote is None and isinstance(fields.get("remote"), bool):
            job.remote = fields["remote"]

        if job.employment_type is None and fields.get("employment_type"):
            job.employment_type = fields["employment_type"]

        if job.experience_min_years is None and fields.get("experience_years_min") is not None:
            job.experience_min_years = fields["experience_years_min"]

        job.updated_at = datetime.utcnow()


# ---------------------------------------------------------------------------
# Core async pipeline
# ---------------------------------------------------------------------------

async def _process_job(
    job: JobRecord,
    groq_api_key: str,
    rate_limiter: _LLMRateLimiter,
    semaphore: asyncio.Semaphore,
    dsn: str,
    *,
    dry_run: bool,
    stats: dict,
) -> None:
    """Enrich a single job with field extraction."""
    async with semaphore:
        await rate_limiter.acquire()
        try:
            fields = await extract_fields(
                title=job.title or "",
                description=job.description or "",
                groq_api_key=groq_api_key,
            )
        except Exception as exc:
            stats["groq_errors"] += 1
            logger.warning(
                "field_extractor: Groq error for job %s (%s): %s",
                job.id, job.title, exc,
            )
            return

        stats["jobs_processed"] += 1

        # Count non-null fields extracted
        non_null = sum(1 for v in fields.values() if v is not None)
        stats["fields_extracted"] += non_null

        _write_extracted_fields(dsn, job.id, fields, dry_run=dry_run)

        logger.debug(
            "Enriched job %s (%s): %d fields extracted",
            job.id, job.title[:60], non_null,
        )


async def run_batch(
    *,
    dsn: str,
    groq_api_key: str,
    limit: int = 1000,
    dry_run: bool = False,
) -> dict:
    """
    Main batch enrichment loop.

    Fetches up to `limit` unenriched jobs, processes them in sub-batches
    of _BATCH_SIZE with _CONCURRENCY concurrent Groq calls.

    Returns a stats dict:
        jobs_processed, fields_extracted, groq_errors, batches_run
    """
    stats = {
        "jobs_processed": 0,
        "fields_extracted": 0,
        "groq_errors": 0,
        "batches_run": 0,
    }

    rate_limiter = _LLMRateLimiter(calls_per_minute=_GROQ_CALLS_PER_MINUTE)
    semaphore = asyncio.Semaphore(_CONCURRENCY)

    offset = 0
    while offset < limit:
        batch_limit = min(_BATCH_SIZE, limit - offset)
        jobs = _fetch_unenriched_jobs(dsn, batch_limit)

        if not jobs:
            logger.info("batch_enricher: no more unenriched jobs found")
            break

        logger.info(
            "batch_enricher: processing batch of %d jobs (offset=%d)",
            len(jobs), offset,
        )

        tasks = [
            _process_job(
                job,
                groq_api_key=groq_api_key,
                rate_limiter=rate_limiter,
                semaphore=semaphore,
                dsn=dsn,
                dry_run=dry_run,
                stats=stats,
            )
            for job in jobs
        ]
        await asyncio.gather(*tasks)

        stats["batches_run"] += 1
        offset += len(jobs)

        # If the DB returned fewer rows than requested we've exhausted the set
        if len(jobs) < batch_limit:
            break

    return stats


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def _parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Batch-enrich jobs with structured field extraction via Groq."
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=1000,
        help="Maximum number of jobs to process (default: 1000)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Extract fields but do not write to DB",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level (default: INFO)",
    )
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> None:
    args = _parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    cfg = Config()
    dsn = cfg.db_dsn
    if not dsn:
        logger.error(
            "No DB DSN configured. Set JOB_SCRAPER_DB_DSN or DATABASE_URL."
        )
        sys.exit(1)

    groq_api_key = cfg.llm_parser.get("groq_api_key")
    if not groq_api_key:
        logger.error(
            "No Groq API key configured. Set GROQ_API_KEY or llm_parser.groq_api_key."
        )
        sys.exit(1)

    if args.dry_run:
        logger.info("batch_enricher: DRY RUN mode — no DB writes will occur")

    logger.info(
        "batch_enricher: starting (limit=%d, dry_run=%s)",
        args.limit, args.dry_run,
    )

    stats = asyncio.run(
        run_batch(
            dsn=dsn,
            groq_api_key=groq_api_key,
            limit=args.limit,
            dry_run=args.dry_run,
        )
    )

    logger.info(
        "batch_enricher: done — jobs_processed=%d fields_extracted=%d "
        "groq_errors=%d batches_run=%d",
        stats["jobs_processed"],
        stats["fields_extracted"],
        stats["groq_errors"],
        stats["batches_run"],
    )


if __name__ == "__main__":
    main()
