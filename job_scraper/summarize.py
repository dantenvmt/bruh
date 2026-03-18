"""
AI summarization pipeline.

- summarize_new_jobs(): called inline after upsert during ingestion
- run_summarize_batch(): backfill tool for existing unsummarized jobs
"""
from __future__ import annotations

import asyncio
import logging
import os
from datetime import datetime
from typing import List

from .config import Config
from .models import Job as JobModel
from .resume import summarize_job
from .storage import JobRecord, get_session

logger = logging.getLogger(__name__)


async def _summarize_and_store(session, job_id, title, company, description, llm_config):
    """Summarize a single job and write results to DB. Returns True on success."""
    result = await summarize_job(
        job_id=str(job_id),
        job_title=title or "",
        job_company=company or "",
        job_description=description,
        tags=[],
        llm_config=llm_config,
    )
    session.query(JobRecord).filter(JobRecord.id == job_id).update({
        "ai_summary_card": result["ai_summary_card"],
        "ai_summary_detail": result["ai_summary_detail"],
        "ai_summarized_at": datetime.utcnow(),
    })
    session.commit()
    return True


async def _summarize_jobs_inline(dsn: str, llm_config: dict, jobs: List[JobModel]) -> dict:
    """Summarize jobs that have descriptions, right after ingestion."""
    eligible = [j for j in jobs if j.description]
    if not eligible:
        return {"summarized": 0, "failed": 0, "total_eligible": 0}

    logger.info("Summarizing %d newly ingested jobs with descriptions", len(eligible))
    session = get_session(dsn)
    summarized = failed = 0

    try:
        for job in eligible:
            try:
                # Look up the DB record by dedupe_key to get the UUID
                from .utils import build_dedupe_key
                dk = build_dedupe_key(job)
                record = session.query(JobRecord).filter(JobRecord.dedupe_key == dk).first()
                if not record:
                    logger.debug("Job not found in DB (dedupe_key=%s), skipping summary", dk)
                    failed += 1
                    continue
                if record.ai_summarized_at is not None:
                    # Already summarized (e.g. duplicate from previous ingest)
                    continue

                await _summarize_and_store(
                    session, record.id, record.title, record.company,
                    record.description, llm_config,
                )
                summarized += 1
                logger.debug("Summarized: %s at %s", record.title, record.company)
                await asyncio.sleep(1)  # rate limit for Groq free tier
            except Exception as exc:
                session.rollback()
                logger.warning("Summary failed for %s: %s", job.title, exc)
                failed += 1

        logger.info("Inline summarization done: %d summarized, %d failed", summarized, failed)
        return {"summarized": summarized, "failed": failed, "total_eligible": len(eligible)}
    finally:
        session.close()


def summarize_new_jobs(dsn: str, jobs: List[JobModel]) -> dict:
    """Sync entry point called from ingest.py after upsert_jobs().

    Summarizes all newly ingested jobs that have descriptions.
    Silently skips if Groq API key is not configured.
    """
    cfg = Config()
    if not cfg.llm_parser.get("groq_api_key"):
        logger.debug("Groq API key not configured — skipping inline summarization")
        return {"summarized": 0, "failed": 0, "skipped": "no_api_key"}

    return asyncio.run(_summarize_jobs_inline(dsn, cfg.llm_parser, jobs))


# ---------------------------------------------------------------------------
# Backfill: for existing unsummarized jobs
# ---------------------------------------------------------------------------

async def _summarize_batch(dsn: str, llm_config: dict, batch_size: int) -> dict:
    """Summarize unsummarized jobs that have descriptions (backfill)."""
    session = get_session(dsn)
    try:
        jobs = (
            session.query(JobRecord)
            .filter(
                JobRecord.description.isnot(None),
                JobRecord.ai_summarized_at.is_(None),
            )
            .order_by(JobRecord.created_at.desc())
            .limit(batch_size)
            .all()
        )

        total_queued = len(jobs)
        if total_queued == 0:
            logger.info("No unsummarized jobs found")
            return {"summarized": 0, "failed": 0, "total_queued": 0}

        logger.info("Backfill: summarizing %d jobs", total_queued)
        summarized = failed = 0

        for job in jobs:
            try:
                await _summarize_and_store(
                    session, job.id, job.title, job.company,
                    job.description, llm_config,
                )
                summarized += 1
                await asyncio.sleep(1)  # rate limit for Groq free tier
            except Exception as exc:
                session.rollback()
                logger.warning("Summary failed for job %s: %s", job.id, exc)
                failed += 1

        logger.info("Backfill complete: %d summarized, %d failed", summarized, failed)
        return {"summarized": summarized, "failed": failed, "total_queued": total_queued}
    finally:
        session.close()


def run_summarize_batch(batch_size: int | None = None) -> dict:
    """Backfill entry point — run manually or from CLI."""
    cfg = Config()
    if not cfg.llm_parser.get("groq_api_key"):
        logger.warning("Groq API key not configured — skipping summarization")
        return {"summarized": 0, "failed": 0, "skipped": "no_api_key"}

    if batch_size is None:
        batch_size = int(os.getenv("AI_SUMMARIZE_BATCH_SIZE", "100"))

    return asyncio.run(_summarize_batch(cfg.db_dsn, cfg.llm_parser, batch_size))
