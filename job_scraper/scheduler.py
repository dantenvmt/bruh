"""
Scheduler entrypoint for nightly ingestion.
"""
from __future__ import annotations

import logging
import os

from apscheduler.schedulers.blocking import BlockingScheduler

from .aggregator import JobAggregator
from .config import Config
from .ingest import run_ingest
from .scrape_ingest import run_scrape_ingest

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def _to_list(value: str | None) -> list[str]:
    if not value:
        return []
    return [item.strip().lower() for item in value.split(",") if item.strip()]


def _resolve_scheduled_sources(include_raw: str | None, exclude_raw: str | None) -> list[str] | None:
    include_sources = _to_list(include_raw)
    exclude_sources = set(_to_list(exclude_raw))

    if include_sources:
        resolved = [source for source in include_sources if source not in exclude_sources]
        return resolved or None

    if exclude_sources:
        resolved = [source for source in JobAggregator.ALL_SOURCES if source.lower() not in exclude_sources]
        return resolved or None

    return None


def main() -> None:
    cfg = Config()
    scheduler = BlockingScheduler()
    scheduled_sources = _resolve_scheduled_sources(
        os.getenv("JOB_SCRAPER_SCHEDULE_SOURCES"),
        os.getenv("JOB_SCRAPER_SCHEDULE_EXCLUDE_SOURCES"),
    )

    logger.info(f"Scheduler starting...")
    logger.info(f"Scheduled job ingest at {cfg.schedule_hour:02d}:{cfg.schedule_minute:02d} UTC daily")
    logger.info(f"Retention: {cfg.retention_days} days | US-only: {cfg.us_only}")
    if scheduled_sources is not None:
        logger.info(f"Scheduler sources: {', '.join(scheduled_sources)}")

    job_kwargs = {}
    if scheduled_sources is not None:
        job_kwargs["sources"] = scheduled_sources

    scheduler.add_job(
        run_ingest,
        "cron",
        hour=cfg.schedule_hour,
        minute=cfg.schedule_minute,
        kwargs=job_kwargs,
        max_instances=1,
        misfire_grace_time=300,
    )

    # Custom site scraper - runs every 15 minutes if enabled
    if os.getenv("JOB_SCRAPER_ENABLE_CUSTOM_SCRAPER", "false").lower() == "true":
        scheduler.add_job(
            run_scrape_ingest,
            "interval",
            minutes=15,
            id="scrape_custom_sites",
            max_instances=1,
            misfire_grace_time=300,
        )
        logger.info("Custom site scraper enabled (every 15 min)")

    scheduler.start()


if __name__ == "__main__":
    main()
