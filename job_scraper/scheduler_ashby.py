"""
Dedicated scheduler for Ashby batch ingestion.

This keeps large Ashby slug ingestion separate from the main scheduler so the
main daily ingest can run with its own cadence and source mix.
"""
from __future__ import annotations

import logging
import os
from typing import List

from apscheduler.schedulers.blocking import BlockingScheduler

from .ingest import run_ingest

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def _to_int(raw: str | None, default: int) -> int:
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _to_list(raw: str | None) -> List[str]:
    if not raw:
        return []
    return [item.strip() for item in raw.split(",") if item.strip()]


def _run_ashby_batch(batch_name: str, slugs: List[str], max_per_source: int) -> None:
    if not slugs:
        logger.warning(f"[ASHBY {batch_name}] No slugs configured, skipping run")
        return

    previous = os.environ.get("ASHBY_COMPANIES")
    os.environ["ASHBY_COMPANIES"] = ",".join(slugs)

    query = os.getenv("ASHBY_BATCH_QUERY")
    location = os.getenv("ASHBY_BATCH_LOCATION")
    dry_run = os.getenv("ASHBY_BATCH_DRY_RUN", "false").lower() in {"1", "true", "yes", "on"}

    try:
        logger.info(
            f"[ASHBY {batch_name}] Starting run with {len(slugs)} slugs | "
            f"query={query!r} | location={location!r} | dry_run={dry_run}"
        )
        run_id = run_ingest(
            query=query or None,
            location=location or None,
            max_per_source=max_per_source,
            sources=["ashby"],
            dry_run=dry_run,
        )
        logger.info(f"[ASHBY {batch_name}] Completed run: {run_id}")
    except Exception:
        logger.exception(f"[ASHBY {batch_name}] Run failed")
        raise
    finally:
        if previous is None:
            os.environ.pop("ASHBY_COMPANIES", None)
        else:
            os.environ["ASHBY_COMPANIES"] = previous


def main() -> None:
    batch_a = _to_list(os.getenv("ASHBY_BATCH_A_COMPANIES"))
    batch_b = _to_list(os.getenv("ASHBY_BATCH_B_COMPANIES"))

    hour_a = _to_int(os.getenv("ASHBY_BATCH_A_HOUR"), 6)
    minute_a = _to_int(os.getenv("ASHBY_BATCH_A_MINUTE"), 0)
    hour_b = _to_int(os.getenv("ASHBY_BATCH_B_HOUR"), 18)
    minute_b = _to_int(os.getenv("ASHBY_BATCH_B_MINUTE"), 0)
    max_per_source = _to_int(os.getenv("ASHBY_BATCH_MAX_PER_SOURCE"), 999999)

    scheduler = BlockingScheduler()

    if batch_a:
        scheduler.add_job(
            _run_ashby_batch,
            "cron",
            id="ashby_batch_a",
            hour=hour_a,
            minute=minute_a,
            kwargs={"batch_name": "A", "slugs": batch_a, "max_per_source": max_per_source},
            coalesce=True,
            max_instances=1,
        )
        logger.info(f"Scheduled Ashby batch A at {hour_a:02d}:{minute_a:02d} UTC ({len(batch_a)} slugs)")
    else:
        logger.warning("ASHBY_BATCH_A_COMPANIES not configured")

    if batch_b:
        scheduler.add_job(
            _run_ashby_batch,
            "cron",
            id="ashby_batch_b",
            hour=hour_b,
            minute=minute_b,
            kwargs={"batch_name": "B", "slugs": batch_b, "max_per_source": max_per_source},
            coalesce=True,
            max_instances=1,
        )
        logger.info(f"Scheduled Ashby batch B at {hour_b:02d}:{minute_b:02d} UTC ({len(batch_b)} slugs)")
    else:
        logger.warning("ASHBY_BATCH_B_COMPANIES not configured")

    if not batch_a and not batch_b:
        logger.error("No Ashby batches configured; exiting")
        return

    logger.info("Ashby scheduler starting...")
    scheduler.start()


if __name__ == "__main__":
    main()
