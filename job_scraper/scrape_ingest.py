"""
Scraping ingestion runner: scrape custom career sites and persist to Postgres.

Follows the same pattern as ingest.py but for direct career site scraping.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta
from typing import List, Tuple

from sqlalchemy import func, or_

from .config import Config
from .enrichment import enrich_job
from .models import Job
from .discovery.selectors import selector_hints_ready_for_scrape
from .scraping.models import ScrapeSite
from .scraping.scraper import scrape_site
from .scraping.types import SiteResult
from .storage import (
    finish_run,
    get_session,
    purge_old_runs,
    record_source_result,
    session_scope,
    start_run,
    upsert_jobs,
)
from .summarize import summarize_new_jobs
from .utils import _is_non_us_location, normalize_text
from .visa import enrich_jobs_with_visa_tags

logger = logging.getLogger(__name__)


def run_scrape_ingest(limit: int = 20, dry_run: bool = False) -> str:
    """
    Sync entry point for scheduler to scrape custom career sites.

    Args:
        limit: Maximum number of sites to scrape in this run
        dry_run: If True, only query sites and log without persisting

    Returns:
        Run ID as string, or "dry-run" or "empty" for special cases

    Raises:
        RuntimeError: If DB DSN not configured and not in dry_run mode
    """
    cfg = Config()

    if not cfg.db_dsn and not dry_run:
        raise RuntimeError("DB DSN not configured. Set JOB_SCRAPER_DB_DSN or config.yaml db.dsn")

    jobs, site_results = asyncio.run(_scrape_due_sites(cfg, limit, dry_run))

    if dry_run:
        logger.info(f"[DRY RUN] Would scrape {len(site_results)} sites, {len(jobs)} jobs")
        return "dry-run"

    if not site_results:
        logger.info("No sites due for scraping")
        return "empty"

    # Start a run and persist jobs
    run_id = start_run(cfg.db_dsn, sources=["custom_scraper"])

    try:
        # Dedupe jobs by URL within batch (selectors may extract same link multiple times)
        seen_urls = set()
        unique_jobs = []
        for job in jobs:
            if job.url and job.url not in seen_urls:
                seen_urls.add(job.url)
                unique_jobs.append(job)
        jobs = unique_jobs

        # Filter explicitly non-US jobs (lenient: keeps jobs with no/ambiguous location)
        if cfg.us_only:
            before = len(jobs)
            jobs = [
                j for j in jobs
                if not j.location or not _is_non_us_location(normalize_text(j.location))
            ]
            filtered = before - len(jobs)
            if filtered:
                logger.info("US-only filter removed %d non-US jobs", filtered)

        # Enrich with visa tags + deterministic fields before upserting
        jobs = enrich_jobs_with_visa_tags(jobs, cfg)
        enrichment_version = int(cfg.enrichment.get("version", 1) or 1)
        jobs = [enrich_job(job, enrichment_version=enrichment_version) for job in jobs]
        stored = upsert_jobs(cfg.db_dsn, run_id, jobs)

        # Summarize newly ingested jobs that have descriptions
        try:
            summarize_new_jobs(cfg.db_dsn, jobs)
        except Exception as exc:
            logger.warning("Inline summarization failed: %s", exc)

        # Record per-site results in run_sources
        for result in site_results:
            record_source_result(
                cfg.db_dsn,
                run_id,
                source="custom_scraper",
                source_target=str(result.site_id),
                jobs_fetched=result.jobs_found,
                jobs_after_dedupe=result.jobs_found,  # No cross-site deduplication yet
                error_message=result.error,
                error_code="scrape_error" if result.error else None,
            )

        # Calculate run status based on failure rate
        success_count = sum(1 for r in site_results if r.success)

        if success_count == len(site_results):
            status = "success"
        elif success_count > 0:
            status = "partial"
        else:
            status = "error"

        finish_run(cfg.db_dsn, run_id, stored, status=status)
        logger.info(f"Scraped {stored} jobs from {success_count}/{len(site_results)} sites")

    except Exception as exc:
        finish_run(cfg.db_dsn, run_id, 0, status="error")
        raise

    finally:
        purge_old_runs(cfg.db_dsn, cfg.retention_days)

    return str(run_id)


async def _scrape_due_sites(cfg: Config, limit: int, dry_run: bool) -> Tuple[List[Job], List[SiteResult]]:
    """
    Query sites due for scraping and scrape them in parallel.

    Strict filters:
    - enabled=True
    - detected_ats='custom'
    - robots_allowed=True or NULL (unprobed sites are allowed)
    - careers_url not null
    - next_scrape_at <= now() or NULL (unscheduled sites scrape immediately)
    - selector_hints validation gate (only when selector_hints present)

    Args:
        cfg: Application config
        limit: Maximum number of sites to scrape
        dry_run: If True, only return empty results for logging

    Returns:
        Tuple of (all_jobs, site_results)
    """
    session = get_session(cfg.db_dsn)

    try:
        now = datetime.utcnow()
        sites = (
            session.query(ScrapeSite)
            .filter(
                ScrapeSite.enabled == True,
                ScrapeSite.detected_ats == "custom",
                or_(ScrapeSite.robots_allowed == True, ScrapeSite.robots_allowed == None),
                ScrapeSite.careers_url != None,
                or_(ScrapeSite.next_scrape_at <= now, ScrapeSite.next_scrape_at == None),
            )
            .order_by(
                ScrapeSite.priority.asc().nullslast(),
                ScrapeSite.next_scrape_at.asc().nullsfirst(),
            )
            .limit(limit)
            .all()
        )

        # Detach from session for async work
        for site in sites:
            session.expunge(site)
    finally:
        session.close()

    logger.info(
        "Site query returned %d eligible sites (enabled=True, ats=custom, careers_url set, schedule due)",
        len(sites),
    )

    if not sites:
        return [], []

    discovery_cfg = cfg.discovery
    min_confidence = float(discovery_cfg.get("selector_min_confidence", 0.6))
    require_approved = bool(discovery_cfg.get("require_approved_selectors", True))
    ready_sites = []
    for site in sites:
        # Sites without selector_hints can still be scraped by the cascade
        # (JSON-LD, RSS, link-graph, LLM). Only gate on selector quality
        # when the site actually has selectors that CSS parsing would use.
        if site.selector_hints:
            ready, reason = selector_hints_ready_for_scrape(
                site.selector_hints,
                selector_confidence=site.selector_confidence,
                min_confidence=min_confidence,
                require_approved=require_approved,
            )
            if not ready:
                logger.info(
                    "Selector gate failed for %s (site_id=%s): %s — falling back to LLM cascade",
                    site.company_name,
                    site.id,
                    reason,
                )
                site.selector_hints = None
        ready_sites.append(site)

    skipped = len(sites) - len(ready_sites)
    if skipped:
        logger.info("Selector gate filtered out %d/%d sites", skipped, len(sites))
    sites = ready_sites
    if not sites:
        logger.info("All sites filtered out by selector gate (require_approved=%s, min_confidence=%.2f)",
                     require_approved, min_confidence)
        return [], []

    if dry_run:
        # Return dummy results for dry run
        return [], [SiteResult(s.id, True, 0) for s in sites]

    # Scrape all sites (sequentially for now to avoid overwhelming targets)
    all_jobs = []
    site_results = []

    for site in sites:
        try:
            jobs, result = await scrape_site(site, cfg)
        except Exception as exc:
            logger.error("Unhandled error scraping %s: %s", site.company_name, exc, exc_info=True)
            jobs = []
            result = SiteResult(
                site_id=site.id, success=False, jobs_found=0,
                error=f"unhandled: {exc}",
            )

        all_jobs.extend(jobs)
        site_results.append(result)

        # Update site state after each attempt
        scrape_interval = getattr(site, "scrape_interval_hours", None) or 6
        _update_site_after_scrape(cfg.db_dsn, site.id, result, hours=scrape_interval)

    return all_jobs, site_results


def _update_site_after_scrape(dsn: str, site_id, result: SiteResult, hours: int = 6) -> None:
    """
    Update site state after a scrape attempt: schedule next run,
    track consecutive failures, auto-disable broken sites, and
    handle stale api_spy endpoints.
    """
    now = datetime.utcnow()
    updates: dict = {
        "next_scrape_at": now + timedelta(hours=hours),
        "last_scraped_at": now,
    }

    if result.success:
        updates["consecutive_failures"] = 0
        updates["last_error_code"] = None
        updates["last_error"] = None
        updates["last_success_at"] = now
    else:
        updates["consecutive_failures"] = func.coalesce(ScrapeSite.consecutive_failures, 0) + 1
        updates["last_error_code"] = (result.error or "unknown")[:32]
        updates["last_error"] = result.error

    # Stale api_spy endpoint — fall back to HTML scraping next run
    if getattr(result, "needs_reprobe", False):
        updates["fetch_mode"] = "static"
        updates["api_endpoint"] = None
        logger.info("Cleared stale api_spy endpoint for site_id=%s, falling back to static", site_id)

    with session_scope(dsn) as session:
        session.query(ScrapeSite).filter(ScrapeSite.id == site_id).update(updates)

        # Auto-disable after too many consecutive failures
        if not result.success:
            site = session.query(ScrapeSite).filter(ScrapeSite.id == site_id).first()
            if site and site.consecutive_failures >= (site.max_failures or 5):
                session.query(ScrapeSite).filter(ScrapeSite.id == site_id).update({"enabled": False})
                logger.warning(
                    "Auto-disabled %s (site_id=%s) after %d consecutive failures",
                    site.company_name, site_id, site.consecutive_failures,
                )
