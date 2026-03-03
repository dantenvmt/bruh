"""
Scraping ingestion runner: scrape custom career sites and persist to Postgres.

Follows the same pattern as ingest.py but for direct career site scraping.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta
from typing import List, Tuple

from .config import Config
from .models import Job
from .discovery.selectors import selector_hints_ready_for_scrape
from .scraping.models import ScrapeSite
from .scraping.types import SiteResult, convert_to_job_models
from .scraping.fetchers.static import fetch_static
from .scraping.fetchers.browser import fetch_with_browser
from .scraping.parsers.css import parse as parse_css, ParseError
from .storage import (
    finish_run,
    get_session,
    purge_old_runs,
    record_source_result,
    session_scope,
    start_run,
    upsert_jobs,
)
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

        # Enrich with visa tags before upserting
        jobs = enrich_jobs_with_visa_tags(jobs, cfg)
        stored = upsert_jobs(cfg.db_dsn, run_id, jobs)

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
        failure_rate = (len(site_results) - success_count) / len(site_results) if site_results else 0

        if failure_rate == 0:
            status = "success"
        elif failure_rate >= 0.5:
            status = "partial"
        else:
            status = "success"

        finish_run(cfg.db_dsn, run_id, stored, status=status)
        logger.info(f"Scraped {stored} jobs from {success_count}/{len(site_results)} sites")

    except Exception as exc:
        finish_run(cfg.db_dsn, run_id, 0, status="error")
        raise exc

    finally:
        purge_old_runs(cfg.db_dsn, cfg.retention_days)

    return str(run_id)


async def _scrape_due_sites(cfg: Config, limit: int, dry_run: bool) -> Tuple[List[Job], List[SiteResult]]:
    """
    Query sites due for scraping and scrape them in parallel.

    Strict filters:
    - enabled=True
    - detected_ats='custom'
    - robots_allowed=True
    - careers_url not null
    - selector_hints present and validation gate passes
    - selector_hints review_status approved (unless config disables requirement)
    - next_scrape_at <= now()

    Args:
        cfg: Application config
        limit: Maximum number of sites to scrape
        dry_run: If True, only return empty results for logging

    Returns:
        Tuple of (all_jobs, site_results)
    """
    session = get_session(cfg.db_dsn)

    try:
        sites = (
            session.query(ScrapeSite)
            .filter(
                ScrapeSite.enabled == True,
                ScrapeSite.detected_ats == "custom",
                ScrapeSite.robots_allowed == True,
                ScrapeSite.careers_url != None,
                ScrapeSite.selector_hints != None,
                ScrapeSite.next_scrape_at <= datetime.utcnow(),
            )
            .order_by(
                ScrapeSite.priority.asc().nullslast(),
                ScrapeSite.next_scrape_at.asc(),
            )
            .limit(limit)
            .all()
        )

        # Detach from session for async work
        for site in sites:
            session.expunge(site)
    finally:
        session.close()

    if not sites:
        return [], []

    discovery_cfg = cfg.discovery
    min_confidence = float(discovery_cfg.get("selector_min_confidence", 0.6))
    require_approved = bool(discovery_cfg.get("require_approved_selectors", True))
    ready_sites = []
    for site in sites:
        ready, reason = selector_hints_ready_for_scrape(
            site.selector_hints,
            selector_confidence=site.selector_confidence,
            min_confidence=min_confidence,
            require_approved=require_approved,
        )
        if ready:
            ready_sites.append(site)
        else:
            logger.info(
                "Skipping %s (site_id=%s): selector gate failed (%s)",
                site.company_name,
                site.id,
                reason,
            )

    sites = ready_sites
    if not sites:
        return [], []

    if dry_run:
        # Return dummy results for dry run
        return [], [SiteResult(s.id, True, 0) for s in sites]

    # Scrape all sites (sequentially for now to avoid overwhelming targets)
    all_jobs = []
    site_results = []

    for site in sites:
        jobs, result = await scrape_site(site, cfg)
        all_jobs.extend(jobs)
        site_results.append(result)

        # Update next_scrape_at immediately after each attempt
        scrape_interval = getattr(site, "scrape_interval_hours", None) or 6
        _update_site_next_scrape(cfg.db_dsn, site.id, hours=scrape_interval)

    return all_jobs, site_results


async def scrape_site(site: ScrapeSite, cfg: Config) -> Tuple[List[Job], SiteResult]:
    """
    Scrape a single site and return jobs + result metadata.

    Args:
        site: Site configuration to scrape
        cfg: Application config

    Returns:
        Tuple of (jobs, site_result)
    """
    logger.info(f"Scraping {site.company_name} at {site.careers_url}")

    try:
        # Hybrid fetch strategy:
        # 1) use configured mode first
        # 2) optional fallback to the other mode when fetch/parse fails
        fetch_mode = (getattr(site, "fetch_mode", "static") or "static").lower()
        allow_fallback = bool(cfg.discovery.get("hybrid_browser_fallback", True))
        mode_order = [fetch_mode]
        if allow_fallback:
            fallback_mode = "browser" if fetch_mode != "browser" else "static"
            mode_order.append(fallback_mode)

        raw_jobs = None
        last_error = None
        for mode in mode_order:
            if mode == "browser":
                html, error = await fetch_with_browser(site.careers_url)
            else:
                html, error = await fetch_static(site.careers_url)

            if error:
                last_error = f"{mode}_fetch_error: {error}"
                logger.warning("Failed to fetch %s (%s): %s", site.careers_url, mode, error)
                continue

            try:
                raw_jobs = parse_css(html, site.selector_hints, site.careers_url)
                if mode != fetch_mode:
                    logger.info(
                        "Fallback mode succeeded for %s (primary=%s, fallback=%s)",
                        site.company_name,
                        fetch_mode,
                        mode,
                    )
                break
            except ParseError as pe:
                last_error = f"{mode}_parse_error: {pe}"
                logger.warning("Parse error for %s (%s): %s", site.company_name, mode, pe)
                continue

        if raw_jobs is None:
            return [], SiteResult(site.id, False, 0, last_error or "scrape_failed")

        if not raw_jobs:
            logger.info(f"No jobs found for {site.company_name}")
            return [], SiteResult(site.id, True, 0)

        # Convert to Job models
        jobs = convert_to_job_models(raw_jobs, site)

        logger.info(f"Scraped {len(jobs)} jobs from {site.company_name}")
        return jobs, SiteResult(site.id, True, len(jobs))

    except Exception as exc:
        logger.error(f"Error scraping {site.company_name}: {exc}", exc_info=True)
        return [], SiteResult(site.id, False, 0, str(exc))


def _update_site_next_scrape(dsn: str, site_id, hours: int = 6) -> None:
    """
    Schedule next scrape time after an attempt.

    Args:
        dsn: Database connection string
        site_id: Site UUID to update
        hours: Hours until next scrape (default 6)
    """
    with session_scope(dsn) as session:
        session.query(ScrapeSite).filter(ScrapeSite.id == site_id).update(
            {
                "next_scrape_at": datetime.utcnow() + timedelta(hours=hours),
                "last_scraped_at": datetime.utcnow(),
            }
        )
