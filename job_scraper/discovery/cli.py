"""
CLI commands for the discovery module.

Uses Typer subcommand pattern: python -m job_scraper.cli discover <subcommand>
"""
import asyncio
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional
from uuid import UUID

import typer
import yaml
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.table import Table

from ..config import Config
from ..storage import get_session
from ..scraping.models import ScrapeSite
from ..scraping.fetchers.browser import fetch_with_browser
from ..scraping.fetchers.static import fetch_static
from .compliance import ComplianceGate
from .dedup import DeduplicationChecker
from .probe import ATSProbe
from .resolver import URLResolver
from .selectors import (
    DEFAULT_SELECTOR_MIN_CONFIDENCE,
    DEFAULT_SELECTOR_MIN_JOBS,
    SelectorDetector,
    build_selector_hint_record,
    selector_hints_ready_for_scrape,
    validate_selector_hints,
)
from .sources import CompanySource
from .types import ATSType, DiscoverySource, DiscoveryStats, SelectorHint


logger = logging.getLogger(__name__)

# Create Typer app for discover subcommand
app = typer.Typer(help="Career site discovery commands")

# Console for output
console = Console()


def _get_db_session():
    """Get a database session."""
    cfg = Config()
    if not cfg.db_dsn:
        console.print("[red]DB DSN not configured. Set JOB_SCRAPER_DB_DSN[/red]")
        raise typer.Exit(code=1)
    return get_session(cfg.db_dsn)


def _append_discovery_note(existing: Optional[str], note: str) -> str:
    base = (existing or "").strip()
    addition = (note or "").strip()
    if not addition:
        return base
    if not base:
        return addition
    return f"{base} | {addition}"


async def _fetch_detection_html(
    url: str,
    *,
    prefer_browser: bool = True,
) -> tuple[Optional[str], str, Optional[str]]:
    """
    Fetch HTML for selector detection.

    Returns:
        (html, mode_used, error)
    """
    if prefer_browser:
        html, err = await fetch_with_browser(url)
        if html:
            return html, "browser", None
        fallback_html, fallback_err = await fetch_static(url)
        if fallback_html:
            return fallback_html, "static", None
        return None, "none", err or fallback_err or "fetch_failed"

    html, err = await fetch_static(url)
    if html:
        return html, "static", None

    browser_html, browser_err = await fetch_with_browser(url)
    if browser_html:
        return browser_html, "browser", None
    return None, "none", err or browser_err or "fetch_failed"


@app.command("build-list")
def build_list(
    source: str = typer.Option(
        ...,
        "--source",
        "-s",
        help="Source to load companies from: seed_csv, hardcoded",
    ),
    max_priority: Optional[int] = typer.Option(
        None,
        "--max-priority",
        "-p",
        help="For seed_csv: only include companies with priority <= this value",
    ),
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="Preview without writing to database",
    ),
):
    """
    Build company list from a source and insert into scrape_sites.

    Upserts on (company_name, source) - safe to re-run.
    """
    # Validate source
    try:
        source_enum = DiscoverySource(source)
    except ValueError:
        console.print(f"[red]Invalid source: {source}. Use: seed_csv, hardcoded[/red]")
        raise typer.Exit(code=1)

    if source_enum in (DiscoverySource.FORTUNE500, DiscoverySource.YC):
        console.print(f"[yellow]Source '{source}' is deferred (not implemented in Phase 1)[/yellow]")
        raise typer.Exit(code=1)

    company_source = CompanySource()

    # Load companies
    companies = list(company_source.load(source_enum, max_priority))

    if not companies:
        console.print(f"[yellow]No companies found from source: {source}[/yellow]")
        return

    console.print(f"[cyan]Loaded {len(companies)} companies from {source}[/cyan]")

    if dry_run:
        # Just show what would be inserted
        table = Table(show_header=True, header_style="bold magenta")
        table.add_column("Company", style="cyan")
        table.add_column("Priority", style="green")
        table.add_column("Category", style="yellow")
        table.add_column("Careers URL", style="blue")

        for company in companies[:20]:
            table.add_row(
                company.name,
                str(company.priority or "-"),
                company.category or "-",
                company.careers_url or "-",
            )

        console.print(table)
        if len(companies) > 20:
            console.print(f"[dim]... and {len(companies) - 20} more[/dim]")
        console.print(f"\n[yellow]Dry run - no changes made[/yellow]")
        return

    # Insert into database
    session = _get_db_session()
    inserted = 0
    skipped = 0

    try:
        for company in companies:
            # Check for existing record (upsert logic)
            existing = session.query(ScrapeSite).filter(
                ScrapeSite.company_name == company.name,
                ScrapeSite.source == source,
            ).first()

            if existing:
                skipped += 1
                continue

            # Create new record
            # Use None for careers_url if not provided (empty string causes unique constraint issues)
            site = ScrapeSite(
                company_name=company.name,
                careers_url=company.careers_url if company.careers_url else None,
                source=source,
                priority=company.priority,
                enabled=False,  # Not enabled until URL resolved and probed
            )
            session.add(site)
            inserted += 1

        session.commit()
        console.print(f"[green]Inserted {inserted} companies, skipped {skipped} existing[/green]")

    except Exception as e:
        session.rollback()
        console.print(f"[red]Error inserting companies: {e}[/red]")
        raise typer.Exit(code=1)
    finally:
        session.close()


@app.command("resolve-urls")
def resolve_urls(
    limit: int = typer.Option(
        50,
        "--limit",
        "-l",
        help="Maximum number of sites to resolve",
    ),
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="Preview without updating database",
    ),
):
    """
    Resolve careers URLs for companies without them.

    Only updates rows where careers_url IS NULL or empty.
    """
    session = _get_db_session()

    try:
        # Find sites without URLs - get IDs and names only to avoid session detachment
        sites_data = session.query(ScrapeSite.id, ScrapeSite.company_name).filter(
            (ScrapeSite.careers_url == None) | (ScrapeSite.careers_url == "")
        ).limit(limit).all()

        if not sites_data:
            console.print("[yellow]No sites need URL resolution[/yellow]")
            return

        console.print(f"Resolving URLs for {len(sites_data)} sites...")

        async def resolve_all():
            async with URLResolver() as resolver:
                results = []
                for site_id, company_name in sites_data:
                    console.print(f"  Resolving: {company_name}...", end="")
                    url = await resolver.resolve(company_name)
                    results.append((site_id, company_name, url))
                    if url:
                        console.print(f" {url}")
                    else:
                        console.print(" [failed]")
                return results

        results = asyncio.run(resolve_all())

        resolved = 0
        failed = 0

        for site_id, company_name, url in results:
            if url:
                if not dry_run:
                    # Update by ID to avoid session issues
                    session.query(ScrapeSite).filter(ScrapeSite.id == site_id).update(
                        {"careers_url": url}
                    )
                resolved += 1
            else:
                failed += 1

        if not dry_run:
            session.commit()
            console.print(f"\nResolved {resolved} URLs, {failed} failed")
        else:
            console.print(f"\nDry run - {resolved} would be resolved, {failed} failed")

    except Exception as e:
        session.rollback()
        console.print(f"Error: {e}")
        raise typer.Exit(code=1)
    finally:
        session.close()


@app.command("probe-ats")
def probe_ats(
    limit: int = typer.Option(
        20,
        "--limit",
        "-l",
        help="Maximum number of sites to probe",
    ),
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="Preview without updating database",
    ),
):
    """
    Probe sites for ATS type detection.

    Only updates rows where detected_ats IS NULL AND careers_url IS NOT NULL.
    """
    session = _get_db_session()

    try:
        # Find sites to probe - get IDs and URLs to avoid session detachment
        sites_data = session.query(
            ScrapeSite.id, ScrapeSite.company_name, ScrapeSite.careers_url
        ).filter(
            ScrapeSite.detected_ats == None,
            ScrapeSite.careers_url != None,
            ScrapeSite.careers_url != "",
        ).limit(limit).all()

        if not sites_data:
            console.print("No sites need ATS probing")
            return

        console.print(f"Probing ATS for {len(sites_data)} sites...")

        async def probe_all():
            async with ATSProbe() as probe:
                results = []
                for site_id, company_name, careers_url in sites_data:
                    console.print(f"  Probing: {company_name}...", end="")
                    result = await probe.probe(careers_url)
                    results.append((site_id, company_name, result))
                    status = "OK" if result.robots_allowed else "blocked"
                    console.print(f" {result.detected_ats.value} ({status})")
                return results

        results = asyncio.run(probe_all())

        # Count by ATS type
        ats_counts = {}

        for site_id, company_name, result in results:
            ats = result.detected_ats.value
            ats_counts[ats] = ats_counts.get(ats, 0) + 1

            if not dry_run:
                # Update by ID to avoid session issues
                session.query(ScrapeSite).filter(ScrapeSite.id == site_id).update({
                    "detected_ats": ats,
                    "detection_probed_at": datetime.utcnow(),
                    "fetch_mode": result.fetch_mode,
                    "robots_allowed": result.robots_allowed,
                })

        if not dry_run:
            session.commit()

        console.print("\nATS Distribution:")
        for ats, count in sorted(ats_counts.items(), key=lambda x: -x[1]):
            console.print(f"  {ats}: {count}")

    except Exception as e:
        session.rollback()
        console.print(f"Error: {e}")
        raise typer.Exit(code=1)
    finally:
        session.close()


@app.command("detect-selectors")
def detect_selectors(
    limit: int = typer.Option(
        10,
        "--limit",
        "-l",
        help="Maximum number of sites to detect selectors for",
    ),
    min_confidence: float = typer.Option(
        DEFAULT_SELECTOR_MIN_CONFIDENCE,
        "--min-confidence",
        help="Minimum confidence required for selector hints",
    ),
    min_jobs: int = typer.Option(
        DEFAULT_SELECTOR_MIN_JOBS,
        "--min-jobs",
        help="Minimum extracted jobs required during validation",
    ),
    prefer_browser: bool = typer.Option(
        True,
        "--prefer-browser/--prefer-static",
        help="Use Playwright first, then fallback to static HTTP",
    ),
    auto_approve: bool = typer.Option(
        False,
        "--auto-approve",
        help="Mark validated selectors as approved (not recommended for production)",
    ),
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="Preview without updating database",
    ),
):
    """
    Generate selector hints for custom sites.

    Only updates rows where detected_ats='custom' AND selector_hints IS NULL.
    """
    session = _get_db_session()

    try:
        # Find custom sites without selectors - get IDs and URLs to avoid session detachment
        sites_data = session.query(
            ScrapeSite.id,
            ScrapeSite.company_name,
            ScrapeSite.careers_url,
            ScrapeSite.fetch_mode,
            ScrapeSite.discovery_notes,
        ).filter(
            ScrapeSite.detected_ats == "custom",
            ScrapeSite.selector_hints == None,
            ScrapeSite.careers_url != None,
        ).limit(limit).all()

        if not sites_data:
            console.print("No custom sites need selector detection")
            return

        console.print(f"Detecting selectors for {len(sites_data)} sites...")

        detector = SelectorDetector()
        updates = []  # Collect updates to apply after async completes

        async def detect_all():
            for site_id, company_name, careers_url, fetch_mode, discovery_notes in sites_data:
                console.print(f"  Detecting: {company_name}...", end="")
                try:
                    site_prefers_browser = (fetch_mode or "").strip().lower() == "browser"
                    html, extraction_mode, fetch_error = await _fetch_detection_html(
                        careers_url,
                        prefer_browser=prefer_browser or site_prefers_browser,
                    )
                    if not html:
                        updates.append(
                            (
                                site_id,
                                None,
                                None,
                                _append_discovery_note(discovery_notes, f"selector_detect_fetch_failed: {fetch_error}"),
                            )
                        )
                        console.print(f" fetch failed ({fetch_error})")
                        continue

                    hint = await detector.detect(
                        html,
                        careers_url,
                        min_confidence=min_confidence,
                    )
                    if not hint:
                        updates.append(
                            (
                                site_id,
                                None,
                                None,
                                _append_discovery_note(
                                    discovery_notes,
                                    f"selector_detect_none (min_conf={min_confidence:.2f}, mode={extraction_mode})",
                                ),
                            )
                        )
                        console.print(" no selector hints")
                        continue

                    passed, validation = validate_selector_hints(
                        html,
                        careers_url,
                        hint,
                        min_confidence=min_confidence,
                        min_jobs=min_jobs,
                        extraction_mode=extraction_mode,
                    )
                    review_status = "approved" if (passed and auto_approve) else "proposed"
                    hint_record = build_selector_hint_record(
                        hint,
                        validation=validation,
                        review_status=review_status,
                        extraction_mode=extraction_mode,
                    )
                    note = _append_discovery_note(
                        discovery_notes,
                        f"selector_detected conf={hint.confidence:.2f} validated={passed} mode={extraction_mode}",
                    )
                    updates.append((site_id, hint_record, hint.confidence, note))

                    if passed:
                        status = "validated+approved" if auto_approve else "validated (pending approval)"
                        console.print(f" confidence {hint.confidence:.2f} {status}")
                    else:
                        console.print(f" rejected ({validation.get('reason', 'validation_failed')})")
                except Exception as e:
                    updates.append(
                        (
                            site_id,
                            None,
                            None,
                            _append_discovery_note(discovery_notes, f"selector_detect_error: {type(e).__name__}"),
                        )
                    )
                    console.print(f" error: {e}")

        asyncio.run(detect_all())

        # Apply updates
        if not dry_run:
            for site_id, hints, confidence, note in updates:
                values = {"discovery_notes": note}
                if hints is not None:
                    values["selector_hints"] = hints
                    values["selector_confidence"] = confidence
                session.query(ScrapeSite).filter(ScrapeSite.id == site_id).update(values)
            session.commit()
            generated = sum(1 for _, hints, _, _ in updates if hints is not None)
            console.print(f"\nGenerated selector hints for {generated}/{len(updates)} sites")
        else:
            generated = sum(1 for _, hints, _, _ in updates if hints is not None)
            console.print(f"\nDry run - would generate hints for {generated}/{len(updates)} sites")

    except Exception as e:
        session.rollback()
        console.print(f"Error: {e}")
        raise typer.Exit(code=1)
    finally:
        session.close()


@app.command("validate-selectors")
def validate_selectors(
    limit: int = typer.Option(
        20,
        "--limit",
        "-l",
        help="Maximum number of custom sites to validate",
    ),
    min_confidence: float = typer.Option(
        DEFAULT_SELECTOR_MIN_CONFIDENCE,
        "--min-confidence",
        help="Minimum confidence required for selector hints",
    ),
    min_jobs: int = typer.Option(
        DEFAULT_SELECTOR_MIN_JOBS,
        "--min-jobs",
        help="Minimum extracted jobs required",
    ),
    prefer_browser: bool = typer.Option(
        True,
        "--prefer-browser/--prefer-static",
        help="Use Playwright first, then fallback to static HTTP",
    ),
    auto_approve: bool = typer.Option(
        False,
        "--auto-approve",
        help="Approve selectors immediately if validation passes",
    ),
    enable_approved: bool = typer.Option(
        False,
        "--enable-approved",
        help="Enable approved sites and schedule immediate scrape",
    ),
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="Preview changes only",
    ),
):
    """
    Re-validate existing selector hints against live pages.
    """
    session = _get_db_session()

    try:
        sites_data = session.query(
            ScrapeSite.id,
            ScrapeSite.company_name,
            ScrapeSite.careers_url,
            ScrapeSite.fetch_mode,
            ScrapeSite.selector_hints,
            ScrapeSite.selector_confidence,
            ScrapeSite.discovery_notes,
        ).filter(
            ScrapeSite.detected_ats == "custom",
            ScrapeSite.careers_url != None,
            ScrapeSite.selector_hints != None,
        ).limit(limit).all()

        if not sites_data:
            console.print("No custom sites with selector hints found")
            return

        console.print(f"Validating selectors for {len(sites_data)} sites...")
        updates = []

        async def validate_all():
            for (
                site_id,
                company_name,
                careers_url,
                fetch_mode,
                selector_hints,
                selector_confidence,
                discovery_notes,
            ) in sites_data:
                console.print(f"  Validating: {company_name}...", end="")
                try:
                    site_prefers_browser = (fetch_mode or "").strip().lower() == "browser"
                    html, extraction_mode, fetch_error = await _fetch_detection_html(
                        careers_url,
                        prefer_browser=prefer_browser or site_prefers_browser,
                    )
                    if not html:
                        note = _append_discovery_note(
                            discovery_notes,
                            f"selector_validate_fetch_failed: {fetch_error}",
                        )
                        updates.append((site_id, selector_hints, selector_confidence, note, False, False))
                        console.print(f" fetch failed ({fetch_error})")
                        continue

                    selector_hint = (
                        SelectorHint.from_dict(selector_hints)
                        if isinstance(selector_hints, dict)
                        else None
                    )
                    passed, validation = validate_selector_hints(
                        html,
                        careers_url,
                        selector_hint,
                        min_confidence=min_confidence,
                        min_jobs=min_jobs,
                        extraction_mode=extraction_mode,
                    )
                    hints_out = dict(selector_hints or {})
                    hints_out["validation"] = validation
                    if passed and auto_approve:
                        hints_out["review_status"] = "approved"
                    elif passed and hints_out.get("review_status") not in {"approved", "manual_approved"}:
                        hints_out["review_status"] = "proposed"
                    elif not passed:
                        hints_out["review_status"] = "rejected"

                    confidence_out = selector_confidence
                    if selector_hint is not None:
                        confidence_out = selector_hint.confidence

                    note = _append_discovery_note(
                        discovery_notes,
                        f"selector_validated passed={passed} mode={extraction_mode}",
                    )
                    updates.append(
                        (
                            site_id,
                            hints_out,
                            confidence_out,
                            note,
                            passed and auto_approve and enable_approved,
                            passed,
                        )
                    )
                    if passed:
                        msg = "validated+approved" if auto_approve else "validated"
                        console.print(f" {msg}")
                    else:
                        console.print(f" failed ({validation.get('reason', 'validation_failed')})")
                except Exception as exc:
                    note = _append_discovery_note(
                        discovery_notes,
                        f"selector_validate_error: {type(exc).__name__}",
                    )
                    updates.append((site_id, selector_hints, selector_confidence, note, False, False))
                    console.print(f" error ({exc})")

        asyncio.run(validate_all())

        if not dry_run:
            now = datetime.utcnow()
            for site_id, hints, confidence, note, enable_now, passed in updates:
                values = {
                    "selector_hints": hints,
                    "selector_confidence": confidence,
                    "discovery_notes": note,
                }
                if enable_now:
                    values["enabled"] = True
                    values["next_scrape_at"] = now
                session.query(ScrapeSite).filter(ScrapeSite.id == site_id).update(values)
            session.commit()

        passed_count = sum(1 for _, _, _, _, _, passed in updates if passed)
        console.print(f"\nValidated {passed_count}/{len(updates)} sites")
        if dry_run:
            console.print("[yellow]Dry run - no changes written[/yellow]")

    except Exception as e:
        session.rollback()
        console.print(f"Error: {e}")
        raise typer.Exit(code=1)
    finally:
        session.close()


@app.command("approve-selectors")
def approve_selectors(
    site_id: str = typer.Argument(..., help="Site ID (UUID) to approve"),
    enable: bool = typer.Option(
        True,
        "--enable/--no-enable",
        help="Enable site immediately after approval",
    ),
    schedule_in_minutes: int = typer.Option(
        0,
        "--schedule-in-minutes",
        help="Delay next_scrape_at by N minutes (default: immediate)",
    ),
):
    """
    Manually approve validated selectors for production scraping.
    """
    session = _get_db_session()

    try:
        try:
            site_uuid = UUID(site_id)
        except ValueError:
            console.print(f"[red]Invalid UUID: {site_id}[/red]")
            raise typer.Exit(code=1)

        site = session.query(ScrapeSite).filter(ScrapeSite.id == site_uuid).first()
        if not site:
            console.print(f"[red]Site not found: {site_id}[/red]")
            raise typer.Exit(code=1)

        ready, reason = selector_hints_ready_for_scrape(
            site.selector_hints,
            selector_confidence=site.selector_confidence,
            require_approved=False,
        )
        if not ready:
            console.print(f"[red]Cannot approve selectors: {reason}[/red]")
            raise typer.Exit(code=1)

        hints = dict(site.selector_hints or {})
        hints["review_status"] = "approved"

        values = {
            "selector_hints": hints,
            "selector_confidence": site.selector_confidence,
            "discovery_notes": _append_discovery_note(site.discovery_notes, "selectors approved"),
        }
        if enable:
            values["enabled"] = True
            values["next_scrape_at"] = datetime.utcnow() + timedelta(minutes=max(0, schedule_in_minutes))

        session.query(ScrapeSite).filter(ScrapeSite.id == site_uuid).update(values)
        session.commit()
        console.print(f"[green]Approved selectors for {site.company_name}[/green]")

    except Exception as e:
        session.rollback()
        console.print(f"Error: {e}")
        raise typer.Exit(code=1)
    finally:
        session.close()


@app.command("status")
def status():
    """
    Show discovery status: total / probed / by-ats-type / custom-ready.
    """
    session = _get_db_session()

    try:
        # Total count
        total = session.query(ScrapeSite).count()

        # With URLs
        with_urls = session.query(ScrapeSite).filter(
            ScrapeSite.careers_url != None,
            ScrapeSite.careers_url != "",
        ).count()

        # Probed
        probed = session.query(ScrapeSite).filter(
            ScrapeSite.detected_ats != None,
        ).count()

        # By ATS type
        from sqlalchemy import func
        ats_counts = session.query(
            ScrapeSite.detected_ats,
            func.count(ScrapeSite.id),
        ).filter(
            ScrapeSite.detected_ats != None,
        ).group_by(
            ScrapeSite.detected_ats,
        ).all()

        # Custom selector state
        custom_selector_rows = session.query(ScrapeSite.selector_hints).filter(
            ScrapeSite.detected_ats == "custom",
            ScrapeSite.selector_hints != None,
        ).all()
        custom_with_selectors = len(custom_selector_rows)
        custom_validated = 0
        custom_approved = 0
        for (hints,) in custom_selector_rows:
            if not isinstance(hints, dict):
                continue
            validation = hints.get("validation") or {}
            if bool(validation.get("passed")):
                custom_validated += 1
            review_status = str(hints.get("review_status", "")).strip().lower()
            if review_status in {"approved", "manual_approved"}:
                custom_approved += 1

        # Robots blocked
        robots_blocked = session.query(ScrapeSite).filter(
            ScrapeSite.robots_allowed == False,
        ).count()

        # High priority custom (for Phase 2 gate)
        high_priority_custom = session.query(ScrapeSite).filter(
            ScrapeSite.detected_ats == "custom",
            ScrapeSite.priority <= 2,
        ).count()

        console.print("[bold]Discovery Status[/bold]")
        console.print("=" * 50)
        console.print(f"Total sites:         {total}")
        console.print(f"With careers URL:    {with_urls}")
        console.print(f"ATS probed:          {probed}")
        console.print(f"Robots blocked:      {robots_blocked}")
        console.print()

        if ats_counts:
            console.print("[cyan]By ATS Type:[/cyan]")
            for ats, count in sorted(ats_counts, key=lambda x: -x[1]):
                ats_type = ATSType(ats) if ats else ATSType.UNKNOWN
                marker = ""
                if ats_type.has_existing_adapter:
                    marker = " [green](has adapter)[/green]"
                elif ats_type.is_deferred:
                    marker = " [yellow](deferred)[/yellow]"
                console.print(f"  {ats or 'unknown'}: {count}{marker}")

        console.print()
        custom_sites_total = next((count for ats, count in ats_counts if ats == "custom"), 0)
        console.print("[cyan]Phase 2 Readiness:[/cyan]")
        console.print(f"  Custom sites:              {custom_sites_total}")
        console.print(f"  Custom with selectors:     {custom_with_selectors}")
        console.print(f"  Custom validated selectors:{custom_validated}")
        console.print(f"  Custom approved selectors: {custom_approved}")
        console.print(f"  High-priority custom (P1/P2): {high_priority_custom}")

        # Gate check
        console.print()
        if high_priority_custom >= 30:
            console.print("[green]G1 PASS: >= 30 high-priority custom sites[/green]")
        else:
            console.print(f"[red]G1 FAIL: {high_priority_custom}/30 high-priority custom sites[/red]")

    finally:
        session.close()


@app.command("export-ats")
def export_ats(
    ats: str = typer.Option(
        ...,
        "--ats",
        "-a",
        help="ATS type to export: greenhouse, lever, ashby, smartrecruiters, workday",
    ),
    output: Path = typer.Option(
        None,
        "--output",
        "-o",
        help="Output YAML file path (default: data/{ats}_discovered.yaml)",
    ),
):
    """
    Export discovered ATS sites to YAML for existing adapters.

    Exports sites that were detected as using a supported ATS platform.
    """
    # Validate ATS type
    valid_ats = {"greenhouse", "lever", "ashby", "smartrecruiters", "workday"}
    ats_lower = ats.lower()
    if ats_lower not in valid_ats:
        console.print(f"[red]Invalid ATS: {ats}. Use: {', '.join(valid_ats)}[/red]")
        raise typer.Exit(code=1)

    if output is None:
        output = Path(f"data/{ats_lower}_discovered.yaml")

    session = _get_db_session()

    try:
        # Find sites with this ATS
        sites = session.query(ScrapeSite).filter(
            ScrapeSite.detected_ats == ats_lower,
            ScrapeSite.robots_allowed == True,
        ).all()

        if not sites:
            console.print(f"[yellow]No {ats} sites found[/yellow]")
            return

        if ats_lower == "workday":
            from .workday_export import export_workday_sites_to_yaml

            urls = [s.careers_url for s in sites if s.careers_url]
            result = export_workday_sites_to_yaml(urls, output, timeout=15.0, validate=True)
            console.print(
                f"[green]Exported {result.get('exported', 0)} workday sites to {output}[/green]"
            )
            console.print("[dim]To use: set JOB_SCRAPER_CONFIG_INCLUDES to include this file[/dim]")
            return

        # Extract ATS tokens from URLs
        from .probe import extract_ats_token
        tokens = []
        for site in sites:
            token = extract_ats_token(site.careers_url, ATSType(ats_lower))
            if token and token not in tokens:
                tokens.append(token)

        if not tokens:
            console.print(f"[yellow]No tokens extracted from {len(sites)} sites[/yellow]")
            return

        # Build YAML structure matching existing adapter configs
        if ats_lower == "greenhouse":
            config = {"greenhouse": {"boards": tokens}}
        elif ats_lower == "lever":
            config = {"lever": {"sites": tokens}}
        elif ats_lower == "ashby":
            config = {"ashby": {"companies": tokens}}
        elif ats_lower == "smartrecruiters":
            config = {"smartrecruiters": {"companies": tokens}}

        # Write YAML
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")

        console.print(f"[green]Exported {len(tokens)} {ats} tokens to {output}[/green]")
        console.print("[dim]To use: set JOB_SCRAPER_CONFIG_INCLUDES to include this file[/dim]")

    finally:
        session.close()


@app.command("review")
def review(
    site_id: str = typer.Argument(..., help="Site ID (UUID) to review"),
):
    """
    Interactive review of a single site.

    Shows all discovery data and allows updating notes.
    """
    session = _get_db_session()

    try:
        # Parse UUID
        try:
            uuid = UUID(site_id)
        except ValueError:
            console.print(f"[red]Invalid UUID: {site_id}[/red]")
            raise typer.Exit(code=1)

        site = session.query(ScrapeSite).filter(ScrapeSite.id == uuid).first()

        if not site:
            console.print(f"[red]Site not found: {site_id}[/red]")
            raise typer.Exit(code=1)

        console.print("[bold]Site Details[/bold]")
        console.print("=" * 50)
        console.print(f"ID:              {site.id}")
        console.print(f"Company:         {site.company_name}")
        console.print(f"Careers URL:     {site.careers_url or '[not set]'}")
        console.print(f"Source:          {site.source or '[unknown]'}")
        console.print(f"Priority:        {site.priority or '-'}")
        console.print(f"Detected ATS:    {site.detected_ats or '[not probed]'}")
        console.print(f"Fetch Mode:      {site.fetch_mode or 'static'}")
        console.print(f"Robots Allowed:  {site.robots_allowed}")
        console.print(f"Enabled:         {site.enabled}")
        console.print()

        if site.selector_hints:
            console.print("[cyan]Selector Hints:[/cyan]")
            for key, value in site.selector_hints.items():
                if key == "validation":
                    continue
                if value:
                    console.print(f"  {key}: {value}")
            console.print(f"  confidence: {site.selector_confidence:.2f}")
            validation = site.selector_hints.get("validation") if isinstance(site.selector_hints, dict) else None
            if isinstance(validation, dict):
                console.print(f"  validation.passed: {validation.get('passed')}")
                console.print(f"  validation.jobs_found: {validation.get('jobs_found')}")
                console.print(f"  validation.reason: {validation.get('reason')}")
            ready, reason = selector_hints_ready_for_scrape(
                site.selector_hints,
                selector_confidence=site.selector_confidence,
                require_approved=True,
            )
            console.print(f"  scrape_ready: {ready} ({reason})")
        else:
            console.print("[yellow]No selector hints[/yellow]")

        console.print()
        if site.discovery_notes:
            console.print(f"[cyan]Notes:[/cyan] {site.discovery_notes}")
        else:
            console.print("[dim]No notes[/dim]")

    finally:
        session.close()
