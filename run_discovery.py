#!/usr/bin/env python
"""
Helper script to run discovery pipeline steps.
Usage: python run_discovery.py <command>
Commands: import, status, resolve, probe, selectors, export_workday
"""
import sys
import asyncio
from datetime import datetime
from pathlib import Path

from job_scraper.config import Config
from job_scraper.storage import get_session
from job_scraper.scraping.models import ScrapeSite
from job_scraper.scraping.fetchers.browser import fetch_with_browser
from job_scraper.scraping.fetchers.static import fetch_static
from job_scraper.discovery.workday_export import export_workday_sites_to_yaml
from job_scraper.discovery.resolver import URLResolver
from job_scraper.discovery.probe import ATSProbe
from job_scraper.discovery.sources import CompanySource
from job_scraper.discovery.types import DiscoverySource
from job_scraper.discovery.selectors import (
    DEFAULT_SELECTOR_MIN_CONFIDENCE,
    DEFAULT_SELECTOR_MIN_JOBS,
    SelectorDetector,
    build_selector_hint_record,
    validate_selector_hints,
)


def get_db_session():
    cfg = Config()
    if not cfg.db_dsn:
        print("Error: DB DSN not configured")
        sys.exit(1)
    return get_session(cfg.db_dsn)


def _append_discovery_note(existing: str | None, note: str) -> str:
    base = (existing or "").strip()
    addition = (note or "").strip()
    if not addition:
        return base
    if not base:
        return addition
    return f"{base} | {addition}"


async def _fetch_detection_html(url: str, prefer_browser: bool = True):
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
    fallback_html, fallback_err = await fetch_with_browser(url)
    if fallback_html:
        return fallback_html, "browser", None
    return None, "none", err or fallback_err or "fetch_failed"


def import_seed_companies() -> int:
    """Import companies from seed CSV into scrape_sites table.

    Upserts on (company_name, source) — safe to re-run.
    New entries are created with enabled=False.

    Returns:
        Number of newly inserted companies.
    """
    session = get_db_session()
    source_label = DiscoverySource.SEED_CSV.value  # "seed_csv"
    company_source = CompanySource()
    companies = list(company_source.load(DiscoverySource.SEED_CSV))

    if not companies:
        print("No companies found in seed CSV")
        return 0

    print(f"Loaded {len(companies)} companies from seed CSV")

    inserted = 0
    skipped = 0
    try:
        for company in companies:
            existing = session.query(ScrapeSite).filter(
                ScrapeSite.company_name == company.name,
                ScrapeSite.source == source_label,
            ).first()

            if existing:
                skipped += 1
                continue

            site = ScrapeSite(
                company_name=company.name,
                careers_url=company.careers_url if company.careers_url else None,
                source=source_label,
                priority=company.priority,
                enabled=False,
            )
            session.add(site)
            inserted += 1

        session.commit()
        print(f"Inserted {inserted} companies, skipped {skipped} existing")
    except Exception as e:
        session.rollback()
        print(f"Error importing companies: {e}")
        raise
    finally:
        session.close()

    return inserted


def show_status():
    """Show discovery status."""
    session = get_db_session()
    try:
        from sqlalchemy import func

        total = session.query(ScrapeSite).count()
        with_urls = session.query(ScrapeSite).filter(
            ScrapeSite.careers_url != None,
            ScrapeSite.careers_url != "",
        ).count()
        probed = session.query(ScrapeSite).filter(
            ScrapeSite.detected_ats != None,
        ).count()

        ats_counts = session.query(
            ScrapeSite.detected_ats,
            func.count(ScrapeSite.id),
        ).filter(
            ScrapeSite.detected_ats != None,
        ).group_by(
            ScrapeSite.detected_ats,
        ).all()

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
            status = str(hints.get("review_status", "")).strip().lower()
            if status in {"approved", "manual_approved"}:
                custom_approved += 1

        high_priority_custom = session.query(ScrapeSite).filter(
            ScrapeSite.detected_ats == "custom",
            ScrapeSite.priority != None,
            ScrapeSite.priority <= 2,
        ).count()

        print("=" * 50)
        print("Discovery Status")
        print("=" * 50)
        print(f"Total sites:         {total}")
        print(f"With careers URL:    {with_urls}")
        print(f"ATS probed:          {probed}")
        print()

        if ats_counts:
            print("By ATS Type:")
            for ats, count in sorted(ats_counts, key=lambda x: -x[1]):
                print(f"  {ats or 'unknown'}: {count}")

        print()
        print("Phase 2 Readiness:")
        custom_count = next((count for ats, count in ats_counts if ats == "custom"), 0)
        print(f"  Custom sites:              {custom_count}")
        print(f"  Custom with selectors:     {custom_with_selectors}")
        print(f"  Custom validated selectors:{custom_validated}")
        print(f"  Custom approved selectors: {custom_approved}")
        print(f"  High-priority custom (P1/P2): {high_priority_custom}")
        print()

        if high_priority_custom >= 30:
            print("G1 PASS: >= 30 high-priority custom sites")
        else:
            print(f"G1 FAIL: {high_priority_custom}/30 high-priority custom sites")

    finally:
        session.close()


def resolve_urls(limit=50) -> dict:
    """Resolve careers URLs for companies without them.

    Returns:
        Dict with keys: resolved (int), failed (int)
    """
    session = get_db_session()
    try:
        sites_data = session.query(ScrapeSite.id, ScrapeSite.company_name).filter(
            (ScrapeSite.careers_url == None) | (ScrapeSite.careers_url == "")
        ).limit(limit).all()

        if not sites_data:
            print("No sites need URL resolution")
            return {"resolved": 0, "failed": 0}

        print(f"Resolving URLs for {len(sites_data)} sites...")

        async def resolve_all():
            async with URLResolver() as resolver:
                results = []
                for site_id, company_name in sites_data:
                    print(f"  {company_name}...", end=" ", flush=True)
                    try:
                        url = await resolver.resolve(company_name)
                        results.append((site_id, company_name, url))
                        if url:
                            print(url)
                        else:
                            print("[failed]")
                    except Exception as e:
                        print(f"[error: {type(e).__name__}]")
                        results.append((site_id, company_name, None))
                return results

        results = asyncio.run(resolve_all())

        resolved = 0
        failed = 0
        for site_id, company_name, url in results:
            if url:
                try:
                    session.query(ScrapeSite).filter(ScrapeSite.id == site_id).update(
                        {"careers_url": url}
                    )
                    resolved += 1
                except Exception as e:
                    print(f"  Failed to update {company_name}: {e}")
                    failed += 1
            else:
                failed += 1

        session.commit()
        print(f"\nResolved {resolved}/{len(sites_data)} URLs ({failed} failed)")
        return {"resolved": resolved, "failed": failed}

    finally:
        session.close()


def probe_ats(limit=50) -> dict:
    """Probe sites for ATS type detection.

    Returns:
        Dict with keys: probed (int), ats_counts (dict)
    """
    session = get_db_session()
    try:
        sites_data = session.query(
            ScrapeSite.id, ScrapeSite.company_name, ScrapeSite.careers_url
        ).filter(
            ScrapeSite.detected_ats == None,
            ScrapeSite.careers_url != None,
            ScrapeSite.careers_url != "",
        ).limit(limit).all()

        if not sites_data:
            print("No sites need ATS probing")
            return {"probed": 0, "ats_counts": {}}

        print(f"Probing ATS for {len(sites_data)} sites...")

        async def probe_all():
            async with ATSProbe() as probe:
                results = []
                for site_id, company_name, careers_url in sites_data:
                    print(f"  {company_name}...", end=" ", flush=True)
                    result = await probe.probe(careers_url)
                    results.append((site_id, company_name, result))
                    status = "OK" if result.robots_allowed else "blocked"
                    print(f"{result.detected_ats.value} ({status})")
                return results

        results = asyncio.run(probe_all())

        ats_counts = {}
        for site_id, company_name, result in results:
            ats = result.detected_ats.value
            ats_counts[ats] = ats_counts.get(ats, 0) + 1
            session.query(ScrapeSite).filter(ScrapeSite.id == site_id).update({
                "detected_ats": ats,
                "detection_probed_at": datetime.utcnow(),
                "fetch_mode": result.fetch_mode,
                "robots_allowed": result.robots_allowed,
            })

        session.commit()

        print("\nATS Distribution:")
        for ats, count in sorted(ats_counts.items(), key=lambda x: -x[1]):
            print(f"  {ats}: {count}")

        return {"probed": len(results), "ats_counts": ats_counts}

    finally:
        session.close()


def export_workday_sites(data_dir: Path | None = None, validate: bool = True) -> dict:
    """
    Export discovered Workday sites from scrape_sites into a YAML include file.

    Queries rows with detected_ats='workday', resolves canonical Workday URL
    triples (host/tenant/site), validates CXS endpoints, and writes
    data/workday_sites.yaml atomically.
    """
    if data_dir is None:
        data_dir = Path(__file__).parent / "data"

    session = get_db_session()
    try:
        rows = session.query(ScrapeSite.careers_url).filter(
            ScrapeSite.detected_ats == "workday",
            ScrapeSite.careers_url != None,
            ScrapeSite.careers_url != "",
        ).all()
    finally:
        session.close()

    urls = [url for (url,) in rows if url]
    output_path = data_dir / "workday_sites.yaml"
    result = export_workday_sites_to_yaml(
        urls,
        output_path,
        timeout=15.0,
        validate=validate,
    )
    print(f"Exported {result.get('exported', 0)} Workday sites to {output_path}")
    return result


def detect_selectors(limit=30):
    """Generate selector hints for custom sites."""
    session = get_db_session()
    try:
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
            print("No custom sites need selector detection")
            return

        print(f"Detecting selectors for {len(sites_data)} sites...")

        detector = SelectorDetector()
        updates = []

        async def detect_all():
            for site_id, company_name, careers_url, fetch_mode, discovery_notes in sites_data:
                print(f"  {company_name}...", end=" ", flush=True)
                try:
                    prefer_browser = (fetch_mode or "").strip().lower() == "browser"
                    html, extraction_mode, fetch_error = await _fetch_detection_html(
                        careers_url,
                        prefer_browser=prefer_browser,
                    )
                    if not html:
                        updates.append(
                            (
                                site_id,
                                None,
                                None,
                                _append_discovery_note(
                                    discovery_notes,
                                    f"selector_detect_fetch_failed: {fetch_error}",
                                ),
                            )
                        )
                        print(f"fetch failed ({fetch_error})")
                        continue

                    hint = await detector.detect(
                        html,
                        careers_url,
                        min_confidence=DEFAULT_SELECTOR_MIN_CONFIDENCE,
                    )
                    if hint is None:
                        updates.append(
                            (
                                site_id,
                                None,
                                None,
                                _append_discovery_note(
                                    discovery_notes,
                                    f"selector_detect_none (min_conf={DEFAULT_SELECTOR_MIN_CONFIDENCE:.2f}, mode={extraction_mode})",
                                ),
                            )
                        )
                        print("no valid selectors")
                        continue

                    passed, validation = validate_selector_hints(
                        html,
                        careers_url,
                        hint,
                        min_confidence=DEFAULT_SELECTOR_MIN_CONFIDENCE,
                        min_jobs=DEFAULT_SELECTOR_MIN_JOBS,
                        extraction_mode=extraction_mode,
                    )
                    hint_record = build_selector_hint_record(
                        hint,
                        validation=validation,
                        review_status="proposed",
                        extraction_mode=extraction_mode,
                    )
                    updates.append(
                        (
                            site_id,
                            hint_record,
                            hint.confidence,
                            _append_discovery_note(
                                discovery_notes,
                                f"selector_detected conf={hint.confidence:.2f} validated={passed} mode={extraction_mode}",
                            ),
                        )
                    )
                    if passed:
                        print(f"confidence {hint.confidence:.2f} validated (pending approval)")
                    else:
                        print(f"rejected ({validation.get('reason', 'validation_failed')})")
                except Exception as e:
                    updates.append(
                        (
                            site_id,
                            None,
                            None,
                            _append_discovery_note(discovery_notes, f"selector_detect_error: {type(e).__name__}"),
                        )
                    )
                    print(f"error: {type(e).__name__}")

        asyncio.run(detect_all())

        for site_id, hints, confidence, note in updates:
            values = {"discovery_notes": note}
            if hints is not None:
                values["selector_hints"] = hints
                values["selector_confidence"] = confidence
            session.query(ScrapeSite).filter(ScrapeSite.id == site_id).update(values)
        session.commit()

        generated = sum(1 for _, hints, _, _ in updates if hints is not None)
        print(f"\nGenerated selector hints for {generated}/{len(updates)} sites")

    finally:
        session.close()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python run_discovery.py <command>")
        print("Commands: import, status, resolve, probe, selectors, export_workday")
        sys.exit(1)

    cmd = sys.argv[1]

    if cmd == "import":
        import_seed_companies()
    elif cmd == "status":
        show_status()
    elif cmd == "resolve":
        limit = int(sys.argv[2]) if len(sys.argv) > 2 else 50
        resolve_urls(limit)
    elif cmd == "probe":
        limit = int(sys.argv[2]) if len(sys.argv) > 2 else 50
        probe_ats(limit)
    elif cmd == "selectors":
        limit = int(sys.argv[2]) if len(sys.argv) > 2 else 30
        detect_selectors(limit)
    elif cmd == "export_workday":
        export_workday_sites()
    else:
        print(f"Unknown command: {cmd}")
        sys.exit(1)
