from __future__ import annotations

import uuid
from types import SimpleNamespace

from job_scraper.models import Job
from job_scraper.scrape_ingest import run_scrape_ingest
from job_scraper.scraping.types import SiteResult


def test_run_scrape_ingest_applies_deterministic_enrichment(monkeypatch):
    cfg = SimpleNamespace(
        db_dsn="postgresql://example",
        enrichment={"version": 3},
        retention_days=30,
        us_only=True,
    )
    monkeypatch.setattr("job_scraper.scrape_ingest.Config", lambda: cfg)

    scraped_jobs = [
        Job(
            title="Senior Python Engineer",
            company="Acme",
            location="Remote",
            url="https://example.com/jobs/1",
            source="custom_scraper",
            job_id="site-1:https://example.com/jobs/1",
        )
    ]
    site_results = [SiteResult(site_id=uuid.uuid4(), success=True, jobs_found=1)]
    async def _fake_scrape_due_sites(_cfg, _limit, _dry_run):
        return (scraped_jobs, site_results)

    monkeypatch.setattr("job_scraper.scrape_ingest._scrape_due_sites", _fake_scrape_due_sites)
    monkeypatch.setattr("job_scraper.scrape_ingest.start_run", lambda dsn, sources: uuid.uuid4())
    monkeypatch.setattr("job_scraper.scrape_ingest.finish_run", lambda *args, **kwargs: None)
    monkeypatch.setattr("job_scraper.scrape_ingest.purge_old_runs", lambda *args, **kwargs: None)
    monkeypatch.setattr("job_scraper.scrape_ingest.record_source_result", lambda *args, **kwargs: None)
    monkeypatch.setattr("job_scraper.scrape_ingest.enrich_jobs_with_visa_tags", lambda jobs, _cfg: jobs)

    captured = {}

    def _fake_upsert(_dsn, _run_id, jobs):
        captured["jobs"] = list(jobs)
        return len(captured["jobs"])

    monkeypatch.setattr("job_scraper.scrape_ingest.upsert_jobs", _fake_upsert)

    run_scrape_ingest(limit=1, dry_run=False)

    assert "jobs" in captured
    assert len(captured["jobs"]) == 1
    job = captured["jobs"][0]
    assert job.enrichment_version == 3
    assert job.work_mode == "remote"
    assert job.experience_level == "senior"
