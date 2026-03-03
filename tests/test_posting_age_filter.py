from datetime import datetime, timedelta, timezone

from job_scraper.models import Job
from job_scraper.utils import filter_recent_jobs, parse_posted_date


def test_parse_posted_date_handles_iso_and_rfc():
    iso = parse_posted_date("2026-02-10T10:30:00Z")
    rfc = parse_posted_date("Fri, 13 Feb 2026 22:01:39 +0000")

    assert iso is not None
    assert rfc is not None
    assert iso.tzinfo is not None
    assert rfc.tzinfo is not None


def test_parse_posted_date_handles_relative_time():
    parsed = parse_posted_date("2 days ago")
    assert parsed is not None

    now = datetime.now(timezone.utc)
    assert now - timedelta(days=3) <= parsed <= now


def test_filter_recent_jobs_drops_stale_when_date_is_known():
    now = datetime.now(timezone.utc)
    fresh_date = (now - timedelta(days=10)).isoformat()
    stale_date = (now - timedelta(days=120)).isoformat()

    jobs = [
        Job(title="Fresh", company="A", url="https://a", posted_date=fresh_date, source="Test"),
        Job(title="Stale", company="B", url="https://b", posted_date=stale_date, source="Test"),
        Job(title="Unknown", company="C", url="https://c", posted_date=None, source="Test"),
    ]

    kept, dropped = filter_recent_jobs(jobs, max_age_days=60)

    assert dropped == 1
    assert len(kept) == 2
    assert {job.title for job in kept} == {"Fresh", "Unknown"}
