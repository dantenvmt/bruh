"""
Tests for sitemap parser URL/title heuristics.
"""

from job_scraper.scraping.parsers.sitemap import (
    _jobs_from_urls,
    _looks_like_job_detail_url,
    _title_from_url,
)


def test_title_from_url_prefers_non_numeric_slug():
    url = "https://jobs.boeing.com/job/hazelwood/experienced-data-scientist/185/92130265760"
    assert _title_from_url(url) == "Experienced Data Scientist"


def test_title_from_url_strips_static_extension():
    url = "https://example.com/jobs/senior-software-engineer.html"
    assert _title_from_url(url) == "Senior Software Engineer"


def test_looks_like_job_detail_url_accepts_careers_role_slug():
    assert _looks_like_job_detail_url("https://example.com/careers/software-engineer")


def test_looks_like_job_detail_url_rejects_marketing_pages():
    assert not _looks_like_job_detail_url("https://www.apple.com/careers/us/accessibility.html")
    assert not _looks_like_job_detail_url("https://www.pfizer.com/about/careers/join-us")
    assert not _looks_like_job_detail_url("https://www.lockheedmartin.com/en-us/careers/locations/texas.html")


def test_jobs_from_urls_filters_garbage_and_keeps_real_jobs():
    urls = [
        "https://www.apple.com/careers/us/accessibility.html",
        "https://www.pfizer.com/about/careers/join-us",
        "https://www.lockheedmartin.com/en-us/careers/locations/texas.html",
        "https://jobs.boeing.com/job/hazelwood/experienced-data-scientist/185/92130265760",
    ]

    jobs = _jobs_from_urls(
        urls,
        base_url="https://example.com/careers",
        company_name="Example Co",
    )

    assert len(jobs) == 1
    assert jobs[0].title == "Experienced Data Scientist"
    assert jobs[0].url == "https://jobs.boeing.com/job/hazelwood/experienced-data-scientist/185/92130265760"
