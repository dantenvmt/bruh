import os
from unittest.mock import patch

from job_scraper.config import Config
from job_scraper.models import Job
from job_scraper.visa import enrich_jobs_with_visa_tags


def test_enrich_jobs_with_visa_tags_company_match_adds_friendly_tag():
    with patch.dict(
        os.environ,
        {
            "JOB_SCRAPER_ENV_ONLY": "true",
            "JOB_SCRAPER_VISA_TAGGING_ENABLED": "true",
            "JOB_SCRAPER_VISA_SPONSOR_COMPANIES": "TechCorp Inc., Example LLC",
        },
        clear=True,
    ):
        cfg = Config()
        job = Job(title="Backend Engineer", company="TechCorp Inc.", description=None, tags=None)

        enrich_jobs_with_visa_tags([job], cfg)

        assert "visa_sponsor_company" in (job.tags or [])
        assert "visa_friendly" in (job.tags or [])


def test_enrich_jobs_with_visa_tags_positive_keywords_adds_friendly_tag():
    with patch.dict(
        os.environ,
        {
            "JOB_SCRAPER_ENV_ONLY": "true",
            "JOB_SCRAPER_VISA_TAGGING_ENABLED": "true",
        },
        clear=True,
    ):
        cfg = Config()
        job = Job(
            title="Software Engineer",
            company="SomeCo",
            description="We offer visa sponsorship and support H1B / OPT candidates.",
            tags=[],
        )

        enrich_jobs_with_visa_tags([job], cfg)

        assert "visa_friendly" in (job.tags or [])
        assert "visa_h1b" in (job.tags or [])
        assert "visa_opt" in (job.tags or [])


def test_enrich_jobs_with_visa_tags_negative_keywords_prevent_friendly_tag():
    with patch.dict(
        os.environ,
        {
            "JOB_SCRAPER_ENV_ONLY": "true",
            "JOB_SCRAPER_VISA_TAGGING_ENABLED": "true",
        },
        clear=True,
    ):
        cfg = Config()
        job = Job(
            title="Data Analyst",
            company="NoSponsorCo",
            description="No visa sponsorship available for this role.",
            tags=None,
        )

        enrich_jobs_with_visa_tags([job], cfg)

        assert "visa_no_sponsorship" in (job.tags or [])
        assert "visa_friendly" not in (job.tags or [])

