"""
Tests for CSS selector parser.

These tests verify the parser correctly extracts job listings from HTML
using CSS selectors. Required for Phase A anti-slop gate.
"""
import pytest
from pathlib import Path

from job_scraper.scraping.parsers.css import parse, ParseError
from job_scraper.scraping.types import RawScrapedJob


FIXTURES_DIR = Path(__file__).parent / "fixtures" / "html"


class TestCSSParser:
    """Tests for CSS selector parsing."""

    def test_simple_job_listing(self):
        """Parse simple job listing structure."""
        html = (FIXTURES_DIR / "simple_jobs.html").read_text()
        selectors = {
            "job_container": ".job-listing",
            "title": ".job-title",
            "link": "a",
            "location": ".location",
        }
        base_url = "https://techco.com/careers"

        jobs = parse(html, selectors, base_url)

        assert len(jobs) == 3

        # First job - relative URL
        assert jobs[0].title == "Senior Software Engineer"
        assert jobs[0].url == "https://techco.com/jobs/123"
        assert jobs[0].location == "San Francisco, CA"

        # Second job - relative URL
        assert jobs[1].title == "Product Manager"
        assert jobs[1].url == "https://techco.com/jobs/456"
        assert jobs[1].location == "New York, NY"

        # Third job - absolute URL preserved
        assert jobs[2].title == "Data Scientist"
        assert jobs[2].url == "https://techco.com/careers/jobs/789"
        assert jobs[2].location == "Remote"

    def test_nested_structure(self):
        """Parse nested HTML structure with deep selectors."""
        html = (FIXTURES_DIR / "nested_structure.html").read_text()
        selectors = {
            "job_container": ".job-card",
            "title": ".job-header h2",
            "link": ".job-link",
            "location": ".loc",
        }
        base_url = "https://acmecorp.com"

        jobs = parse(html, selectors, base_url)

        assert len(jobs) == 2

        assert jobs[0].title == "Backend Engineer"
        assert jobs[0].url == "https://acmecorp.com/careers/apply/eng-001"
        assert jobs[0].location == "Austin, TX"

        assert jobs[1].title == "UX Designer"
        assert jobs[1].url == "https://acmecorp.com/careers/apply/des-002"
        assert jobs[1].location == "Boston, MA"

    def test_table_layout(self):
        """Parse table-based job listings."""
        html = (FIXTURES_DIR / "table_layout.html").read_text()
        selectors = {
            "job_container": ".job-row",
            "title": ".title",
            "link": "a",
            "location": ".location",
        }
        base_url = "https://widgetinc.com"

        jobs = parse(html, selectors, base_url)

        assert len(jobs) == 3

        assert jobs[0].title == "DevOps Engineer"
        assert jobs[0].url == "https://widgetinc.com/apply?id=devops-1"
        assert jobs[0].location == "Seattle, WA"

        assert jobs[1].title == "QA Engineer"
        assert jobs[1].location == "Portland, OR"

        assert jobs[2].title == "Frontend Developer"
        assert jobs[2].location == "Denver, CO"

    def test_empty_containers_raises_error(self):
        """ParseError raised when no job containers found."""
        html = (FIXTURES_DIR / "empty_jobs.html").read_text()
        selectors = {
            "job_container": ".job-listing",
            "title": ".title",
            "link": "a",
        }

        with pytest.raises(ParseError) as exc_info:
            parse(html, selectors, "https://example.com")

        assert "No job containers found" in str(exc_info.value)

    def test_missing_required_selector_raises_error(self):
        """ParseError raised when required selectors missing."""
        html = "<html><body></body></html>"

        with pytest.raises(ParseError) as exc_info:
            parse(html, {"title": "h1"}, "https://example.com")

        assert "Missing required selectors" in str(exc_info.value)
        assert "job_container" in str(exc_info.value)
        assert "link" in str(exc_info.value)

    def test_optional_location_can_be_omitted(self):
        """Location selector is optional."""
        html = """
        <div class="job">
            <h2>Engineer</h2>
            <a href="/apply">Apply</a>
        </div>
        """
        selectors = {
            "job_container": ".job",
            "title": "h2",
            "link": "a",
        }

        jobs = parse(html, selectors, "https://example.com")

        assert len(jobs) == 1
        assert jobs[0].title == "Engineer"
        assert jobs[0].url == "https://example.com/apply"
        assert jobs[0].location is None

    def test_relative_url_resolution(self):
        """Relative URLs correctly resolved against base_url."""
        html = """
        <div class="job">
            <h2>Role</h2>
            <a href="../job-1">Link</a>
        </div>
        """
        selectors = {
            "job_container": ".job",
            "title": "h2",
            "link": "a",
        }

        jobs = parse(html, selectors, "https://example.com/pages/careers/")

        assert len(jobs) == 1
        # ../job-1 from /pages/careers/ resolves to /pages/job-1
        assert jobs[0].url == "https://example.com/pages/job-1"

    def test_skips_jobs_without_title(self):
        """Jobs missing title are skipped, not errored."""
        html = """
        <div class="job">
            <h2></h2>
            <a href="/job-1">Apply</a>
        </div>
        <div class="job">
            <h2>Valid Job</h2>
            <a href="/job-2">Apply</a>
        </div>
        """
        selectors = {
            "job_container": ".job",
            "title": "h2",
            "link": "a",
        }

        jobs = parse(html, selectors, "https://example.com")

        assert len(jobs) == 1
        assert jobs[0].title == "Valid Job"

    def test_skips_jobs_without_link(self):
        """Jobs missing href are skipped."""
        html = """
        <div class="job">
            <h2>Job Without Link</h2>
            <a>No href</a>
        </div>
        <div class="job">
            <h2>Valid Job</h2>
            <a href="/apply">Apply</a>
        </div>
        """
        selectors = {
            "job_container": ".job",
            "title": "h2",
            "link": "a",
        }

        jobs = parse(html, selectors, "https://example.com")

        assert len(jobs) == 1
        assert jobs[0].title == "Valid Job"

    def test_returns_rawscrappedjob_type(self):
        """Parser returns list of RawScrapedJob dataclasses."""
        html = """
        <div class="job">
            <h2>Test Job</h2>
            <a href="/job">Apply</a>
        </div>
        """
        selectors = {
            "job_container": ".job",
            "title": "h2",
            "link": "a",
        }

        jobs = parse(html, selectors, "https://example.com")

        assert len(jobs) == 1
        assert isinstance(jobs[0], RawScrapedJob)
        assert hasattr(jobs[0], "title")
        assert hasattr(jobs[0], "url")
        assert hasattr(jobs[0], "location")
        assert hasattr(jobs[0], "company")
