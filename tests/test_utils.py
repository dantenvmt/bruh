"""
Tests for job_scraper.utils module
"""
import pytest
from job_scraper.models import Job
from job_scraper.utils import (
    normalize_text,
    normalize_url,
    build_dedupe_key,
    deduplicate_jobs,
    is_us_job,
    is_us_job_for_source,
)


class TestNormalizeText:
    """Test suite for normalize_text function"""

    def test_normalize_text_with_extra_whitespace(self):
        """Test normalize_text removes extra whitespace"""
        assert normalize_text("  hello   world  ") == "hello world"

    def test_normalize_text_with_newlines(self):
        """Test normalize_text replaces newlines with spaces"""
        assert normalize_text("hello\nworld") == "hello world"
        assert normalize_text("hello\n\nworld") == "hello world"

    def test_normalize_text_with_tabs(self):
        """Test normalize_text replaces tabs with spaces"""
        assert normalize_text("hello\tworld") == "hello world"

    def test_normalize_text_with_mixed_whitespace(self):
        """Test normalize_text handles mixed whitespace"""
        assert normalize_text("  hello \n\t  world  \n") == "hello world"

    def test_normalize_text_with_none(self):
        """Test normalize_text handles None"""
        assert normalize_text(None) == ""

    def test_normalize_text_with_empty_string(self):
        """Test normalize_text handles empty string"""
        assert normalize_text("") == ""

    def test_normalize_text_with_only_whitespace(self):
        """Test normalize_text handles whitespace-only strings"""
        assert normalize_text("   ") == ""
        assert normalize_text("\n\t\n") == ""

    def test_normalize_text_normal_text(self):
        """Test normalize_text preserves normal text"""
        assert normalize_text("Hello World") == "Hello World"


class TestNormalizeUrl:
    """Test suite for normalize_url function"""

    def test_normalize_url_removes_trailing_slash(self):
        """Test normalize_url removes trailing slash"""
        assert normalize_url("https://example.com/") == "https://example.com"
        assert normalize_url("https://example.com/jobs/") == "https://example.com/jobs"

    def test_normalize_url_preserves_url_without_trailing_slash(self):
        """Test normalize_url preserves URLs without trailing slash"""
        assert normalize_url("https://example.com") == "https://example.com"

    def test_normalize_url_normalizes_whitespace(self):
        """Test normalize_url normalizes whitespace"""
        assert normalize_url("  https://example.com  ") == "https://example.com"
        # Note: normalize_url uses normalize_text which collapses internal whitespace
        # So "https://example.com  /" becomes "https://example.com /" then strip trailing /
        assert normalize_url("https://example.com  /") == "https://example.com "

    def test_normalize_url_with_none(self):
        """Test normalize_url handles None"""
        assert normalize_url(None) == ""

    def test_normalize_url_with_empty_string(self):
        """Test normalize_url handles empty string"""
        assert normalize_url("") == ""

    def test_normalize_url_with_query_params(self):
        """Test normalize_url preserves query parameters"""
        assert normalize_url("https://example.com/jobs?id=123") == "https://example.com/jobs?id=123"

    def test_normalize_url_with_multiple_trailing_slashes(self):
        """Test normalize_url handles multiple trailing slashes"""
        # Note: only removes one trailing slash due to rstrip("/")
        assert normalize_url("https://example.com///") == "https://example.com"


class TestBuildDedupeKey:
    """Test suite for build_dedupe_key function"""

    def test_build_dedupe_key_with_url(self):
        """Test build_dedupe_key with URL present"""
        job = Job(
            title="Python Developer",
            company="TechCo",
            url="https://example.com/job/123",
        )
        expected = "https://example.com/job/123|python developer|techco"
        assert build_dedupe_key(job) == expected

    def test_build_dedupe_key_without_url(self):
        """Test build_dedupe_key without URL"""
        job = Job(
            title="Python Developer",
            company="TechCo",
            url=None,
        )
        expected = "python developer|techco"
        assert build_dedupe_key(job) == expected

    def test_build_dedupe_key_normalizes_whitespace(self):
        """Test build_dedupe_key normalizes whitespace"""
        job = Job(
            title="  Python   Developer  ",
            company="  Tech  Co  ",
            url="  https://example.com/job/123  ",
        )
        expected = "https://example.com/job/123|python developer|tech co"
        assert build_dedupe_key(job) == expected

    def test_build_dedupe_key_is_lowercase(self):
        """Test build_dedupe_key converts to lowercase"""
        job = Job(
            title="PYTHON DEVELOPER",
            company="TECHCO",
            url="HTTPS://EXAMPLE.COM/JOB",
        )
        expected = "https://example.com/job|python developer|techco"
        assert build_dedupe_key(job) == expected

    def test_build_dedupe_key_removes_trailing_slash(self):
        """Test build_dedupe_key removes trailing slash from URL"""
        job = Job(
            title="Developer",
            company="Co",
            url="https://example.com/job/",
        )
        expected = "https://example.com/job|developer|co"
        assert build_dedupe_key(job) == expected

    def test_build_dedupe_key_with_empty_url(self):
        """Test build_dedupe_key treats empty URL as no URL"""
        job = Job(
            title="Developer",
            company="Co",
            url="",
        )
        expected = "developer|co"
        assert build_dedupe_key(job) == expected

    def test_build_dedupe_key_with_none_company(self):
        """Test build_dedupe_key handles None company"""
        job = Job(
            title="Developer",
            company=None,
        )
        expected = "developer|"
        assert build_dedupe_key(job) == expected


class TestDeduplicateJobs:
    """Test suite for deduplicate_jobs function"""

    def test_deduplicate_jobs_removes_exact_duplicates(self, duplicate_jobs):
        """Test deduplicate_jobs removes exact duplicates"""
        unique = deduplicate_jobs(duplicate_jobs)

        # Should have 3 unique jobs (first 3 are duplicates, 4th unique, 5th unique)
        assert len(unique) == 3

        # First job should be kept
        assert unique[0].title.strip() == "Python Developer"
        assert unique[0].company == "Company A"
        assert "job/1" in unique[0].url

    def test_deduplicate_jobs_preserves_first_occurrence(self, duplicate_jobs):
        """Test deduplicate_jobs keeps first occurrence of duplicates"""
        unique = deduplicate_jobs(duplicate_jobs)

        # First occurrence should be preserved exactly
        assert unique[0] == duplicate_jobs[0]

    def test_deduplicate_jobs_normalizes_whitespace(self):
        """Test deduplicate_jobs treats whitespace variations as duplicates"""
        jobs = [
            Job(title="Developer", company="Co", url="https://example.com/1"),
            Job(title="  Developer  ", company="Co", url="https://example.com/1"),
            Job(title="Developer\n", company="Co", url="https://example.com/1  "),
        ]
        unique = deduplicate_jobs(jobs)
        assert len(unique) == 1

    def test_deduplicate_jobs_normalizes_trailing_slashes(self):
        """Test deduplicate_jobs treats URLs with/without trailing slash as same"""
        jobs = [
            Job(title="Dev", company="Co", url="https://example.com/job"),
            Job(title="Dev", company="Co", url="https://example.com/job/"),
        ]
        unique = deduplicate_jobs(jobs)
        assert len(unique) == 1

    def test_deduplicate_jobs_case_insensitive(self):
        """Test deduplicate_jobs is case-insensitive"""
        jobs = [
            Job(title="Python Developer", company="TechCo", url="https://example.com/1"),
            Job(title="PYTHON DEVELOPER", company="TECHCO", url="https://example.com/1"),
        ]
        unique = deduplicate_jobs(jobs)
        assert len(unique) == 1

    def test_deduplicate_jobs_different_urls_different_jobs(self):
        """Test deduplicate_jobs treats different URLs as different jobs"""
        jobs = [
            Job(title="Developer", company="Co", url="https://example.com/1"),
            Job(title="Developer", company="Co", url="https://example.com/2"),
        ]
        unique = deduplicate_jobs(jobs)
        assert len(unique) == 2

    def test_deduplicate_jobs_without_urls(self, jobs_no_url):
        """Test deduplicate_jobs handles jobs without URLs"""
        unique = deduplicate_jobs(jobs_no_url)

        # Should dedupe based on title + company
        assert len(unique) == 3
        assert unique[0].title == "Frontend Dev"
        assert unique[0].company == "WebCo"

    def test_deduplicate_jobs_empty_list(self):
        """Test deduplicate_jobs handles empty list"""
        unique = deduplicate_jobs([])
        assert unique == []

    def test_deduplicate_jobs_single_job(self, sample_job_full):
        """Test deduplicate_jobs handles single job"""
        unique = deduplicate_jobs([sample_job_full])
        assert len(unique) == 1
        assert unique[0] == sample_job_full

    def test_deduplicate_jobs_preserves_order(self):
        """Test deduplicate_jobs preserves insertion order"""
        jobs = [
            Job(title="Job A", company="Co"),
            Job(title="Job B", company="Co"),
            Job(title="Job C", company="Co"),
            Job(title="Job A", company="Co"),  # Duplicate
        ]
        unique = deduplicate_jobs(jobs)

        assert len(unique) == 3
        assert unique[0].title == "Job A"
        assert unique[1].title == "Job B"
        assert unique[2].title == "Job C"

    def test_deduplicate_jobs_with_different_metadata(self):
        """Test deduplicate_jobs ignores metadata differences if key matches"""
        jobs = [
            Job(
                title="Developer",
                company="Co",
                url="https://example.com/1",
                salary="$100k",
                description="First description",
            ),
            Job(
                title="Developer",
                company="Co",
                url="https://example.com/1",
                salary="$120k",  # Different salary
                description="Different description",  # Different description
            ),
        ]
        unique = deduplicate_jobs(jobs)

        # Should be treated as duplicate
        assert len(unique) == 1
        # First occurrence preserved with its metadata
        assert unique[0].salary == "$100k"
        assert unique[0].description == "First description"


class TestIsUsJob:
    """Test suite for is_us_job function"""

    def test_is_us_job_with_state_abbreviation(self):
        """Test is_us_job recognizes US state abbreviations"""
        job = Job(title="Dev", company="Co", location="San Francisco, CA")
        assert is_us_job(job) is True

        job = Job(title="Dev", company="Co", location="New York, NY")
        assert is_us_job(job) is True

        job = Job(title="Dev", company="Co", location="Austin, TX")
        assert is_us_job(job) is True

    def test_is_us_job_with_state_name(self):
        """Test is_us_job recognizes US state names"""
        job = Job(title="Dev", company="Co", location="California")
        assert is_us_job(job) is True

        job = Job(title="Dev", company="Co", location="New York")
        assert is_us_job(job) is True

        job = Job(title="Dev", company="Co", location="Austin, Texas")
        assert is_us_job(job) is True

    def test_is_us_job_with_usa_mention(self):
        """Test is_us_job recognizes 'USA', 'United States', etc."""
        job = Job(title="Dev", company="Co", location="Remote, USA")
        assert is_us_job(job) is True

        job = Job(title="Dev", company="Co", location="Remote, United States")
        assert is_us_job(job) is True

        job = Job(title="Dev", company="Co", location="Anywhere, U.S.")
        assert is_us_job(job) is True

    def test_is_us_job_with_washington_dc(self):
        """Test is_us_job recognizes Washington DC"""
        job = Job(title="Dev", company="Co", location="Washington, DC")
        assert is_us_job(job) is True

        job = Job(title="Dev", company="Co", location="District of Columbia")
        assert is_us_job(job) is True

    def test_is_us_job_with_foreign_country(self):
        """Test is_us_job rejects foreign countries"""
        job = Job(title="Dev", company="Co", location="London, UK")
        assert is_us_job(job) is False

        job = Job(title="Dev", company="Co", location="Toronto, Canada")
        assert is_us_job(job) is False

        job = Job(title="Dev", company="Co", location="Berlin, Germany")
        assert is_us_job(job) is False

        job = Job(title="Dev", company="Co", location="Mumbai, India")
        assert is_us_job(job) is False

    def test_is_us_job_with_uk_variations(self):
        """Test is_us_job rejects UK variations"""
        job = Job(title="Dev", company="Co", location="London, United Kingdom")
        assert is_us_job(job) is False

        job = Job(title="Dev", company="Co", location="Manchester, UK")
        assert is_us_job(job) is False

    def test_is_us_job_remote_without_location(self):
        """Test is_us_job treats remote jobs without location as US"""
        job = Job(title="Dev", company="Co", remote=True)
        assert is_us_job(job) is True

        job = Job(title="Dev", company="Co", location="", remote=True)
        assert is_us_job(job) is True

    def test_is_us_job_remote_explicit_us(self):
        """Test is_us_job with remote US jobs"""
        job = Job(title="Dev", company="Co", location="Remote, USA", remote=True)
        assert is_us_job(job) is True

    def test_is_us_job_no_location_not_remote(self):
        """Test is_us_job returns False for jobs without location info"""
        job = Job(title="Dev", company="Co")
        assert is_us_job(job) is False

        job = Job(title="Dev", company="Co", location="")
        assert is_us_job(job) is False

    def test_is_us_job_case_insensitive(self):
        """Test is_us_job is case-insensitive"""
        job = Job(title="Dev", company="Co", location="CALIFORNIA")
        assert is_us_job(job) is True

        job = Job(title="Dev", company="Co", location="london, uk")
        assert is_us_job(job) is False

    def test_is_us_job_state_abbreviation_word_boundary(self):
        """Test is_us_job requires word boundaries for state codes"""
        # "CA" should match
        job = Job(title="Dev", company="Co", location="CA")
        assert is_us_job(job) is True

        # "CA" in California should match
        job = Job(title="Dev", company="Co", location="San Francisco, CA")
        assert is_us_job(job) is True

    def test_is_us_job_ambiguous_locations(self):
        """Test is_us_job handles ambiguous locations"""
        # "Remote" without context - not US
        job = Job(title="Dev", company="Co", location="Remote")
        assert is_us_job(job) is False

        # But if marked as remote job, assume US
        job = Job(title="Dev", company="Co", location="Remote", remote=True)
        assert is_us_job(job) is True

    def test_is_us_job_with_multiple_locations(self):
        """Test is_us_job with multiple locations in string"""
        # If contains foreign country, should be False even if US is mentioned
        job = Job(title="Dev", company="Co", location="London, UK or New York, NY")
        assert is_us_job(job) is False

        # Multiple US locations
        job = Job(title="Dev", company="Co", location="San Francisco, CA or Austin, TX")
        assert is_us_job(job) is True

    def test_is_us_job_whitespace_normalization(self):
        """Test is_us_job normalizes whitespace"""
        job = Job(title="Dev", company="Co", location="  California  ")
        assert is_us_job(job) is True

        job = Job(title="Dev", company="Co", location=" London,  UK ")
        assert is_us_job(job) is False


class TestIsUsJobForSource:
    """Test suite for is_us_job_for_source function"""

    def test_is_us_job_for_source_with_us_scoped_source(self):
        """Test is_us_job_for_source treats missing location as US for US-only sources"""
        us_sources = {"indeed", "linkedin", "usajobs"}

        # Job from US-only source without location should be US
        job = Job(title="Dev", company="Co", source="indeed")
        assert is_us_job_for_source(job, us_sources) is True

        job = Job(title="Dev", company="Co", location="", source="linkedin")
        assert is_us_job_for_source(job, us_sources) is True

    def test_is_us_job_for_source_with_explicit_us_location(self):
        """Test is_us_job_for_source with explicit US location"""
        us_sources = {"indeed"}

        job = Job(title="Dev", company="Co", location="California", source="indeed")
        assert is_us_job_for_source(job, us_sources) is True

    def test_is_us_job_for_source_with_foreign_location_us_source(self):
        """Test is_us_job_for_source rejects foreign location even for US source"""
        us_sources = {"indeed"}

        # Even from US source, explicit foreign location should be rejected
        job = Job(title="Dev", company="Co", location="London, UK", source="indeed")
        assert is_us_job_for_source(job, us_sources) is False

    def test_is_us_job_for_source_with_non_us_source(self):
        """Test is_us_job_for_source with non-US-scoped source"""
        us_sources = {"indeed"}

        # Job from international source without location should be False
        job = Job(title="Dev", company="Co", source="international_api")
        assert is_us_job_for_source(job, us_sources) is False

        # Even with explicit US location
        job = Job(title="Dev", company="Co", location="California", source="international_api")
        assert is_us_job_for_source(job, us_sources) is True

    def test_is_us_job_for_source_case_sensitivity(self):
        """Test is_us_job_for_source source matching is case-insensitive"""
        us_sources = {"indeed", "linkedin"}

        job = Job(title="Dev", company="Co", source="INDEED")
        assert is_us_job_for_source(job, us_sources) is True

        job = Job(title="Dev", company="Co", source="LinkedIn")
        assert is_us_job_for_source(job, us_sources) is True

    def test_is_us_job_for_source_empty_source_set(self):
        """Test is_us_job_for_source with empty US sources set"""
        us_sources = set()

        # Without US-scoped sources, falls back to is_us_job logic
        job = Job(title="Dev", company="Co")
        assert is_us_job_for_source(job, us_sources) is False

        job = Job(title="Dev", company="Co", location="California")
        assert is_us_job_for_source(job, us_sources) is True

    def test_is_us_job_for_source_no_source_attribute(self):
        """Test is_us_job_for_source when job has no source"""
        us_sources = {"indeed"}

        job = Job(title="Dev", company="Co")
        assert is_us_job_for_source(job, us_sources) is False

        job = Job(title="Dev", company="Co", location="California")
        assert is_us_job_for_source(job, us_sources) is True

    def test_is_us_job_for_source_remote_job(self):
        """Test is_us_job_for_source with remote jobs"""
        us_sources = {"indeed"}

        # Remote job from US source without location
        job = Job(title="Dev", company="Co", remote=True, source="indeed")
        assert is_us_job_for_source(job, us_sources) is True

        # Remote job from international source without location
        job = Job(title="Dev", company="Co", remote=True, source="international")
        assert is_us_job_for_source(job, us_sources) is True

    def test_is_us_job_for_source_with_whitespace_in_location(self):
        """Test is_us_job_for_source normalizes location whitespace"""
        us_sources = {"indeed"}

        # Location with only whitespace should be treated as missing
        job = Job(title="Dev", company="Co", location="   ", source="indeed")
        assert is_us_job_for_source(job, us_sources) is True

        job = Job(title="Dev", company="Co", location="\n\t", source="indeed")
        assert is_us_job_for_source(job, us_sources) is True
