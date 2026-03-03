"""
Tests for job_scraper.models module
"""
import pytest
from job_scraper.models import Job


class TestJobModel:
    """Test suite for Job dataclass"""

    def test_job_creation_with_all_fields(self, sample_job_full):
        """Test creating a Job with all fields populated"""
        job = sample_job_full

        # Assert all fields are set correctly
        assert job.title == "Senior Python Engineer"
        assert job.company == "TechCorp Inc."
        assert job.location == "San Francisco, CA"
        assert job.url == "https://example.com/jobs/12345"
        assert job.description == "Build scalable systems with Python and AWS"
        assert job.salary == "$150k - $200k"
        assert job.employment_type == "full-time"
        assert job.posted_date == "2026-01-20"
        assert job.source == "example_api"
        assert job.job_id == "job-12345"
        assert job.category == "Engineering"
        assert job.tags == ["python", "backend", "aws"]
        assert job.skills == ["Python", "Django", "PostgreSQL"]
        assert job.remote is False
        assert job.raw_payload == {"id": 12345, "type": "engineering"}

    def test_job_creation_with_minimal_fields(self, sample_job_minimal):
        """Test creating a Job with only required fields"""
        job = sample_job_minimal

        # Assert required fields
        assert job.title == "Software Developer"
        assert job.company == "StartUp Co"

        # Assert optional fields default to None
        assert job.location is None
        assert job.url is None
        assert job.description is None
        assert job.salary is None
        assert job.employment_type is None
        assert job.posted_date is None
        assert job.source is None
        assert job.job_id is None
        assert job.category is None
        assert job.tags is None
        assert job.skills is None
        assert job.remote is None
        assert job.raw_payload is None

    def test_job_creation_with_partial_fields(self):
        """Test creating a Job with some optional fields"""
        job = Job(
            title="DevOps Engineer",
            company="CloudCo",
            location="Remote",
            salary="$120k",
            remote=True,
        )

        assert job.title == "DevOps Engineer"
        assert job.company == "CloudCo"
        assert job.location == "Remote"
        assert job.salary == "$120k"
        assert job.remote is True
        assert job.url is None
        assert job.description is None

    def test_unique_key_with_url(self, sample_job_full):
        """Test unique_key property with URL present"""
        job = sample_job_full
        expected_key = "https://example.com/jobs/12345|senior python engineer|techcorp inc."

        assert job.unique_key == expected_key

    def test_unique_key_without_url(self, sample_job_minimal):
        """Test unique_key property without URL"""
        job = sample_job_minimal
        expected_key = "software developer|startup co"

        assert job.unique_key == expected_key

    def test_unique_key_with_empty_url(self):
        """Test unique_key when URL is empty string"""
        job = Job(
            title="QA Engineer",
            company="TestCo",
            url="",
        )
        expected_key = "qa engineer|testco"

        assert job.unique_key == expected_key

    def test_unique_key_with_whitespace(self):
        """Test unique_key normalizes whitespace"""
        job = Job(
            title="  Senior  Developer  ",
            company="  Tech Corp  ",
            url="  https://example.com/job  ",
        )
        expected_key = "https://example.com/job|senior  developer|tech corp"

        assert job.unique_key == expected_key

    def test_unique_key_case_insensitive(self):
        """Test unique_key is lowercase"""
        job1 = Job(title="Python Developer", company="TechCorp")
        job2 = Job(title="PYTHON DEVELOPER", company="TECHCORP")

        assert job1.unique_key == job2.unique_key
        assert job1.unique_key == "python developer|techcorp"

    def test_unique_key_with_none_values(self):
        """Test unique_key handles None values gracefully"""
        job = Job(
            title="Developer",
            company=None,
            url=None,
        )
        expected_key = "developer|"

        assert job.unique_key == expected_key

    def test_to_dict_full_job(self, sample_job_full):
        """Test to_dict() method with all fields"""
        job = sample_job_full
        job_dict = job.to_dict()

        # Verify it's a dictionary
        assert isinstance(job_dict, dict)

        # Verify all fields are present
        assert job_dict["title"] == "Senior Python Engineer"
        assert job_dict["company"] == "TechCorp Inc."
        assert job_dict["location"] == "San Francisco, CA"
        assert job_dict["url"] == "https://example.com/jobs/12345"
        assert job_dict["description"] == "Build scalable systems with Python and AWS"
        assert job_dict["salary"] == "$150k - $200k"
        assert job_dict["employment_type"] == "full-time"
        assert job_dict["posted_date"] == "2026-01-20"
        assert job_dict["source"] == "example_api"
        assert job_dict["job_id"] == "job-12345"
        assert job_dict["category"] == "Engineering"
        assert job_dict["tags"] == ["python", "backend", "aws"]
        assert job_dict["skills"] == ["Python", "Django", "PostgreSQL"]
        assert job_dict["remote"] is False
        assert job_dict["raw_payload"] == {"id": 12345, "type": "engineering"}

    def test_to_dict_minimal_job(self, sample_job_minimal):
        """Test to_dict() method with minimal fields"""
        job = sample_job_minimal
        job_dict = job.to_dict()

        assert isinstance(job_dict, dict)
        assert job_dict["title"] == "Software Developer"
        assert job_dict["company"] == "StartUp Co"

        # None values should be included
        assert "location" in job_dict
        assert job_dict["location"] is None

    def test_to_dict_roundtrip(self):
        """Test that to_dict() can be used to recreate a Job"""
        original = Job(
            title="Backend Engineer",
            company="DataCo",
            location="Seattle, WA",
            url="https://example.com/123",
            salary="$140k",
            remote=False,
        )

        job_dict = original.to_dict()
        recreated = Job(**job_dict)

        # Verify the recreated job matches
        assert recreated.title == original.title
        assert recreated.company == original.company
        assert recreated.location == original.location
        assert recreated.url == original.url
        assert recreated.salary == original.salary
        assert recreated.remote == original.remote

    def test_job_with_empty_strings(self):
        """Test Job handles empty strings"""
        job = Job(
            title="",
            company="",
            location="",
            url="",
        )

        assert job.title == ""
        assert job.company == ""
        assert job.location == ""
        assert job.url == ""
        assert job.unique_key == "|"

    def test_job_with_special_characters(self):
        """Test Job handles special characters in fields"""
        job = Job(
            title="C++ Developer (Senior)",
            company="Tech & Co.",
            location="São Paulo, Brazil",
            url="https://example.com/jobs?id=123&ref=email",
        )

        assert job.title == "C++ Developer (Senior)"
        assert job.company == "Tech & Co."
        assert job.location == "São Paulo, Brazil"
        assert "c++ developer (senior)" in job.unique_key

    def test_job_list_fields(self):
        """Test Job properly handles list fields"""
        tags = ["python", "django"]
        skills = ["PostgreSQL", "Redis"]

        job = Job(
            title="Backend Developer",
            company="WebCo",
            tags=tags,
            skills=skills,
        )

        # Lists should be stored as-is
        assert job.tags == ["python", "django"]
        assert job.skills == ["PostgreSQL", "Redis"]

        # Note: Dataclasses store list references, not copies
        # Modifying original list will affect job (this is expected Python behavior)
        tags.append("flask")
        assert job.tags == ["python", "django", "flask"]

        # To avoid this, pass a copy: tags=tags.copy()
        job2 = Job(
            title="Backend Developer 2",
            company="WebCo",
            tags=tags.copy(),
        )
        tags.append("unittest")
        assert job2.tags == ["python", "django", "flask"]  # Does not include 'unittest'

    def test_job_equality(self):
        """Test Job instances can be compared"""
        job1 = Job(title="Developer", company="TechCo")
        job2 = Job(title="Developer", company="TechCo")
        job3 = Job(title="Engineer", company="TechCo")

        # Dataclasses have auto-generated equality
        assert job1 == job2
        assert job1 != job3
