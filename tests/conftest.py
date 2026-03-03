"""
Pytest fixtures for Multi-API Job Aggregator test suite
"""
import pytest
from job_scraper.models import Job


@pytest.fixture
def sample_job_full():
    """Job with all fields populated"""
    return Job(
        title="Senior Python Engineer",
        company="TechCorp Inc.",
        location="San Francisco, CA",
        url="https://example.com/jobs/12345",
        description="Build scalable systems with Python and AWS",
        salary="$150k - $200k",
        employment_type="full-time",
        posted_date="2026-01-20",
        source="example_api",
        job_id="job-12345",
        category="Engineering",
        tags=["python", "backend", "aws"],
        skills=["Python", "Django", "PostgreSQL"],
        remote=False,
        raw_payload={"id": 12345, "type": "engineering"},
    )


@pytest.fixture
def sample_job_minimal():
    """Job with only required fields"""
    return Job(
        title="Software Developer",
        company="StartUp Co",
    )


@pytest.fixture
def sample_job_remote():
    """Remote job with US-implied location"""
    return Job(
        title="Remote Full-Stack Developer",
        company="Remote First Inc",
        location="Remote, USA",
        url="https://remotejobs.com/job/999",
        remote=True,
        source="remote_api",
    )


@pytest.fixture
def sample_job_international():
    """Job with international location"""
    return Job(
        title="Software Engineer",
        company="Global Tech Ltd",
        location="London, United Kingdom",
        url="https://example.com/uk-job",
        source="international_api",
    )


@pytest.fixture
def sample_job_ny():
    """Job in New York"""
    return Job(
        title="Data Engineer",
        company="NYC Analytics",
        location="New York, NY",
        url="https://nyjobs.com/123",
    )


@pytest.fixture
def duplicate_jobs():
    """List of jobs with duplicates for deduplication testing"""
    return [
        Job(
            title="Python Developer",
            company="Company A",
            url="https://example.com/job/1",
        ),
        Job(
            title="  Python Developer  ",  # Same but with extra whitespace
            company="Company A",
            url="https://example.com/job/1",
        ),
        Job(
            title="Python Developer",
            company="Company A",
            url="https://example.com/job/1/",  # Same URL with trailing slash
        ),
        Job(
            title="Java Developer",
            company="Company B",
            url="https://example.com/job/2",
        ),
        Job(
            title="Python Developer",  # Same title/company, different URL
            company="Company A",
            url="https://example.com/job/999",
        ),
    ]


@pytest.fixture
def mixed_location_jobs():
    """List of jobs with various US and international locations"""
    return [
        Job(title="Job 1", company="Co A", location="San Francisco, CA"),
        Job(title="Job 2", company="Co B", location="London, UK"),
        Job(title="Job 3", company="Co C", location="Austin, Texas"),
        Job(title="Job 4", company="Co D", location="Toronto, Canada"),
        Job(title="Job 5", company="Co E", location="Remote, USA"),
        Job(title="Job 6", company="Co F", location="Berlin, Germany"),
        Job(title="Job 7", company="Co G", location="New York, NY"),
        Job(title="Job 8", company="Co H", location="Mumbai, India"),
        Job(title="Job 9", company="Co I", remote=True),  # Remote without explicit location
        Job(title="Job 10", company="Co J", location="Washington DC"),
    ]


@pytest.fixture
def jobs_no_url():
    """Jobs without URLs for testing title+company deduplication"""
    return [
        Job(title="Frontend Dev", company="WebCo"),
        Job(title="Frontend Dev", company="WebCo"),  # Duplicate
        Job(title="Frontend Dev", company="Different Co"),  # Same title, different company
        Job(title="Backend Dev", company="WebCo"),  # Different title, same company
    ]
