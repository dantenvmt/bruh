"""
Tests for newly added enrichment/recommendation/analytics/saved-jobs API surface.
"""
from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime
from unittest.mock import MagicMock, patch
import uuid

from fastapi.testclient import TestClient

from job_scraper.api.app import app
from job_scraper.storage import JobRecord


def _sample_job() -> JobRecord:
    now = datetime.utcnow()
    return JobRecord(
        id=uuid.uuid4(),
        dedupe_key="sample-dedupe",
        source="greenhouse",
        source_job_id="sample-1",
        title="Senior Python Engineer",
        company="Acme",
        location="Remote, US",
        url="https://example.com/jobs/1",
        description="Build backend services with Python and SQL.",
        salary="$120k-$160k",
        employment_type="full-time",
        posted_date="2026-02-01T00:00:00Z",
        remote=True,
        category="Engineering",
        tags=["python", "visa_friendly"],
        skills=["python", "sqlalchemy"],
        experience_level="senior",
        experience_min_years=5,
        experience_max_years=10,
        required_skills=["python", "sqlalchemy"],
        industry="Software",
        industry_confidence=0.9,
        work_mode="remote",
        role_pop_reasons=["Remote-first role", "Strong Python stack", "Visa friendly", "Comp visible"],
        enrichment_version=1,
        enrichment_updated_at=now,
        created_at=now,
        updated_at=now,
        last_seen_at=now,
    )


@contextmanager
def _mock_session_scope(session):
    yield session


def test_jobs_endpoint_includes_enrichment_fields():
    client = TestClient(app)
    job = _sample_job()
    mock_session = MagicMock()
    mock_result = MagicMock()
    mock_result.scalars.return_value.all.return_value = [job]
    mock_session.execute.return_value = mock_result

    mock_cfg = MagicMock()
    mock_cfg.db_dsn = "postgresql://x"
    with patch("job_scraper.api.app._config", mock_cfg):
        with patch("job_scraper.api.app.session_scope", side_effect=lambda dsn: _mock_session_scope(mock_session)):
            response = client.get("/api/v1/jobs")
    assert response.status_code == 200
    data = response.json()
    assert data["items"]
    item = data["items"][0]
    assert item["experience_level"] == "senior"
    assert item["required_skills"] == ["python", "sqlalchemy"]
    assert item["industry"] == "Software"
    assert item["work_mode"] == "remote"
    assert isinstance(item["role_pop_reasons"], list)


def test_jobs_recommended_endpoint_returns_scored_items():
    client = TestClient(app)
    job = _sample_job()
    mock_session = MagicMock()
    mock_result = MagicMock()
    mock_result.scalars.return_value.all.return_value = [job]
    mock_session.execute.return_value = mock_result

    mock_cfg = MagicMock()
    mock_cfg.db_dsn = "postgresql://x"
    mock_cfg.recommendation = {"pool_size": 100}
    with patch("job_scraper.api.app._config", mock_cfg):
        with patch("job_scraper.api.app.session_scope", side_effect=lambda dsn: _mock_session_scope(mock_session)):
            response = client.get(
                "/api/v1/jobs/recommended",
                params={
                    "profile_experience_years": 6,
                    "profile_skills": "python,sqlalchemy",
                    "profile_industries": "software",
                    "profile_work_mode": "remote",
                    "limit": 20,
                },
            )
    assert response.status_code == 200
    payload = response.json()
    assert payload["items"]
    first = payload["items"][0]
    assert isinstance(first["recommendation_score"], int)
    assert isinstance(first["recommendation_reasons"], list)
    assert isinstance(first["match_breakdown"], dict)


def test_match_endpoint_returns_score():
    client = TestClient(app)
    job = _sample_job()
    mock_session = MagicMock()
    mock_query = MagicMock()
    mock_query.filter.return_value.first.return_value = job
    mock_session.query.return_value = mock_query

    mock_cfg = MagicMock()
    mock_cfg.db_dsn = "postgresql://x"
    with patch("job_scraper.api.app._config", mock_cfg):
        with patch("job_scraper.api.app.session_scope", side_effect=lambda dsn: _mock_session_scope(mock_session)):
            response = client.post(
                f"/api/v1/jobs/{job.id}/match",
                json={
                    "profile_experience_years": 6,
                    "profile_skills": ["python", "sqlalchemy"],
                    "profile_industries": ["software"],
                    "profile_work_mode": "remote",
                },
            )
    assert response.status_code == 200
    body = response.json()
    assert body["job_id"] == str(job.id)
    assert isinstance(body["match_score"], int)
    assert "breakdown" in body


def test_analytics_events_requires_identity():
    client = TestClient(app)
    job_id = str(uuid.uuid4())
    mock_cfg = MagicMock()
    mock_cfg.db_dsn = "postgresql://x"
    mock_cfg.analytics = {"max_batch": 50}
    with patch("job_scraper.api.app._config", mock_cfg):
        response = client.post(
            "/api/v1/analytics/events",
            json={"events": [{"job_id": job_id, "event_type": "view"}]},
        )
    assert response.status_code == 400
    assert "user_id or guest_session_id" in response.json()["detail"]


def test_saved_jobs_requires_identity():
    client = TestClient(app)
    mock_cfg = MagicMock()
    mock_cfg.db_dsn = "postgresql://x"
    with patch("job_scraper.api.app._config", mock_cfg):
        response = client.get("/api/v1/saved-jobs")
    assert response.status_code == 400
    assert "user_id or guest_session_id" in response.json()["detail"]
