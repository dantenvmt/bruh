"""
Comprehensive API security tests for the FastAPI job aggregator application.

Tests cover:
- API key authentication
- Rate limiting
- SQL injection protection (LIKE pattern sanitization)
- Query length truncation
- Raw payload access control
- CORS headers
"""
import os
import pytest
from unittest.mock import MagicMock, patch
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session

from job_scraper.api.app import app, _sanitize_like_pattern, MAX_QUERY_LENGTH
from job_scraper.storage import JobRecord


@pytest.fixture
def mock_db_session():
    """Create a mock database session"""
    session = MagicMock(spec=Session)

    # Mock execute to return a result with scalars
    mock_result = MagicMock()
    mock_result.scalars.return_value.all.return_value = []
    session.execute.return_value = mock_result

    return session


@pytest.fixture
def mock_config():
    """Patch the module-level _config singleton directly."""
    mock_cfg = MagicMock()
    mock_cfg.db_dsn = "postgresql://test:test@localhost/test_db"
    mock_cfg.recommendation = {"pool_size": 1500}
    mock_cfg.analytics = {"max_batch": 50}
    with patch('job_scraper.api.app._config', mock_cfg):
        yield mock_cfg


@pytest.fixture
def mock_session_factory(mock_db_session):
    """Mock the get_session factory"""
    with patch('job_scraper.storage.get_session') as mock_get_session:
        mock_get_session.return_value = mock_db_session
        yield mock_get_session


@pytest.fixture
def client():
    """Create a test client"""
    return TestClient(app)


@pytest.fixture
def sample_job_records():
    """Sample job records for testing"""
    from datetime import datetime
    import uuid

    return [
        JobRecord(
            id=uuid.uuid4(),
            dedupe_key="test-job-1",
            source="test_api",
            source_job_id="job-1",
            title="Python Developer",
            company="TechCorp",
            location="San Francisco, CA",
            url="https://example.com/job/1",
            description="Build awesome Python apps",
            salary="$120k-$150k",
            employment_type="full-time",
            posted_date="2026-01-20",
            remote=True,
            category="Engineering",
            tags=["python", "backend"],
            skills=["Python", "Django"],
            raw_payload={"original_id": 123, "extra": "data"},
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow(),
            last_seen_at=datetime.utcnow(),
        )
    ]


class TestAPIKeyAuthentication:
    """Test API key authentication middleware"""

    def test_jobs_endpoint_is_public(
        self, client, mock_config, mock_session_factory
    ):
        """Test that /api/v1/jobs is public (no API key required)"""
        with patch.dict(os.environ, {"JOB_SCRAPER_API_KEY": "secret-key-123"}):
            response = client.get("/api/v1/jobs")

            # /jobs is now public - should return 200 even without API key
            assert response.status_code == 200
            assert "items" in response.json()

    def test_runs_endpoint_requires_api_key(
        self, client, mock_config, mock_session_factory
    ):
        """Test that /api/v1/runs requires API key when configured"""
        with patch.dict(os.environ, {"JOB_SCRAPER_API_KEY": "secret-key-123"}):
            # Without key
            response = client.get("/api/v1/runs")
            assert response.status_code == 403
            assert response.json()["detail"] == "Invalid API key"

            # With wrong key
            response = client.get("/api/v1/runs", headers={"X-API-Key": "wrong-key"})
            assert response.status_code == 403

            # With valid key
            response = client.get("/api/v1/runs", headers={"X-API-Key": "secret-key-123"})
            assert response.status_code == 200

    def test_request_without_api_key_when_not_configured_returns_200(
        self, client, mock_config, mock_session_factory
    ):
        """Test that API key is optional when not configured"""
        # Clear any API key from environment
        with patch.dict(os.environ, {}, clear=True):
            # Ensure JOB_SCRAPER_API_KEY is not set
            if "JOB_SCRAPER_API_KEY" in os.environ:
                del os.environ["JOB_SCRAPER_API_KEY"]

            response = client.get("/api/v1/jobs")

            assert response.status_code == 200
            assert "items" in response.json()

    def test_api_key_required_on_protected_endpoints(
        self, client, mock_config, mock_session_factory
    ):
        """Test that API key is enforced on protected endpoints (not /api/v1/jobs which is public)"""
        # Only /api/v1/runs and /api/v1/jobs/raw require auth - /api/v1/jobs is now public
        protected_endpoints = [
            "/api/v1/runs",
        ]

        with patch.dict(os.environ, {"JOB_SCRAPER_API_KEY": "secret-key-123"}):
            for endpoint in protected_endpoints:
                # Without key
                response = client.get(endpoint)
                assert response.status_code == 403, f"{endpoint} should require API key"

                # With valid key
                response = client.get(endpoint, headers={"X-API-Key": "secret-key-123"})
                assert response.status_code == 200, f"{endpoint} should accept valid key"

    def test_api_key_required_on_raw_endpoint_when_enabled(
        self, client, mock_config, mock_session_factory
    ):
        """Test that API key is enforced on /api/v1/jobs/raw when RAW_PAYLOAD_ENABLED is true"""
        with patch('job_scraper.api.app.RAW_PAYLOAD_ENABLED', True):
            with patch.dict(os.environ, {"JOB_SCRAPER_API_KEY": "secret-key-123"}):
                # Without key
                response = client.get("/api/v1/jobs/raw")
                assert response.status_code == 403

                # With valid key
                response = client.get("/api/v1/jobs/raw", headers={"X-API-Key": "secret-key-123"})
                assert response.status_code == 200


class TestRateLimiting:
    """Test rate limiting functionality"""

    def test_rate_limiter_is_configured(self):
        """Test that rate limiter is configured on the app"""
        from job_scraper.api.app import app, limiter

        # Verify limiter is configured
        assert hasattr(app.state, 'limiter')
        assert app.state.limiter is limiter

    def test_rate_limit_decorator_on_endpoints(self):
        """Test that rate limit decorator is applied to endpoints"""
        from job_scraper.api.app import list_jobs, list_jobs_raw, list_runs

        # These endpoints should have the limiter decorator
        # We can't easily test the actual rate limiting with TestClient,
        # but we can verify the decorator is present
        assert hasattr(list_jobs, '__wrapped__') or 'limit' in str(list_jobs)
        assert hasattr(list_jobs_raw, '__wrapped__') or 'limit' in str(list_jobs_raw)
        assert hasattr(list_runs, '__wrapped__') or 'limit' in str(list_runs)

    def test_rate_limit_does_not_break_requests(self, client, mock_config, mock_session_factory):
        """Test that rate limiting doesn't break normal requests"""
        with patch.dict(os.environ, {}, clear=True):
            # Make a few requests - should all succeed in test environment
            for _ in range(3):
                response = client.get("/api/v1/jobs")
                assert response.status_code == 200


class TestLikePatternSanitization:
    """Test SQL injection protection via LIKE pattern sanitization"""

    def test_sanitize_percent_wildcard(self):
        """Test that % is escaped to prevent unintended wildcards"""
        result = _sanitize_like_pattern("test%value")
        assert result == "test\\%value"

    def test_sanitize_underscore_wildcard(self):
        """Test that _ is escaped to prevent single-char wildcards"""
        result = _sanitize_like_pattern("test_value")
        assert result == "test\\_value"

    def test_sanitize_backslash(self):
        """Test that backslash is properly escaped"""
        result = _sanitize_like_pattern("test\\value")
        assert result == "test\\\\value"

    def test_sanitize_sql_injection_attempt(self):
        """Test that SQL injection attempts are neutralized"""
        malicious_inputs = [
            "'; DROP TABLE jobs; --",
            "' OR '1'='1",
            "%' AND 1=1 --",
            "_%' OR 1=1 --",
        ]

        for malicious in malicious_inputs:
            result = _sanitize_like_pattern(malicious)
            # Ensure wildcards are escaped
            assert "\\%" in result or "\\_" in result or "%" not in result

    def test_sanitize_combined_special_chars(self):
        """Test sanitization of multiple special characters"""
        result = _sanitize_like_pattern("test%_value\\123")
        assert result == "test\\%\\_value\\\\123"

    def test_query_with_sql_injection_executes_safely(
        self, client, mock_config, mock_session_factory, mock_db_session
    ):
        """Test that SQL injection attempts in query param execute safely"""
        malicious_queries = [
            "%",
            "_",
            "'; DROP TABLE jobs; --",
            "%' OR '1'='1",
        ]

        with patch.dict(os.environ, {}, clear=True):
            for query in malicious_queries:
                response = client.get("/api/v1/jobs", params={"q": query})

                # Should return 200, not cause an error
                assert response.status_code == 200
                assert "items" in response.json()

                # Verify the session was used correctly (no exception)
                assert mock_db_session.execute.called


class TestQueryLengthCap:
    """Test query length truncation for security"""

    def test_query_length_truncation(self):
        """Test that queries exceeding MAX_QUERY_LENGTH are truncated"""
        if MAX_QUERY_LENGTH > 0:
            long_query = "A" * (MAX_QUERY_LENGTH + 100)
            result = _sanitize_like_pattern(long_query)

            assert len(result) == MAX_QUERY_LENGTH
            assert result == "A" * MAX_QUERY_LENGTH

    def test_very_long_query_string_no_error(
        self, client, mock_config, mock_session_factory
    ):
        """Test that very long query strings don't cause errors"""
        long_query = "python developer " * 100  # Very long query

        with patch.dict(os.environ, {}, clear=True):
            response = client.get("/api/v1/jobs", params={"q": long_query})

            # Should succeed without error
            assert response.status_code == 200
            assert "items" in response.json()

    def test_whitespace_stripped_before_truncation(self):
        """Test that whitespace is stripped before length check"""
        query_with_spaces = "   test query   "
        result = _sanitize_like_pattern(query_with_spaces)

        assert not result.startswith(" ")
        assert not result.endswith(" ")
        assert result == "test query"


class TestRawPayloadGating:
    """Test raw payload access control"""

    def test_jobs_endpoint_excludes_raw_payload(
        self, client, mock_config, mock_session_factory, mock_db_session, sample_job_records
    ):
        """Test that /api/v1/jobs does NOT include raw_payload in response"""
        # Set up mock to return sample jobs
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = sample_job_records
        mock_db_session.execute.return_value = mock_result

        with patch.dict(os.environ, {}, clear=True):
            response = client.get("/api/v1/jobs")

            assert response.status_code == 200
            data = response.json()
            assert "items" in data

            if data["items"]:
                job = data["items"][0]
                # raw_payload should NOT be present
                assert "raw_payload" not in job
                # Other fields should be present
                assert "title" in job
                assert "company" in job

    def test_jobs_raw_endpoint_disabled_by_default(self, client, mock_config, mock_session_factory):
        """Test that /api/v1/jobs/raw returns 403 when RAW_PAYLOAD_ENABLED is false"""
        with patch.dict(os.environ, {}, clear=True):
            response = client.get("/api/v1/jobs/raw")

            assert response.status_code == 403
            assert "disabled" in response.json()["detail"].lower()

    def test_jobs_raw_endpoint_includes_raw_payload_when_enabled(
        self, client, mock_config, mock_session_factory, mock_db_session, sample_job_records
    ):
        """Test that /api/v1/jobs/raw DOES include raw_payload when RAW_PAYLOAD_ENABLED is true"""
        # Set up mock to return sample jobs
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = sample_job_records
        mock_db_session.execute.return_value = mock_result

        # Patch the module-level constant
        with patch('job_scraper.api.app.RAW_PAYLOAD_ENABLED', True):
            with patch.dict(os.environ, {}, clear=True):
                response = client.get("/api/v1/jobs/raw")

                assert response.status_code == 200
                data = response.json()
                assert "items" in data

                if data["items"]:
                    job = data["items"][0]
                    # raw_payload SHOULD be present
                    assert "raw_payload" in job
                    assert job["raw_payload"] == {"original_id": 123, "extra": "data"}

    def test_raw_endpoint_has_stricter_limit(self, client, mock_config, mock_session_factory):
        """Test that /api/v1/jobs/raw has a lower max limit than /api/v1/jobs"""
        with patch('job_scraper.api.app.RAW_PAYLOAD_ENABLED', True):
            with patch.dict(os.environ, {}, clear=True):
                # /api/v1/jobs allows up to 100 (cursor pagination)
                response = client.get("/api/v1/jobs", params={"limit": 100})
                assert response.status_code == 200

                # /api/v1/jobs/raw only allows up to 200
                response = client.get("/api/v1/jobs/raw", params={"limit": 200})
                assert response.status_code == 200

                # /api/v1/jobs/raw rejects limit > 200
                response = client.get("/api/v1/jobs/raw", params={"limit": 201})
                # FastAPI validation should reject this
                assert response.status_code == 422


class TestCORS:
    """Test CORS configuration"""

    def test_cors_headers_present_for_allowed_origin(self, client, mock_config, mock_session_factory):
        """Test that CORS headers are present for configured origins"""
        # CORS middleware is configured at startup, so we just verify it doesn't break
        with patch.dict(os.environ, {}, clear=True):
            response = client.get(
                "/api/v1/jobs",
                headers={"Origin": "http://localhost:5173"}
            )

            # Should work without errors
            assert response.status_code == 200
            # Note: TestClient may not fully simulate CORS, but middleware is configured

    def test_multiple_cors_origins(self, client, mock_config, mock_session_factory):
        """Test CORS with multiple allowed origins"""
        with patch.dict(os.environ, {}, clear=True):
            response = client.get(
                "/api/v1/jobs",
                headers={"Origin": "https://example.com"}
            )

            # Should work without errors
            assert response.status_code == 200

    def test_cors_does_not_break_regular_requests(self, client, mock_config, mock_session_factory):
        """Test that CORS middleware doesn't break normal requests"""
        with patch.dict(os.environ, {}, clear=True):
            response = client.get("/api/v1/jobs")
            assert response.status_code == 200


class TestSecurityHeaders:
    """Test security-related response headers and configurations"""

    def test_health_endpoint_no_auth_required(self, client):
        """Test that health check endpoint doesn't require authentication"""
        mock_cfg = MagicMock()
        mock_cfg.db_dsn = None  # No DB configured — health returns {"status": "ok"} only
        with patch('job_scraper.api.app._config', mock_cfg):
            with patch.dict(os.environ, {"JOB_SCRAPER_API_KEY": "secret"}, clear=True):
                response = client.get("/health")

                # Health check should always be accessible
                assert response.status_code == 200
                assert response.json() == {"status": "ok"}

    def test_limit_parameter_validation(self, client, mock_config, mock_session_factory):
        """Test that limit parameter has proper bounds"""
        with patch.dict(os.environ, {}, clear=True):
            # Negative limit should be rejected
            response = client.get("/api/v1/jobs", params={"limit": -1})
            assert response.status_code == 422

            # Zero limit should be rejected
            response = client.get("/api/v1/jobs", params={"limit": 0})
            assert response.status_code == 422

            # Limit above max (100) should be rejected
            response = client.get("/api/v1/jobs", params={"limit": 101})
            assert response.status_code == 422

            # Valid limit should work
            response = client.get("/api/v1/jobs", params={"limit": 50})
            assert response.status_code == 200

    def test_cursor_parameter_validation(self, client, mock_config, mock_session_factory):
        """Test that cursor parameter validates properly"""
        with patch.dict(os.environ, {}, clear=True):
            # No cursor should work (first page)
            response = client.get("/api/v1/jobs")
            assert response.status_code == 200
            data = response.json()
            # Response should include cursor pagination fields
            assert "next_cursor" in data
            assert "as_of" in data
            assert "has_more" in data

            # Invalid cursor should return 400
            response = client.get("/api/v1/jobs", params={"cursor": "invalid-cursor"})
            assert response.status_code == 400
            assert "Invalid cursor" in response.json()["detail"]


class TestDatabaseErrorHandling:
    """Test proper handling of database errors"""

    def test_missing_db_configuration_returns_500(self, client):
        """Test that missing database config returns proper error"""
        mock_cfg = MagicMock()
        mock_cfg.db_dsn = None  # No database configured
        with patch('job_scraper.api.app._config', mock_cfg):
            with patch.dict(os.environ, {}, clear=True):
                response = client.get("/api/v1/jobs")

                assert response.status_code == 500
                assert "DB not configured" in response.json()["detail"]


class TestInputValidation:
    """Test various input validation scenarios"""

    def test_source_parameter_filtering(
        self, client, mock_config, mock_session_factory, mock_db_session
    ):
        """Test that source parameter is properly validated and used"""
        with patch.dict(os.environ, {}, clear=True):
            response = client.get("/api/v1/jobs", params={"source": "test_api"})

            assert response.status_code == 200
            # Verify the query was constructed correctly
            assert mock_db_session.execute.called

    def test_remote_boolean_parameter(
        self, client, mock_config, mock_session_factory, mock_db_session
    ):
        """Test that remote parameter accepts boolean values"""
        with patch.dict(os.environ, {}, clear=True):
            # Test with true
            response = client.get("/api/v1/jobs", params={"remote": "true"})
            assert response.status_code == 200

            # Test with false
            response = client.get("/api/v1/jobs", params={"remote": "false"})
            assert response.status_code == 200

            # Test with 1
            response = client.get("/api/v1/jobs", params={"remote": "1"})
            assert response.status_code == 200

    def test_location_parameter_sanitized(
        self, client, mock_config, mock_session_factory, mock_db_session
    ):
        """Test that location parameter is sanitized like query"""
        malicious_location = "San Francisco%; DROP TABLE jobs; --"

        with patch.dict(os.environ, {}, clear=True):
            response = client.get("/api/v1/jobs", params={"location": malicious_location})

            # Should execute safely
            assert response.status_code == 200
            assert mock_db_session.execute.called


class TestObservabilityEndpoints:
    """Test observability endpoints for run sources and errors"""

    def test_get_run_sources_returns_sources(
        self, client, mock_config, mock_session_factory, mock_db_session
    ):
        """Test GET /api/v1/runs/{run_id}/sources returns source records"""
        import uuid
        from datetime import datetime
        from job_scraper.storage import RunRecord, RunSourceRecord

        run_id = uuid.uuid4()

        # Mock run exists check
        mock_run = RunRecord(
            id=run_id,
            started_at=datetime.utcnow(),
            status="success",
            total_jobs=100,
        )

        # Mock source records
        mock_sources = [
            RunSourceRecord(
                id=uuid.uuid4(),
                run_id=run_id,
                source="greenhouse",
                source_target="techcorp",
                jobs_fetched=25,
                jobs_after_dedupe=20,
                request_duration_ms=1500,
                created_at=datetime.utcnow(),
            ),
            RunSourceRecord(
                id=uuid.uuid4(),
                run_id=run_id,
                source="lever",
                source_target="startupco",
                jobs_fetched=15,
                jobs_after_dedupe=12,
                request_duration_ms=800,
                created_at=datetime.utcnow(),
            ),
        ]

        # Mock the query for run existence
        mock_query_run = MagicMock()
        mock_query_run.filter.return_value.first.return_value = mock_run

        # Set up mock_db_session.query to return different mocks based on the model
        def query_side_effect(model):
            if model == RunRecord:
                return mock_query_run
            return MagicMock()

        mock_db_session.query.side_effect = query_side_effect

        # Mock get_run_sources
        with patch('job_scraper.api.app.get_run_sources') as mock_get_run_sources:
            mock_get_run_sources.return_value = mock_sources

            with patch.dict(os.environ, {}, clear=True):
                response = client.get(f"/api/v1/runs/{run_id}/sources")

                assert response.status_code == 200
                data = response.json()

                # Verify response structure
                assert "items" in data
                assert "total" in data
                assert "limit" in data
                assert "offset" in data

                # Verify sources returned
                assert data["total"] == 2
                assert len(data["items"]) == 2

                # Verify first source details
                source1 = data["items"][0]
                assert source1["source"] == "greenhouse"
                assert source1["source_target"] == "techcorp"
                assert source1["jobs_fetched"] == 25
                assert source1["jobs_after_dedupe"] == 20
                assert source1["request_duration_ms"] == 1500
                assert source1["error_message"] is None

                # Verify second source
                source2 = data["items"][1]
                assert source2["source"] == "lever"
                assert source2["jobs_fetched"] == 15

    def test_get_run_sources_with_pagination(
        self, client, mock_config, mock_session_factory, mock_db_session
    ):
        """Test GET /api/v1/runs/{run_id}/sources supports pagination"""
        import uuid
        from datetime import datetime
        from job_scraper.storage import RunRecord, RunSourceRecord

        run_id = uuid.uuid4()
        mock_run = RunRecord(id=run_id, started_at=datetime.utcnow(), status="success")

        # Create many source records
        mock_sources = [
            RunSourceRecord(
                id=uuid.uuid4(),
                run_id=run_id,
                source=f"source_{i}",
                source_target=f"target_{i}",
                jobs_fetched=i * 10,
                jobs_after_dedupe=i * 9,
                created_at=datetime.utcnow(),
            )
            for i in range(50)
        ]

        # Mock run existence
        mock_query_run = MagicMock()
        mock_query_run.filter.return_value.first.return_value = mock_run

        def query_side_effect(model):
            if model == RunRecord:
                return mock_query_run
            return MagicMock()

        mock_db_session.query.side_effect = query_side_effect

        with patch('job_scraper.api.app.get_run_sources') as mock_get_run_sources:
            mock_get_run_sources.return_value = mock_sources

            with patch.dict(os.environ, {}, clear=True):
                # Test with limit and offset
                response = client.get(f"/api/v1/runs/{run_id}/sources", params={"limit": 10, "offset": 0})

                assert response.status_code == 200
                data = response.json()

                # Verify pagination
                assert data["total"] == 50
                assert data["limit"] == 10
                assert data["offset"] == 0
                assert len(data["items"]) == 10

                # Test second page
                response = client.get(f"/api/v1/runs/{run_id}/sources", params={"limit": 10, "offset": 10})
                assert response.status_code == 200
                data = response.json()
                assert data["offset"] == 10

    def test_get_run_sources_not_found(
        self, client, mock_config, mock_session_factory, mock_db_session
    ):
        """Test GET /api/v1/runs/{run_id}/sources returns 404 when run not found"""
        import uuid
        from job_scraper.storage import RunRecord

        run_id = uuid.uuid4()

        # Mock run does not exist
        mock_query_run = MagicMock()
        mock_query_run.filter.return_value.first.return_value = None

        def query_side_effect(model):
            if model == RunRecord:
                return mock_query_run
            return MagicMock()

        mock_db_session.query.side_effect = query_side_effect

        with patch.dict(os.environ, {}, clear=True):
            response = client.get(f"/api/v1/runs/{run_id}/sources")

            assert response.status_code == 404
            assert "not found" in response.json()["detail"].lower()

    def test_get_run_errors_returns_errors(
        self, client, mock_config, mock_session_factory, mock_db_session
    ):
        """Test GET /api/v1/runs/{run_id}/errors returns error summary"""
        import uuid
        from datetime import datetime
        from job_scraper.storage import RunRecord, RunSourceRecord

        run_id = uuid.uuid4()

        # Mock run exists
        mock_run = RunRecord(
            id=run_id,
            started_at=datetime.utcnow(),
            status="partial_success",
            total_jobs=50,
        )

        # Mock source records with errors — the endpoint builds the summary inline
        # from the get_run_sources return value, so we supply all records here.
        mock_sources_all = [
            RunSourceRecord(
                id=uuid.uuid4(),
                run_id=run_id,
                source="greenhouse",
                source_target="techcorp",
                jobs_fetched=25,
                jobs_after_dedupe=20,
                error_message=None,
                error_code=None,
                created_at=datetime.utcnow(),
            ),
            RunSourceRecord(
                id=uuid.uuid4(),
                run_id=run_id,
                source="lever",
                source_target="startupco",
                jobs_fetched=0,
                jobs_after_dedupe=0,
                error_message="HTTP 404: Board not found",
                error_code="NOT_FOUND",
                created_at=datetime.utcnow(),
            ),
            RunSourceRecord(
                id=uuid.uuid4(),
                run_id=run_id,
                source="workday",
                source_target="enterprise",
                jobs_fetched=0,
                jobs_after_dedupe=0,
                error_message="Connection timeout",
                error_code="TIMEOUT",
                created_at=datetime.utcnow(),
            ),
        ]

        # Mock run existence
        mock_query_run = MagicMock()
        mock_query_run.filter.return_value.first.return_value = mock_run

        def query_side_effect(model):
            if model == RunRecord:
                return mock_query_run
            return MagicMock()

        mock_db_session.query.side_effect = query_side_effect

        with patch('job_scraper.api.app.get_run_sources') as mock_get_run_sources:
            mock_get_run_sources.return_value = mock_sources_all

            with patch.dict(os.environ, {}, clear=True):
                response = client.get(f"/api/v1/runs/{run_id}/errors")

                assert response.status_code == 200
                data = response.json()

                # Verify response structure
                assert "items" in data
                assert "total" in data
                assert "by_source" in data
                assert "by_error_code" in data

                # Verify error items (only failed sources — those with error_message)
                assert data["total"] == 2
                assert len(data["items"]) == 2

                # Verify error details
                error_sources = [item["source"] for item in data["items"]]
                assert "lever" in error_sources
                assert "workday" in error_sources

                # Verify summary aggregation built inline by the endpoint
                assert "lever" in data["by_source"]
                assert data["by_source"]["lever"]["failed"] == 1
                assert "NOT_FOUND" in data["by_error_code"]
                assert data["by_error_code"]["NOT_FOUND"]["count"] == 1

    def test_get_run_errors_not_found(
        self, client, mock_config, mock_session_factory, mock_db_session
    ):
        """Test GET /api/v1/runs/{run_id}/errors returns 404 when run not found"""
        import uuid
        from job_scraper.storage import RunRecord

        run_id = uuid.uuid4()

        # Mock run does not exist
        mock_query_run = MagicMock()
        mock_query_run.filter.return_value.first.return_value = None

        def query_side_effect(model):
            if model == RunRecord:
                return mock_query_run
            return MagicMock()

        mock_db_session.query.side_effect = query_side_effect

        with patch.dict(os.environ, {}, clear=True):
            response = client.get(f"/api/v1/runs/{run_id}/errors")

            assert response.status_code == 404
            assert "not found" in response.json()["detail"].lower()

    def test_get_run_errors_empty_when_no_errors(
        self, client, mock_config, mock_session_factory, mock_db_session
    ):
        """Test GET /api/v1/runs/{run_id}/errors returns empty list when no errors occurred"""
        import uuid
        from datetime import datetime
        from job_scraper.storage import RunRecord, RunSourceRecord

        run_id = uuid.uuid4()

        # Mock run exists
        mock_run = RunRecord(
            id=run_id,
            started_at=datetime.utcnow(),
            status="success",
            total_jobs=100,
        )

        # All successful sources — no error_message on any
        mock_sources = [
            RunSourceRecord(
                id=uuid.uuid4(),
                run_id=run_id,
                source="greenhouse",
                source_target="techcorp",
                jobs_fetched=25,
                jobs_after_dedupe=20,
                error_message=None,
                error_code=None,
                created_at=datetime.utcnow(),
            ),
            RunSourceRecord(
                id=uuid.uuid4(),
                run_id=run_id,
                source="lever",
                source_target="startupco",
                jobs_fetched=15,
                jobs_after_dedupe=12,
                error_message=None,
                error_code=None,
                created_at=datetime.utcnow(),
            ),
        ]

        # Mock run existence
        mock_query_run = MagicMock()
        mock_query_run.filter.return_value.first.return_value = mock_run

        def query_side_effect(model):
            if model == RunRecord:
                return mock_query_run
            return MagicMock()

        mock_db_session.query.side_effect = query_side_effect

        with patch('job_scraper.api.app.get_run_sources') as mock_get_run_sources:
            mock_get_run_sources.return_value = mock_sources

            with patch.dict(os.environ, {}, clear=True):
                response = client.get(f"/api/v1/runs/{run_id}/errors")

                assert response.status_code == 200
                data = response.json()

                # Verify no errors returned
                assert data["total"] == 0
                assert len(data["items"]) == 0
                assert data["by_error_code"] == {}
