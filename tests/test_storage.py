"""
Tests for storage layer observability features.

Tests cover:
- record_source_result() for tracking per-source ingestion results
- get_run_sources() for retrieving source results by run
- get_source_error_summary() for aggregating error statistics
"""
import uuid
import pytest
from datetime import datetime
from unittest.mock import MagicMock, patch

from job_scraper.storage import (
    record_source_result,
    get_run_sources,
    get_source_error_summary,
    RunSourceRecord,
)


@pytest.fixture
def mock_db_dsn():
    """Mock database connection string"""
    return "postgresql://test:test@localhost/test_db"


@pytest.fixture
def mock_run_id():
    """Generate a test run ID"""
    return uuid.uuid4()


class TestRecordSourceResult:
    """Test recording source fetch results"""

    def test_record_source_result_success(self, mock_db_dsn, mock_run_id):
        """Test recording a successful source fetch"""
        with patch('job_scraper.storage.session_scope') as mock_session_scope:
            mock_session = MagicMock()
            mock_session_scope.return_value.__enter__.return_value = mock_session

            # Record a successful fetch
            record_source_result(
                dsn=mock_db_dsn,
                run_id=mock_run_id,
                source="greenhouse",
                source_target="techcorp",
                jobs_fetched=25,
                jobs_after_dedupe=20,
                duration_ms=1500,
            )

            # Verify session.add was called with a RunSourceRecord
            assert mock_session.add.called
            added_record = mock_session.add.call_args[0][0]
            assert isinstance(added_record, RunSourceRecord)
            assert added_record.run_id == mock_run_id
            assert added_record.source == "greenhouse"
            assert added_record.source_target == "techcorp"
            assert added_record.jobs_fetched == 25
            assert added_record.jobs_after_dedupe == 20
            assert added_record.error_message is None
            assert added_record.error_code is None
            assert added_record.request_duration_ms == 1500

    def test_record_source_result_with_error(self, mock_db_dsn, mock_run_id):
        """Test recording a failed source fetch with error details"""
        with patch('job_scraper.storage.session_scope') as mock_session_scope:
            mock_session = MagicMock()
            mock_session_scope.return_value.__enter__.return_value = mock_session

            # Record a failed fetch
            record_source_result(
                dsn=mock_db_dsn,
                run_id=mock_run_id,
                source="lever",
                source_target="startupco",
                jobs_fetched=0,
                jobs_after_dedupe=0,
                error_message="HTTP 404: Board not found",
                error_code="NOT_FOUND",
                duration_ms=500,
            )

            # Verify error details are recorded
            assert mock_session.add.called
            added_record = mock_session.add.call_args[0][0]
            assert isinstance(added_record, RunSourceRecord)
            assert added_record.run_id == mock_run_id
            assert added_record.source == "lever"
            assert added_record.source_target == "startupco"
            assert added_record.jobs_fetched == 0
            assert added_record.jobs_after_dedupe == 0
            assert added_record.error_message == "HTTP 404: Board not found"
            assert added_record.error_code == "NOT_FOUND"
            assert added_record.request_duration_ms == 500

    def test_record_source_result_without_target(self, mock_db_dsn, mock_run_id):
        """Test recording result for sources without specific targets (e.g., public APIs)"""
        with patch('job_scraper.storage.session_scope') as mock_session_scope:
            mock_session = MagicMock()
            mock_session_scope.return_value.__enter__.return_value = mock_session

            # Record result for a source without a specific target
            record_source_result(
                dsn=mock_db_dsn,
                run_id=mock_run_id,
                source="usajobs",
                source_target=None,  # No target for public API
                jobs_fetched=100,
                jobs_after_dedupe=95,
                duration_ms=3000,
            )

            # Verify source_target can be None
            assert mock_session.add.called
            added_record = mock_session.add.call_args[0][0]
            assert added_record.source == "usajobs"
            assert added_record.source_target is None
            assert added_record.jobs_fetched == 100

    def test_record_source_result_partial_error(self, mock_db_dsn, mock_run_id):
        """Test recording a partial failure (some jobs fetched but error occurred)"""
        with patch('job_scraper.storage.session_scope') as mock_session_scope:
            mock_session = MagicMock()
            mock_session_scope.return_value.__enter__.return_value = mock_session

            # Record partial success (got some jobs but hit rate limit)
            record_source_result(
                dsn=mock_db_dsn,
                run_id=mock_run_id,
                source="adzuna",
                source_target=None,
                jobs_fetched=50,
                jobs_after_dedupe=45,
                error_message="Rate limit exceeded after 50 results",
                error_code="RATE_LIMIT",
                duration_ms=2000,
            )

            # Verify both jobs and error are recorded
            assert mock_session.add.called
            added_record = mock_session.add.call_args[0][0]
            assert added_record.jobs_fetched == 50
            assert added_record.jobs_after_dedupe == 45
            assert added_record.error_message == "Rate limit exceeded after 50 results"
            assert added_record.error_code == "RATE_LIMIT"


class TestGetRunSources:
    """Test retrieving source results for a run"""

    def test_get_run_sources_returns_all_records(self, mock_db_dsn, mock_run_id):
        """Test that get_run_sources retrieves all source records for a run"""
        with patch('job_scraper.storage.session_scope') as mock_session_scope:
            mock_session = MagicMock()
            mock_session_scope.return_value.__enter__.return_value = mock_session

            # Create mock source records
            mock_records = [
                RunSourceRecord(
                    id=uuid.uuid4(),
                    run_id=mock_run_id,
                    source="greenhouse",
                    source_target="techcorp",
                    jobs_fetched=25,
                    jobs_after_dedupe=20,
                    created_at=datetime.utcnow(),
                ),
                RunSourceRecord(
                    id=uuid.uuid4(),
                    run_id=mock_run_id,
                    source="lever",
                    source_target="startupco",
                    jobs_fetched=15,
                    jobs_after_dedupe=12,
                    created_at=datetime.utcnow(),
                ),
            ]

            # Mock the query
            mock_query = MagicMock()
            mock_query.filter.return_value.all.return_value = mock_records
            mock_session.query.return_value = mock_query

            # Get run sources
            results = get_run_sources(mock_db_dsn, mock_run_id)

            # Verify query was made with correct filter
            mock_session.query.assert_called_once_with(RunSourceRecord)
            assert mock_query.filter.called

            # Verify all records returned
            assert len(results) == 2
            assert results[0].source == "greenhouse"
            assert results[1].source == "lever"

            # Verify session.expunge_all was called (detach from session)
            assert mock_session.expunge_all.called

    def test_get_run_sources_empty_result(self, mock_db_dsn, mock_run_id):
        """Test get_run_sources returns empty list when no sources found"""
        with patch('job_scraper.storage.session_scope') as mock_session_scope:
            mock_session = MagicMock()
            mock_session_scope.return_value.__enter__.return_value = mock_session

            # Mock empty query result
            mock_query = MagicMock()
            mock_query.filter.return_value.all.return_value = []
            mock_session.query.return_value = mock_query

            # Get run sources
            results = get_run_sources(mock_db_dsn, mock_run_id)

            # Verify empty list returned
            assert results == []
            assert isinstance(results, list)


class TestGetSourceErrorSummary:
    """Test error summary aggregation"""

    def test_get_source_error_summary_with_mixed_results(self, mock_db_dsn, mock_run_id):
        """Test error summary with both successful and failed sources"""
        with patch('job_scraper.storage.session_scope') as mock_session_scope:
            mock_session = MagicMock()
            mock_session_scope.return_value.__enter__.return_value = mock_session

            # Create mock records with mix of success and errors
            mock_records = [
                # Successful sources
                RunSourceRecord(
                    id=uuid.uuid4(),
                    run_id=mock_run_id,
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
                    run_id=mock_run_id,
                    source="greenhouse",
                    source_target="bigcorp",
                    jobs_fetched=30,
                    jobs_after_dedupe=28,
                    error_message=None,
                    error_code=None,
                    created_at=datetime.utcnow(),
                ),
                # Failed sources
                RunSourceRecord(
                    id=uuid.uuid4(),
                    run_id=mock_run_id,
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
                    run_id=mock_run_id,
                    source="workday",
                    source_target="enterprise",
                    jobs_fetched=0,
                    jobs_after_dedupe=0,
                    error_message="Connection timeout",
                    error_code="TIMEOUT",
                    created_at=datetime.utcnow(),
                ),
            ]

            # Mock the query
            mock_query = MagicMock()
            mock_query.filter.return_value.all.return_value = mock_records
            mock_session.query.return_value = mock_query

            # Get error summary
            summary = get_source_error_summary(mock_db_dsn, mock_run_id)

            # Verify summary structure
            assert summary["total_sources"] == 4
            assert summary["successful"] == 2
            assert summary["failed"] == 2

            # Verify by_source grouping
            assert "greenhouse" in summary["by_source"]
            assert summary["by_source"]["greenhouse"]["total"] == 2
            assert summary["by_source"]["greenhouse"]["successful"] == 2
            assert summary["by_source"]["greenhouse"]["failed"] == 0
            assert len(summary["by_source"]["greenhouse"]["errors"]) == 0

            assert "lever" in summary["by_source"]
            assert summary["by_source"]["lever"]["total"] == 1
            assert summary["by_source"]["lever"]["successful"] == 0
            assert summary["by_source"]["lever"]["failed"] == 1
            assert len(summary["by_source"]["lever"]["errors"]) == 1
            assert summary["by_source"]["lever"]["errors"][0]["error_code"] == "NOT_FOUND"

            # Verify by_error_code grouping
            assert "NOT_FOUND" in summary["by_error_code"]
            assert summary["by_error_code"]["NOT_FOUND"]["count"] == 1
            assert summary["by_error_code"]["NOT_FOUND"]["sources"][0]["source"] == "lever"

            assert "TIMEOUT" in summary["by_error_code"]
            assert summary["by_error_code"]["TIMEOUT"]["count"] == 1
            assert summary["by_error_code"]["TIMEOUT"]["sources"][0]["source"] == "workday"

    def test_get_source_error_summary_all_successful(self, mock_db_dsn, mock_run_id):
        """Test error summary when all sources succeed"""
        with patch('job_scraper.storage.session_scope') as mock_session_scope:
            mock_session = MagicMock()
            mock_session_scope.return_value.__enter__.return_value = mock_session

            # All successful sources
            mock_records = [
                RunSourceRecord(
                    id=uuid.uuid4(),
                    run_id=mock_run_id,
                    source="usajobs",
                    source_target=None,
                    jobs_fetched=100,
                    jobs_after_dedupe=95,
                    error_message=None,
                    error_code=None,
                    created_at=datetime.utcnow(),
                ),
                RunSourceRecord(
                    id=uuid.uuid4(),
                    run_id=mock_run_id,
                    source="adzuna",
                    source_target=None,
                    jobs_fetched=50,
                    jobs_after_dedupe=48,
                    error_message=None,
                    error_code=None,
                    created_at=datetime.utcnow(),
                ),
            ]

            mock_query = MagicMock()
            mock_query.filter.return_value.all.return_value = mock_records
            mock_session.query.return_value = mock_query

            summary = get_source_error_summary(mock_db_dsn, mock_run_id)

            # Verify no errors
            assert summary["total_sources"] == 2
            assert summary["successful"] == 2
            assert summary["failed"] == 0
            assert summary["by_error_code"] == {}

    def test_get_source_error_summary_all_failed(self, mock_db_dsn, mock_run_id):
        """Test error summary when all sources fail"""
        with patch('job_scraper.storage.session_scope') as mock_session_scope:
            mock_session = MagicMock()
            mock_session_scope.return_value.__enter__.return_value = mock_session

            # All failed sources
            mock_records = [
                RunSourceRecord(
                    id=uuid.uuid4(),
                    run_id=mock_run_id,
                    source="lever",
                    source_target="company1",
                    jobs_fetched=0,
                    jobs_after_dedupe=0,
                    error_message="API key invalid",
                    error_code="AUTH_ERROR",
                    created_at=datetime.utcnow(),
                ),
                RunSourceRecord(
                    id=uuid.uuid4(),
                    run_id=mock_run_id,
                    source="lever",
                    source_target="company2",
                    jobs_fetched=0,
                    jobs_after_dedupe=0,
                    error_message="API key invalid",
                    error_code="AUTH_ERROR",
                    created_at=datetime.utcnow(),
                ),
            ]

            mock_query = MagicMock()
            mock_query.filter.return_value.all.return_value = mock_records
            mock_session.query.return_value = mock_query

            summary = get_source_error_summary(mock_db_dsn, mock_run_id)

            # Verify all failed
            assert summary["total_sources"] == 2
            assert summary["successful"] == 0
            assert summary["failed"] == 2

            # Verify error grouping
            assert "AUTH_ERROR" in summary["by_error_code"]
            assert summary["by_error_code"]["AUTH_ERROR"]["count"] == 2

    def test_get_source_error_summary_empty_run(self, mock_db_dsn, mock_run_id):
        """Test error summary for a run with no source records"""
        with patch('job_scraper.storage.session_scope') as mock_session_scope:
            mock_session = MagicMock()
            mock_session_scope.return_value.__enter__.return_value = mock_session

            # Empty result
            mock_query = MagicMock()
            mock_query.filter.return_value.all.return_value = []
            mock_session.query.return_value = mock_query

            summary = get_source_error_summary(mock_db_dsn, mock_run_id)

            # Verify empty summary
            assert summary["total_sources"] == 0
            assert summary["successful"] == 0
            assert summary["failed"] == 0
            assert summary["by_source"] == {}
            assert summary["by_error_code"] == {}

    def test_get_source_error_summary_same_error_multiple_sources(self, mock_db_dsn, mock_run_id):
        """Test error summary when same error occurs across multiple sources"""
        with patch('job_scraper.storage.session_scope') as mock_session_scope:
            mock_session = MagicMock()
            mock_session_scope.return_value.__enter__.return_value = mock_session

            # Same error code across different sources
            mock_records = [
                RunSourceRecord(
                    id=uuid.uuid4(),
                    run_id=mock_run_id,
                    source="greenhouse",
                    source_target="techcorp",
                    jobs_fetched=0,
                    jobs_after_dedupe=0,
                    error_message="Rate limit exceeded",
                    error_code="RATE_LIMIT",
                    created_at=datetime.utcnow(),
                ),
                RunSourceRecord(
                    id=uuid.uuid4(),
                    run_id=mock_run_id,
                    source="lever",
                    source_target="startupco",
                    jobs_fetched=0,
                    jobs_after_dedupe=0,
                    error_message="API rate limit hit",
                    error_code="RATE_LIMIT",
                    created_at=datetime.utcnow(),
                ),
                RunSourceRecord(
                    id=uuid.uuid4(),
                    run_id=mock_run_id,
                    source="adzuna",
                    source_target=None,
                    jobs_fetched=0,
                    jobs_after_dedupe=0,
                    error_message="Too many requests",
                    error_code="RATE_LIMIT",
                    created_at=datetime.utcnow(),
                ),
            ]

            mock_query = MagicMock()
            mock_query.filter.return_value.all.return_value = mock_records
            mock_session.query.return_value = mock_query

            summary = get_source_error_summary(mock_db_dsn, mock_run_id)

            # Verify error aggregation by code
            assert summary["by_error_code"]["RATE_LIMIT"]["count"] == 3
            assert len(summary["by_error_code"]["RATE_LIMIT"]["sources"]) == 3

            # Verify sources are listed
            sources_with_rate_limit = [
                s["source"] for s in summary["by_error_code"]["RATE_LIMIT"]["sources"]
            ]
            assert "greenhouse" in sources_with_rate_limit
            assert "lever" in sources_with_rate_limit
            assert "adzuna" in sources_with_rate_limit
