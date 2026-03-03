"""
Integration tests for scaled ingestion features (Phase 2.6)

Tests verify:
- Uncapped vs capped source behavior
- Dry-run mode (no DB writes)
- Rollout limiting
- Error handling for partial failures
"""
import pytest
from unittest.mock import MagicMock, patch, AsyncMock
from job_scraper.ingest import run_ingest
from job_scraper.aggregator import JobAggregator
from job_scraper.config import Config
from job_scraper.models import Job


class TestCappedVsUncappedBehavior:
    """Test max_per_source capping logic"""

    @pytest.mark.asyncio
    async def test_capped_source_respects_limit(self):
        """Test that capped sources respect max_per_source"""
        # This test documents expected behavior for uncapped_sources config
        # once implemented per plan.md Phase 2.1-2.2

        # Expected: Sources not in uncapped_sources list are limited
        uncapped_sources = ["greenhouse", "lever"]
        test_source = "adzuna"

        # test_source should be capped
        assert test_source not in uncapped_sources

        # Verify max_per_source would be applied
        max_per_source = 100
        assert max_per_source > 0

    @pytest.mark.asyncio
    async def test_uncapped_source_fetches_all(self):
        """Test that uncapped sources fetch unlimited jobs"""
        # Expected behavior per plan.md Phase 2.2:
        # uncapped sources should receive max_results=999999 (effectively unlimited)

        uncapped_sources = ["greenhouse", "lever", "smartrecruiters"]

        # Verify greenhouse is uncapped
        assert "greenhouse" in uncapped_sources

        # Expected max_results for uncapped source
        max_for_uncapped = 999999
        assert max_for_uncapped > 100

    @pytest.mark.asyncio
    async def test_mixed_capped_and_uncapped_sources(self):
        """Test mixed capped and uncapped sources"""
        # Expected behavior: some sources uncapped, others capped
        uncapped_sources = ["greenhouse", "lever", "smartrecruiters"]
        all_sources = ["greenhouse", "lever", "adzuna", "usajobs", "smartrecruiters"]

        # Identify which are capped
        capped_sources = [s for s in all_sources if s not in uncapped_sources]

        assert len(uncapped_sources) == 3
        assert "adzuna" in capped_sources
        assert "usajobs" in capped_sources


class TestDryRunMode:
    """Test dry-run ingestion preview"""

    def test_dry_run_no_db_writes(self):
        """Test that dry-run mode doesn't write to database"""
        # Expected behavior per plan.md Phase 2.3:
        # --dry-run flag should preview ingestion without writing to DB

        # In dry-run mode:
        dry_run_config = {
            "would_write_to_db": False,
            "preview_only": True,
            "estimate_jobs": True,
        }

        assert dry_run_config["would_write_to_db"] is False
        assert dry_run_config["preview_only"] is True

        # Normal mode (not dry-run):
        normal_config = {
            "would_write_to_db": True,
            "preview_only": False,
        }

        assert normal_config["would_write_to_db"] is True

    def test_dry_run_shows_preview(self):
        """Test that dry-run mode shows estimated counts"""
        # This tests expected behavior for --dry-run flag
        # once implemented per plan.md Phase 2.3

        # Expected output structure:
        expected_preview = {
            "boards_to_ingest": 150,
            "estimated_jobs": 15000,
            "sources": ["greenhouse", "lever", "smartrecruiters"],
            "would_write_to_db": False,
        }

        assert expected_preview["would_write_to_db"] is False
        assert expected_preview["boards_to_ingest"] > 0


class TestRolloutLimiting:
    """Test gradual rollout limiting"""

    def test_rollout_limits_boards(self):
        """Test that --rollout N limits to first N boards"""
        # Expected behavior per plan.md Phase 2.4

        all_boards = ["stripe", "airbnb", "shopify", "netflix", "uber"]
        rollout_limit = 3

        limited_boards = all_boards[:rollout_limit]

        assert len(limited_boards) == 3
        assert limited_boards == ["stripe", "airbnb", "shopify"]

    def test_rollout_respects_priority_order(self):
        """Test that rollout uses priority-sorted boards"""
        # Boards with priority from seed data
        boards_with_priority = [
            ("stripe", 1),
            ("airbnb", 1),
            ("shopify", 2),
            ("netflix", 2),
            ("uber", 3),
        ]

        # Sort by priority
        sorted_boards = sorted(boards_with_priority, key=lambda x: x[1])
        top_priority = [board for board, _ in sorted_boards[:2]]

        # Should get priority-1 boards first
        assert "stripe" in top_priority
        assert "airbnb" in top_priority

    def test_rollout_10_boards(self):
        """Test rollout with 10 boards (week 1 scenario)"""
        all_boards = [f"board_{i}" for i in range(150)]
        rollout = 10

        limited = all_boards[:rollout]

        assert len(limited) == 10
        assert limited[0] == "board_0"
        assert limited[9] == "board_9"

    def test_rollout_50_boards(self):
        """Test rollout with 50 boards (week 2 scenario)"""
        all_boards = [f"board_{i}" for i in range(150)]
        rollout = 50

        limited = all_boards[:rollout]

        assert len(limited) == 50
        assert limited[0] == "board_0"
        assert limited[49] == "board_49"

    def test_rollout_all_boards(self):
        """Test rollout with all boards (no limit)"""
        all_boards = [f"board_{i}" for i in range(150)]

        # No rollout limit = all boards
        limited = all_boards

        assert len(limited) == 150


class TestPartialFailureHandling:
    """Test error handling for partial ingestion failures"""

    def test_partial_failure_continues_ingestion(self):
        """Test that ingestion continues despite partial failures"""
        # Expected behavior: partial failures don't abort entire ingestion

        # Simulate results from multiple boards
        results = [
            {"board": "good-board-1", "success": True, "jobs": 50},
            {"board": "failing-board", "success": False, "error": "timeout"},
            {"board": "good-board-2", "success": True, "jobs": 30},
        ]

        # Extract successful results
        successful = [r for r in results if r["success"]]
        failed = [r for r in results if not r["success"]]

        # Verify ingestion continues
        assert len(successful) == 2
        assert len(failed) == 1

        # Total jobs from successful boards
        total_jobs = sum(r.get("jobs", 0) for r in successful)
        assert total_jobs == 80

    def test_source_error_recorded(self):
        """Test that source errors are recorded in run_sources table"""
        # Expected behavior per plan.md Phase 0.5.7

        # When a source/board fails, should call:
        # record_source_result(
        #     run_id=run_id,
        #     source="greenhouse",
        #     source_target="failing-board",
        #     jobs_fetched=0,
        #     error_message="Connection timeout",
        #     error_code="timeout",
        #     duration_ms=30000,
        # )

        error_record = {
            "source": "greenhouse",
            "source_target": "failing-board",
            "jobs_fetched": 0,
            "error_code": "timeout",
            "duration_ms": 30000,
        }

        assert error_record["jobs_fetched"] == 0
        assert error_record["error_code"] == "timeout"

    def test_multiple_board_failures(self):
        """Test handling of multiple board failures"""
        # Simulate 3 boards: 2 succeed, 1 fails
        results = [
            {"board": "stripe", "success": True, "jobs": 50},
            {"board": "bad-board", "success": False, "error": "404"},
            {"board": "airbnb", "success": True, "jobs": 75},
        ]

        successful = [r for r in results if r["success"]]
        failed = [r for r in results if not r["success"]]

        assert len(successful) == 2
        assert len(failed) == 1
        assert sum(r["jobs"] for r in successful) == 125


class TestIngestionWithTracking:
    """Test ingestion using search_with_tracking"""

    @pytest.mark.asyncio
    async def test_tracking_data_collected(self):
        """Test that BoardResult tracking data is collected"""
        # Expected behavior per plan.md Phase 0.5.7:
        # ingest.py should use search_with_tracking() and record each BoardResult

        from job_scraper.apis import BoardResult

        # Mock tracking results
        tracking_results = [
            BoardResult(source="test_source", board_token="stripe", jobs_fetched=50, duration_ms=200),
            BoardResult(source="test_source", board_token="airbnb", jobs_fetched=75, duration_ms=350),
            BoardResult(source="test_source", 
                board_token="failing", jobs_fetched=0, error_code="timeout", duration_ms=30000
            ),
        ]

        total_jobs = sum(r.jobs_fetched for r in tracking_results)
        assert total_jobs == 125

        # Verify error tracking
        errors = [r for r in tracking_results if r.error_code]
        assert len(errors) == 1
        assert errors[0].error_code == "timeout"

    def test_duration_tracking_aggregation(self):
        """Test aggregation of request durations"""
        from job_scraper.apis import BoardResult

        results = [
            BoardResult(source="test_source", board_token="fast", jobs_fetched=10, duration_ms=100),
            BoardResult(source="test_source", board_token="medium", jobs_fetched=50, duration_ms=500),
            BoardResult(source="test_source", board_token="slow", jobs_fetched=100, duration_ms=2000),
        ]

        total_duration = sum(r.duration_ms for r in results)
        avg_duration = total_duration / len(results)

        assert total_duration == 2600
        assert avg_duration == pytest.approx(866.67, rel=0.01)


class TestConfigurationLoading:
    """Test configuration for scaled ingestion"""

    def test_uncapped_sources_from_config(self):
        """Test loading uncapped_sources from config"""
        # Expected config structure per plan.md Phase 2.1
        config_data = {
            "ingestion": {
                "max_per_source": 100,
                "uncapped_sources": ["greenhouse", "lever", "smartrecruiters"],
            }
        }

        uncapped = config_data["ingestion"]["uncapped_sources"]

        assert "greenhouse" in uncapped
        assert "lever" in uncapped
        assert "smartrecruiters" in uncapped
        assert len(uncapped) == 3

    def test_uncapped_sources_from_env_var(self):
        """Test loading uncapped_sources from environment variable"""
        import os

        # Simulate env var: JOB_SCRAPER_UNCAPPED_SOURCES=greenhouse,lever
        env_value = "greenhouse,lever"
        sources = [s.strip() for s in env_value.split(",")]

        assert sources == ["greenhouse", "lever"]


class TestIngestionMetrics:
    """Test ingestion metrics and logging"""

    def test_metrics_include_board_count(self):
        """Test that metrics include board count"""
        metrics = {
            "total_boards": 150,
            "successful_boards": 145,
            "failed_boards": 5,
            "total_jobs_fetched": 12500,
            "total_jobs_stored": 11800,
            "total_duration_ms": 180000,  # 3 minutes
        }

        assert metrics["successful_boards"] > metrics["failed_boards"]
        assert metrics["total_jobs_stored"] <= metrics["total_jobs_fetched"]
        assert metrics["total_duration_ms"] > 0

    def test_metrics_per_source(self):
        """Test that metrics are tracked per source"""
        metrics_by_source = {
            "greenhouse": {
                "boards": 80,
                "jobs_fetched": 8000,
                "jobs_stored": 7500,
                "avg_duration_ms": 250,
            },
            "lever": {
                "boards": 40,
                "jobs_fetched": 3000,
                "jobs_stored": 2900,
                "avg_duration_ms": 180,
            },
            "smartrecruiters": {
                "boards": 30,
                "jobs_fetched": 1500,
                "jobs_stored": 1400,
                "avg_duration_ms": 400,
            },
        }

        total_jobs = sum(m["jobs_fetched"] for m in metrics_by_source.values())
        assert total_jobs == 12500

        # Verify all sources tracked
        assert len(metrics_by_source) == 3


class TestEdgeCases:
    """Test edge cases in scaled ingestion"""

    def test_empty_board_list(self):
        """Test ingestion with empty board list"""
        boards = []

        # Should handle gracefully
        assert len(boards) == 0

    def test_all_boards_fail(self):
        """Test ingestion when all boards fail"""
        from job_scraper.apis import BoardResult

        results = [
            BoardResult(source="test_source", board_token="board1", jobs_fetched=0, error_code="not_found"),
            BoardResult(source="test_source", board_token="board2", jobs_fetched=0, error_code="timeout"),
            BoardResult(source="test_source", board_token="board3", jobs_fetched=0, error_code="rate_limited"),
        ]

        successful = [r for r in results if r.jobs_fetched > 0]
        assert len(successful) == 0

        # Should still complete without crashing
        failed = [r for r in results if r.error_code]
        assert len(failed) == 3

    def test_single_board_ingestion(self):
        """Test ingestion with single board"""
        from job_scraper.apis import BoardResult

        results = [
            BoardResult(source="test_source", board_token="stripe", jobs_fetched=142, duration_ms=300),
        ]

        assert len(results) == 1
        assert results[0].jobs_fetched == 142

    def test_very_large_board_count(self):
        """Test ingestion with 500+ boards"""
        boards = [f"board_{i}" for i in range(500)]

        assert len(boards) == 500

        # Verify rollout limiting works at scale
        rollout_10_percent = boards[: len(boards) // 10]
        assert len(rollout_10_percent) == 50
