"""
Integration tests for NetworkSpy → probe → scraper pipeline.

Verifies:
 - ProbeResult accepts fetch_mode='api_spy' + api_endpoint dict
 - ATSProbe._try_network_spy converts DiscoveredEndpoint to stored dict
 - _json_jobs_to_raw maps various JSON field naming conventions
 - _scrape_via_api_spy replays the stored endpoint via ReplayClient
 - scrape_site dispatches to the api_spy path end-to-end (mocked)
"""

import json
import uuid
from dataclasses import dataclass, field
from types import SimpleNamespace
from typing import Any, Optional
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from job_scraper.discovery.types import ATSType, ProbeResult
from job_scraper.scraping.scraper import (
    _extract_str,
    _json_jobs_to_raw,
    _scrape_via_api_spy,
    scrape_site,
)

# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------

SAMPLE_ENDPOINT_DICT = {
    "url": "https://example.com/api/jobs?page=1",
    "method": "GET",
    "replay_headers": {"Authorization": "Bearer tok"},
    "request_post_data": None,
    "confidence": 0.85,
    "job_count_estimate": 25,
    "pagination": {
        "style": "page",
        "param_name": "page",
        "current_value": 1,
        "in_body": False,
    },
}

SAMPLE_JOBS_JSON = [
    {"title": "Backend Engineer", "url": "https://co.com/apply/1", "location": "Remote"},
    {"title": "Frontend Dev", "url": "https://co.com/apply/2", "location": "NYC"},
    {"title": "Data Scientist", "url": "https://co.com/apply/3", "city": "Austin"},
]


def _make_site(fetch_mode="api_spy", api_endpoint=None, company_name="TestCo"):
    """Return a lightweight site-like object for scraper tests."""
    return SimpleNamespace(
        id=uuid.uuid4(),
        company_name=company_name,
        careers_url="https://example.com/careers",
        fetch_mode=fetch_mode,
        api_endpoint=api_endpoint or SAMPLE_ENDPOINT_DICT,
        selector_hints=None,
    )


def _make_replay_response(status=200, json_body=None, ok=True):
    """Return a mock ReplayResponse-like object."""
    resp = SimpleNamespace(
        url="https://example.com/api/jobs",
        method="GET",
        status=status,
        ok=ok,
        headers={"content-type": "application/json"},
        body=b"",
        text=json.dumps(json_body) if json_body is not None else "",
        json_body=json_body,
        error=None,
    )
    resp.extract_jobs = lambda: json_body if isinstance(json_body, list) else (
        json_body.get("jobs", []) if isinstance(json_body, dict) else []
    )
    return resp


# ===================================================================
# ProbeResult with api_spy
# ===================================================================


class TestProbeResultApiSpy:
    """ProbeResult should accept 'api_spy' fetch_mode and carry api_endpoint."""

    def test_api_spy_fetch_mode_accepted(self):
        result = ProbeResult(
            careers_url="https://co.com/careers",
            final_url="https://co.com/careers",
            detected_ats=ATSType.CUSTOM,
            confidence=1.0,
            fetch_mode="api_spy",
            robots_allowed=True,
            detection_method="network_spy",
            api_endpoint=SAMPLE_ENDPOINT_DICT,
        )
        assert result.fetch_mode == "api_spy"
        assert result.api_endpoint is not None
        assert result.api_endpoint["url"] == "https://example.com/api/jobs?page=1"

    def test_api_endpoint_is_none_by_default(self):
        result = ProbeResult(
            careers_url="https://co.com/careers",
            final_url="https://co.com/careers",
            detected_ats=ATSType.CUSTOM,
            confidence=1.0,
            fetch_mode="static",
            robots_allowed=True,
        )
        assert result.api_endpoint is None

    def test_invalid_fetch_mode_still_rejected(self):
        with pytest.raises(ValueError, match="api_spy"):
            ProbeResult(
                careers_url="https://co.com",
                final_url="https://co.com",
                detected_ats=ATSType.CUSTOM,
                confidence=1.0,
                fetch_mode="magic",
                robots_allowed=True,
            )


# ===================================================================
# _extract_str
# ===================================================================


class TestExtractStr:
    """_extract_str should handle strings, lists, and nested dicts."""

    def test_direct_string(self):
        assert _extract_str({"title": "Engineer"}, ("title",)) == "Engineer"

    def test_first_non_empty_key(self):
        obj = {"name": "", "role": "PM"}
        assert _extract_str(obj, ("name", "role")) == "PM"

    def test_list_of_strings(self):
        assert _extract_str({"location": ["Remote", "NYC"]}, ("location",)) == "Remote"

    def test_list_of_dicts(self):
        obj = {"locations": [{"name": "San Francisco"}]}
        assert _extract_str(obj, ("locations",)) == "San Francisco"

    def test_none_when_missing(self):
        assert _extract_str({"x": 1}, ("title", "name")) is None

    def test_whitespace_only_skipped(self):
        assert _extract_str({"title": "   ", "name": "Valid"}, ("title", "name")) == "Valid"


# ===================================================================
# _json_jobs_to_raw
# ===================================================================


class TestJsonJobsToRaw:
    """_json_jobs_to_raw should convert heterogeneous JSON dicts to RawScrapedJob."""

    def test_standard_fields(self):
        items = [{"title": "Eng", "url": "https://x.com/1", "location": "Remote"}]
        raw = _json_jobs_to_raw(items, "ACME")
        assert len(raw) == 1
        assert raw[0].title == "Eng"
        assert raw[0].url == "https://x.com/1"
        assert raw[0].location == "Remote"
        assert raw[0].company == "ACME"

    def test_alternative_field_names(self):
        items = [{"jobTitle": "PM", "applyUrl": "https://x.com/apply"}]
        raw = _json_jobs_to_raw(items, "ACME")
        assert len(raw) == 1
        assert raw[0].title == "PM"
        assert raw[0].url == "https://x.com/apply"

    def test_skips_items_without_title(self):
        items = [{"url": "https://x.com/1"}]  # no title
        raw = _json_jobs_to_raw(items, "ACME")
        assert raw == []

    def test_skips_items_without_url(self):
        items = [{"title": "Eng"}]  # no url
        raw = _json_jobs_to_raw(items, "ACME")
        assert raw == []

    def test_skips_non_dict_items(self):
        items = [42, "string", None, {"title": "Valid", "url": "https://x.com/1"}]
        raw = _json_jobs_to_raw(items, "ACME")
        assert len(raw) == 1

    def test_empty_list(self):
        assert _json_jobs_to_raw([], "ACME") == []

    def test_location_from_city_key(self):
        items = [{"title": "Eng", "url": "https://x.com/1", "city": "Austin"}]
        raw = _json_jobs_to_raw(items, "ACME")
        assert raw[0].location == "Austin"

    def test_field_map_overrides_title_key(self):
        items = [{"listing_name": "PM", "apply_link": "https://x.com/1"}]
        raw = _json_jobs_to_raw(items, "ACME", field_map={"title": "listing_name", "url": "apply_link"})
        assert len(raw) == 1
        assert raw[0].title == "PM"
        assert raw[0].url == "https://x.com/1"

    def test_field_map_falls_back_when_mapped_key_missing(self):
        """If the mapped key isn't in the item, fall back to the ranked defaults."""
        items = [{"title": "Engineer", "url": "https://x.com/1"}]
        raw = _json_jobs_to_raw(items, "ACME", field_map={"title": "nonexistent_key"})
        assert len(raw) == 1
        assert raw[0].title == "Engineer"  # fell through to default "title" key

    def test_field_map_partial_override(self):
        """Mapping only some fields still applies defaults for unmapped ones."""
        items = [{"listing_name": "Dev", "url": "https://x.com/1", "city": "LA"}]
        raw = _json_jobs_to_raw(
            items, "ACME",
            field_map={"title": "listing_name"},  # no url/location override
        )
        assert raw[0].title == "Dev"
        assert raw[0].url == "https://x.com/1"   # default key
        assert raw[0].location == "LA"            # default city key


# ===================================================================
# _scrape_via_api_spy
# ===================================================================


class TestScrapeViaApiSpy:
    """_scrape_via_api_spy should fetch + parse using ReplayClient."""

    @pytest.mark.asyncio
    async def test_missing_api_endpoint(self):
        site = _make_site(api_endpoint=None)
        site.api_endpoint = None
        raw, error = await _scrape_via_api_spy(site)
        assert raw is None
        assert "api_endpoint is not set" in error

    @pytest.mark.asyncio
    async def test_successful_single_page(self):
        site = _make_site(api_endpoint={
            "url": "https://co.com/api/jobs",
            "method": "GET",
            "replay_headers": {},
        })
        mock_resp = _make_replay_response(json_body=SAMPLE_JOBS_JSON)

        with patch("job_scraper.scraping.fetchers.replay.ReplayClient") as MockClient:
            instance = MockClient.return_value
            instance.fetch = AsyncMock(return_value=mock_resp)

            raw, error = await _scrape_via_api_spy(site)

        assert error is None
        assert len(raw) == 3
        assert raw[0].title == "Backend Engineer"
        assert raw[2].location == "Austin"

    @pytest.mark.asyncio
    async def test_pagination_calls_paginate(self):
        site = _make_site()  # uses SAMPLE_ENDPOINT_DICT which has pagination
        page1 = _make_replay_response(json_body=SAMPLE_JOBS_JSON[:2])
        page2 = _make_replay_response(json_body=SAMPLE_JOBS_JSON[2:])

        with patch("job_scraper.scraping.fetchers.replay.ReplayClient") as MockClient:
            instance = MockClient.return_value
            instance.paginate = AsyncMock(return_value=[page1, page2])

            raw, error = await _scrape_via_api_spy(site)

        assert error is None
        assert len(raw) == 3
        instance.paginate.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_stale_status_returns_endpoint_expired_error(self):
        """401/403/404/410 → endpoint_expired:N (signals reprobe needed)."""
        site = _make_site()
        for status in (401, 403, 404, 410):
            bad_resp = _make_replay_response(status=status, json_body=[], ok=False)

            with patch("job_scraper.scraping.fetchers.replay.ReplayClient") as MockClient:
                instance = MockClient.return_value
                instance.paginate = AsyncMock(return_value=[bad_resp])

                raw, error = await _scrape_via_api_spy(site)

            assert raw == [], f"status={status}"
            assert error == f"endpoint_expired:{status}", f"status={status}"

    @pytest.mark.asyncio
    async def test_transient_server_error_stops_silently(self):
        """5xx errors are transient — stop pagination but don't flag as stale."""
        site = _make_site()
        bad_resp = _make_replay_response(status=500, json_body=[], ok=False)

        with patch("job_scraper.scraping.fetchers.replay.ReplayClient") as MockClient:
            instance = MockClient.return_value
            instance.paginate = AsyncMock(return_value=[bad_resp])

            raw, error = await _scrape_via_api_spy(site)

        assert error is None
        assert raw == []

    @pytest.mark.asyncio
    async def test_replay_exception_returns_error(self):
        site = _make_site()

        with patch("job_scraper.scraping.fetchers.replay.ReplayClient") as MockClient:
            instance = MockClient.return_value
            instance.paginate = AsyncMock(side_effect=RuntimeError("connection reset"))

            raw, error = await _scrape_via_api_spy(site)

        assert raw is None
        assert "replay_error" in error

    @pytest.mark.asyncio
    async def test_field_map_applied_from_endpoint_config(self):
        """field_map stored in api_endpoint is passed through to _json_jobs_to_raw."""
        endpoint = {
            "url": "https://co.com/api/jobs",
            "method": "GET",
            "replay_headers": {},
            "field_map": {"title": "listing_name", "url": "apply_link"},
        }
        site = _make_site(api_endpoint=endpoint)
        unusual_jobs = [
            {"listing_name": "Backend Engineer", "apply_link": "https://co.com/apply/1"},
            {"listing_name": "PM", "apply_link": "https://co.com/apply/2"},
        ]
        mock_resp = _make_replay_response(json_body=unusual_jobs)

        with patch("job_scraper.scraping.fetchers.replay.ReplayClient") as MockClient:
            instance = MockClient.return_value
            instance.fetch = AsyncMock(return_value=mock_resp)

            raw, error = await _scrape_via_api_spy(site)

        assert error is None
        assert len(raw) == 2
        assert raw[0].title == "Backend Engineer"
        assert raw[1].title == "PM"


# ===================================================================
# scrape_site with api_spy
# ===================================================================


class TestScrapeSiteApiSpy:
    """End-to-end scrape_site should dispatch to api_spy path."""

    @pytest.mark.asyncio
    async def test_api_spy_returns_jobs(self):
        site = _make_site()
        mock_resp = _make_replay_response(json_body=SAMPLE_JOBS_JSON)

        with patch("job_scraper.scraping.fetchers.replay.ReplayClient") as MockClient:
            instance = MockClient.return_value
            instance.paginate = AsyncMock(return_value=[mock_resp])

            jobs, result = await scrape_site(site)

        assert result.success is True
        assert result.jobs_found == 3
        assert len(jobs) == 3
        assert jobs[0].title == "Backend Engineer"
        assert jobs[0].company == "TestCo"

    @pytest.mark.asyncio
    async def test_api_spy_failure_returns_error_result(self):
        site = _make_site(api_endpoint=None)
        site.api_endpoint = None

        jobs, result = await scrape_site(site)

        assert result.success is False
        assert "api_endpoint is not set" in result.error
        assert jobs == []
        assert result.needs_reprobe is False

    @pytest.mark.asyncio
    async def test_stale_endpoint_sets_needs_reprobe(self):
        """HTTP 401/403/404/410 from replay → needs_reprobe=True on SiteResult."""
        site = _make_site()
        stale_resp = _make_replay_response(status=401, json_body=[], ok=False)

        with patch("job_scraper.scraping.fetchers.replay.ReplayClient") as MockClient:
            instance = MockClient.return_value
            instance.paginate = AsyncMock(return_value=[stale_resp])

            jobs, result = await scrape_site(site)

        assert result.success is False
        assert result.needs_reprobe is True
        assert result.error == "endpoint_expired:401"
        assert jobs == []

    @pytest.mark.asyncio
    async def test_transient_failure_does_not_set_needs_reprobe(self):
        """5xx / replay exception → needs_reprobe stays False."""
        site = _make_site()
        server_err = _make_replay_response(status=503, json_body=[], ok=False)

        with patch("job_scraper.scraping.fetchers.replay.ReplayClient") as MockClient:
            instance = MockClient.return_value
            instance.paginate = AsyncMock(return_value=[server_err])

            jobs, result = await scrape_site(site)

        assert result.success is True  # returned 0 jobs cleanly, not an error
        assert result.needs_reprobe is False

    @pytest.mark.asyncio
    async def test_static_mode_unaffected(self):
        """Verify that fetch_mode='static' still follows the old path."""
        site = _make_site(fetch_mode="static")

        with patch("job_scraper.scraping.scraper.fetch_static", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = ("<html></html>", None)

            with patch("job_scraper.scraping.scraper._scrape_via_api_spy") as spy_fn:
                # CSS parser import will fail — that's fine, we just verify routing
                try:
                    await scrape_site(site)
                except Exception:
                    pass

                spy_fn.assert_not_called()
                mock_fetch.assert_awaited_once()


# ===================================================================
# ATSProbe._try_network_spy
# ===================================================================


class TestProbeNetworkSpy:
    """ATSProbe._try_network_spy should run NetworkSpy and return endpoint dict."""

    @pytest.mark.asyncio
    async def test_returns_endpoint_dict_on_success(self):
        from job_scraper.discovery.probe import ATSProbe

        mock_endpoint = SimpleNamespace(
            url="https://co.com/api/v1/jobs",
            method="GET",
            replay_headers={"X-Api": "key"},
            request_post_data=None,
            confidence=0.9,
            job_count_estimate=42,
            pagination=SimpleNamespace(
                style="page", param_name="page", current_value=1, in_body=False,
            ),
        )
        mock_spy_instance = MagicMock()
        mock_spy_instance.spy = AsyncMock(return_value=[mock_endpoint])

        with patch("job_scraper.scraping.fetchers.network_spy.NetworkSpy", return_value=mock_spy_instance) as MockSpy:
            probe = ATSProbe(try_api_spy=True)
            result = await probe._try_network_spy("https://co.com/careers")

        assert result is not None
        assert result["url"] == "https://co.com/api/v1/jobs"
        assert result["method"] == "GET"
        assert result["confidence"] == 0.9
        assert result["pagination"]["style"] == "page"

    @pytest.mark.asyncio
    async def test_returns_none_when_no_endpoints(self):
        from job_scraper.discovery.probe import ATSProbe

        mock_spy_instance = MagicMock()
        mock_spy_instance.spy = AsyncMock(return_value=[])

        with patch("job_scraper.scraping.fetchers.network_spy.NetworkSpy", return_value=mock_spy_instance):
            probe = ATSProbe(try_api_spy=True)
            result = await probe._try_network_spy("https://co.com/careers")

        assert result is None

    @pytest.mark.asyncio
    async def test_returns_none_on_exception(self):
        from job_scraper.discovery.probe import ATSProbe

        mock_spy_instance = MagicMock()
        mock_spy_instance.spy = AsyncMock(side_effect=RuntimeError("browser crashed"))

        with patch("job_scraper.scraping.fetchers.network_spy.NetworkSpy", return_value=mock_spy_instance):
            probe = ATSProbe(try_api_spy=True)
            result = await probe._try_network_spy("https://co.com/careers")

        assert result is None

    @pytest.mark.asyncio
    async def test_no_pagination_field_when_absent(self):
        from job_scraper.discovery.probe import ATSProbe

        mock_endpoint = SimpleNamespace(
            url="https://co.com/api/jobs",
            method="POST",
            replay_headers={},
            request_post_data='{"q": ""}',
            confidence=0.7,
            job_count_estimate=10,
            pagination=None,
        )
        mock_spy_instance = MagicMock()
        mock_spy_instance.spy = AsyncMock(return_value=[mock_endpoint])

        with patch("job_scraper.scraping.fetchers.network_spy.NetworkSpy", return_value=mock_spy_instance):
            probe = ATSProbe(try_api_spy=True)
            result = await probe._try_network_spy("https://co.com/careers")

        assert result is not None
        assert "pagination" not in result
        assert result["method"] == "POST"
        assert result["request_post_data"] == '{"q": ""}'
