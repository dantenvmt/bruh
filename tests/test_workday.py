"""Tests for Workday adapter, URL parser, config, and discovery integration."""
from __future__ import annotations

import asyncio
import json
import os
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from job_scraper.apis.workday import WorkdayAPI, WorkdaySite, parse_workday_url
from job_scraper.config import Config, _parse_workday_sites


# ---------------------------------------------------------------------------
# URL parsing
# ---------------------------------------------------------------------------

class TestParseWorkdayUrl:
    def test_parse_url_wd5(self):
        """nvidia.wd5.myworkdayjobs.com/en-US/Site → host includes wd5"""
        result = parse_workday_url("https://nvidia.wd5.myworkdayjobs.com/en-US/NVIDIAExternalCareerSite")
        assert result is not None
        assert result.host == "nvidia.wd5.myworkdayjobs.com"
        assert result.tenant == "nvidia"
        assert result.site == "NVIDIAExternalCareerSite"

    def test_parse_url_no_wd(self):
        """company.myworkdayjobs.com/Site → host without wd"""
        result = parse_workday_url("https://amazon.myworkdayjobs.com/AmazonJobs")
        assert result is not None
        assert result.host == "amazon.myworkdayjobs.com"
        assert result.tenant == "amazon"
        assert result.site == "AmazonJobs"

    def test_parse_url_locale_variants(self):
        """en-US, fr-FR, and no locale all parse correctly."""
        # en-US
        r1 = parse_workday_url("https://nvidia.wd5.myworkdayjobs.com/en-US/Site")
        assert r1 is not None and r1.site == "Site"

        # fr-FR
        r2 = parse_workday_url("https://nvidia.wd5.myworkdayjobs.com/fr-FR/Site")
        assert r2 is not None and r2.site == "Site"

        # No locale
        r3 = parse_workday_url("https://nvidia.wd5.myworkdayjobs.com/Site")
        assert r3 is not None and r3.site == "Site"

    def test_parse_url_invalid(self):
        """Non-workday URLs return None."""
        assert parse_workday_url("https://example.com/jobs") is None
        assert parse_workday_url("https://greenhouse.io/boards/foo") is None
        assert parse_workday_url("not-a-url") is None
        assert parse_workday_url("") is None

    def test_parse_url_filters_non_site(self):
        """Paths like /wday, /cxs, /api are filtered out."""
        assert parse_workday_url("https://nvidia.wd5.myworkdayjobs.com/wday") is None
        assert parse_workday_url("https://nvidia.wd5.myworkdayjobs.com/cxs") is None
        assert parse_workday_url("https://nvidia.wd5.myworkdayjobs.com/api") is None
        assert parse_workday_url("https://nvidia.wd5.myworkdayjobs.com/js") is None
        assert parse_workday_url("https://nvidia.wd5.myworkdayjobs.com/css") is None
        assert parse_workday_url("https://nvidia.wd5.myworkdayjobs.com/static") is None


class TestWorkdaySite:
    def test_api_base(self):
        site = WorkdaySite(host="nvidia.wd5.myworkdayjobs.com", tenant="nvidia", site="NVIDIAExternalCareerSite")
        assert site.api_base == "https://nvidia.wd5.myworkdayjobs.com/wday/cxs/nvidia/NVIDIAExternalCareerSite"

    def test_careers_url(self):
        site = WorkdaySite(host="nvidia.wd5.myworkdayjobs.com", tenant="nvidia", site="NVIDIAExternalCareerSite")
        assert site.careers_url == "https://nvidia.wd5.myworkdayjobs.com/NVIDIAExternalCareerSite"


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

class TestConfigWorkday:
    def test_config_json_env(self, monkeypatch):
        """WORKDAY_SITES JSON parsed correctly."""
        sites_json = json.dumps([
            {"host": "nvidia.wd5.myworkdayjobs.com", "tenant": "nvidia", "site": "NVIDIAExternalCareerSite"},
        ])
        monkeypatch.setenv("WORKDAY_SITES", sites_json)
        result = _parse_workday_sites(sites_json)
        assert len(result) == 1
        assert result[0]["host"] == "nvidia.wd5.myworkdayjobs.com"
        assert result[0]["tenant"] == "nvidia"
        assert result[0]["site"] == "NVIDIAExternalCareerSite"

    def test_config_yaml(self):
        """YAML list loaded correctly (passed as list of dicts)."""
        sites = [
            {"host": "nvidia.wd5.myworkdayjobs.com", "tenant": "nvidia", "site": "NVIDIAExternalCareerSite"},
            {"host": "amazon.myworkdayjobs.com", "tenant": "amazon", "site": "AmazonJobs"},
        ]
        result = _parse_workday_sites(sites)
        assert len(result) == 2
        assert result[0]["tenant"] == "nvidia"
        assert result[1]["tenant"] == "amazon"

    def test_config_host_validation(self):
        """Non-myworkdayjobs.com hosts rejected."""
        sites = [
            {"host": "evil.example.com", "tenant": "evil", "site": "EvilSite"},
            {"host": "nvidia.wd5.myworkdayjobs.com", "tenant": "nvidia", "site": "NVIDIAExternalCareerSite"},
        ]
        result = _parse_workday_sites(sites)
        assert len(result) == 1
        assert result[0]["tenant"] == "nvidia"

    def test_config_dedupes(self):
        """Duplicate entries are deduped."""
        sites = [
            {"host": "nvidia.wd5.myworkdayjobs.com", "tenant": "nvidia", "site": "NVIDIAExternalCareerSite"},
            {"host": "nvidia.wd5.myworkdayjobs.com", "tenant": "NVIDIA", "site": "NVIDIAExternalCareerSite"},
        ]
        result = _parse_workday_sites(sites)
        assert len(result) == 1

    def test_config_empty(self):
        assert _parse_workday_sites(None) == []
        assert _parse_workday_sites("") == []
        assert _parse_workday_sites("[]") == []

    def test_config_invalid_json(self):
        assert _parse_workday_sites("not-json{") == []

    def test_config_property(self, monkeypatch):
        """Config.workday property returns workday section."""
        monkeypatch.setenv("JOB_SCRAPER_ENV_ONLY", "true")
        cfg = Config()
        assert isinstance(cfg.workday, dict)
        assert "sites" in cfg.workday

    def test_empty_env_does_not_override_included_yaml(self, monkeypatch, tmp_path):
        """Empty WORKDAY_SITES should not wipe sites loaded from include YAML."""
        include_file = tmp_path / "workday_sites.yaml"
        include_file.write_text(
            "workday:\n"
            "  sites:\n"
            "    - host: nvidia.wd5.myworkdayjobs.com\n"
            "      tenant: nvidia\n"
            "      site: NVIDIAExternalCareerSite\n",
            encoding="utf-8",
        )

        monkeypatch.setenv("JOB_SCRAPER_CONFIG", str(tmp_path / "missing_config.yaml"))
        monkeypatch.setenv("JOB_SCRAPER_CONFIG_INCLUDES", str(include_file))
        monkeypatch.setenv("WORKDAY_SITES", "")

        cfg = Config()
        sites = cfg.workday.get("sites", [])
        assert len(sites) == 1
        assert sites[0]["host"] == "nvidia.wd5.myworkdayjobs.com"


# ---------------------------------------------------------------------------
# Aggregator
# ---------------------------------------------------------------------------

class TestAggregatorWorkday:
    def test_aggregator_has_workday(self, monkeypatch):
        """Workday is in ALL_SOURCES and _initialize_apis."""
        monkeypatch.setenv("JOB_SCRAPER_ENV_ONLY", "true")
        from job_scraper.aggregator import JobAggregator
        assert "workday" in JobAggregator.ALL_SOURCES

        agg = JobAggregator()
        assert "workday" in agg.apis
        assert isinstance(agg.apis["workday"], WorkdayAPI)


# ---------------------------------------------------------------------------
# CSRF behaviour
# ---------------------------------------------------------------------------

def _make_cookie(cookie_name: str, cookie_value: str):
    """Create a mock cookie object with the correct .name attribute."""
    cookie = MagicMock()
    cookie.name = cookie_name
    cookie.value = cookie_value
    return cookie


class TestCSRF:
    @pytest.fixture
    def api(self):
        return WorkdayAPI(
            sites=[{"host": "test.myworkdayjobs.com", "tenant": "test", "site": "TestSite"}],
        )

    def test_csrf_401_triggers_retry(self, api):
        """401 → acquires CSRF → retries."""
        mock_401 = MagicMock(status_code=401, content=b"")
        mock_200 = MagicMock(status_code=200, content=b'{"jobPostings":[],"total":0}')
        mock_200.json.return_value = {"jobPostings": [], "total": 0}

        # Mock CSRF GET response with cookie
        mock_csrf_resp = MagicMock()
        mock_csrf_resp.cookies.jar = [_make_cookie("CALYPSO_CSRF_TOKEN", "token123")]

        call_count = {"post": 0}

        async def mock_post(*args, **kwargs):
            call_count["post"] += 1
            if call_count["post"] == 1:
                return mock_401
            return mock_200

        async def mock_get(*args, **kwargs):
            return mock_csrf_resp

        client = AsyncMock()
        client.post = mock_post
        client.get = mock_get

        from job_scraper.utils import ExponentialBackoff
        backoff = ExponentialBackoff()

        result = asyncio.get_event_loop().run_until_complete(
            api._post_with_csrf(client, "https://test.myworkdayjobs.com/wday/cxs/test/TestSite/jobs", {}, "test.myworkdayjobs.com", backoff)
        )
        assert result.status_code == 200
        assert "test.myworkdayjobs.com" in api._csrf_cache

    def test_csrf_403_triggers_retry(self, api):
        """403 → acquires CSRF → retries."""
        mock_403 = MagicMock(status_code=403, content=b"")
        mock_200 = MagicMock(status_code=200, content=b'{"jobPostings":[],"total":0}')
        mock_200.json.return_value = {"jobPostings": [], "total": 0}

        mock_csrf_resp = MagicMock()
        mock_csrf_resp.cookies.jar = [_make_cookie("CALYPSO_CSRF_TOKEN", "tok")]

        call_count = {"post": 0}

        async def mock_post(*args, **kwargs):
            call_count["post"] += 1
            if call_count["post"] == 1:
                return mock_403
            return mock_200

        async def mock_get(*args, **kwargs):
            return mock_csrf_resp

        client = AsyncMock()
        client.post = mock_post
        client.get = mock_get

        from job_scraper.utils import ExponentialBackoff
        backoff = ExponentialBackoff()

        result = asyncio.get_event_loop().run_until_complete(
            api._post_with_csrf(client, "https://url", {}, "test.myworkdayjobs.com", backoff)
        )
        assert result.status_code == 200

    def test_csrf_422_no_retry(self, api):
        """422 → does NOT retry with CSRF."""
        mock_422 = MagicMock(status_code=422, content=b"")

        async def mock_post(*args, **kwargs):
            return mock_422

        client = AsyncMock()
        client.post = mock_post
        # get should NOT be called
        client.get = AsyncMock(side_effect=AssertionError("GET should not be called for 422"))

        from job_scraper.utils import ExponentialBackoff
        backoff = ExponentialBackoff()

        result = asyncio.get_event_loop().run_until_complete(
            api._post_with_csrf(client, "https://url", {}, "test.myworkdayjobs.com", backoff)
        )
        assert result.status_code == 422
        assert "test.myworkdayjobs.com" not in api._csrf_cache


# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------

class TestDiscovery:
    def test_discovery_dedupe(self):
        """Duplicate URLs → single canonical entry."""
        from job_scraper.ats_discovery import extract_ats_tokens

        html = """
        <a href="https://nvidia.wd5.myworkdayjobs.com/en-US/NVIDIAExternalCareerSite">Jobs</a>
        <a href="https://nvidia.wd5.myworkdayjobs.com/fr-FR/NVIDIAExternalCareerSite">Emplois</a>
        <a href="https://nvidia.wd5.myworkdayjobs.com/NVIDIAExternalCareerSite">Jobs</a>
        """
        result = extract_ats_tokens(html, {"workday"})
        assert "workday" in result
        # All three URLs should dedupe to one entry
        assert len(result["workday"]) == 1

    def test_discovery_export_format(self):
        """Output has {host, tenant, site} dicts."""
        from job_scraper.ats_discovery import extract_ats_tokens

        html = '<a href="https://nvidia.wd5.myworkdayjobs.com/NVIDIAExternalCareerSite">Jobs</a>'
        result = extract_ats_tokens(html, {"workday"})
        assert "workday" in result
        token_json = list(result["workday"])[0]
        site_dict = json.loads(token_json)
        assert "host" in site_dict
        assert "tenant" in site_dict
        assert "site" in site_dict
        assert site_dict["host"] == "nvidia.wd5.myworkdayjobs.com"


# ---------------------------------------------------------------------------
# Job parsing
# ---------------------------------------------------------------------------

class TestJobParsing:
    def test_locations_text_preferred(self):
        """locationsText is used over bulletFields for location."""
        api = WorkdayAPI(sites=[])
        posting = {
            "title": "Software Engineer",
            "locationsText": "San Francisco, CA",
            "bulletFields": ["Software Engineer", "Mountain View, CA"],
            "externalPath": "/job/123",
            "postedOn": "2026-01-01",
        }
        job = api._parse_job(posting, "test.myworkdayjobs.com", "test", "TestSite")
        assert job is not None
        assert job.location == "San Francisco, CA"

    def test_bullet_fields_fallback(self):
        """bulletFields[1] used when locationsText missing."""
        api = WorkdayAPI(sites=[])
        posting = {
            "title": "Data Scientist",
            "bulletFields": ["Data Scientist", "New York, NY"],
            "externalPath": "/job/456",
        }
        job = api._parse_job(posting, "test.myworkdayjobs.com", "test", "TestSite")
        assert job is not None
        assert job.location == "New York, NY"

    def test_url_uses_host(self):
        """Job URL uses the full host including wd5."""
        api = WorkdayAPI(sites=[])
        posting = {
            "title": "PM",
            "externalPath": "/job/789",
        }
        job = api._parse_job(posting, "nvidia.wd5.myworkdayjobs.com", "nvidia", "Site")
        assert job is not None
        assert job.url == "https://nvidia.wd5.myworkdayjobs.com/job/789"

    def test_remote_detection(self):
        """Remote flag set when location contains 'remote'."""
        api = WorkdayAPI(sites=[])
        posting = {
            "title": "Engineer",
            "locationsText": "Remote - US",
            "externalPath": "/job/001",
        }
        job = api._parse_job(posting, "test.myworkdayjobs.com", "test", "Site")
        assert job is not None
        assert job.remote is True
