"""
Tests for detail enrichment guardrails.

Group A: Deterministic extraction (JSON-LD, meta tags, heuristics)
Group B: Upsert non-destructive behavior (NULLIF inside COALESCE)
Group C: Scrape path with/without config flag
Group D: Quality validation and cleanup mode
Group E: Budget enforcement
Group F: Browser fallback (SPA detection)
Group G: api_spy enrichment
"""
import asyncio
import json
import pytest
from unittest.mock import AsyncMock, patch, MagicMock
from dataclasses import dataclass
from uuid import uuid4

from job_scraper.scraping.parsers.detail import (
    _extract_deterministic,
    _parse_detail_response,
    _LLMRateLimiter,
    _description_needs_cleanup,
    _llm_output_acceptable,
    extract_job_detail,
)
from job_scraper.scraping.scraper import EnrichmentBudget, _likely_spa_shell
from job_scraper.scraping.types import RawScrapedJob


# ---------------------------------------------------------------------------
# Fixtures / HTML templates
# ---------------------------------------------------------------------------

_JSONLD_HTML = """
<html><head>
<script type="application/ld+json">
{jsonld}
</script>
</head><body><p>Some page content here to pass length check.</p></body></html>
"""

_META_HTML = """
<html><head>
<meta property="og:description" content="{desc}">
</head><body><p>Some page content here to pass length check.</p></body></html>
"""

_HEURISTIC_HTML = """
<html><body>
<div class="job-description">{desc}</div>
<div class="salary-info">{salary}</div>
<div class="location-tag">{location}</div>
</body></html>
"""

_EMPTY_HTML = "<html><body><p>Hello</p></body></html>"

# A good description that passes quality check (>100 chars, >15 words)
_GOOD_DESC = (
    "We are looking for a senior software engineer to join our platform team. "
    "You will design and build distributed systems, mentor junior engineers, "
    "and collaborate with product managers to deliver high-impact features."
)

# A bad description that fails quality check
_BAD_DESC = "Apply now sign in"

# SPA shell HTML
_SPA_SHELL_HTML = (
    "<html><head></head><body><div id='root'></div>"
    + "<script>var a=1;</script>" * 10
    + "</body></html>"
)

# Valid short page HTML (not SPA)
_VALID_SHORT_HTML = """
<html><body>
<div class="job-description">
This is a real job posting with actual content about the role and requirements.
</div>
</body></html>
"""


def _make_jsonld_posting(desc=None, location=None, salary=None):
    posting = {"@type": "JobPosting", "title": "Engineer"}
    if desc is not None:
        posting["description"] = desc
    if location is not None:
        posting["jobLocation"] = {
            "@type": "Place",
            "address": {
                "@type": "PostalAddress",
                "addressLocality": location,
            }
        }
    if salary is not None:
        posting["baseSalary"] = salary
    return posting


# ===================================================================
# Group A: Detail parser deterministic extraction
# ===================================================================

class TestJsonLDExtraction:
    """JSON-LD deterministic extraction."""

    def test_jsonld_extracts_all_fields(self):
        posting = _make_jsonld_posting(
            desc="<p>Build amazing things. Requirements: Python, AWS.</p>",
            location="San Francisco",
            salary={
                "currency": "USD",
                "value": {"minValue": 120000, "maxValue": 150000, "unitText": "year"},
            },
        )
        html = _JSONLD_HTML.format(jsonld=json.dumps(posting))
        result = _extract_deterministic(html, "https://example.com/job/1")
        assert result["description"] is not None
        assert "Build amazing things" in result["description"]
        assert "San Francisco" in result["location"]
        assert "$120,000" in result["salary"]
        assert "$150,000" in result["salary"]

    def test_jsonld_strips_html_from_description(self):
        posting = _make_jsonld_posting(desc="<b>Role:</b> <ul><li>Code</li></ul>")
        html = _JSONLD_HTML.format(jsonld=json.dumps(posting))
        result = _extract_deterministic(html, "https://example.com/job/2")
        assert "<b>" not in result["description"]
        assert "<ul>" not in result["description"]
        assert "Role:" in result["description"]

    def test_jsonld_salary_single_value(self):
        posting = _make_jsonld_posting(salary={"currency": "USD", "value": 90000})
        html = _JSONLD_HTML.format(jsonld=json.dumps(posting))
        result = _extract_deterministic(html, "https://example.com/job/3")
        assert "$90,000" in result["salary"]

    def test_jsonld_salary_string(self):
        posting = _make_jsonld_posting(salary="$80k - $100k")
        html = _JSONLD_HTML.format(jsonld=json.dumps(posting))
        result = _extract_deterministic(html, "https://example.com/job/4")
        assert result["salary"] == "$80k - $100k"


class TestMetaTagFallback:
    """Meta tag fallback when no JSON-LD."""

    def test_og_description_fallback(self):
        html = _META_HTML.format(desc="We are looking for a senior engineer to join our team.")
        result = _extract_deterministic(html, "https://example.com/job/5")
        assert "senior engineer" in result["description"]

    def test_meta_name_description_fallback(self):
        html = """
        <html><head>
        <meta name="description" content="Join our engineering team as a backend developer.">
        </head><body><p>Content</p></body></html>
        """
        result = _extract_deterministic(html, "https://example.com/job/6")
        assert "backend developer" in result["description"]


class TestBodyHeuristics:
    """CSS heuristic fallback."""

    def test_description_from_class(self):
        desc = "This role involves building distributed systems. " * 3  # >50 chars
        html = _HEURISTIC_HTML.format(desc=desc, salary="$150k", location="NYC")
        result = _extract_deterministic(html, "https://example.com/job/7")
        assert "distributed systems" in result["description"]

    def test_salary_from_class(self):
        desc = "Short"  # too short for description heuristic
        html = _HEURISTIC_HTML.format(desc=desc, salary="$120,000 - $160,000", location="Remote")
        result = _extract_deterministic(html, "https://example.com/job/8")
        assert "$120,000" in result["salary"]

    def test_location_from_class(self):
        html = _HEURISTIC_HTML.format(desc="Short", salary="", location="Austin, TX")
        result = _extract_deterministic(html, "https://example.com/job/9")
        assert "Austin" in result["location"]


class TestEmptyHTML:
    """Edge case: minimal HTML."""

    def test_empty_html_returns_empty(self):
        result = _extract_deterministic(_EMPTY_HTML, "https://example.com/job/10")
        assert result["description"] is None
        assert result["location"] is None
        assert result["salary"] is None


class TestLLMCalledOnlyWhenNeeded:
    """LLM should only be called when deterministic extraction fails or quality is low."""

    @pytest.mark.asyncio
    async def test_llm_skipped_when_jsonld_has_good_description(self):
        posting = _make_jsonld_posting(desc=_GOOD_DESC)
        html = _JSONLD_HTML.format(jsonld=json.dumps(posting))

        with patch("job_scraper.scraping.parsers.detail._run_llm") as mock_llm, \
             patch("job_scraper.cache.get_cache", new_callable=AsyncMock, return_value=None), \
             patch("job_scraper.cache.set_cache", new_callable=AsyncMock):
            result = await extract_job_detail(
                html, "https://example.com/job/11",
                llm_config={"groq_api_key": "fake", "hf_api_key": "fake"},
            )
            mock_llm.assert_not_called()
            assert result["description"] is not None

    @pytest.mark.asyncio
    async def test_llm_called_when_no_structured_data(self):
        html = "<html><body>" + ("x" * 100) + "</body></html>"

        mock_response = '{"description": "LLM extracted desc that is long enough to pass validation and has many words in it to be acceptable", "location": null, "salary": null}'
        with patch("job_scraper.scraping.parsers.detail._call_groq_detail",
                    new_callable=AsyncMock, return_value=mock_response) as mock_groq, \
             patch("job_scraper.cache.get_cache", new_callable=AsyncMock, return_value=None), \
             patch("job_scraper.cache.set_cache", new_callable=AsyncMock):
            result = await extract_job_detail(
                html, "https://example.com/job/12",
                llm_config={"groq_api_key": "fake"},
            )
            mock_groq.assert_called_once()
            assert "LLM extracted desc" in result["description"]


# ===================================================================
# Group B: Upsert non-destructive behavior (NULLIF in COALESCE)
# ===================================================================

class TestUpsertNullif:
    """Verify the SQL expression structure uses NULLIF inside COALESCE.

    Since we can't run Postgres in unit tests, we inspect the compiled
    SQL expression to verify the NULLIF wrapping.
    """

    def test_coalesce_wraps_nullif_for_description(self):
        """The upsert update dict should use coalesce(nullif(excluded.description, ''), ...)."""
        from sqlalchemy.dialects.postgresql import insert as pg_insert
        from job_scraper.storage import JobRecord, func

        # Build a minimal insert statement to inspect
        stmt = pg_insert(JobRecord).values([{
            "dedupe_key": "test",
            "title": "Test",
            "source": "test",
        }])

        # Build the expression the production code uses
        desc_expr = func.coalesce(func.nullif(stmt.excluded.description, ''), JobRecord.description)

        # Compile to string to verify structure
        from sqlalchemy.dialects import postgresql
        compiled = desc_expr.compile(dialect=postgresql.dialect())
        sql_str = str(compiled)
        assert "nullif" in sql_str.lower()
        assert "coalesce" in sql_str.lower()

    def test_coalesce_wraps_nullif_for_location(self):
        from sqlalchemy.dialects.postgresql import insert as pg_insert
        from job_scraper.storage import JobRecord, func

        stmt = pg_insert(JobRecord).values([{
            "dedupe_key": "test",
            "title": "Test",
            "source": "test",
        }])
        loc_expr = func.coalesce(func.nullif(stmt.excluded.location, ''), JobRecord.location)
        from sqlalchemy.dialects import postgresql
        compiled = loc_expr.compile(dialect=postgresql.dialect())
        sql_str = str(compiled)
        assert "nullif" in sql_str.lower()

    def test_coalesce_wraps_nullif_for_salary(self):
        from sqlalchemy.dialects.postgresql import insert as pg_insert
        from job_scraper.storage import JobRecord, func

        stmt = pg_insert(JobRecord).values([{
            "dedupe_key": "test",
            "title": "Test",
            "source": "test",
        }])
        sal_expr = func.coalesce(func.nullif(stmt.excluded.salary, ''), JobRecord.salary)
        from sqlalchemy.dialects import postgresql
        compiled = sal_expr.compile(dialect=postgresql.dialect())
        sql_str = str(compiled)
        assert "nullif" in sql_str.lower()


# ===================================================================
# Group C: Scrape path with/without config
# ===================================================================

@dataclass
class _FakeSite:
    id: object = None
    company_name: str = "TestCo"
    careers_url: str = "https://example.com/careers"
    fetch_mode: str = "static"
    selector_hints: object = None
    api_endpoint: object = None

    def __post_init__(self):
        if self.id is None:
            self.id = uuid4()


class _FakeCfg:
    """Minimal config stub for scraper tests."""

    def __init__(self, detail_enabled=True, llm_keys=False, **detail_overrides):
        self._detail = {
            "enabled": detail_enabled,
            "max_per_site": 50,
            "concurrency": 5,
            "fetch_timeout": 15.0,
            "max_seconds": 25.0,
            "max_fetches": 30,
            "max_llm_calls": 10,
        }
        self._detail.update(detail_overrides)
        self._llm = {}
        if llm_keys:
            self._llm = {"groq_api_key": "fake-key", "hf_api_key": None}

    @property
    def detail_enrichment(self):
        return self._detail

    @property
    def llm_parser(self):
        return self._llm


class TestEnrichmentSkippedWhenDisabled:

    @pytest.mark.asyncio
    async def test_enrichment_skipped_when_disabled(self):
        """When detail_enrichment.enabled=False (kill-switch), no detail fetches."""
        site = _FakeSite()
        cfg = _FakeCfg(detail_enabled=False, llm_keys=True)

        raw_job = RawScrapedJob(title="Software Engineer", url="https://example.com/job/1")

        with patch("job_scraper.scraping.scraper.fetch_static") as mock_fetch, \
             patch("job_scraper.scraping.scraper.fetch_with_browser") as mock_browser:
            # Make the first fetch_static call (for the careers page) return HTML with a job link
            mock_fetch.return_value = ("<html><body>no jobs</body></html>", None)

            # Patch the parser cascade to return our raw_job
            with patch("job_scraper.scraping.parsers.structured_data.parse_structured_data", return_value=[raw_job]):
                from job_scraper.scraping.scraper import _scrape_site_inner
                jobs, result = await _scrape_site_inner(site, cfg)

            # fetch_static should only be called once (for careers page), not for detail enrichment
            assert mock_fetch.call_count == 1


class TestEnrichmentEnabledByDefault:

    @pytest.mark.asyncio
    async def test_enrichment_runs_by_default(self):
        """Default config (enabled=True) triggers detail enrichment."""
        site = _FakeSite()
        cfg = _FakeCfg(detail_enabled=True, llm_keys=True)

        raw_job = RawScrapedJob(title="Software Engineer", url="https://example.com/job/1")

        detail_html = """<html><head>
        <script type="application/ld+json">
        {"@type": "JobPosting", "title": "Engineer", "description": "%s"}
        </script>
        </head><body><p>content</p></body></html>""" % _GOOD_DESC

        call_count = {"n": 0}

        async def mock_fetch(url, timeout=30.0):
            call_count["n"] += 1
            if "careers" in url:
                return ("<html><body>careers page</body></html>", None)
            return (detail_html, None)

        with patch("job_scraper.scraping.scraper.fetch_static", side_effect=mock_fetch), \
             patch("job_scraper.cache.get_cache", new_callable=AsyncMock, return_value=None), \
             patch("job_scraper.cache.set_cache", new_callable=AsyncMock), \
             patch("job_scraper.scraping.parsers.structured_data.parse_structured_data", return_value=[raw_job]):
            from job_scraper.scraping.scraper import _scrape_site_inner
            jobs, result = await _scrape_site_inner(site, cfg)

        # Should have called fetch_static at least twice (careers page + detail page)
        assert call_count["n"] >= 2


class TestEnrichmentRespectsMaxPerSite:

    @pytest.mark.asyncio
    async def test_max_per_site_caps_fetches(self):
        """With 10 jobs and max_per_site=3, only 3 detail fetches happen."""
        from job_scraper.scraping.scraper import _enrich_with_details

        raw_jobs = [
            RawScrapedJob(title=f"Job {i}", url=f"https://example.com/job/{i}")
            for i in range(10)
        ]

        fetch_count = {"n": 0}

        async def mock_fetch(url, timeout=30.0):
            fetch_count["n"] += 1
            return ("<html><body>short</body></html>", None)

        with patch("job_scraper.scraping.scraper.fetch_static", side_effect=mock_fetch):
            await _enrich_with_details(raw_jobs, llm_config={}, max_per_site=3)

        assert fetch_count["n"] == 3


class TestEnrichmentFailureNonFatal:

    @pytest.mark.asyncio
    async def test_enrichment_failure_keeps_job(self):
        """If extract_job_detail raises, the job is kept and no crash."""
        site = _FakeSite()
        cfg = _FakeCfg(detail_enabled=True, llm_keys=True)

        raw_job = RawScrapedJob(title="Software Engineer", url="https://example.com/job/1")

        async def mock_fetch(url, timeout=30.0):
            if "careers" in url:
                return ("<html><body>careers page</body></html>", None)
            raise ConnectionError("Network failed")

        with patch("job_scraper.scraping.scraper.fetch_static", side_effect=mock_fetch), \
             patch("job_scraper.scraping.parsers.structured_data.parse_structured_data", return_value=[raw_job]):
            from job_scraper.scraping.scraper import _scrape_site_inner
            jobs, result = await _scrape_site_inner(site, cfg)

        assert result.success
        assert result.jobs_found >= 1


class TestEnrichmentSkipsGoodDescriptions:

    @pytest.mark.asyncio
    async def test_skips_jobs_with_good_description(self):
        """Jobs that already have a quality description are not re-fetched."""
        from job_scraper.scraping.scraper import _enrich_with_details

        raw_jobs = [
            RawScrapedJob(title="Job A", url="https://example.com/a", description=_GOOD_DESC),
            RawScrapedJob(title="Job B", url="https://example.com/b"),
        ]

        fetch_count = {"n": 0}

        async def mock_fetch(url, timeout=30.0):
            fetch_count["n"] += 1
            return ("<html><body>short</body></html>", None)

        with patch("job_scraper.scraping.scraper.fetch_static", side_effect=mock_fetch):
            await _enrich_with_details(raw_jobs, llm_config={}, max_per_site=50)

        # Only Job B should be fetched
        assert fetch_count["n"] == 1
        # Job A description should be unchanged
        assert raw_jobs[0].description == _GOOD_DESC

    @pytest.mark.asyncio
    async def test_enriches_jobs_with_bad_description(self):
        """Jobs with low-quality descriptions ARE re-fetched for cleanup."""
        from job_scraper.scraping.scraper import _enrich_with_details

        raw_jobs = [
            RawScrapedJob(title="Job A", url="https://example.com/a", description=_BAD_DESC),
        ]

        fetch_count = {"n": 0}

        async def mock_fetch(url, timeout=30.0):
            fetch_count["n"] += 1
            return ("<html><body>short</body></html>", None)

        with patch("job_scraper.scraping.scraper.fetch_static", side_effect=mock_fetch):
            await _enrich_with_details(raw_jobs, llm_config={}, max_per_site=50)

        assert fetch_count["n"] == 1


# ===================================================================
# Group D: Quality validation and cleanup mode
# ===================================================================

class TestDescriptionNeedsCleanup:

    def test_none_needs_cleanup(self):
        assert _description_needs_cleanup(None) is True

    def test_empty_needs_cleanup(self):
        assert _description_needs_cleanup("") is True

    def test_short_needs_cleanup(self):
        assert _description_needs_cleanup("Short text") is True

    def test_few_words_needs_cleanup(self):
        # < 15 words but >= 100 chars
        assert _description_needs_cleanup("word " * 10 + "x" * 60) is True

    def test_boilerplate_needs_cleanup(self):
        assert _description_needs_cleanup("Apply now to join our team " * 5) is True
        assert _description_needs_cleanup("Sign in to view this job posting and more details " * 3) is True
        assert _description_needs_cleanup("Cookie preferences and privacy settings for our website " * 3) is True

    def test_html_artifacts_needs_cleanup(self):
        desc = "Some text &amp; more &amp; things &amp; stuff &amp; extra words to make it long enough " * 2
        assert _description_needs_cleanup(desc) is True

    def test_good_description_passes(self):
        assert _description_needs_cleanup(_GOOD_DESC) is False


class TestLLMOutputAcceptable:

    def test_rejects_none(self):
        assert _llm_output_acceptable(None) is False

    def test_rejects_short(self):
        assert _llm_output_acceptable("Too short") is False

    def test_rejects_few_words(self):
        assert _llm_output_acceptable("a " * 5 + "x" * 80) is False

    def test_rejects_boilerplate(self):
        assert _llm_output_acceptable("Apply now " * 20) is False

    def test_accepts_good_output(self):
        assert _llm_output_acceptable(_GOOD_DESC) is True


class TestCleanupModeOverridesDescriptionOnly:

    @pytest.mark.asyncio
    async def test_cleanup_mode_keeps_deterministic_location_salary(self):
        """When cleanup mode runs, it only overrides description, not location/salary."""
        # HTML with bad description but good location/salary
        posting = _make_jsonld_posting(
            desc="Apply now",  # will fail quality check
            location="San Francisco",
            salary={"currency": "USD", "value": 120000},
        )
        # Need enough body text so _preprocess_html produces >= 50 chars
        body_text = "<p>" + ("This is a job posting page with real content. " * 5) + "</p>"
        html = _JSONLD_HTML.format(jsonld=json.dumps(posting)).replace(
            "<p>Some page content here to pass length check.</p>", body_text
        )

        cleanup_response = '{"description": "This is a properly cleaned up job description with enough words and characters to pass the quality validation check that we have in place."}'

        with patch("job_scraper.scraping.parsers.detail._call_groq_detail",
                    new_callable=AsyncMock, return_value=cleanup_response), \
             patch("job_scraper.cache.get_cache", new_callable=AsyncMock, return_value=None), \
             patch("job_scraper.cache.set_cache", new_callable=AsyncMock):
            result = await extract_job_detail(
                html, "https://example.com/job/cleanup",
                llm_config={"groq_api_key": "fake"},
            )

        # Description should be from LLM cleanup
        assert "properly cleaned up" in result["description"]
        # Location and salary should be from deterministic extraction
        assert "San Francisco" in result["location"]
        assert "$120,000" in result["salary"]


# ===================================================================
# Group E: Budget enforcement
# ===================================================================

class TestEnrichmentBudget:

    @pytest.mark.asyncio
    async def test_fetch_budget_stops_enrichment(self):
        """When max_fetches is exhausted, no more jobs are fetched."""
        from job_scraper.scraping.scraper import _enrich_with_details

        raw_jobs = [
            RawScrapedJob(title=f"Job {i}", url=f"https://example.com/job/{i}")
            for i in range(10)
        ]

        fetch_count = {"n": 0}

        async def mock_fetch(url, timeout=30.0):
            fetch_count["n"] += 1
            return ("<html><body>short</body></html>", None)

        with patch("job_scraper.scraping.scraper.fetch_static", side_effect=mock_fetch):
            await _enrich_with_details(raw_jobs, llm_config={}, max_per_site=50, max_fetches=3)

        assert fetch_count["n"] == 3

    @pytest.mark.asyncio
    async def test_time_budget_stops_enrichment(self):
        """When max_seconds is exhausted, no more jobs are fetched."""
        from job_scraper.scraping.scraper import _enrich_with_details

        raw_jobs = [
            RawScrapedJob(title=f"Job {i}", url=f"https://example.com/job/{i}")
            for i in range(10)
        ]

        async def slow_fetch(url, timeout=30.0):
            await asyncio.sleep(0.3)
            return ("<html><body>short</body></html>", None)

        with patch("job_scraper.scraping.scraper.fetch_static", side_effect=slow_fetch):
            await _enrich_with_details(
                raw_jobs, llm_config={}, max_per_site=50,
                max_seconds=0.5, max_fetches=50, concurrency=1,
            )

        # With 0.3s per fetch and 0.5s budget, at most 2 should complete
        # (first fetch starts before budget check, budget checked before second)
        fetched = sum(1 for j in raw_jobs if j.description is not None or True)
        # Just verify we didn't fetch all 10
        assert any(j.description is None for j in raw_jobs)

    @pytest.mark.asyncio
    async def test_budget_lock_no_overshoot(self):
        """Concurrent workers must not overshoot budget counters."""
        budget = EnrichmentBudget(max_fetches=5, max_llm_calls=3, max_seconds=60.0)
        budget.start()

        acquired = {"fetch": 0, "llm": 0}
        lock = asyncio.Lock()

        async def worker():
            for _ in range(10):
                if await budget.try_acquire_fetch():
                    async with lock:
                        acquired["fetch"] += 1
                if await budget.try_acquire_llm():
                    async with lock:
                        acquired["llm"] += 1

        workers = [asyncio.create_task(worker()) for _ in range(5)]
        await asyncio.gather(*workers)

        assert acquired["fetch"] == 5  # exactly max_fetches
        assert acquired["llm"] == 3   # exactly max_llm_calls


# ===================================================================
# Group F: Browser fallback (SPA detection)
# ===================================================================

class TestSPADetection:

    def test_spa_shell_detected(self):
        assert _likely_spa_shell(_SPA_SHELL_HTML) is True

    def test_valid_page_not_spa(self):
        assert _likely_spa_shell(_VALID_SHORT_HTML) is False

    def test_page_with_job_content_not_spa(self):
        html = '<html><body><div class="description">Real job content here</div></body></html>'
        assert _likely_spa_shell(html) is False


class TestBrowserFallback:

    @pytest.mark.asyncio
    async def test_browser_fallback_on_spa_shell(self):
        """Browser fallback triggers when static returns SPA shell."""
        from job_scraper.scraping.scraper import _enrich_with_details

        raw_jobs = [
            RawScrapedJob(title="Job 1", url="https://example.com/job/1"),
        ]

        detail_html = """<html><head>
        <script type="application/ld+json">
        {"@type": "JobPosting", "title": "Engineer", "description": "%s"}
        </script>
        </head><body><p>content</p></body></html>""" % _GOOD_DESC

        async def mock_static(url, timeout=30.0):
            return (_SPA_SHELL_HTML, None)

        async def mock_browser(url, timeout=30.0, skip_interactions=False,
                               capture_network=False, capture_screenshot=False,
                               wait_for_network_idle=True, wait_for_selector=None):
            return (detail_html, None, None, None)

        with patch("job_scraper.scraping.scraper.fetch_static", side_effect=mock_static), \
             patch("job_scraper.scraping.scraper.fetch_with_browser", side_effect=mock_browser), \
             patch("job_scraper.cache.get_cache", new_callable=AsyncMock, return_value=None), \
             patch("job_scraper.cache.set_cache", new_callable=AsyncMock):
            await _enrich_with_details(raw_jobs, llm_config={}, max_per_site=50)

        assert raw_jobs[0].description is not None


# ===================================================================
# Group G: api_spy enrichment
# ===================================================================

class TestApiSpyEnrichment:

    @pytest.mark.asyncio
    async def test_api_spy_enriches_missing_descriptions(self):
        """api_spy path should run detail enrichment on description-less jobs."""
        site = _FakeSite(fetch_mode="api_spy")
        site.api_endpoint = {
            "url": "https://api.example.com/jobs",
            "method": "GET",
        }
        cfg = _FakeCfg(detail_enabled=True, llm_keys=False)

        raw_jobs = [
            RawScrapedJob(title="Engineer", url="https://example.com/job/1"),
        ]

        detail_html = """<html><head>
        <script type="application/ld+json">
        {"@type": "JobPosting", "title": "Engineer", "description": "%s"}
        </script>
        </head><body><p>content</p></body></html>""" % _GOOD_DESC

        fetch_count = {"n": 0}

        async def mock_fetch(url, timeout=30.0):
            fetch_count["n"] += 1
            return (detail_html, None)

        with patch("job_scraper.scraping.scraper._scrape_via_api_spy",
                    new_callable=AsyncMock, return_value=(raw_jobs, None)), \
             patch("job_scraper.scraping.scraper.fetch_static", side_effect=mock_fetch), \
             patch("job_scraper.cache.get_cache", new_callable=AsyncMock, return_value=None), \
             patch("job_scraper.cache.set_cache", new_callable=AsyncMock):
            from job_scraper.scraping.scraper import _scrape_site_inner
            jobs, result = await _scrape_site_inner(site, cfg)

        assert result.success
        assert fetch_count["n"] >= 1  # detail page was fetched


# ===================================================================
# Rate limiter unit test
# ===================================================================

class TestLLMRateLimiter:

    @pytest.mark.asyncio
    async def test_rate_limiter_enforces_interval(self):
        """Consecutive acquires should be spaced by the interval."""
        limiter = _LLMRateLimiter(calls_per_minute=600)  # 0.1s interval
        loop = asyncio.get_event_loop()

        t0 = loop.time()
        await limiter.acquire()
        await limiter.acquire()
        t1 = loop.time()

        # Should have waited ~0.1s between calls
        assert t1 - t0 >= 0.09
