"""
Tests for LLM-based parser.

Covers HTML preprocessing, JSON extraction, hallucination guard,
provider waterfall, error handling, and config resolution.
"""
import json
import pytest
from unittest.mock import AsyncMock, patch, MagicMock

from job_scraper.scraping.parsers.llm import (
    LLMParseError,
    _preprocess_html,
    _extract_json,
    _jobs_from_response,
    _resolve_config,
    parse_with_llm,
    _build_user_prompt,
    _DEFAULT_MAX_HTML_CHARS,
    _DEFAULT_GROQ_MODEL,
    _DEFAULT_HF_MODEL,
    _DEFAULT_TIMEOUT_SECONDS,
)
from job_scraper.scraping.types import RawScrapedJob


# ---------------------------------------------------------------------------
# _preprocess_html
# ---------------------------------------------------------------------------


class TestPreprocessHtml:
    """Tests for HTML preprocessing."""

    def test_strips_script_tags(self):
        html = '<html><body><script>var x=1;</script><p>hello</p></body></html>'
        result = _preprocess_html(html)
        assert "var x=1" not in result
        assert "hello" in result

    def test_strips_style_tags(self):
        html = '<html><body><style>.x{color:red}</style><p>content</p></body></html>'
        result = _preprocess_html(html)
        assert "color:red" not in result
        assert "content" in result

    def test_strips_noscript_svg_img(self):
        html = (
            '<html><body>'
            '<noscript>Enable JS</noscript>'
            '<svg><circle/></svg>'
            '<img src="logo.png">'
            '<p>visible</p>'
            '</body></html>'
        )
        result = _preprocess_html(html)
        assert "Enable JS" not in result
        assert "circle" not in result
        assert "logo.png" not in result
        assert "visible" in result

    def test_preserves_links_as_markdown(self):
        html = '<html><body><a href="/jobs/123">Software Engineer</a></body></html>'
        result = _preprocess_html(html)
        assert "[Software Engineer](/jobs/123)" in result

    def test_preserves_link_href_only_when_no_text(self):
        html = '<html><body><a href="/jobs/456"></a></body></html>'
        result = _preprocess_html(html)
        assert "(/jobs/456)" in result

    def test_link_without_href_preserves_text(self):
        html = '<html><body><a>Click here</a></body></html>'
        result = _preprocess_html(html)
        assert "Click here" in result

    def test_does_not_strip_nav_footer_header(self):
        """Nav/footer/header can contain job listings, so they should not be removed."""
        html = (
            '<html><body>'
            '<nav><a href="/jobs/1">Job in Nav</a></nav>'
            '<header><a href="/jobs/2">Job in Header</a></header>'
            '<footer><a href="/jobs/3">Job in Footer</a></footer>'
            '</body></html>'
        )
        result = _preprocess_html(html)
        assert "Job in Nav" in result
        assert "Job in Header" in result
        assert "Job in Footer" in result

    def test_truncates_to_max_chars(self):
        long_html = '<html><body>' + 'x' * 1000 + '</body></html>'
        result = _preprocess_html(long_html, max_chars=100)
        assert len(result) <= 100

    def test_collapses_excessive_newlines(self):
        html = '<html><body><p>A</p><p></p><p></p><p></p><p></p><p>B</p></body></html>'
        result = _preprocess_html(html)
        assert "\n\n\n" not in result

    def test_full_careers_page(self):
        """Integration test with realistic careers page HTML."""
        html = """
        <html>
        <head>
            <title>Careers</title>
            <script>analytics.track();</script>
            <style>.nav{display:flex}</style>
        </head>
        <body>
            <h1>Open Positions</h1>
            <div class="jobs">
                <div class="job">
                    <a href="/careers/senior-engineer">Senior Engineer</a>
                    <span>San Francisco, CA</span>
                </div>
                <div class="job">
                    <a href="/careers/product-manager">Product Manager</a>
                    <span>Remote</span>
                </div>
            </div>
        </body>
        </html>
        """
        result = _preprocess_html(html)
        assert "analytics.track" not in result
        assert ".nav{display" not in result
        assert "[Senior Engineer](/careers/senior-engineer)" in result
        assert "[Product Manager](/careers/product-manager)" in result
        assert "San Francisco, CA" in result


# ---------------------------------------------------------------------------
# _extract_json
# ---------------------------------------------------------------------------


class TestExtractJson:
    """Tests for JSON extraction from LLM responses."""

    def test_clean_json_array(self):
        raw = '[{"title": "Engineer", "url": "/jobs/1"}]'
        result = _extract_json(raw)
        assert len(result) == 1
        assert result[0]["title"] == "Engineer"

    def test_markdown_fenced_json(self):
        raw = '```json\n[{"title": "PM", "url": "/jobs/2"}]\n```'
        result = _extract_json(raw)
        assert len(result) == 1
        assert result[0]["title"] == "PM"

    def test_markdown_fenced_no_lang(self):
        raw = '```\n[{"title": "QA"}]\n```'
        result = _extract_json(raw)
        assert result[0]["title"] == "QA"

    def test_json_with_surrounding_text(self):
        raw = 'Here are the jobs:\n[{"title": "Dev", "url": "/dev"}]\nDone!'
        result = _extract_json(raw)
        assert result[0]["title"] == "Dev"

    def test_whitespace_padding(self):
        raw = '   \n[{"title": "A"}]\n   '
        result = _extract_json(raw)
        assert result[0]["title"] == "A"

    def test_invalid_json_raises(self):
        with pytest.raises((json.JSONDecodeError, ValueError)):
            _extract_json("not json at all")

    def test_empty_array(self):
        result = _extract_json("[]")
        assert result == []


# ---------------------------------------------------------------------------
# _jobs_from_response
# ---------------------------------------------------------------------------


class TestJobsFromResponse:
    """Tests for LLM response parsing with validation guards."""

    BASE_URL = "https://example.com/careers"
    SAMPLE_HTML = '<html><body><a href="/jobs/eng-1">Engineer</a><a href="/jobs/pm-2">PM</a></body></html>'

    def test_valid_response(self):
        response = json.dumps([
            {"title": "Engineer", "url": "https://example.com/jobs/eng-1", "location": "NYC"},
            {"title": "Product Manager", "url": "https://example.com/jobs/pm-2", "location": None},
        ])
        jobs = _jobs_from_response(response, self.BASE_URL, self.SAMPLE_HTML)
        assert len(jobs) == 2
        assert jobs[0].title == "Engineer"
        assert jobs[0].location == "NYC"
        assert jobs[1].title == "Product Manager"

    def test_resolves_relative_urls(self):
        response = json.dumps([
            {"title": "Engineer", "url": "/jobs/eng-1"},
        ])
        jobs = _jobs_from_response(response, self.BASE_URL, self.SAMPLE_HTML)
        assert len(jobs) == 1
        assert jobs[0].url == "https://example.com/jobs/eng-1"

    def test_drops_hallucinated_urls(self):
        """URLs whose paths don't appear in original HTML are dropped."""
        response = json.dumps([
            {"title": "Engineer", "url": "/jobs/eng-1"},
            {"title": "Fake Job", "url": "https://example.com/invented/path"},
        ])
        jobs = _jobs_from_response(response, self.BASE_URL, self.SAMPLE_HTML)
        assert len(jobs) == 1
        assert jobs[0].title == "Engineer"

    def test_allows_root_path(self):
        """URLs with root path '/' pass the hallucination guard (caught by _is_valid_job instead)."""
        response = json.dumps([
            {"title": "Engineer", "url": "https://example.com/jobs/eng-1"},
        ])
        # Root paths are not hallucination-checked (they'd be caught by _is_valid_job)
        jobs = _jobs_from_response(response, self.BASE_URL, self.SAMPLE_HTML)
        assert len(jobs) == 1

    def test_skips_missing_title(self):
        response = json.dumps([
            {"title": "", "url": "/jobs/eng-1"},
            {"title": "Valid", "url": "/jobs/eng-1"},
        ])
        jobs = _jobs_from_response(response, self.BASE_URL, self.SAMPLE_HTML)
        assert len(jobs) == 1
        assert jobs[0].title == "Valid"

    def test_skips_missing_url(self):
        response = json.dumps([
            {"title": "No URL", "url": ""},
            {"title": "Valid", "url": "/jobs/eng-1"},
        ])
        jobs = _jobs_from_response(response, self.BASE_URL, self.SAMPLE_HTML)
        assert len(jobs) == 1

    def test_skips_non_dict_items(self):
        response = json.dumps([
            "not a dict",
            42,
            {"title": "Valid", "url": "/jobs/eng-1"},
        ])
        jobs = _jobs_from_response(response, self.BASE_URL, self.SAMPLE_HTML)
        assert len(jobs) == 1

    def test_deduplicates_by_url(self):
        response = json.dumps([
            {"title": "Engineer", "url": "/jobs/eng-1"},
            {"title": "Engineer (Duplicate)", "url": "/jobs/eng-1"},
        ])
        jobs = _jobs_from_response(response, self.BASE_URL, self.SAMPLE_HTML)
        assert len(jobs) == 1

    def test_invalid_json_raises_llm_parse_error(self):
        with pytest.raises(LLMParseError, match="Failed to parse"):
            _jobs_from_response("not valid json", self.BASE_URL, self.SAMPLE_HTML)

    def test_non_list_json_raises_llm_parse_error(self):
        with pytest.raises(LLMParseError, match="non-list"):
            _jobs_from_response('{"title": "oops"}', self.BASE_URL, self.SAMPLE_HTML)

    def test_location_stripped_and_normalized(self):
        response = json.dumps([
            {"title": "Engineer", "url": "/jobs/eng-1", "location": "  NYC  "},
        ])
        jobs = _jobs_from_response(response, self.BASE_URL, self.SAMPLE_HTML)
        assert jobs[0].location == "NYC"

    def test_empty_location_becomes_none(self):
        response = json.dumps([
            {"title": "Engineer", "url": "/jobs/eng-1", "location": "   "},
        ])
        jobs = _jobs_from_response(response, self.BASE_URL, self.SAMPLE_HTML)
        assert jobs[0].location is None


# ---------------------------------------------------------------------------
# _resolve_config
# ---------------------------------------------------------------------------


class TestResolveConfig:
    """Tests for config resolution."""

    def test_defaults_when_no_config(self):
        rc = _resolve_config(None)
        assert rc["groq_api_key"] is None
        assert rc["hf_api_key"] is None
        assert rc["groq_model"] == _DEFAULT_GROQ_MODEL
        assert rc["hf_model"] == _DEFAULT_HF_MODEL
        assert rc["timeout"] == _DEFAULT_TIMEOUT_SECONDS
        assert rc["max_html_chars"] == _DEFAULT_MAX_HTML_CHARS

    def test_defaults_when_empty_config(self):
        rc = _resolve_config({})
        assert rc["groq_api_key"] is None
        assert rc["groq_model"] == _DEFAULT_GROQ_MODEL

    def test_uses_provided_keys(self):
        rc = _resolve_config({
            "groq_api_key": "gsk_test123",
            "hf_api_key": "hf_test456",
            "groq_model": "custom-model",
            "hf_model": "custom-hf",
            "timeout": 60,
            "max_html_chars": 100_000,
        })
        assert rc["groq_api_key"] == "gsk_test123"
        assert rc["hf_api_key"] == "hf_test456"
        assert rc["groq_model"] == "custom-model"
        assert rc["hf_model"] == "custom-hf"
        assert rc["timeout"] == 60.0
        assert rc["max_html_chars"] == 100_000

    def test_empty_string_key_treated_as_none(self):
        rc = _resolve_config({"groq_api_key": "", "hf_api_key": ""})
        assert rc["groq_api_key"] is None
        assert rc["hf_api_key"] is None


# ---------------------------------------------------------------------------
# _build_user_prompt
# ---------------------------------------------------------------------------


class TestBuildUserPrompt:

    def test_includes_base_url(self):
        prompt = _build_user_prompt("content", "https://example.com")
        assert "https://example.com" in prompt

    def test_includes_content(self):
        prompt = _build_user_prompt("some page text", "https://example.com")
        assert "some page text" in prompt


# ---------------------------------------------------------------------------
# parse_with_llm (integration, mocked providers)
# ---------------------------------------------------------------------------


class TestParseWithLlm:
    """Integration tests for the full parse_with_llm function with mocked LLM calls."""

    SAMPLE_HTML = '<html><body><a href="/jobs/eng-1">Engineer</a></body></html>'
    BASE_URL = "https://example.com/careers"
    GOOD_RESPONSE = json.dumps([
        {"title": "Engineer", "url": "/jobs/eng-1", "location": "NYC"},
    ])

    @pytest.mark.asyncio
    async def test_raises_when_no_keys_configured(self):
        with pytest.raises(LLMParseError, match="No LLM provider configured"):
            await parse_with_llm(self.SAMPLE_HTML, self.BASE_URL, llm_config={})

    @pytest.mark.asyncio
    async def test_groq_success(self):
        with patch(
            "job_scraper.scraping.parsers.llm._call_groq",
            new_callable=AsyncMock,
            return_value=self.GOOD_RESPONSE,
        ):
            jobs = await parse_with_llm(
                self.SAMPLE_HTML, self.BASE_URL,
                llm_config={"groq_api_key": "gsk_test"},
            )
        assert len(jobs) == 1
        assert jobs[0].title == "Engineer"

    @pytest.mark.asyncio
    async def test_groq_failure_falls_through_to_hf(self):
        with patch(
            "job_scraper.scraping.parsers.llm._call_groq",
            new_callable=AsyncMock,
            side_effect=RuntimeError("Groq is down"),
        ), patch(
            "job_scraper.scraping.parsers.llm._call_hf",
            new_callable=AsyncMock,
            return_value=self.GOOD_RESPONSE,
        ):
            jobs = await parse_with_llm(
                self.SAMPLE_HTML, self.BASE_URL,
                llm_config={"groq_api_key": "gsk_test", "hf_api_key": "hf_test"},
            )
        assert len(jobs) == 1
        assert jobs[0].title == "Engineer"

    @pytest.mark.asyncio
    async def test_groq_zero_results_falls_through_to_hf(self):
        with patch(
            "job_scraper.scraping.parsers.llm._call_groq",
            new_callable=AsyncMock,
            return_value="[]",
        ), patch(
            "job_scraper.scraping.parsers.llm._call_hf",
            new_callable=AsyncMock,
            return_value=self.GOOD_RESPONSE,
        ):
            jobs = await parse_with_llm(
                self.SAMPLE_HTML, self.BASE_URL,
                llm_config={"groq_api_key": "gsk_test", "hf_api_key": "hf_test"},
            )
        assert len(jobs) == 1

    @pytest.mark.asyncio
    async def test_hf_only_success(self):
        with patch(
            "job_scraper.scraping.parsers.llm._call_hf",
            new_callable=AsyncMock,
            return_value=self.GOOD_RESPONSE,
        ):
            jobs = await parse_with_llm(
                self.SAMPLE_HTML, self.BASE_URL,
                llm_config={"hf_api_key": "hf_test"},
            )
        assert len(jobs) == 1

    @pytest.mark.asyncio
    async def test_all_providers_fail_raises(self):
        with patch(
            "job_scraper.scraping.parsers.llm._call_groq",
            new_callable=AsyncMock,
            side_effect=RuntimeError("Groq down"),
        ), patch(
            "job_scraper.scraping.parsers.llm._call_hf",
            new_callable=AsyncMock,
            side_effect=RuntimeError("HF down"),
        ):
            with pytest.raises(LLMParseError, match="All LLM providers failed"):
                await parse_with_llm(
                    self.SAMPLE_HTML, self.BASE_URL,
                    llm_config={"groq_api_key": "gsk_test", "hf_api_key": "hf_test"},
                )

    @pytest.mark.asyncio
    async def test_all_providers_zero_results_raises(self):
        with patch(
            "job_scraper.scraping.parsers.llm._call_groq",
            new_callable=AsyncMock,
            return_value="[]",
        ), patch(
            "job_scraper.scraping.parsers.llm._call_hf",
            new_callable=AsyncMock,
            return_value="[]",
        ):
            with pytest.raises(LLMParseError, match="0 valid jobs"):
                await parse_with_llm(
                    self.SAMPLE_HTML, self.BASE_URL,
                    llm_config={"groq_api_key": "gsk_test", "hf_api_key": "hf_test"},
                )

    @pytest.mark.asyncio
    async def test_llm_parse_error_from_groq_propagates(self):
        """LLMParseError from response parsing is re-raised, not swallowed."""
        with patch(
            "job_scraper.scraping.parsers.llm._call_groq",
            new_callable=AsyncMock,
            return_value="this is not json",
        ):
            with pytest.raises(LLMParseError, match="Failed to parse"):
                await parse_with_llm(
                    self.SAMPLE_HTML, self.BASE_URL,
                    llm_config={"groq_api_key": "gsk_test"},
                )

    @pytest.mark.asyncio
    async def test_llm_parse_error_from_hf_propagates(self):
        """LLMParseError from HF response parsing is re-raised, not swallowed."""
        with patch(
            "job_scraper.scraping.parsers.llm._call_hf",
            new_callable=AsyncMock,
            return_value="this is not json",
        ):
            with pytest.raises(LLMParseError, match="Failed to parse"):
                await parse_with_llm(
                    self.SAMPLE_HTML, self.BASE_URL,
                    llm_config={"hf_api_key": "hf_test"},
                )

    @pytest.mark.asyncio
    async def test_passes_config_to_groq(self):
        """Verify config values are forwarded to the provider call."""
        mock_groq = AsyncMock(return_value=self.GOOD_RESPONSE)
        with patch("job_scraper.scraping.parsers.llm._call_groq", mock_groq):
            await parse_with_llm(
                self.SAMPLE_HTML, self.BASE_URL,
                llm_config={
                    "groq_api_key": "gsk_custom",
                    "groq_model": "custom-model",
                    "timeout": 45,
                },
            )
        call_kwargs = mock_groq.call_args
        assert call_kwargs.kwargs["api_key"] == "gsk_custom"
        assert call_kwargs.kwargs["model"] == "custom-model"
        assert call_kwargs.kwargs["timeout"] == 45.0
