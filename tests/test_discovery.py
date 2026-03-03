"""
Tests for the discovery module.

Tests deduplication, compliance, sources, ATS detection, and selector hints.
"""
import pytest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

from job_scraper.discovery.types import (
    ATSType,
    DiscoveredCompany,
    DiscoverySource,
    ProbeResult,
    SelectorHint,
)
from job_scraper.discovery.dedup import (
    canonicalize_domain,
    canonicalize_company_name,
    DeduplicationChecker,
)
from job_scraper.discovery.probe import (
    detect_ats_from_url,
    detect_ats_from_html,
    extract_ats_token,
    detect_requires_js,
)
from job_scraper.discovery.selectors import (
    assess_selector_hint,
    selector_hints_ready_for_scrape,
    generate_selector_hints,
    validate_selector_hints,
)
from job_scraper.discovery.sources import (
    load_seed_csv,
    load_hardcoded_yaml,
)


class TestCanonicalizeUrl:
    """Tests for URL canonicalization."""

    def test_basic_domain(self):
        assert canonicalize_domain("https://stripe.com/jobs") == "stripe.com"

    def test_www_prefix_removed(self):
        assert canonicalize_domain("https://www.google.com/careers") == "google.com"

    def test_uppercase_normalized(self):
        assert canonicalize_domain("https://WWW.Stripe.COM/jobs/") == "stripe.com"

    def test_no_scheme(self):
        assert canonicalize_domain("stripe.com/jobs") == "stripe.com"

    def test_subdomain_preserved(self):
        assert canonicalize_domain("https://careers.google.com") == "careers.google.com"

    def test_greenhouse_domain(self):
        assert canonicalize_domain("https://boards.greenhouse.io/airbnb") == "boards.greenhouse.io"

    def test_port_removed(self):
        assert canonicalize_domain("https://localhost:8080/jobs") == "localhost"


class TestCanonicalizeCompanyName:
    """Tests for company name canonicalization."""

    def test_basic_name(self):
        assert canonicalize_company_name("Google") == "google"

    def test_inc_suffix_removed(self):
        assert canonicalize_company_name("Google, Inc.") == "google"

    def test_llc_suffix_removed(self):
        assert canonicalize_company_name("Stripe LLC") == "stripe"

    def test_whitespace_normalized(self):
        assert canonicalize_company_name("  JPMorgan  Chase  ") == "jpmorgan chase"

    def test_and_co_removed(self):
        assert canonicalize_company_name("JPMorgan Chase & Co.") == "jpmorgan chase"


class TestDeduplicationChecker:
    """Tests for deduplication checker."""

    def test_add_and_check_domain(self):
        checker = DeduplicationChecker()
        checker.add_domain("https://stripe.com/jobs")

        assert checker.is_duplicate_domain("https://stripe.com/careers")
        assert not checker.is_duplicate_domain("https://plaid.com/jobs")

    def test_add_and_check_company(self):
        checker = DeduplicationChecker()
        checker.add_company("Google, Inc.")

        assert checker.is_duplicate_company("google")
        assert not checker.is_duplicate_company("Microsoft")

    def test_check_duplicate_combined(self):
        checker = DeduplicationChecker()
        checker.add_company("Google")
        checker.add_domain("https://stripe.com")
        checker.add_ats_token("greenhouse", "airbnb")

        is_dup, reason = checker.check_duplicate(company_name="Google")
        assert is_dup
        assert "google" in reason.lower()

        is_dup, reason = checker.check_duplicate(careers_url="https://stripe.com/jobs")
        assert is_dup

        is_dup, reason = checker.check_duplicate(ats="greenhouse", ats_token="airbnb")
        assert is_dup

        is_dup, reason = checker.check_duplicate(company_name="NewCompany")
        assert not is_dup


class TestATSDetectionFromUrl:
    """Tests for ATS detection from URLs."""

    def test_greenhouse_detection(self):
        result = detect_ats_from_url("https://boards.greenhouse.io/airbnb")
        assert result is not None
        ats, confidence, method = result
        assert ats == ATSType.GREENHOUSE
        assert confidence == 1.0
        assert method == "url"

    def test_lever_detection(self):
        result = detect_ats_from_url("https://jobs.lever.co/netflix")
        assert result is not None
        ats, confidence, method = result
        assert ats == ATSType.LEVER
        assert confidence == 1.0

    def test_ashby_detection(self):
        result = detect_ats_from_url("https://jobs.ashbyhq.com/notion")
        assert result is not None
        ats, confidence, method = result
        assert ats == ATSType.ASHBY

    def test_smartrecruiters_detection(self):
        result = detect_ats_from_url("https://careers.smartrecruiters.com/Spotify")
        assert result is not None
        ats, confidence, method = result
        assert ats == ATSType.SMARTRECRUITERS

    def test_workday_detection(self):
        result = detect_ats_from_url("https://walmart.wd5.myworkdayjobs.com/careers")
        assert result is not None
        ats, confidence, method = result
        assert ats == ATSType.WORKDAY

    def test_custom_no_detection(self):
        result = detect_ats_from_url("https://stripe.com/jobs")
        assert result is None


class TestATSDetectionFromHtml:
    """Tests for ATS detection from HTML content."""

    def test_lever_class_detection(self):
        html = '<div class="lever-jobs-container">jobs</div>'
        result = detect_ats_from_html(html)
        assert result is not None
        ats, confidence, method = result
        assert ats == ATSType.LEVER
        assert confidence == 0.7

    def test_greenhouse_embed_detection(self):
        html = '<iframe src="https://greenhouse.io/embed/job_board"></iframe>'
        result = detect_ats_from_html(html)
        assert result is not None
        ats, confidence, method = result
        assert ats == ATSType.GREENHOUSE

    def test_no_ats_detected(self):
        html = '<div class="jobs-list"><a href="/job/1">Engineer</a></div>'
        result = detect_ats_from_html(html)
        assert result is None


class TestATSTokenExtraction:
    """Tests for extracting ATS tokens from URLs."""

    def test_greenhouse_token(self):
        token = extract_ats_token(
            "https://boards.greenhouse.io/airbnb/jobs/123",
            ATSType.GREENHOUSE,
        )
        assert token == "airbnb"

    def test_lever_token(self):
        token = extract_ats_token(
            "https://jobs.lever.co/netflix/job/456",
            ATSType.LEVER,
        )
        assert token == "netflix"

    def test_ashby_token(self):
        token = extract_ats_token(
            "https://jobs.ashbyhq.com/notion",
            ATSType.ASHBY,
        )
        assert token == "notion"

    def test_smartrecruiters_token(self):
        token = extract_ats_token(
            "https://careers.smartrecruiters.com/Spotify/jobs",
            ATSType.SMARTRECRUITERS,
        )
        assert token == "Spotify"


class TestJsDetection:
    """Tests for JavaScript requirement detection."""

    def test_minimal_content_requires_js(self):
        html = "<html><body><div id='root'></div></body></html>"
        assert detect_requires_js(html) is True

    def test_rich_content_no_js(self):
        html = """
        <html><body>
        <h1>Careers at Company</h1>
        <p>Join our team of talented individuals working on exciting projects.
        We are looking for passionate people to help us build the future of technology.
        Our team is growing rapidly and we need great engineers, designers, and product managers.</p>
        <ul>
            <li><a href="/job/1">Software Engineer - Full Stack</a> - San Francisco, CA</li>
            <li><a href="/job/2">Product Manager - Growth</a> - New York, NY</li>
            <li><a href="/job/3">Senior Designer - UX</a> - Remote</li>
            <li><a href="/job/4">Data Scientist</a> - Austin, TX</li>
            <li><a href="/job/5">DevOps Engineer</a> - Seattle, WA</li>
        </ul>
        </body></html>
        """
        assert detect_requires_js(html) is False

    def test_react_root_requires_js(self):
        html = '<html><body><div id="root" data-reactroot></div></body></html>'
        assert detect_requires_js(html) is True


class TestSelectorHints:
    """Tests for selector hint generation."""

    def test_basic_job_list_detection(self):
        html = """
        <html><body>
        <div class="jobs-container">
            <div class="job-card">
                <a href="/job/1"><h3>Software Engineer</h3></a>
                <span class="location">San Francisco, CA</span>
            </div>
            <div class="job-card">
                <a href="/job/2"><h3>Product Manager</h3></a>
                <span class="location">New York, NY</span>
            </div>
            <div class="job-card">
                <a href="/job/3"><h3>Senior Developer</h3></a>
                <span class="location">Remote</span>
            </div>
            <div class="job-card">
                <a href="/job/4"><h3>Data Analyst</h3></a>
                <span class="location">Chicago, IL</span>
            </div>
        </div>
        </body></html>
        """
        hint = generate_selector_hints(html, "https://company.com/careers")
        assert hint is not None
        assert hint.job_container is not None
        assert hint.link is not None
        assert hint.sample_count >= 3

    def test_no_jobs_returns_none(self):
        html = "<html><body><h1>About Us</h1><p>Company info</p></body></html>"
        hint = generate_selector_hints(html, "https://company.com/about")
        assert hint is None

    def test_assess_selector_hint_rejects_generic_container(self):
        hint = SelectorHint(
            job_container="div",
            title="h3",
            link="a",
            confidence=0.9,
            sample_count=5,
        )
        ok, reason = assess_selector_hint(hint, min_confidence=0.6)
        assert ok is False
        assert "generic" in reason.lower()

    def test_validate_selector_hints_executes_parse_check(self):
        html = """
        <html><body>
        <ul class="jobs-list">
            <li class="job-row">
                <a class="job-link" href="/jobs/1"><h3>Senior Software Engineer</h3></a>
                <span class="location">San Francisco, CA</span>
            </li>
            <li class="job-row">
                <a class="job-link" href="/jobs/2"><h3>Product Manager</h3></a>
                <span class="location">New York, NY</span>
            </li>
            <li class="job-row">
                <a class="job-link" href="/jobs/3"><h3>Data Analyst</h3></a>
                <span class="location">Remote</span>
            </li>
            <li class="job-row">
                <a class="job-link" href="/jobs/4"><h3>UX Designer</h3></a>
                <span class="location">Austin, TX</span>
            </li>
        </ul>
        </body></html>
        """
        hint = generate_selector_hints(
            html,
            "https://company.com/careers",
            min_confidence=0.6,
        )
        assert hint is not None
        passed, validation = validate_selector_hints(
            html,
            "https://company.com/careers",
            hint,
            min_confidence=0.6,
            min_jobs=3,
            extraction_mode="static",
        )
        assert passed is True
        assert validation["passed"] is True
        assert validation["jobs_found"] >= 3

    def test_selector_hints_ready_requires_approval_status(self):
        hints = {
            "job_container": "li.job-row",
            "title": "h3",
            "link": "a.job-link",
            "confidence": 0.9,
            "sample_count": 5,
            "review_status": "proposed",
            "validation": {
                "passed": True,
                "jobs_found": 5,
                "min_jobs": 3,
            },
        }
        ready, reason = selector_hints_ready_for_scrape(
            hints,
            selector_confidence=0.9,
            min_confidence=0.6,
            require_approved=True,
        )
        assert ready is False
        assert "review_status" in reason

        hints["review_status"] = "approved"
        ready, reason = selector_hints_ready_for_scrape(
            hints,
            selector_confidence=0.9,
            min_confidence=0.6,
            require_approved=True,
        )
        assert ready is True


class TestDiscoveredCompany:
    """Tests for DiscoveredCompany dataclass."""

    def test_valid_company(self):
        company = DiscoveredCompany(
            name="Stripe",
            source=DiscoverySource.SEED_CSV,
            priority=1,
        )
        assert company.name == "Stripe"
        assert company.source == DiscoverySource.SEED_CSV

    def test_empty_name_raises(self):
        with pytest.raises(ValueError):
            DiscoveredCompany(name="", source=DiscoverySource.HARDCODED)

    def test_whitespace_name_raises(self):
        with pytest.raises(ValueError):
            DiscoveredCompany(name="   ", source=DiscoverySource.HARDCODED)


class TestProbeResult:
    """Tests for ProbeResult dataclass."""

    def test_valid_result(self):
        result = ProbeResult(
            careers_url="https://stripe.com/jobs",
            final_url="https://stripe.com/jobs",
            detected_ats=ATSType.CUSTOM,
            confidence=0.8,
            fetch_mode="static",
            robots_allowed=True,
        )
        assert result.detected_ats == ATSType.CUSTOM

    def test_invalid_confidence_raises(self):
        with pytest.raises(ValueError):
            ProbeResult(
                careers_url="https://test.com",
                final_url="https://test.com",
                detected_ats=ATSType.CUSTOM,
                confidence=1.5,  # Invalid
                fetch_mode="static",
                robots_allowed=True,
            )

    def test_invalid_fetch_mode_raises(self):
        with pytest.raises(ValueError):
            ProbeResult(
                careers_url="https://test.com",
                final_url="https://test.com",
                detected_ats=ATSType.CUSTOM,
                confidence=0.5,
                fetch_mode="invalid",  # Invalid
                robots_allowed=True,
            )


class TestSelectorHintValidation:
    """Tests for SelectorHint validation."""

    def test_valid_hint(self):
        hint = SelectorHint(
            job_container="div.job",
            title="h3",
            link="a",
            confidence=0.7,
        )
        assert hint.is_valid() is True

    def test_missing_required_invalid(self):
        hint = SelectorHint(
            job_container="div.job",
            title=None,
            link="a",
        )
        assert hint.is_valid() is False

    def test_to_dict_roundtrip(self):
        hint = SelectorHint(
            job_container="div.job",
            title="h3",
            link="a",
            location="span.loc",
            confidence=0.8,
            sample_count=5,
        )
        data = hint.to_dict()
        restored = SelectorHint.from_dict(data)
        assert restored.job_container == hint.job_container
        assert restored.confidence == hint.confidence


class TestATSTypeProperties:
    """Tests for ATSType enum properties."""

    def test_greenhouse_has_adapter(self):
        assert ATSType.GREENHOUSE.has_existing_adapter is True

    def test_lever_has_adapter(self):
        assert ATSType.LEVER.has_existing_adapter is True

    def test_workday_is_deferred(self):
        assert ATSType.WORKDAY.is_deferred is True

    def test_custom_no_adapter(self):
        assert ATSType.CUSTOM.has_existing_adapter is False
        assert ATSType.CUSTOM.is_deferred is False


class TestSourcesLoading:
    """Tests for loading companies from sources."""

    def test_load_seed_csv_nonexistent(self, tmp_path):
        """Loading a non-existent CSV returns no companies."""
        companies = list(load_seed_csv(tmp_path / "nonexistent.csv"))
        assert companies == []

    def test_load_seed_csv_valid(self, tmp_path):
        """Loading a valid CSV returns companies."""
        csv_file = tmp_path / "seed.csv"
        csv_file.write_text(
            "company_name,priority,category\n"
            "Google,1,big_tech\n"
            "Microsoft,2,big_tech\n"
        )
        companies = list(load_seed_csv(csv_file))
        assert len(companies) == 2
        assert companies[0].name == "Google"
        assert companies[0].priority == 1
        assert companies[0].source == DiscoverySource.SEED_CSV

    def test_load_seed_csv_with_priority_filter(self, tmp_path):
        """Priority filter excludes low-priority companies."""
        csv_file = tmp_path / "seed.csv"
        csv_file.write_text(
            "company_name,priority,category\n"
            "Google,1,big_tech\n"
            "Microsoft,2,big_tech\n"
            "StartupCo,3,startup\n"
        )
        companies = list(load_seed_csv(csv_file, max_priority=2))
        assert len(companies) == 2
        assert all(c.priority <= 2 for c in companies)

    def test_load_hardcoded_yaml_valid(self, tmp_path):
        """Loading a valid YAML returns companies."""
        yaml_file = tmp_path / "companies.yaml"
        yaml_file.write_text(
            "companies:\n"
            "  - name: Stripe\n"
            "    careers_url: https://stripe.com/jobs\n"
            "    priority: 1\n"
            "  - name: Plaid\n"
        )
        companies = list(load_hardcoded_yaml(yaml_file))
        assert len(companies) == 2
        assert companies[0].name == "Stripe"
        assert companies[0].careers_url == "https://stripe.com/jobs"
        assert companies[0].source == DiscoverySource.HARDCODED

    def test_load_hardcoded_yaml_nonexistent(self, tmp_path):
        """Loading a non-existent YAML returns no companies."""
        companies = list(load_hardcoded_yaml(tmp_path / "nonexistent.yaml"))
        assert companies == []
