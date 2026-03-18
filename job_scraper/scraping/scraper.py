"""
Core scraping logic for Phase A scraper.

Orchestrates fetching (static/browser/api_spy), parsing, and error handling.
"""
import asyncio
import hashlib
import logging
import re
from typing import Any, List, Optional, Tuple

from .fetchers.static import fetch_static
from .fetchers.browser import fetch_with_browser
from .types import RawScrapedJob, SiteResult, convert_to_job_models
from ..models import Job

logger = logging.getLogger(__name__)

# Per-site wall-clock timeout (seconds).  Prevents a single failing SPA
# from blocking the entire run (e.g. 30s nav timeout × 2 modes + scroll +
# Groq retries + NetworkSpy = 100s+ without a cap).
_SITE_TIMEOUT_S = 90.0

# JSON field names to try when extracting each job attribute, in priority order.
_TITLE_KEYS = ("title", "job_title", "jobTitle", "position", "name", "role")
_URL_KEYS = ("url", "apply_url", "applyUrl", "absolute_url", "hostedUrl", "link", "href", "applicationUrl")
_LOCATION_KEYS = ("location", "locations", "city", "office", "workplace", "remote")


class ParseError(Exception):
    """Raised when HTML parsing fails."""
    pass


def _extract_str(obj: dict, keys: tuple) -> Optional[str]:
    """Return the first non-empty string found under any of *keys*."""
    for key in keys:
        val = obj.get(key)
        if isinstance(val, str) and val.strip():
            return val.strip()
        if isinstance(val, list) and val:
            first = val[0]
            if isinstance(first, str) and first.strip():
                return first.strip()
            if isinstance(first, dict):
                # e.g. locations: [{name: "Remote"}]
                for sub in ("name", "text", "value", "city"):
                    sv = first.get(sub)
                    if isinstance(sv, str) and sv.strip():
                        return sv.strip()
    return None


def _json_jobs_to_raw(
    job_items: List[Any],
    company_name: str,
    field_map: Optional[dict] = None,
) -> List[RawScrapedJob]:
    """
    Convert a list of raw JSON job dicts (from a NetworkSpy-discovered endpoint)
    into RawScrapedJob objects.

    *field_map* is an optional dict stored in api_endpoint["field_map"] that maps
    logical field names to the actual key names used by this particular site, e.g.::

        {"title": "listing_name", "url": "apply_link", "location": "office_city"}

    When a mapping is present its key is tried *first*, before the ranked fallback
    lists.  Missing or empty values still fall through to the defaults so partial
    mappings are fine.
    """
    fm = field_map or {}

    # Prepend site-specific override keys so they win over the generic ranked list.
    title_keys = ((fm["title"],) if "title" in fm else ()) + _TITLE_KEYS
    url_keys = ((fm["url"],) if "url" in fm else ()) + _URL_KEYS
    location_keys = ((fm["location"],) if "location" in fm else ()) + _LOCATION_KEYS

    raw: List[RawScrapedJob] = []
    for item in job_items:
        if not isinstance(item, dict):
            continue
        title = _extract_str(item, title_keys)
        url = _extract_str(item, url_keys)
        if not title or not url:
            continue
        location = _extract_str(item, location_keys)
        raw.append(RawScrapedJob(title=title, url=url, location=location, company=company_name))
    return raw


async def _scrape_via_api_spy(site, max_pages: int = 20) -> Tuple[Optional[List[RawScrapedJob]], Optional[str]]:
    """
    Fetch jobs by replaying the stored NetworkSpy-discovered API endpoint.

    Returns (raw_jobs, error).  raw_jobs is None on failure.
    """
    endpoint_cfg = getattr(site, "api_endpoint", None)
    if not endpoint_cfg:
        return None, "api_spy mode but api_endpoint is not set on site"

    try:
        from .fetchers.replay import ReplayClient
        from .fetchers.network_spy import DiscoveredEndpoint, PaginationHint
    except ImportError as exc:
        return None, f"replay/network_spy not available: {exc}"

    # Reconstruct a lightweight DiscoveredEndpoint from stored config
    pag_cfg = endpoint_cfg.get("pagination")
    pagination = None
    if pag_cfg:
        pagination = PaginationHint(
            style=pag_cfg.get("style", "page"),
            param_name=pag_cfg.get("param_name", "page"),
            current_value=pag_cfg.get("current_value", 1),
            in_body=pag_cfg.get("in_body", False),
        )

    class _StoredEndpoint:
        url = endpoint_cfg["url"]
        method = endpoint_cfg.get("method", "GET")
        replay_headers = endpoint_cfg.get("replay_headers", {})
        request_post_data = endpoint_cfg.get("request_post_data")

    stored = _StoredEndpoint()
    stored.pagination = pagination  # type: ignore[attr-defined]

    field_map = endpoint_cfg.get("field_map")
    client = ReplayClient()

    try:
        if pagination and not pagination.in_body and pagination.style in ("page", "offset"):
            pages = await client.paginate(stored, max_pages=max_pages, stop_on_empty=True)
        else:
            pages = [await client.fetch(stored)]
    except Exception as exc:
        return None, f"replay_error: {exc}"

    # HTTP status codes that mean the stored endpoint is permanently stale —
    # auth token expired, URL changed, or resource removed.
    _STALE_STATUSES = {401, 403, 404, 410}

    all_raw: List[RawScrapedJob] = []
    for page in pages:
        if not page.ok:
            if page.status in _STALE_STATUSES:
                logger.warning(
                    "api_spy endpoint stale for %s: HTTP %d — needs re-probe",
                    site.company_name, page.status,
                )
                return [], f"endpoint_expired:{page.status}"
            logger.warning(
                "api_spy replay got HTTP %d for %s", page.status, site.company_name
            )
            break
        items = page.extract_jobs()
        all_raw.extend(_json_jobs_to_raw(items, site.company_name, field_map=field_map))

    return all_raw, None


def _persist_api_endpoint(cfg, site_id, endpoint) -> None:
    """
    Persist a NetworkSpy-discovered endpoint to the DB so future runs
    skip the cascade and go straight to JSON replay (fetch_mode='api_spy').
    """
    try:
        from ..storage import session_scope
        from .models import ScrapeSite

        pag = endpoint.pagination
        endpoint_dict = {
            "url": endpoint.url,
            "method": endpoint.method,
            "replay_headers": endpoint.replay_headers,
            "request_post_data": endpoint.request_post_data,
            "confidence": endpoint.confidence,
            "pagination": {
                "style": pag.style,
                "param_name": pag.param_name,
                "current_value": pag.current_value,
                "in_body": pag.in_body,
            } if pag else None,
        }

        with session_scope(cfg.db_dsn) as session:
            session.query(ScrapeSite).filter(ScrapeSite.id == site_id).update(
                {"fetch_mode": "api_spy", "api_endpoint": endpoint_dict},
                synchronize_session=False,
            )
        logger.info(
            "Persisted api_spy endpoint for site_id=%s: %s (conf=%.2f)",
            site_id,
            endpoint.url,
            endpoint.confidence,
        )
    except Exception as exc:
        logger.warning(
            "Failed to persist api_spy endpoint for site_id=%s: %s", site_id, exc
        )


async def scrape_site(site, cfg=None) -> Tuple[List[Job], SiteResult]:
    """
    Scrape a single site with a wall-clock timeout cap.

    Delegates to :func:`_scrape_site_inner` and wraps it with
    ``asyncio.wait_for`` so no single site can stall the entire run.
    """
    try:
        return await asyncio.wait_for(
            _scrape_site_inner(site, cfg), timeout=_SITE_TIMEOUT_S
        )
    except asyncio.TimeoutError:
        logger.warning(
            "Site timeout (%.0fs) for %s", _SITE_TIMEOUT_S, site.company_name
        )
        return [], SiteResult(
            site_id=site.id,
            success=False,
            jobs_found=0,
            error=f"site_timeout_{int(_SITE_TIMEOUT_S)}s",
        )


class EnrichmentBudget:
    """Thread-safe budget counters for detail enrichment.

    Tracks fetch count, LLM call count, elapsed time, and sampled warnings
    under an asyncio lock so concurrent workers don't overshoot limits.
    """

    def __init__(
        self,
        max_fetches: int = 30,
        max_llm_calls: int = 10,
        max_seconds: float = 25.0,
        max_warnings: int = 3,
    ):
        self._lock = asyncio.Lock()
        self.max_fetches = max_fetches
        self.max_llm_calls = max_llm_calls
        self.max_seconds = max_seconds
        self.max_warnings = max_warnings

        self.fetches_used = 0
        self.llm_calls_used = 0
        self.warnings_emitted = 0
        self.browser_fallbacks = 0
        self.llm_cleanups = 0
        self.fetch_failures = 0
        self._start_time: Optional[float] = None

    def start(self) -> None:
        self._start_time = asyncio.get_event_loop().time()

    @property
    def elapsed(self) -> float:
        if self._start_time is None:
            return 0.0
        return asyncio.get_event_loop().time() - self._start_time

    async def try_acquire_fetch(self) -> bool:
        async with self._lock:
            if self.fetches_used >= self.max_fetches or self.elapsed >= self.max_seconds:
                return False
            self.fetches_used += 1
            return True

    async def try_acquire_llm(self) -> bool:
        async with self._lock:
            if self.llm_calls_used >= self.max_llm_calls or self.elapsed >= self.max_seconds:
                return False
            self.llm_calls_used += 1
            return True

    async def record_browser_fallback(self) -> None:
        async with self._lock:
            self.browser_fallbacks += 1

    async def record_llm_cleanup(self) -> None:
        async with self._lock:
            self.llm_cleanups += 1

    async def record_fetch_failure(self, url: str, error: str) -> None:
        async with self._lock:
            self.fetch_failures += 1
            if self.warnings_emitted < self.max_warnings:
                self.warnings_emitted += 1
                logger.warning("Detail fetch failed for %s: %s", url, error)

    @property
    def exhausted(self) -> bool:
        return (
            self.fetches_used >= self.max_fetches
            or self.elapsed >= self.max_seconds
        )

    def summary(self) -> str:
        return (
            f"fetched={self.fetches_used}/{self.max_fetches} "
            f"llm={self.llm_calls_used}/{self.max_llm_calls} "
            f"browser_fallbacks={self.browser_fallbacks} "
            f"cleanups={self.llm_cleanups} "
            f"fetch_failures={self.fetch_failures} "
            f"elapsed={self.elapsed:.1f}s/{self.max_seconds:.0f}s"
        )


def _likely_spa_shell(html: str) -> bool:
    """Heuristic: returns True when HTML looks like an SPA shell with no job content.

    Checks for missing job-content selectors, high script ratio, and low visible text.
    """
    html_lower = html.lower()

    # Check for job-content selectors
    job_selectors = [
        'class="description', "class='description",
        'class="job-detail', "class='job-detail",
        'id="description', "id='description",
        'class="job_description', "class='job_description",
    ]
    has_job_content = any(sel in html_lower for sel in job_selectors)
    if has_job_content:
        return False

    # Count script tags vs visible text
    script_count = html_lower.count("<script")
    # Strip tags for visible text estimate
    visible = re.sub(r"<[^>]+>", " ", html)
    visible = re.sub(r"\s+", " ", visible).strip()

    if script_count > 5 and len(visible) < 200:
        return True

    # Text-to-HTML ratio
    if len(html) > 0 and len(visible) / len(html) < 0.05:
        return True

    return False


async def _enrich_with_details(
    raw_jobs: List[RawScrapedJob],
    llm_config: dict,
    concurrency: int = 5,
    max_per_site: int = 50,
    fetch_timeout: float = 15.0,
    max_seconds: float = 25.0,
    max_fetches: int = 30,
    max_llm_calls: int = 10,
) -> None:
    """Fetch each job's detail page and extract description/location/salary.

    Mutates *raw_jobs* in-place, filling in ``description``, ``location``,
    and ``salary`` fields from the individual job posting pages.

    Uses a producer/worker pattern with budget enforcement — stops
    enqueueing new jobs when time, fetch, or LLM budgets are exhausted.
    """
    from .parsers.detail import extract_job_detail, _LLMRateLimiter, _description_needs_cleanup

    # Filter to jobs needing enrichment: missing or low-quality description
    to_enrich = [
        j for j in raw_jobs
        if j.url and (not j.description or _description_needs_cleanup(j.description))
    ]
    if not to_enrich:
        return

    to_enrich = to_enrich[:max_per_site]
    sem = asyncio.Semaphore(concurrency)
    browser_sem = asyncio.Semaphore(1)  # browser fallback concurrency=1
    rate_limiter = _LLMRateLimiter(calls_per_minute=25)
    budget = EnrichmentBudget(
        max_fetches=max_fetches,
        max_llm_calls=max_llm_calls,
        max_seconds=max_seconds,
    )
    budget.start()

    async def _enrich_one(job: RawScrapedJob) -> None:
        async with sem:
            # Check budget before doing any work
            if not await budget.try_acquire_fetch():
                return

            try:
                html, error = await fetch_static(job.url, timeout=fetch_timeout)

                # Browser fallback: static failed entirely, or SPA shell detected
                need_browser = False
                if error or not html:
                    need_browser = True
                elif _likely_spa_shell(html):
                    need_browser = True

                if need_browser:
                    async with browser_sem:
                        try:
                            browser_html, browser_err, _, _ = await fetch_with_browser(
                                job.url, timeout=8.0, skip_interactions=True,
                                capture_network=False, capture_screenshot=False,
                            )
                            if browser_html and not browser_err:
                                # Keep whichever is longer
                                if not html or len(browser_html) > len(html):
                                    html = browser_html
                                await budget.record_browser_fallback()
                        except Exception:
                            pass  # browser not available or failed

                if not html:
                    await budget.record_fetch_failure(job.url, error or "empty_response")
                    return

                detail = await extract_job_detail(
                    html, job.url,
                    llm_config=llm_config,
                    rate_limiter=rate_limiter,
                    budget=budget,
                )
                if detail.get("description"):
                    job.description = detail["description"]
                if detail.get("location") and not job.location:
                    job.location = detail["location"]
                if detail.get("salary") and not job.salary:
                    job.salary = detail["salary"]
            except Exception as exc:
                logger.debug("Detail enrichment failed for %s: %s", job.url, exc)

    # Producer/worker: process jobs sequentially through the queue,
    # but run up to `concurrency` workers in parallel.  Stop enqueueing
    # when budget is exhausted.
    queue: asyncio.Queue[RawScrapedJob] = asyncio.Queue()
    for j in to_enrich:
        queue.put_nowait(j)

    async def _worker() -> None:
        while True:
            if budget.exhausted:
                return
            try:
                job = queue.get_nowait()
            except asyncio.QueueEmpty:
                return
            await _enrich_one(job)
            queue.task_done()

    workers = [asyncio.create_task(_worker()) for _ in range(concurrency)]
    await asyncio.gather(*workers)

    enriched = sum(1 for j in to_enrich if j.description and not _description_needs_cleanup(j.description))
    logger.info(
        "Detail enrichment: %d/%d enriched — %s",
        enriched, len(to_enrich), budget.summary(),
    )


async def _scrape_site_inner(site, cfg=None) -> Tuple[List[Job], SiteResult]:
    """
    Scrape a single site: fetch jobs, parse, convert to Job models.

    Args:
        site: ScrapeSite model instance with careers_url, fetch_mode, selector_hints,
              and (when fetch_mode='api_spy') api_endpoint.
        cfg: Optional config object (reserved for future use)

    Returns:
        Tuple of (jobs, site_result):
            - jobs: List of Job model instances
            - site_result: SiteResult with outcome metadata

    Fetch modes
    -----------
    static / browser
        Fetch careers page HTML, then run a multi-strategy parser cascade:
        1. JSON-LD / Schema.org structured data (free, instant, stable)
        2. RSS/Atom feed (if <link rel="alternate"> detected in HTML)
        2.5. Sitemap.xml (probes common paths, filters job URLs)
        3. Link-graph extraction (all <a> tags filtered by URL pattern)
        4. LLM parser (Groq/HF — smart fallback, needs API keys)
        5. CSS selector parser (last resort, fragile)
        6. NetworkSpy fallback (inline, scores pre-captured network calls
           from the browser fetch — discovers hidden JSON APIs on SPAs
           without launching a second browser)
        Falls back to the other fetch mode on failure.
    api_spy
        Replay the JSON API endpoint discovered by NetworkSpy.  Skips HTML
        fetching and parsing entirely — uses JSON field mapping instead.
    """
    try:
        primary_mode = (getattr(site, "fetch_mode", "static") or "static").lower()

        # --- api_spy path (fastest, most reliable) ---
        if primary_mode == "api_spy":
            logger.info("Fetching %s via api_spy (direct JSON replay)", site.company_name)
            raw_jobs, error = await _scrape_via_api_spy(site)
            if error or raw_jobs is None:
                needs_reprobe = error is not None and error.startswith("endpoint_expired:")
                logger.warning("api_spy failed for %s: %s", site.company_name, error)
                return [], SiteResult(
                    site_id=site.id,
                    success=False,
                    jobs_found=0,
                    error=error or "api_spy_no_results",
                    needs_reprobe=needs_reprobe,
                )

            # Enrich api_spy jobs missing/low-quality descriptions
            detail_cfg = getattr(cfg, "detail_enrichment", {}) if cfg else {}
            if detail_cfg.get("enabled", True) and raw_jobs:
                llm_cfg = getattr(cfg, "llm_parser", {}) if cfg else {}
                try:
                    await _enrich_with_details(
                        raw_jobs,
                        llm_config=llm_cfg,
                        concurrency=detail_cfg.get("concurrency", 5),
                        max_per_site=detail_cfg.get("max_per_site", 50),
                        fetch_timeout=detail_cfg.get("fetch_timeout", 15.0),
                        max_seconds=detail_cfg.get("max_seconds", 25.0),
                        max_fetches=detail_cfg.get("max_fetches", 30),
                        max_llm_calls=detail_cfg.get("max_llm_calls", 10),
                    )
                except Exception as exc:
                    logger.warning("Detail enrichment error for %s (api_spy): %s", site.company_name, exc)

            jobs = convert_to_job_models(raw_jobs, site)
            logger.info("api_spy scraped %d jobs from %s", len(jobs), site.company_name)
            return jobs, SiteResult(site_id=site.id, success=True, jobs_found=len(jobs))

        # --- static / browser path (hybrid fallback) ---
        mode_order = [primary_mode]
        fallback_mode = "browser" if primary_mode != "browser" else "static"
        mode_order.append(fallback_mode)

        raw_jobs = None
        last_error = None
        used_browser = False
        captured_calls = None  # network calls captured during browser fetch
        screenshot_png = None  # full-page screenshot for vision parser
        llm_tried = False      # prevent duplicate LLM calls across fetch modes
        for mode in mode_order:
            if mode == "browser":
                logger.info(f"Fetching {site.company_name} with browser (JS-enabled)")
                html, error, captured_calls, screenshot_png = await fetch_with_browser(
                    site.careers_url, capture_network=True, capture_screenshot=True
                )
                if not error:
                    used_browser = True
            else:
                logger.info(f"Fetching {site.company_name} with static HTTP")
                html, error = await fetch_static(site.careers_url)

            if error:
                last_error = f"{mode}_fetch_error: {error}"
                continue

            # ---- Parser cascade (first successful result wins) ----

            # 1. JSON-LD / Schema.org structured data (free, instant, stable)
            try:
                from .parsers.structured_data import parse_structured_data
                raw_jobs = parse_structured_data(html, site.careers_url)
                if raw_jobs:
                    logger.info("JSON-LD parsed %d jobs for %s", len(raw_jobs), site.company_name)
                    break
            except Exception as exc:
                logger.debug("JSON-LD parser failed for %s: %s", site.company_name, exc)

            # 2. RSS/Atom feed (if detected in HTML)
            try:
                from .parsers.rss import detect_feed_url, fetch_and_parse_feed
                feed_url = detect_feed_url(html, site.careers_url)
                if feed_url:
                    raw_jobs = await fetch_and_parse_feed(feed_url, site.careers_url)
                    if raw_jobs:
                        logger.info("RSS feed parsed %d jobs for %s", len(raw_jobs), site.company_name)
                        break
            except Exception as exc:
                logger.debug("RSS parser failed for %s: %s", site.company_name, exc)

            # 2.5 Sitemap parser (structured, fast, no JS needed)
            try:
                from .parsers.sitemap import fetch_and_parse_sitemap
                raw_jobs = await fetch_and_parse_sitemap(site.careers_url, company_name=site.company_name)
                if raw_jobs:
                    logger.info("Sitemap parsed %d jobs for %s", len(raw_jobs), site.company_name)
                    break
            except Exception as exc:
                logger.debug("Sitemap parser failed for %s: %s", site.company_name, exc)

            # 3. Link-graph extraction (no selectors needed, fast)
            try:
                from .parsers.link_graph import parse_link_graph
                raw_jobs = parse_link_graph(html, site.careers_url)
                if raw_jobs:
                    logger.info("Link-graph parsed %d jobs for %s", len(raw_jobs), site.company_name)
                    break
            except Exception as exc:
                logger.debug("Link-graph parser failed for %s: %s", site.company_name, exc)

            # 4. LLM parser (smart fallback, needs API keys)
            #    Skip if already tried on a previous fetch mode to avoid
            #    duplicate Groq calls and 429 rate-limit delays.
            llm_cfg = getattr(cfg, "llm_parser", {}) if cfg else {}
            llm_enabled = llm_cfg.get("enabled", True)
            llm_has_keys = bool(llm_cfg.get("groq_api_key") or llm_cfg.get("hf_api_key"))

            if llm_enabled and not llm_has_keys:
                logger.warning(
                    "Skipping parser for %s: enabled but no API keys configured "
                    "(set GROQ_API_KEY or HF_API_KEY)",
                    site.company_name,
                )

            if llm_enabled and llm_has_keys and not llm_tried:
                llm_tried = True
                try:
                    from .parsers.llm import parse_with_llm
                    raw_jobs = await parse_with_llm(html, site.careers_url, llm_config=llm_cfg)
                    if raw_jobs:
                        logger.info("LLM parsed %d jobs for %s", len(raw_jobs), site.company_name)
                        break
                    logger.debug("LLM returned 0 jobs for %s", site.company_name)
                except Exception as exc:
                    logger.debug("LLM parser failed for %s: %s", site.company_name, exc)

            # 4.5. Vision parser (screenshot → Groq Llama 4 Scout)
            #      Only fires when text LLM returned 0 + we have a screenshot.
            if not raw_jobs and screenshot_png and llm_has_keys:
                try:
                    from .parsers.vision import parse_with_vision
                    raw_jobs = await parse_with_vision(
                        screenshot_png, site.careers_url, html,
                        llm_config=llm_cfg,
                    )
                    if raw_jobs:
                        logger.info("Vision parsed %d jobs for %s", len(raw_jobs), site.company_name)
                        break
                    logger.debug("Vision returned 0 jobs for %s", site.company_name)
                except Exception as exc:
                    logger.debug("Vision parser failed for %s: %s", site.company_name, exc)

            # 5. CSS selector parser (last resort, fragile)
            try:
                from .parsers.css import parse_with_selectors
                selector_hints = getattr(site, "selector_hints", None)
                if selector_hints:
                    raw_jobs = parse_with_selectors(html, selector_hints, site.careers_url)
                    if raw_jobs:
                        logger.info("CSS parsed %d jobs for %s", len(raw_jobs), site.company_name)
                        break
            except ImportError:
                logger.error("CSS parser not available for %s", site.company_name)
                last_error = "css_parser_unavailable"
                continue
            except Exception as exc:
                last_error = f"{mode}_parse_error: {exc}"
                continue

        # After full cascade: if 0 jobs and we captured network calls from
        # the browser fetch, score them to discover hidden JSON APIs (SPAs).
        # This reuses the already-captured traffic — no second browser launch.
        if not raw_jobs and used_browser and captured_calls:
            logger.info(
                "0 jobs from cascade for %s — scoring %d captured network calls",
                site.company_name,
                len(captured_calls),
            )
            try:
                from .fetchers.network_spy import NetworkSpy
                from .fetchers.replay import ReplayClient, ReplayResponse

                spy = NetworkSpy(min_confidence=0.35)
                endpoints = spy.score_captured(captured_calls)
                if endpoints:
                    best = endpoints[0]
                    logger.info(
                        "NetworkSpy found endpoint (conf=%.2f): %s",
                        best.confidence,
                        best.url,
                    )

                    # Extract jobs from already-captured response (no HTTP call)
                    captured_resp = ReplayResponse(
                        url=best.url,
                        method=best.method,
                        status=best.response_status,
                        headers=best.response_headers,
                        body=None,
                        text=best.response_text,
                        json_body=best.response_json,
                    )
                    spy_jobs: List[RawScrapedJob] = _json_jobs_to_raw(
                        captured_resp.extract_jobs(), site.company_name
                    )

                    # Only replay for additional pages if pagination detected
                    if spy_jobs and best.pagination and not best.pagination.in_body:
                        client = ReplayClient()
                        if best.pagination.style in ("page", "offset"):
                            extra_pages = await client.paginate(
                                best, max_pages=4, stop_on_empty=True
                            )
                            for pg in extra_pages[1:]:  # skip page 1 — already have it
                                spy_jobs.extend(
                                    _json_jobs_to_raw(pg.extract_jobs(), site.company_name)
                                )

                    if spy_jobs:
                        raw_jobs = spy_jobs
                        logger.info(
                            "NetworkSpy recovered %d jobs for %s",
                            len(spy_jobs),
                            site.company_name,
                        )
                        # Persist endpoint for future runs (skip cascade next time)
                        if cfg:
                            _persist_api_endpoint(cfg, site.id, best)
            except Exception as exc:
                logger.warning(
                    "Inline NetworkSpy failed for %s: %s", site.company_name, exc
                )

        if not raw_jobs:
            logger.warning(f"Fetch/parse failed for {site.company_name}: {last_error}")
            return [], SiteResult(
                site_id=site.id,
                success=False,
                jobs_found=0,
                error=last_error or "scrape_failed",
            )

        # Enrich jobs with detail page data (description, location, salary)
        detail_cfg = getattr(cfg, "detail_enrichment", {}) if cfg else {}
        if detail_cfg.get("enabled", True):
            llm_cfg = getattr(cfg, "llm_parser", {}) if cfg else {}
            try:
                await _enrich_with_details(
                    raw_jobs,
                    llm_config=llm_cfg,
                    concurrency=detail_cfg.get("concurrency", 5),
                    max_per_site=detail_cfg.get("max_per_site", 50),
                    fetch_timeout=detail_cfg.get("fetch_timeout", 15.0),
                    max_seconds=detail_cfg.get("max_seconds", 25.0),
                    max_fetches=detail_cfg.get("max_fetches", 30),
                    max_llm_calls=detail_cfg.get("max_llm_calls", 10),
                )
            except Exception as exc:
                logger.warning("Detail enrichment error for %s: %s", site.company_name, exc)

        jobs = convert_to_job_models(raw_jobs, site)

        logger.info(f"Successfully scraped {len(jobs)} jobs from {site.company_name}")
        return jobs, SiteResult(
            site_id=site.id,
            success=True,
            jobs_found=len(jobs),
            error=None
        )

    except ParseError as e:
        logger.warning(f"Parse error for {site.company_name}: {e}")
        return [], SiteResult(
            site_id=site.id,
            success=False,
            jobs_found=0,
            error=f"parse_error: {str(e)}"
        )

    except Exception as e:
        logger.error(f"Unexpected scrape error for {site.company_name}: {e}", exc_info=True)
        return [], SiteResult(
            site_id=site.id,
            success=False,
            jobs_found=0,
            error=f"unexpected_error: {str(e)}"
        )
