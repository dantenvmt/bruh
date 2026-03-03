"""
Core scraping logic for Phase A scraper.

Orchestrates fetching (static/browser/api_spy), parsing, and error handling.
"""
import logging
from typing import Any, List, Optional, Tuple

from .fetchers.static import fetch_static
from .fetchers.browser import fetch_with_browser
from .types import RawScrapedJob, SiteResult, convert_to_job_models
from ..models import Job

logger = logging.getLogger(__name__)

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


async def scrape_site(site, cfg=None) -> Tuple[List[Job], SiteResult]:
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
        Fetch careers page HTML, parse with CSS selector hints.
        Falls back to the other mode on failure.
    api_spy
        Replay the JSON API endpoint discovered by NetworkSpy.  Skips HTML
        fetching and CSS parsing entirely — uses JSON field mapping instead.
    """
    try:
        primary_mode = (getattr(site, "fetch_mode", "static") or "static").lower()

        # --- api_spy path ---
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
            jobs = convert_to_job_models(raw_jobs, site)
            logger.info("api_spy scraped %d jobs from %s", len(jobs), site.company_name)
            return jobs, SiteResult(site_id=site.id, success=True, jobs_found=len(jobs))

        # --- static / browser path (hybrid fallback) ---
        mode_order = [primary_mode]
        fallback_mode = "browser" if primary_mode != "browser" else "static"
        mode_order.append(fallback_mode)

        raw_jobs = None
        last_error = None
        for mode in mode_order:
            if mode == "browser":
                logger.info(f"Fetching {site.company_name} with browser (JS-enabled)")
                html, error = await fetch_with_browser(site.careers_url)
            else:
                logger.info(f"Fetching {site.company_name} with static HTTP")
                html, error = await fetch_static(site.careers_url)

            if error:
                last_error = f"{mode}_fetch_error: {error}"
                continue

            # --- LLM parser (primary) ---
            llm_succeeded = False
            try:
                from .parsers.llm import parse_with_llm
                raw_jobs = await parse_with_llm(html, site.careers_url)
                if raw_jobs:
                    llm_succeeded = True
                    break
                logger.debug("LLM returned 0 jobs for %s, trying CSS", site.company_name)
            except Exception as exc:
                logger.debug("LLM parser unavailable/failed for %s: %s", site.company_name, exc)

            # --- CSS parser (fallback) ---
            if not llm_succeeded:
                try:
                    from .parsers.css import parse_with_selectors
                    raw_jobs = parse_with_selectors(html, site.selector_hints, site.careers_url)
                    break
                except ImportError:
                    logger.error("CSS parser not available for %s", site.company_name)
                    raise ParseError("CSS parser not implemented (parsers/css.py missing)")
                except Exception as exc:
                    last_error = f"{mode}_parse_error: {exc}"
                    continue

        if raw_jobs is None:
            logger.warning(f"Fetch/parse failed for {site.company_name}: {last_error}")
            return [], SiteResult(
                site_id=site.id,
                success=False,
                jobs_found=0,
                error=last_error or "scrape_failed",
            )

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
