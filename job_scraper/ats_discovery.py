"""
ATS discovery helpers.

Goal: take a list of company names or domains and discover public ATS boards
(Greenhouse/Lever/SmartRecruiters/etc.) by scanning career pages for known URL
patterns. This is a best-effort approach designed for "more data" bootstrapping.
"""

from __future__ import annotations

import asyncio
import logging
import re
import time
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple
from urllib.parse import urljoin

import httpx

logger = logging.getLogger(__name__)


CLEARBIT_SUGGEST_URL = "https://autocomplete.clearbit.com/v1/companies/suggest"


_ATS_PATTERNS: Dict[str, List[re.Pattern]] = {
    "greenhouse": [
        re.compile(r"boards\\.greenhouse\\.io/([a-z0-9_-]+)", re.IGNORECASE),
        re.compile(r"boards-api\\.greenhouse\\.io/v1/boards/([a-z0-9_-]+)", re.IGNORECASE),
    ],
    "lever": [
        re.compile(r"jobs\\.lever\\.co/([a-z0-9_-]+)", re.IGNORECASE),
        re.compile(r"api\\.lever\\.co/v0/postings/([a-z0-9_-]+)", re.IGNORECASE),
    ],
    "smartrecruiters": [
        re.compile(r"jobs\\.smartrecruiters\\.com/([a-z0-9_-]+)", re.IGNORECASE),
        re.compile(r"api\\.smartrecruiters\\.com/v1/companies/([a-z0-9_-]+)", re.IGNORECASE),
    ],
    "workable": [
        re.compile(r"apply\\.workable\\.com/([a-z0-9_-]+)", re.IGNORECASE),
    ],
    "ashby": [
        re.compile(r"jobs\\.ashbyhq\\.com/([a-z0-9_-]+)", re.IGNORECASE),
    ],
    "workday": [
        # Workday URLs are extracted structurally via parse_workday_url().
        # This regex is used as a fast pre-filter to detect Workday URLs in HTML.
        re.compile(
            r"(https?://[^\"'\s]+\.myworkdayjobs\.com/[^\"'\s]+)",
            re.IGNORECASE,
        ),
    ],
}


def _norm_name(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (text or "").lower())


def extract_ats_tokens(html_text: str, platforms: Set[str]) -> Dict[str, Set[str]]:
    found: Dict[str, Set[str]] = {}
    if not html_text:
        return found
    for platform in platforms:
        if platform == "workday":
            # Workday uses structured URL parsing instead of simple token extraction
            found_workday = _extract_workday_sites(html_text)
            if found_workday:
                found["workday"] = found_workday
            continue

        patterns = _ATS_PATTERNS.get(platform) or []
        for pat in patterns:
            for match in pat.findall(html_text):
                token = (match or "").strip().lower()
                if not token:
                    continue
                found.setdefault(platform, set()).add(token)
    return found


def _extract_workday_sites(html_text: str) -> Set[str]:
    """Extract canonical Workday site identifiers from HTML.

    Uses parse_workday_url() for structured extraction.  Returns a set of
    JSON-encoded ``{host, tenant, site}`` strings (we use JSON so the set
    can dedupe; callers unpack later).
    """
    import json as _json
    from .apis.workday import parse_workday_url

    seen: Set[Tuple[str, str, str]] = set()
    results: Set[str] = set()

    # Fast pre-filter: find all myworkdayjobs.com URLs in the HTML
    url_pattern = _ATS_PATTERNS.get("workday", [])
    for pat in url_pattern:
        for raw_url in pat.findall(html_text):
            site_obj = parse_workday_url(raw_url)
            if not site_obj:
                continue
            # Host safety: reject non-myworkdayjobs.com
            if not site_obj.host.endswith(".myworkdayjobs.com"):
                continue
            key = (site_obj.host.lower(), site_obj.tenant.lower(), site_obj.site.lower())
            if key in seen:
                continue
            seen.add(key)
            # Encode as JSON string for set storage
            results.add(_json.dumps({
                "host": site_obj.host.lower(),
                "tenant": site_obj.tenant,
                "site": site_obj.site,
            }, sort_keys=True))
    return results


def _extract_candidate_links(html_text: str, base_url: str, limit: int = 3) -> List[str]:
    """
    Extract a small set of career-ish links from a page for follow-up scanning.
    """
    links = []
    if not html_text:
        return links

    for href in re.findall(r'href=[\"\\\']([^\"\\\']+)[\"\\\']', html_text, flags=re.IGNORECASE):
        if not href or href.startswith("#") or href.lower().startswith("mailto:"):
            continue
        text = href.lower()
        if "careers" not in text and "/jobs" not in text and "jobs" not in text:
            continue
        abs_url = urljoin(base_url, href)
        if abs_url not in links:
            links.append(abs_url)
        if len(links) >= limit:
            break
    return links


def generate_slug_variants(company_name: str) -> List[str]:
    """
    Generate multiple slug variants for a company name.

    Returns 2-5 variants to probe for ATS boards. Handles edge cases like
    ampersands, parentheticals, and abbreviations.

    Examples:
        "Block (formerly Square)" -> ["block", "blocksquare", ...]
        "Ernst & Young" -> ["ernstyoung", "ernstandyoung", "ey", ...]
        "JPMorgan Chase" -> ["jpmorganchase", "jpmorgan-chase", "jpmorgan", ...]
    """
    if not company_name or not company_name.strip():
        return []

    variants: Set[str] = set()

    # 1. Clean parentheticals: "Block (formerly Square)" -> "Block"
    name = re.sub(r'\s*\([^)]*\)', '', company_name)

    # 2. Remove common suffixes
    suffixes = [
        r'\s+Inc\.?$', r'\s+Corp\.?$', r'\s+LLC$', r'\s+Ltd\.?$',
        r'\s+Co\.?$', r'\s+Technologies$', r'\s+Industries$',
        r'\s+Group$', r'\s+Holdings$', r'\s+International$'
    ]
    for suffix in suffixes:
        name = re.sub(suffix, '', name, flags=re.IGNORECASE)

    name = name.strip()

    # 3. Variant: full name, no spaces, no special chars
    full = re.sub(r'[^a-z0-9]', '', name.lower())
    if full:
        variants.add(full)

    # 4. Variant: hyphenated (spaces to hyphens, remove other special chars)
    hyphenated = re.sub(r'\s+', '-', name.lower())
    hyphenated = re.sub(r'[^a-z0-9-]', '', hyphenated)
    # Clean up multiple consecutive hyphens
    hyphenated = re.sub(r'-+', '-', hyphenated).strip('-')
    if hyphenated and hyphenated != full:
        variants.add(hyphenated)

    # 5. Variant: first word only
    words = name.split()
    if words:
        first = re.sub(r'[^a-z0-9]', '', words[0].lower())
        if first and len(first) >= 2:
            variants.add(first)

    # 6. Variant: initials (for multi-word names)
    if len(words) > 1:
        initials = ''.join(w[0].lower() for w in words if w and w[0].isalpha())
        if len(initials) >= 2:
            variants.add(initials)

    # 7. Variant: ampersand -> "and"
    if '&' in company_name:
        and_version = company_name.replace('&', 'and')
        and_slug = re.sub(r'[^a-z0-9]', '', and_version.lower())
        if and_slug and and_slug not in variants:
            variants.add(and_slug)

    # Return as list (deterministic order for testing)
    return sorted(list(variants))


async def validate_token(
    client: httpx.AsyncClient,
    platform: str,
    token: str,
    limiter: _RateLimiter,
) -> Tuple[bool, int]:
    """
    Validate that a token returns at least 1 job.

    Returns:
        (is_valid, job_count) tuple
    """
    await limiter.wait()

    if platform == "greenhouse":
        url = f"https://boards-api.greenhouse.io/v1/boards/{token}/jobs"
    elif platform == "lever":
        url = f"https://api.lever.co/v0/postings/{token}?mode=json"
    elif platform == "smartrecruiters":
        url = f"https://api.smartrecruiters.com/v1/companies/{token}/postings"
    else:
        return (False, 0)

    try:
        resp = await client.get(url, timeout=15.0)
        if resp.status_code != 200:
            return (False, 0)
        data = resp.json()

        # Extract job count based on platform
        if platform == "greenhouse":
            jobs = data.get("jobs", [])
        elif platform == "lever":
            jobs = data if isinstance(data, list) else []
        elif platform == "smartrecruiters":
            jobs = data.get("content", [])
        else:
            jobs = []

        return (len(jobs) > 0, len(jobs))
    except Exception as e:
        logger.debug(f"Token validation failed for {platform}/{token}: {e}")
        return (False, 0)


@dataclass
class CompanyDiscovery:
    company: str
    domain: Optional[str]
    urls_scanned: List[str]
    found: Dict[str, List[str]]
    validation: Optional[Dict[str, Dict[str, Any]]] = field(default=None)  # platform -> {token: str, job_count: int, validated: bool}


class _RateLimiter:
    def __init__(self, requests_per_minute: int):
        self._rpm = max(1, int(requests_per_minute or 60))
        self._lock = asyncio.Lock()
        self._last = 0.0

    async def wait(self) -> None:
        min_interval = 60.0 / self._rpm if self._rpm > 0 else 0.0
        if min_interval <= 0:
            return
        async with self._lock:
            now = time.monotonic()
            elapsed = now - self._last
            wait_seconds = max(0.0, min_interval - elapsed)
            if wait_seconds > 0:
                await asyncio.sleep(wait_seconds)
            self._last = time.monotonic()


async def _clearbit_suggest(client: httpx.AsyncClient, query: str, limiter: _RateLimiter) -> List[dict]:
    await limiter.wait()
    resp = await client.get(CLEARBIT_SUGGEST_URL, params={"query": query})
    if resp.status_code != 200:
        return []
    data = resp.json() if resp.content else []
    return data if isinstance(data, list) else []


async def resolve_domain_via_clearbit(
    client: httpx.AsyncClient,
    company_name: str,
    limiter: _RateLimiter,
) -> Optional[str]:
    suggestions = await _clearbit_suggest(client, company_name, limiter)
    if not suggestions:
        return None

    target = _norm_name(company_name)
    best = None
    best_score = -1
    for item in suggestions:
        if not isinstance(item, dict):
            continue
        name = item.get("name") or ""
        domain = item.get("domain") or ""
        if not domain:
            continue
        score = 0
        if _norm_name(name) == target:
            score = 100
        elif _norm_name(name).startswith(target) or target.startswith(_norm_name(name)):
            score = 50
        if score > best_score:
            best_score = score
            best = domain
    return best


async def _fetch_html(client: httpx.AsyncClient, url: str, limiter: _RateLimiter) -> Optional[str]:
    await limiter.wait()
    try:
        resp = await client.get(
            url,
            follow_redirects=True,
            headers={"User-Agent": "multi-api-aggregator/1.0"},
        )
    except (httpx.TimeoutException, httpx.TransportError):
        return None
    if resp.status_code != 200:
        return None
    ct = (resp.headers.get("content-type") or "").lower()
    if "text/html" not in ct and "application/xhtml+xml" not in ct:
        return None
    return resp.text


async def validate_token(
    client: httpx.AsyncClient,
    platform: str,
    token: str,
    limiter: _RateLimiter,
) -> Tuple[bool, int]:
    """
    Validate that a token returns at least one job from the ATS API.

    Args:
        client: httpx async client
        platform: "greenhouse", "lever", or "smartrecruiters"
        token: The board/company token to validate
        limiter: Rate limiter for API requests

    Returns:
        Tuple of (is_valid, job_count):
            - is_valid: True if token exists and returns jobs
            - job_count: Number of jobs found (0 if invalid)
    """
    await limiter.wait()

    # Map platform to API endpoint
    if platform == "greenhouse":
        url = f"https://boards-api.greenhouse.io/v1/boards/{token}/jobs"
    elif platform == "lever":
        url = f"https://api.lever.co/v0/postings/{token}?mode=json"
    elif platform == "smartrecruiters":
        url = f"https://api.smartrecruiters.com/v1/companies/{token}/postings"
    elif platform == "workday":
        return await _validate_workday_token(client, token, limiter)
    else:
        logger.warning(f"Unknown platform for validation: {platform}")
        return (False, 0)

    try:
        resp = await client.get(
            url,
            timeout=15.0,
            headers={"User-Agent": "multi-api-aggregator/1.0"},
            follow_redirects=True,
        )

        if resp.status_code != 200:
            logger.debug(f"Token validation failed for {platform}/{token}: HTTP {resp.status_code}")
            return (False, 0)

        data: Any = resp.json() if resp.content else {}

        # Extract job count based on platform
        jobs: List[Any] = []
        if platform == "greenhouse":
            jobs = data.get("jobs", []) if isinstance(data, dict) else []
        elif platform == "lever":
            jobs = data if isinstance(data, list) else []
        elif platform == "smartrecruiters":
            jobs = data.get("content", []) if isinstance(data, dict) else []

        job_count = len(jobs) if isinstance(jobs, list) else 0
        is_valid = job_count > 0

        if is_valid:
            logger.debug(f"Token validated: {platform}/{token} -> {job_count} jobs")
        else:
            logger.debug(f"Token validation failed: {platform}/{token} -> empty response")

        return (is_valid, job_count)

    except httpx.TimeoutException:
        logger.debug(f"Token validation timeout: {platform}/{token}")
        return (False, 0)
    except httpx.TransportError as e:
        logger.debug(f"Token validation transport error: {platform}/{token} - {e}")
        return (False, 0)
    except Exception as e:
        logger.warning(f"Token validation unexpected error: {platform}/{token} - {e}")
        return (False, 0)


async def _validate_workday_token(
    client: httpx.AsyncClient,
    token_json: str,
    limiter: _RateLimiter,
) -> Tuple[bool, int]:
    """Validate a Workday site via CXS POST with limit=1."""
    import json as _json

    await limiter.wait()

    try:
        site = _json.loads(token_json)
    except (ValueError, TypeError):
        return (False, 0)

    host = site.get("host", "")
    tenant = site.get("tenant", "")
    site_name = site.get("site", "")
    if not host or not tenant or not site_name:
        return (False, 0)

    url = f"https://{host}/wday/cxs/{tenant}/{site_name}/jobs"
    body = {"appliedFacets": {}, "limit": 1, "offset": 0, "searchText": ""}

    try:
        resp = await client.post(
            url,
            json=body,
            timeout=15.0,
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
        )
        if resp.status_code == 422:
            logger.debug(f"Workday validation failed: {tenant}/{site_name} — 422 invalid site")
            return (False, 0)
        if resp.status_code != 200:
            logger.debug(f"Workday validation failed: {tenant}/{site_name} — HTTP {resp.status_code}")
            return (False, 0)

        data = resp.json() if resp.content else {}
        total = data.get("total", 0)
        is_valid = total > 0
        if is_valid:
            logger.debug(f"Workday validated: {tenant}/{site_name} -> {total} jobs")
        return (is_valid, total)

    except httpx.TimeoutException:
        logger.debug(f"Workday validation timeout: {tenant}/{site_name}")
        return (False, 0)
    except Exception as e:
        logger.debug(f"Workday validation error: {tenant}/{site_name} — {e}")
        return (False, 0)


def _candidate_urls_for_domain(domain: str) -> List[str]:
    d = (domain or "").strip()
    if not d:
        return []
    # Keep this list short to avoid hammering. Root scan can yield a careers link.
    return [
        f"https://{d}/careers",
        f"https://careers.{d}",
        f"https://jobs.{d}",
        f"https://{d}/jobs",
        f"https://{d}",
    ]


async def discover_company_ats(
    client: httpx.AsyncClient,
    company: str,
    platforms: Set[str],
    http_limiter: _RateLimiter,
    clearbit_limiter: _RateLimiter,
    treat_input_as_domain: bool = False,
    known_tokens: Optional[Dict[str, Any]] = None,
) -> CompanyDiscovery:
    """
    Discover ATS tokens for a company.

    Args:
        known_tokens: Optional dict with structure {"overrides": {"Company": {"platform": "token_or_null"}}}
                     If provided, checks known overrides before HTML scraping.
    """
    urls_scanned: List[str] = []
    found: Dict[str, Set[str]] = {}

    # Check known_tokens first (Phase 1.5.3)
    if known_tokens and not treat_input_as_domain:
        overrides = known_tokens.get("overrides", {})
        if company in overrides:
            company_overrides = overrides[company]
            logger.debug(f"Found known token overrides for {company}: {company_overrides}")

            for platform in platforms:
                if platform in company_overrides:
                    token = company_overrides[platform]
                    if token is None:
                        # null means skip this platform
                        logger.debug(f"Skipping {platform} for {company} (null override)")
                    elif isinstance(token, str) and token.strip():
                        # Use the known token
                        found.setdefault(platform, set()).add(token.strip().lower())
                        logger.info(f"Using known token for {company}/{platform}: {token}")

            # If we have overrides for all requested platforms, return early
            if all(platform in company_overrides for platform in platforms):
                found_lists = {k: sorted(list(v)) for k, v in found.items()}
                return CompanyDiscovery(
                    company=company,
                    domain=None,  # Don't need domain if using known tokens
                    urls_scanned=urls_scanned,
                    found=found_lists
                )

    # Proceed with domain resolution and HTML scraping for platforms not in overrides
    domain = company if treat_input_as_domain else await resolve_domain_via_clearbit(client, company, clearbit_limiter)

    if not domain:
        return CompanyDiscovery(company=company, domain=None, urls_scanned=urls_scanned, found={})

    candidates = _candidate_urls_for_domain(domain)
    for url in candidates:
        if len(urls_scanned) >= 8:
            break
        html_text = await _fetch_html(client, url, http_limiter)
        urls_scanned.append(url)
        if not html_text:
            continue
        tokens = extract_ats_tokens(html_text, platforms)
        for platform, items in tokens.items():
            found.setdefault(platform, set()).update(items)

        # If we didn't find anything on root, try following a couple careers links.
        if not found and url.rstrip("/") == f"https://{domain}".rstrip("/"):
            for extra in _extract_candidate_links(html_text, url, limit=3):
                if len(urls_scanned) >= 8:
                    break
                extra_html = await _fetch_html(client, extra, http_limiter)
                urls_scanned.append(extra)
                if not extra_html:
                    continue
                tokens2 = extract_ats_tokens(extra_html, platforms)
                for platform, items in tokens2.items():
                    found.setdefault(platform, set()).update(items)

        if found:
            # Found at least one ATS; stop early for this company.
            break

    found_lists = {k: sorted(list(v)) for k, v in found.items()}
    return CompanyDiscovery(company=company, domain=domain, urls_scanned=urls_scanned, found=found_lists)


async def discover_ats_targets(
    companies_or_domains: Iterable[str],
    platforms: Optional[Set[str]] = None,
    max_companies: int = 200,
    concurrency: int = 8,
    http_requests_per_minute: int = 120,
    clearbit_requests_per_minute: int = 60,
    treat_input_as_domain: bool = False,
    known_tokens: Optional[Dict[str, Any]] = None,
    validate: bool = True,
    min_jobs: int = 1,
) -> Tuple[Dict[str, List[str]], List[CompanyDiscovery], Dict[str, Any]]:
    """
    Discover ATS targets from a list of company names/domains.

    Args:
        companies_or_domains: List of company names or domains
        platforms: Set of platforms to discover (greenhouse, lever, smartrecruiters)
        max_companies: Maximum companies to process
        concurrency: Parallelism level
        http_requests_per_minute: HTTP rate limit
        clearbit_requests_per_minute: Clearbit API rate limit
        treat_input_as_domain: If True, skip Clearbit lookup
        known_tokens: Dict with known token overrides
        validate: If True, validate tokens return jobs
        min_jobs: Minimum job count to consider token valid

    Returns:
        Tuple of:
        - config-ish dict: {greenhouse: [...], lever: [...], ...}
        - per-company details (for auditing)
        - metadata dict for ats_discovery_log.yaml
    """
    from datetime import datetime, timezone

    start_time = datetime.now(timezone.utc)

    platforms = platforms or {"greenhouse", "lever", "smartrecruiters"}
    platforms = {p.strip().lower() for p in platforms if p and str(p).strip()}
    valid_platforms = set(_ATS_PATTERNS.keys())
    platforms = {p for p in platforms if p in valid_platforms}
    if not platforms:
        platforms = {"greenhouse", "lever", "smartrecruiters"}

    items = [str(c).strip() for c in companies_or_domains if str(c).strip()]
    if max_companies and max_companies > 0:
        items = items[: int(max_companies)]

    http_limiter = _RateLimiter(http_requests_per_minute)
    clearbit_limiter = _RateLimiter(clearbit_requests_per_minute)
    validation_limiter = _RateLimiter(http_requests_per_minute)  # Reuse same rate limit
    sem = asyncio.Semaphore(max(1, int(concurrency or 8)))

    async with httpx.AsyncClient(timeout=20.0) as client:

        async def _run_one(name: str) -> CompanyDiscovery:
            async with sem:
                discovery = await discover_company_ats(
                    client,
                    company=name,
                    platforms=platforms,
                    http_limiter=http_limiter,
                    clearbit_limiter=clearbit_limiter,
                    treat_input_as_domain=treat_input_as_domain,
                    known_tokens=known_tokens,
                )

                # Validate tokens if requested
                if validate and discovery.found:
                    validation_results = {}
                    for platform, tokens in discovery.found.items():
                        for token in tokens:
                            is_valid, job_count = await validate_token(
                                client, platform, token, validation_limiter
                            )
                            validation_results[platform] = {
                                "token": token,
                                "job_count": job_count,
                                "validated": is_valid and job_count >= min_jobs,
                            }
                    discovery.validation = validation_results

                return discovery

        results = await asyncio.gather(*[_run_one(x) for x in items])

    # Aggregate across companies, only including validated tokens if validation is enabled
    aggregated: Dict[str, Set[str]] = {p: set() for p in platforms}
    tokens_found = 0
    tokens_validated = 0

    for r in results:
        for platform, tokens in (r.found or {}).items():
            for token in tokens:
                tokens_found += 1
                # Only include if validation passed OR validation is disabled
                if validate:
                    if r.validation and platform in r.validation:
                        if r.validation[platform].get("validated", False):
                            aggregated.setdefault(platform, set()).add(token.lower())
                            tokens_validated += 1
                else:
                    aggregated.setdefault(platform, set()).add(token.lower())
                    tokens_validated += 1

    config_targets: Dict[str, List[str]] = {k: sorted(list(v)) for k, v in aggregated.items() if v}

    # Build metadata
    end_time = datetime.now(timezone.utc)
    metadata = {
        "discovery_run": {
            "timestamp": start_time.isoformat(),
            "companies_processed": len(items),
            "tokens_found": tokens_found,
            "tokens_validated": tokens_validated,
            "validation_enabled": validate,
            "min_jobs_threshold": min_jobs if validate else None,
        },
        "by_company": {}
    }

    for r in results:
        if not r.found:
            continue

        company_key = r.company.lower().replace(" ", "_").replace("&", "and")
        company_key = re.sub(r'[^a-z0-9_]', '', company_key)

        metadata["by_company"][company_key] = {
            "source_company": r.company,
            "discovered_at": start_time.isoformat(),
            "domain": r.domain,
            "platforms": {}
        }

        for platform, tokens in r.found.items():
            for token in tokens:
                validation_info = {}
                if r.validation and platform in r.validation:
                    val = r.validation[platform]
                    validation_info = {
                        "token": val.get("token", token),
                        "job_count": val.get("job_count", 0),
                        "validated": val.get("validated", False),
                    }
                    # Check if this came from known_tokens
                    if known_tokens and not r.urls_scanned:
                        validation_info["note"] = "from known_tokens.yaml override"
                else:
                    validation_info = {
                        "token": token,
                        "job_count": None,
                        "validated": None,
                    }

                metadata["by_company"][company_key]["platforms"][platform] = validation_info

    return config_targets, results, metadata

