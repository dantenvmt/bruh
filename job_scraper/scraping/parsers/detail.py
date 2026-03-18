"""
LLM-based parser for extracting details from a single job posting page.

Given the HTML of an individual job detail page, extracts:
  - description (plain text, truncated to ~2000 chars)
  - location
  - salary

Tries deterministic extraction first (JSON-LD, meta tags, CSS heuristics),
falling back to Groq/HF LLM only when structured data is insufficient.
When deterministic extraction produces a low-quality description, uses
LLM in "cleanup mode" to rewrite it from the raw HTML context.
"""
import asyncio
import hashlib
import json
import logging
import re
from typing import TYPE_CHECKING, Dict, Optional

from .llm import _preprocess_html, _resolve_config

if TYPE_CHECKING:
    from ..scraper import EnrichmentBudget

logger = logging.getLogger(__name__)

_DETAIL_CACHE_TTL = 6 * 3600  # 6 hours
_DETAIL_PARSER_VERSION = "v2"
_MAX_DESCRIPTION_CHARS = 2000

_DETAIL_SYSTEM_PROMPT = (
    "You are a precise data extractor. "
    "Extract the job description, location, and salary from this job posting page. "
    "Return ONLY a valid JSON object -- no markdown fences, no explanation.\n"
    'Format: {"description": string|null, "location": string|null, "salary": string|null}\n'
    "For description: provide a plain-text summary of the role (responsibilities, requirements, qualifications). "
    "Keep it concise — max 2000 characters. "
    "For salary: include the full compensation string if present (e.g. '$120k-$150k/year'). "
    "Return null for any field not found on the page."
)

_DETAIL_CLEANUP_SYSTEM_PROMPT = (
    "You are a precise data extractor. "
    "You are given a partially extracted job description that is low quality "
    "(jumbled text, boilerplate, HTML artifacts, or too short). "
    "Using the raw page content as context, rewrite the job description as clean plain text. "
    "Include: role summary, responsibilities, requirements, and qualifications. "
    "Return ONLY a valid JSON object -- no markdown fences, no explanation.\n"
    'Format: {"description": string|null}\n'
    "Keep it concise — max 2000 characters. Return null if no real job description is found."
)

# Boilerplate prefixes that indicate nav/cookie/auth text, not job content
_BOILERPLATE_PREFIXES = (
    "apply", "sign in", "sign up", "log in", "cookie", "menu",
    "navigation", "accept", "we use cookies", "privacy",
    "terms of", "subscribe", "follow us", "share this",
)


# ---------------------------------------------------------------------------
# Quality validation
# ---------------------------------------------------------------------------

def _description_needs_cleanup(desc: Optional[str]) -> bool:
    """Return True when the description is missing or too low quality to use.

    Checks for:
    - None or very short (< 100 chars)
    - Too few words (< 15)
    - Starts with boilerplate (nav text, cookie banners, auth prompts)
    - Excessive HTML artifacts
    """
    if not desc:
        return True

    desc = desc.strip()
    if len(desc) < 100:
        return True

    words = desc.split()
    if len(words) < 15:
        return True

    # Boilerplate prefix check
    lower = desc.lower().lstrip()
    if any(lower.startswith(bp) for bp in _BOILERPLATE_PREFIXES):
        return True

    # Excessive HTML artifacts
    artifact_count = sum(
        desc.count(art) for art in ("&amp;", "&nbsp;", "&#", "\\n", "&lt;", "&gt;")
    )
    if artifact_count > 3:
        return True

    return False


def _llm_output_acceptable(desc: Optional[str]) -> bool:
    """Validate LLM output before accepting it as a description.

    Rejects boilerplate-heavy, too-short, or nav/legal fragment outputs.
    """
    if not desc:
        return False

    desc = desc.strip()
    if len(desc) < 80:
        return False

    words = desc.split()
    if len(words) < 12:
        return False

    lower = desc.lower().lstrip()
    if any(lower.startswith(bp) for bp in _BOILERPLATE_PREFIXES):
        return False

    # Strip leading nav/legal fragments
    legal_patterns = [
        r"^(cookie policy|privacy policy|terms of service|terms and conditions)[.\s]*",
    ]
    for pat in legal_patterns:
        if re.match(pat, lower):
            return False

    return True


# ---------------------------------------------------------------------------
# LLM rate limiter
# ---------------------------------------------------------------------------

class _LLMRateLimiter:
    """Simple async rate limiter for LLM calls (token-bucket, 1 token)."""

    def __init__(self, calls_per_minute: int = 25):
        self._interval = 60.0 / calls_per_minute
        self._lock = asyncio.Lock()
        self._last_call = 0.0

    async def acquire(self):
        async with self._lock:
            loop = asyncio.get_event_loop()
            now = loop.time()
            wait = self._last_call + self._interval - now
            if wait > 0:
                await asyncio.sleep(wait)
            self._last_call = asyncio.get_event_loop().time()


# ---------------------------------------------------------------------------
# Deterministic extraction (no LLM needed)
# ---------------------------------------------------------------------------

def _format_base_salary(salary_obj) -> Optional[str]:
    """Format a Schema.org baseSalary object into a human-readable string."""
    if isinstance(salary_obj, str) and salary_obj.strip():
        return salary_obj.strip()
    if not isinstance(salary_obj, dict):
        return None

    value = salary_obj.get("value")
    currency = salary_obj.get("currency", "USD")

    if isinstance(value, (int, float)):
        return f"${value:,.0f}/{_salary_unit(salary_obj)}"

    if isinstance(value, dict):
        min_val = value.get("minValue")
        max_val = value.get("maxValue")
        unit = _salary_unit(value) or _salary_unit(salary_obj)
        if min_val is not None and max_val is not None:
            return f"${min_val:,.0f} - ${max_val:,.0f}/{unit}"
        if min_val is not None:
            return f"${min_val:,.0f}+/{unit}"
        if max_val is not None:
            return f"Up to ${max_val:,.0f}/{unit}"

    return None


def _salary_unit(obj: dict) -> str:
    """Extract salary unit period from a Schema.org salary object."""
    unit = obj.get("unitText", "")
    if isinstance(unit, str) and unit.strip():
        return unit.strip().lower()
    return "year"


def _extract_deterministic(html: str, url: str) -> Dict[str, Optional[str]]:
    """Try to extract detail fields without LLM, using structured data and heuristics.

    Returns a dict with description/location/salary (any may be None).
    """
    from bs4 import BeautifulSoup
    from .structured_data import _extract_jsonld_blocks, _find_job_postings, _extract_location

    result: Dict[str, Optional[str]] = {
        "description": None,
        "location": None,
        "salary": None,
    }

    # --- (a) JSON-LD ---
    blocks = _extract_jsonld_blocks(html)
    postings = _find_job_postings(blocks) if blocks else []

    if postings:
        posting = postings[0]  # detail pages typically have one JobPosting

        # Description
        desc = posting.get("description")
        if isinstance(desc, str) and desc.strip():
            # Strip HTML tags that are common in JSON-LD description fields
            soup = BeautifulSoup(desc, "html.parser")
            plain = soup.get_text(separator=" ", strip=True)
            if plain:
                result["description"] = plain[:_MAX_DESCRIPTION_CHARS]

        # Location
        result["location"] = _extract_location(posting)

        # Salary
        result["salary"] = _format_base_salary(posting.get("baseSalary"))

    # --- (b) Meta tags (description fallback) ---
    if not result["description"]:
        soup = BeautifulSoup(html, "html.parser")

        og_desc = soup.find("meta", attrs={"property": "og:description"})
        if og_desc and og_desc.get("content", "").strip():
            result["description"] = og_desc["content"].strip()[:_MAX_DESCRIPTION_CHARS]

        if not result["description"]:
            meta_desc = soup.find("meta", attrs={"name": "description"})
            if meta_desc and meta_desc.get("content", "").strip():
                result["description"] = meta_desc["content"].strip()[:_MAX_DESCRIPTION_CHARS]

    # --- (c) Body heuristics ---
    if not result["description"]:
        soup = BeautifulSoup(html, "html.parser") if "soup" not in dir() else soup
        # Re-parse if we didn't enter the meta-tag branch (soup not yet created)
        try:
            soup  # noqa: B018
        except NameError:
            soup = BeautifulSoup(html, "html.parser")

        desc_selectors = [
            "[class*=description]",
            "[class*=job-detail]",
            "[id*=description]",
            "[class*=job_description]",
        ]
        for sel in desc_selectors:
            el = soup.select_one(sel)
            if el:
                text = el.get_text(separator=" ", strip=True)
                if len(text) > 50:
                    result["description"] = text[:_MAX_DESCRIPTION_CHARS]
                    break

    if not result["salary"]:
        try:
            soup  # noqa: B018
        except NameError:
            soup = BeautifulSoup(html, "html.parser")

        salary_selectors = ["[class*=salary]", "[class*=compensation]"]
        for sel in salary_selectors:
            el = soup.select_one(sel)
            if el:
                text = el.get_text(separator=" ", strip=True)
                if text:
                    result["salary"] = text[:255]
                    break

    if not result["location"]:
        try:
            soup  # noqa: B018
        except NameError:
            soup = BeautifulSoup(html, "html.parser")

        loc_selectors = ["[class*=location]"]
        for sel in loc_selectors:
            el = soup.select_one(sel)
            if el:
                text = el.get_text(separator=" ", strip=True)
                if text:
                    result["location"] = text[:255]
                    break

    return result


# ---------------------------------------------------------------------------
# LLM helpers
# ---------------------------------------------------------------------------

def _build_detail_prompt(text: str, url: str) -> str:
    return (
        f"Job posting URL: {url}\n\n"
        f"Page content:\n{text}\n\n"
        "Return the JSON object now:"
    )


def _build_cleanup_prompt(partial_desc: str, url: str, html_text: str) -> str:
    return (
        f"Job posting URL: {url}\n\n"
        f"Partially extracted description (low quality):\n{partial_desc[:500]}\n\n"
        f"Raw page content:\n{html_text}\n\n"
        "Rewrite the job description as clean plain text. Return the JSON object now:"
    )


def _parse_detail_response(raw: str) -> Dict[str, Optional[str]]:
    """Parse LLM response into a detail dict."""
    raw = raw.strip()
    fenced = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", raw, re.DOTALL)
    if fenced:
        raw = fenced.group(1)
    brace = re.search(r"\{.*\}", raw, re.DOTALL)
    if brace:
        raw = brace.group(0)

    try:
        data = json.loads(raw)
    except (json.JSONDecodeError, ValueError):
        logger.debug("Failed to parse detail LLM response: %s", raw[:200])
        return {"description": None, "location": None, "salary": None}

    if not isinstance(data, dict):
        return {"description": None, "location": None, "salary": None}

    desc = data.get("description")
    if isinstance(desc, str) and desc.strip():
        desc = desc.strip()[:_MAX_DESCRIPTION_CHARS]
    else:
        desc = None

    location = data.get("location")
    if isinstance(location, str) and location.strip():
        location = location.strip()
    else:
        location = None

    salary = data.get("salary")
    if isinstance(salary, str) and salary.strip():
        salary = salary.strip()
    else:
        salary = None

    return {"description": desc, "location": location, "salary": salary}


async def _call_groq_detail(
    text: str, url: str, *, api_key: str, model: str, timeout: float,
    system_prompt: Optional[str] = None, user_prompt: Optional[str] = None,
) -> str:
    """Call Groq with the detail extraction or cleanup prompt."""
    try:
        from groq import AsyncGroq
    except ImportError:
        raise RuntimeError("groq package is not installed")
    client = AsyncGroq(api_key=api_key, timeout=timeout)
    response = await client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": system_prompt or _DETAIL_SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt or _build_detail_prompt(text, url)},
        ],
        temperature=0,
        max_tokens=2048,
    )
    return response.choices[0].message.content


async def _call_hf_detail(
    text: str, url: str, *, api_key: str, model: str, timeout: float,
    system_prompt: Optional[str] = None, user_prompt: Optional[str] = None,
) -> str:
    """Call HuggingFace with the detail extraction or cleanup prompt."""
    try:
        from huggingface_hub import AsyncInferenceClient
    except ImportError:
        raise RuntimeError("huggingface_hub package is not installed")
    client = AsyncInferenceClient(model=model, token=api_key, timeout=timeout)
    response = await client.chat_completion(
        messages=[
            {"role": "system", "content": system_prompt or _DETAIL_SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt or _build_detail_prompt(text, url)},
        ],
        temperature=0,
        max_tokens=2048,
    )
    return response.choices[0].message.content


# ---------------------------------------------------------------------------
# Cache key helpers
# ---------------------------------------------------------------------------

def _content_fingerprint(html: str) -> str:
    """Stable short hash from preprocessed text for cache key consistency."""
    cleaned = _preprocess_html(html, max_chars=5000)
    return hashlib.md5(cleaned.encode("utf-8", errors="replace")).hexdigest()[:8]


def _detail_cache_key(url: str, html: str) -> str:
    """Build a versioned cache key with content fingerprint."""
    from ...cache import content_key
    fp = _content_fingerprint(html)
    return content_key(_DETAIL_PARSER_VERSION, f"{url}:{fp}")


# ---------------------------------------------------------------------------
# LLM call dispatcher (extraction or cleanup mode)
# ---------------------------------------------------------------------------

async def _run_llm(
    cleaned_html: str,
    url: str,
    rc: dict,
    rate_limiter: Optional[_LLMRateLimiter],
    budget: Optional["EnrichmentBudget"],
    *,
    system_prompt: Optional[str] = None,
    user_prompt: Optional[str] = None,
    is_cleanup: bool = False,
) -> Optional[Dict[str, Optional[str]]]:
    """Run LLM extraction/cleanup with budget check, rate limiting, and provider fallback.

    Returns parsed detail dict or None if all providers fail or budget exhausted.
    """
    # Budget check
    if budget:
        if not await budget.try_acquire_llm():
            return None
        if is_cleanup:
            await budget.record_llm_cleanup()

    groq_key = rc["groq_api_key"]
    hf_key = rc["hf_api_key"]

    if not groq_key and not hf_key:
        return None

    # Rate limit
    if rate_limiter:
        await rate_limiter.acquire()

    llm_result = None

    if groq_key:
        try:
            raw = await _call_groq_detail(
                cleaned_html, url,
                api_key=groq_key, model=rc["groq_model"], timeout=rc["timeout"],
                system_prompt=system_prompt, user_prompt=user_prompt,
            )
            llm_result = _parse_detail_response(raw)
        except Exception as exc:
            logger.debug("Detail Groq failed for %s: %s", url, exc)

    if (not llm_result or not llm_result.get("description")) and hf_key:
        if rate_limiter and llm_result is None:
            await rate_limiter.acquire()
        try:
            raw = await _call_hf_detail(
                cleaned_html, url,
                api_key=hf_key, model=rc["hf_model"], timeout=rc["timeout"],
                system_prompt=system_prompt, user_prompt=user_prompt,
            )
            llm_result = _parse_detail_response(raw)
        except Exception as exc:
            logger.debug("Detail HF failed for %s: %s", url, exc)

    return llm_result


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

async def extract_job_detail(
    html: str,
    url: str,
    llm_config: Optional[Dict] = None,
    rate_limiter: Optional[_LLMRateLimiter] = None,
    budget: Optional["EnrichmentBudget"] = None,
) -> Dict[str, Optional[str]]:
    """Extract description, location, and salary from a job detail page.

    Flow:
    1. Check cache (versioned key with content fingerprint)
    2. Deterministic extraction (JSON-LD, meta tags, heuristics)
    3. If description passes quality check → cache and return (no LLM)
    4. If description is None → LLM extraction mode
    5. If description exists but fails quality → LLM cleanup mode
       (only overrides description; deterministic location/salary stay)
    6. Validate LLM output before accepting

    Args:
        html: Raw HTML of the job detail page.
        url: The URL the HTML was fetched from.
        llm_config: LLM provider config (same format as listing parser).
        rate_limiter: Optional LLM rate limiter instance.
        budget: Optional EnrichmentBudget for concurrency-safe budget tracking.

    Returns:
        Dict with keys: description, location, salary (any may be None).
    """
    from ...cache import get_cache, set_cache

    empty = {"description": None, "location": None, "salary": None}

    if not html or len(html.strip()) < 50:
        return empty

    # 1. Check Redis cache (versioned + content fingerprint)
    cache_k = _detail_cache_key(url, html)
    cached = await get_cache(cache_k)
    if cached is not None:
        logger.debug("Detail cache hit for %s", url)
        return cached

    # 2. Deterministic extraction
    det = _extract_deterministic(html, url)

    # 3. Quality gate: good description → cache and return
    if det["description"] and not _description_needs_cleanup(det["description"]):
        logger.debug("Deterministic extraction succeeded for %s", url)
        await set_cache(cache_k, det, _DETAIL_CACHE_TTL)
        return det

    # 4/5. Need LLM — either extraction (no desc) or cleanup (bad desc)
    rc = _resolve_config(llm_config)
    groq_key = rc["groq_api_key"]
    hf_key = rc["hf_api_key"]

    if not groq_key and not hf_key:
        # No LLM keys — return whatever deterministic found
        if any(v for v in det.values()):
            await set_cache(cache_k, det, _DETAIL_CACHE_TTL)
        return det

    cleaned = _preprocess_html(html, max_chars=30_000)
    if len(cleaned) < 50:
        return det

    is_cleanup = det["description"] is not None  # has desc but it's low quality

    if is_cleanup:
        # Cleanup mode: send partial desc + raw HTML, ask for clean description only
        llm_result = await _run_llm(
            cleaned, url, rc, rate_limiter, budget,
            system_prompt=_DETAIL_CLEANUP_SYSTEM_PROMPT,
            user_prompt=_build_cleanup_prompt(det["description"], url, cleaned),
            is_cleanup=True,
        )
    else:
        # Extraction mode: no description at all
        llm_result = await _run_llm(
            cleaned, url, rc, rate_limiter, budget,
        )

    # Merge results — deterministic location/salary are always authoritative
    merged = dict(det)
    if llm_result:
        llm_desc = llm_result.get("description")
        if llm_desc and _llm_output_acceptable(llm_desc):
            merged["description"] = llm_desc

        # Only fill empty location/salary from extraction mode (not cleanup)
        if not is_cleanup:
            if not merged["location"] and llm_result.get("location"):
                merged["location"] = llm_result["location"]
            if not merged["salary"] and llm_result.get("salary"):
                merged["salary"] = llm_result["salary"]

    if merged["description"]:
        await set_cache(cache_k, merged, _DETAIL_CACHE_TTL)

    return merged
