"""
LLM-based parser for extracting job listings from HTML.

Primary provider: Groq (llama-3.1-8b-instant) -- fast, generous free tier.
Fallback provider: HuggingFace Inference API (Qwen/Qwen2.5-7B-Instruct).

HTML is preprocessed with BeautifulSoup before sending to the LLM:
  - Strips <script>, <style>, <noscript>, <svg>
  - Converts links to [text](href) markdown so the LLM can see URLs
  - Truncates to a configurable char limit (default 50 000 chars)

URL hallucination guard: every URL returned by the LLM must have its path
appear in the original HTML source; any invented URL is silently dropped.
"""
import dataclasses
import json
import logging
import re
from typing import Dict, List, Optional
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup, Tag

from .css import _is_valid_job, _dedupe_jobs
from ..types import RawScrapedJob

logger = logging.getLogger(__name__)

_DEFAULT_MAX_HTML_CHARS = 50_000
_DEFAULT_GROQ_MODEL = "llama-3.1-8b-instant"
_DEFAULT_HF_MODEL = "Qwen/Qwen2.5-7B-Instruct"
_DEFAULT_TIMEOUT_SECONDS = 30

# Tags that are pure noise -- safe to remove entirely.
_NOISE_TAGS = ["script", "style", "noscript", "svg", "img"]


class LLMParseError(Exception):
    """Raised when all LLM providers fail or return no jobs."""
    pass


# ---------------------------------------------------------------------------
# HTML preprocessing
# ---------------------------------------------------------------------------

def _preprocess_html(html: str, max_chars: int = _DEFAULT_MAX_HTML_CHARS) -> str:
    """Clean HTML and convert to a compact text representation that preserves link URLs.

    Links are rendered as ``[text](href)`` so the LLM can extract real URLs.
    Other noise (scripts, styles, SVGs) is stripped.
    """
    soup = BeautifulSoup(html, "html.parser")

    for tag in soup.find_all(_NOISE_TAGS):
        tag.decompose()

    # Convert <a> tags to markdown-style links *before* extracting text,
    # so the LLM receives real URLs from the page.
    for a_tag in soup.find_all("a"):
        if not isinstance(a_tag, Tag):
            continue
        href = a_tag.get("href", "")
        text = a_tag.get_text(strip=True)
        if href and text:
            a_tag.replace_with(f"[{text}]({href})")
        elif href:
            a_tag.replace_with(f"({href})")
        else:
            a_tag.replace_with(text or "")

    text = soup.get_text(separator="\n", strip=True)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text[:max_chars]


# ---------------------------------------------------------------------------
# Prompt + response parsing
# ---------------------------------------------------------------------------

_SYSTEM_PROMPT = (
    "You are a precise data extractor. "
    "Extract job listings from the careers page content below. "
    "Return ONLY a valid JSON array -- no markdown fences, no explanation. "
    'Each element: {"title": string, "url": string, "location": string|null}. '
    "Use the exact URLs from the page content. Never invent or guess URLs."
)


def _build_user_prompt(text: str, base_url: str) -> str:
    return (
        f"Base URL (use for resolving relative links): {base_url}\n\n"
        f"Page content:\n{text}\n\n"
        "Return the JSON array of job listings now:"
    )


def _extract_json(raw: str) -> list:
    """Pull a JSON array out of an LLM response, even if wrapped in markdown."""
    raw = raw.strip()
    fenced = re.search(r"```(?:json)?\s*(\[.*?\])\s*```", raw, re.DOTALL)
    if fenced:
        raw = fenced.group(1)
    bracket = re.search(r"\[.*\]", raw, re.DOTALL)
    if bracket:
        raw = bracket.group(0)
    return json.loads(raw)


def _jobs_from_response(
    raw_response: str, base_url: str, original_html: str
) -> List[RawScrapedJob]:
    """Parse LLM response text into validated RawScrapedJob objects.

    Applies two guards:
    - URL hallucination guard: URL path must appear in original HTML.
    - Reuses css._is_valid_job() for title/URL quality filtering.
    """
    try:
        items = _extract_json(raw_response)
    except (json.JSONDecodeError, ValueError) as exc:
        raise LLMParseError(f"Failed to parse LLM JSON response: {exc}") from exc

    if not isinstance(items, list):
        raise LLMParseError("LLM returned non-list JSON")

    jobs: List[RawScrapedJob] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        title = (item.get("title") or "").strip()
        url = (item.get("url") or "").strip()
        location = item.get("location") or None
        if location:
            location = str(location).strip() or None

        if not title or not url:
            continue

        # Resolve relative URLs
        if not urlparse(url).scheme:
            url = urljoin(base_url, url)

        # Hallucination guard: URL path must appear somewhere in original HTML
        parsed_url = urlparse(url)
        path = parsed_url.path
        if path and path != "/" and path not in original_html and url not in original_html:
            logger.debug("LLM hallucinated URL, dropping: %s", url)
            continue

        job = RawScrapedJob(title=title, url=url, location=location)
        if _is_valid_job(job, base_url):
            jobs.append(job)

    return _dedupe_jobs(jobs)


# ---------------------------------------------------------------------------
# Provider implementations
# ---------------------------------------------------------------------------

async def _call_groq(
    text: str, base_url: str, *, api_key: str, model: str, timeout: float
) -> str:
    try:
        from groq import AsyncGroq
    except ImportError:
        raise LLMParseError("groq package is not installed (pip install groq)")
    client = AsyncGroq(api_key=api_key, timeout=timeout)
    response = await client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": _SYSTEM_PROMPT},
            {"role": "user", "content": _build_user_prompt(text, base_url)},
        ],
        temperature=0,
        max_tokens=4096,
    )
    return response.choices[0].message.content


async def _call_hf(
    text: str, base_url: str, *, api_key: str, model: str, timeout: float
) -> str:
    try:
        from huggingface_hub import AsyncInferenceClient
    except ImportError:
        raise LLMParseError(
            "huggingface_hub package is not installed (pip install huggingface_hub)"
        )
    client = AsyncInferenceClient(model=model, token=api_key, timeout=timeout)
    response = await client.chat_completion(
        messages=[
            {"role": "system", "content": _SYSTEM_PROMPT},
            {"role": "user", "content": _build_user_prompt(text, base_url)},
        ],
        temperature=0,
        max_tokens=4096,
    )
    return response.choices[0].message.content


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def _resolve_config(llm_config: Optional[Dict] = None) -> Dict:
    """Build a resolved config dict, preferring the passed-in config over env vars."""
    cfg = llm_config or {}
    return {
        "groq_api_key": cfg.get("groq_api_key") or None,
        "hf_api_key": cfg.get("hf_api_key") or None,
        "groq_model": cfg.get("groq_model") or _DEFAULT_GROQ_MODEL,
        "hf_model": cfg.get("hf_model") or _DEFAULT_HF_MODEL,
        "timeout": float(cfg.get("timeout", _DEFAULT_TIMEOUT_SECONDS)),
        "max_html_chars": int(cfg.get("max_html_chars", _DEFAULT_MAX_HTML_CHARS)),
    }


_LLM_CACHE_TTL = 6 * 3600  # 6 hours


async def parse_with_llm(
    html: str,
    base_url: str,
    llm_config: Optional[Dict] = None,
) -> List[RawScrapedJob]:
    """Extract job listings from HTML using an LLM.

    Args:
        html: Raw HTML of the careers page.
        base_url: The URL the HTML was fetched from.
        llm_config: Optional dict with keys ``groq_api_key``, ``hf_api_key``,
            ``groq_model``, ``hf_model``, ``timeout``, ``max_html_chars``.
            Typically ``Config().llm_parser``.

    Provider waterfall:
      1. Groq  (if groq_api_key is set)
      2. HuggingFace Inference API (if hf_api_key is set)

    Results are cached in Redis (if available) for 6 hours.

    Raises LLMParseError if no provider is configured or all fail.
    """
    from ...cache import get_cache, set_cache, content_key

    rc = _resolve_config(llm_config)
    groq_key = rc["groq_api_key"]
    hf_key = rc["hf_api_key"]

    if not groq_key and not hf_key:
        raise LLMParseError("No LLM provider configured (set groq_api_key or hf_api_key)")

    cleaned = _preprocess_html(html, max_chars=rc["max_html_chars"])

    # Check Redis cache before calling LLM
    cache_k = content_key("llm", base_url, cleaned[:500])
    cached = await get_cache(cache_k)
    if cached is not None:
        logger.debug("LLM cache hit for %s (%d jobs)", base_url, len(cached))
        return [RawScrapedJob(**item) for item in cached]

    last_error: Optional[Exception] = None

    # --- Groq ---
    if groq_key:
        try:
            raw = await _call_groq(
                cleaned, base_url,
                api_key=groq_key, model=rc["groq_model"], timeout=rc["timeout"],
            )
            jobs = _jobs_from_response(raw, base_url, html)
            if jobs:
                logger.info("Groq extracted %d jobs from %s", len(jobs), base_url)
                await set_cache(cache_k, [dataclasses.asdict(j) for j in jobs], _LLM_CACHE_TTL)
                return jobs
            logger.debug("Groq returned 0 valid jobs for %s, trying next provider", base_url)
        except LLMParseError:
            raise
        except Exception as exc:
            last_error = exc
            logger.warning("Groq call failed for %s: %s", base_url, exc)

    # --- HuggingFace ---
    if hf_key:
        try:
            raw = await _call_hf(
                cleaned, base_url,
                api_key=hf_key, model=rc["hf_model"], timeout=rc["timeout"],
            )
            jobs = _jobs_from_response(raw, base_url, html)
            if jobs:
                logger.info("HF extracted %d jobs from %s", len(jobs), base_url)
                await set_cache(cache_k, [dataclasses.asdict(j) for j in jobs], _LLM_CACHE_TTL)
                return jobs
            logger.debug("HF returned 0 valid jobs for %s", base_url)
        except LLMParseError:
            raise
        except Exception as exc:
            last_error = exc
            logger.warning("HF call failed for %s: %s", base_url, exc)

    if last_error:
        raise LLMParseError(f"All LLM providers failed: {last_error}") from last_error
    raise LLMParseError("All LLM providers returned 0 valid jobs")
