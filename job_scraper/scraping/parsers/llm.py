"""
LLM-based parser for extracting job listings from HTML.

Primary provider: Groq (llama-3.1-8b-instant) — fast, generous free tier.
Fallback provider: HuggingFace Inference API (Qwen/Qwen2.5-7B-Instruct).

HTML is preprocessed with BeautifulSoup before sending to the LLM:
  - Strips <script>, <style>, <nav>, <footer>, <header>, <noscript>, <svg>
  - Converts to plain text with a ~12 000 char limit (~3 000 tokens)

URL hallucination guard: every URL returned by the LLM must appear verbatim
in the original HTML source; any invented URL is silently dropped.
"""
import json
import logging
import os
import re
from typing import List, Optional
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from .css import _is_valid_job, _dedupe_jobs
from ..types import RawScrapedJob

logger = logging.getLogger(__name__)

# Module-level config read once at import time.
_GROQ_API_KEY: Optional[str] = os.getenv("GROQ_API_KEY")
_HF_API_KEY: Optional[str] = os.getenv("HF_API_KEY")
_GROQ_MODEL: str = os.getenv("LLM_PARSER_GROQ_MODEL", "llama-3.1-8b-instant")
_HF_MODEL: str = os.getenv("LLM_PARSER_HF_MODEL", "Qwen/Qwen2.5-7B-Instruct")

_MAX_HTML_CHARS = 12_000  # ~3 000 tokens — fits all 7B model context windows


class LLMParseError(Exception):
    """Raised when all LLM providers fail or return no jobs."""
    pass


# ---------------------------------------------------------------------------
# HTML preprocessing
# ---------------------------------------------------------------------------

def _preprocess_html(html: str) -> str:
    """Strip noise tags, collapse whitespace, truncate to token budget."""
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup.find_all(["script", "style", "nav", "footer", "header", "noscript", "svg", "img"]):
        tag.decompose()
    text = soup.get_text(separator="\n", strip=True)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text[:_MAX_HTML_CHARS]


# ---------------------------------------------------------------------------
# Prompt + response parsing
# ---------------------------------------------------------------------------

_SYSTEM_PROMPT = (
    "You are a precise data extractor. "
    "Extract job listings from careers page text. "
    "Return ONLY a valid JSON array — no markdown, no explanation. "
    'Each element: {"title": string, "url": string, "location": string|null}. '
    "Only include real job postings. Never invent URLs."
)


def _build_user_prompt(text: str, base_url: str) -> str:
    return (
        f"Base URL: {base_url}\n\n"
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
    """
    Parse LLM response text into validated RawScrapedJob objects.

    Applies two guards:
    - URL hallucination guard: path segment must appear in original HTML.
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
        if parsed_url.path and parsed_url.path not in original_html and url not in original_html:
            logger.debug("LLM hallucinated URL, dropping: %s", url)
            continue

        job = RawScrapedJob(title=title, url=url, location=location)
        if _is_valid_job(job, base_url):
            jobs.append(job)

    return _dedupe_jobs(jobs)


# ---------------------------------------------------------------------------
# Provider implementations
# ---------------------------------------------------------------------------

async def _call_groq(text: str, base_url: str) -> str:
    from groq import AsyncGroq
    client = AsyncGroq(api_key=_GROQ_API_KEY)
    response = await client.chat.completions.create(
        model=_GROQ_MODEL,
        messages=[
            {"role": "system", "content": _SYSTEM_PROMPT},
            {"role": "user", "content": _build_user_prompt(text, base_url)},
        ],
        temperature=0,
        max_tokens=4096,
    )
    return response.choices[0].message.content


async def _call_hf(text: str, base_url: str) -> str:
    from huggingface_hub import AsyncInferenceClient
    client = AsyncInferenceClient(model=_HF_MODEL, token=_HF_API_KEY)
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

async def parse_with_llm(html: str, base_url: str) -> List[RawScrapedJob]:
    """
    Extract job listings from HTML using an LLM.

    Provider waterfall:
      1. Groq (if GROQ_API_KEY is set)
      2. HuggingFace Inference API (if HF_API_KEY is set)

    Raises LLMParseError if no provider is configured or all fail.
    """
    if not _GROQ_API_KEY and not _HF_API_KEY:
        raise LLMParseError("No LLM provider configured (set GROQ_API_KEY or HF_API_KEY)")

    cleaned = _preprocess_html(html)

    if _GROQ_API_KEY:
        try:
            raw = await _call_groq(cleaned, base_url)
            jobs = _jobs_from_response(raw, base_url, html)
            if jobs:
                logger.info("Groq extracted %d jobs from %s", len(jobs), base_url)
                return jobs
            logger.debug("Groq returned 0 valid jobs for %s, trying HF", base_url)
        except LLMParseError:
            raise
        except Exception as exc:
            logger.warning("Groq call failed for %s: %s — trying HF", base_url, exc)

    if _HF_API_KEY:
        raw = await _call_hf(cleaned, base_url)
        jobs = _jobs_from_response(raw, base_url, html)
        logger.info("HF extracted %d jobs from %s", len(jobs), base_url)
        return jobs

    raise LLMParseError("All LLM providers failed")
