"""
Vision-based parser for extracting job listings from career page screenshots.

Sends a full-page PNG screenshot to Groq's Llama 4 Scout vision model
and extracts structured job data in one API call.  This handles SPAs where
the HTML has no readable text but the rendered page shows job listings.

Reuses validation helpers from the text-based LLM parser (llm.py).
"""
import base64
import dataclasses
import logging
from typing import Dict, List, Optional

from .llm import LLMParseError, _extract_json, _jobs_from_response, _resolve_config
from ..types import RawScrapedJob

logger = logging.getLogger(__name__)

_DEFAULT_VISION_MODEL = "meta-llama/llama-4-scout-17b-16e-instruct"
_DEFAULT_VISION_TIMEOUT = 45
_VISION_CACHE_TTL = 6 * 3600  # 6 hours

_VISION_SYSTEM_PROMPT = (
    "You are a precise data extractor. You will see a screenshot of a company "
    "careers page. Extract all visible job listings. Return ONLY a valid JSON array. "
    'Each element: {"title": string, "url": string, "location": string|null}. '
    "Use exact URLs visible on screen. If only job titles are visible without URLs, "
    "use null for the url field. If no job listings are visible, return []."
)


async def parse_with_vision(
    screenshot_png: bytes,
    base_url: str,
    original_html: str,
    llm_config: Optional[Dict] = None,
) -> List[RawScrapedJob]:
    """Extract job listings from a career page screenshot using vision LLM.

    Args:
        screenshot_png: Raw PNG bytes of the full-page screenshot.
        base_url: The URL the page was fetched from (used for URL resolution).
        original_html: The rendered HTML (page.content()) — used by the
            hallucination guard to validate extracted URLs.
        llm_config: Optional config dict (from ``Config().llm_parser``).

    Returns:
        List of validated RawScrapedJob objects.

    Raises:
        LLMParseError: If the vision call fails or returns unparseable output.
    """
    from ...cache import get_cache, set_cache, content_key

    try:
        from groq import AsyncGroq
    except ImportError:
        raise LLMParseError("groq package is not installed (pip install groq)")

    rc = _resolve_config(llm_config)
    cfg = llm_config or {}

    api_key = rc["groq_api_key"]
    if not api_key:
        raise LLMParseError("No Groq API key configured for vision parser")

    model = cfg.get("vision_model") or _DEFAULT_VISION_MODEL
    timeout = float(cfg.get("vision_timeout", _DEFAULT_VISION_TIMEOUT))

    # Check Redis cache before calling vision LLM
    cache_k = content_key("vision", base_url, screenshot_png)
    cached = await get_cache(cache_k)
    if cached is not None:
        logger.debug("Vision cache hit for %s (%d jobs)", base_url, len(cached))
        return [RawScrapedJob(**item) for item in cached]

    # Base64-encode the screenshot for the data URI
    b64_image = base64.b64encode(screenshot_png).decode("ascii")
    data_uri = f"data:image/png;base64,{b64_image}"

    client = AsyncGroq(api_key=api_key, timeout=timeout)
    response = await client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": _VISION_SYSTEM_PROMPT},
            {
                "role": "user",
                "content": [
                    {
                        "type": "image_url",
                        "image_url": {"url": data_uri},
                    },
                    {
                        "type": "text",
                        "text": (
                            f"Base URL: {base_url}\n\n"
                            "Extract all job listings visible in this screenshot. "
                            "Return the JSON array now:"
                        ),
                    },
                ],
            },
        ],
        temperature=0,
        max_tokens=4096,
    )

    raw_text = response.choices[0].message.content
    logger.debug("Vision model raw response for %s: %s", base_url, raw_text[:500])

    # Reuse the same JSON extraction + hallucination guard as the text LLM parser
    jobs = _jobs_from_response(raw_text, base_url, original_html)
    if jobs:
        await set_cache(cache_k, [dataclasses.asdict(j) for j in jobs], _VISION_CACHE_TTL)
    return jobs
