"""
Per-job structured field extraction via Groq.

Given a job title + description, extracts:
  - salary_min / salary_max  (int, annual USD, null if unknown)
  - remote                   (bool — true if remote or hybrid)
  - experience_years_min     (int, null if unknown)
  - visa_sponsorship         (bool, null if unknown — only true if explicitly stated)
  - employment_type          (full_time | part_time | contract | internship | null)
  - seniority                (intern | junior | mid | senior | staff | principal |
                               manager | director | vp | executive | null)

Returns a dict with those keys. Any field that cannot be extracted with
confidence is returned as None.

Provider: Groq llama-3.1-8b-instant, temperature=0.
"""
from __future__ import annotations

import json
import logging
import re
from typing import Optional

logger = logging.getLogger(__name__)

_MODEL = "llama-3.1-8b-instant"
_TIMEOUT = 30.0
_MAX_DESC_CHARS = 4_000

_VALID_EMPLOYMENT_TYPES = frozenset(
    {"full_time", "part_time", "contract", "internship"}
)
_VALID_SENIORITY = frozenset(
    {
        "intern", "junior", "mid", "senior", "staff",
        "principal", "manager", "director", "vp", "executive",
    }
)

_SYSTEM_PROMPT = """\
You are a structured data extractor for job postings.
Given a job title and description, extract these fields and return ONLY a valid \
JSON object — no markdown fences, no explanation.

Fields to extract:
- salary_min: integer annual USD (e.g. 90000), null if not stated
- salary_max: integer annual USD (e.g. 130000), null if not stated
- remote: true if the role is remote or hybrid, false if strictly on-site, null if unclear
- experience_years_min: integer minimum years of experience required, null if not stated
- visa_sponsorship: true ONLY if the posting explicitly says visa sponsorship is available, \
false ONLY if it explicitly says no sponsorship, null if not mentioned
- employment_type: one of "full_time", "part_time", "contract", "internship", null if unclear
- seniority: one of "intern", "junior", "mid", "senior", "staff", "principal", \
"manager", "director", "vp", "executive", null if unclear

Rules:
- Salary: convert hourly/monthly to annual. If only one bound is given, set the other to null.
- Do not guess or infer what is not stated. Return null when uncertain.
- Return exactly this JSON shape (no extra keys):
{"salary_min": ..., "salary_max": ..., "remote": ..., "experience_years_min": ..., \
"visa_sponsorship": ..., "employment_type": ..., "seniority": ...}
"""


def _build_user_prompt(title: str, description: str) -> str:
    desc_truncated = description[:_MAX_DESC_CHARS]
    return (
        f"Job title: {title}\n\n"
        f"Job description:\n{desc_truncated}\n\n"
        "Return the JSON object now:"
    )


def _parse_response(raw: str) -> dict:
    """Extract and validate JSON from an LLM response string."""
    raw = raw.strip()

    # Strip markdown fences if present
    fenced = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", raw, re.DOTALL)
    if fenced:
        raw = fenced.group(1)
    else:
        brace = re.search(r"\{.*\}", raw, re.DOTALL)
        if brace:
            raw = brace.group(0)

    try:
        data = json.loads(raw)
    except (json.JSONDecodeError, ValueError):
        logger.debug("field_extractor: failed to parse JSON: %.200s", raw)
        return _empty_result()

    if not isinstance(data, dict):
        return _empty_result()

    return _validate_fields(data)


def _empty_result() -> dict:
    return {
        "salary_min": None,
        "salary_max": None,
        "remote": None,
        "experience_years_min": None,
        "visa_sponsorship": None,
        "employment_type": None,
        "seniority": None,
    }


def _validate_fields(data: dict) -> dict:
    """Coerce and validate extracted fields; null out anything invalid."""
    result = _empty_result()

    # salary_min / salary_max — must be positive ints
    for field in ("salary_min", "salary_max"):
        val = data.get(field)
        if val is not None:
            try:
                v = int(val)
                if 1_000 <= v <= 10_000_000:  # sanity bounds: $1k–$10M
                    result[field] = v
            except (TypeError, ValueError):
                pass

    # remote — must be bool
    val = data.get("remote")
    if isinstance(val, bool):
        result["remote"] = val

    # experience_years_min — must be non-negative int
    val = data.get("experience_years_min")
    if val is not None:
        try:
            v = int(val)
            if 0 <= v <= 40:
                result["experience_years_min"] = v
        except (TypeError, ValueError):
            pass

    # visa_sponsorship — must be bool
    val = data.get("visa_sponsorship")
    if isinstance(val, bool):
        result["visa_sponsorship"] = val

    # employment_type — must be one of the allowed values
    val = data.get("employment_type")
    if isinstance(val, str) and val.lower() in _VALID_EMPLOYMENT_TYPES:
        result["employment_type"] = val.lower()

    # seniority — must be one of the allowed values
    val = data.get("seniority")
    if isinstance(val, str) and val.lower() in _VALID_SENIORITY:
        result["seniority"] = val.lower()

    return result


async def extract_fields(
    title: str,
    description: str,
    groq_api_key: str,
    model: str = _MODEL,
    timeout: float = _TIMEOUT,
) -> dict:
    """
    Call Groq to extract structured fields from a job title + description.

    Returns a dict with keys:
        salary_min, salary_max, remote, experience_years_min,
        visa_sponsorship, employment_type, seniority

    Any field that cannot be extracted with confidence is None.
    Raises RuntimeError if the Groq call fails (caller should catch and log).
    """
    try:
        from groq import AsyncGroq
    except ImportError:
        raise RuntimeError(
            "groq package is not installed — run: pip install groq"
        )

    if not title and not description:
        return _empty_result()

    client = AsyncGroq(api_key=groq_api_key, timeout=timeout)
    response = await client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": _SYSTEM_PROMPT},
            {"role": "user", "content": _build_user_prompt(title or "", description or "")},
        ],
        temperature=0,
        max_tokens=256,
    )
    raw = response.choices[0].message.content
    return _parse_response(raw)
