"""
Single-call job normalization pipeline.

Makes one Groq call per job to extract structured fields, salary, seniority,
visa sponsorship signal, required skills, and AI summary in one shot.
"""
from __future__ import annotations

import asyncio
import json
import logging
import re
from datetime import datetime, timezone
from typing import List, Optional

from .models import Job

logger = logging.getLogger(__name__)

_MODEL = "llama-3.1-8b-instant"
_MAX_DESC_CHARS = 4_000

_VALID_SENIORITY = frozenset({
    "intern", "junior", "mid", "senior", "staff",
    "principal", "manager", "director", "vp", "executive",
})
_VALID_EMPLOYMENT_TYPES = frozenset({
    "full_time", "part_time", "contract", "internship",
})
_VALID_WORK_MODES = frozenset({"remote", "hybrid", "onsite"})
_VALID_INDUSTRIES = frozenset({
    "Technology", "Finance", "Healthcare", "Education", "Government",
    "Legal", "Sales", "Marketing", "Retail", "Logistics", "Manufacturing", "Other",
})
_VALID_EXPERIENCE_LEVELS = frozenset({"entry", "mid", "senior", "lead"})

_SYSTEM_PROMPT = """\
You are a structured data extractor for job postings.
Given job details, extract fields and return ONLY a valid JSON object — no markdown, no explanation.

Return exactly this JSON shape:
{
  "salary_min": <int annual USD or null>,
  "salary_max": <int annual USD or null>,
  "salary_text": <string like "$120k-$150k/yr" or null>,
  "remote": <true if remote/hybrid, false if onsite, null if unknown>,
  "work_mode": <"remote"|"hybrid"|"onsite"|null>,
  "employment_type": <"full_time"|"part_time"|"contract"|"internship"|null>,
  "experience_min_years": <int 0-40 or null>,
  "seniority": <"intern"|"junior"|"mid"|"senior"|"staff"|"principal"|"manager"|"director"|"vp"|"executive"|null>,
  "visa_sponsorship": <true ONLY if explicitly offered, false ONLY if explicitly denied, null otherwise>,
  "required_skills": [<up to 10 canonical skill name strings>],
  "industry": <"Technology"|"Finance"|"Healthcare"|"Education"|"Government"|"Legal"|"Sales"|"Marketing"|"Retail"|"Logistics"|"Manufacturing"|"Other"|null>,
  "experience_level": <"entry"|"mid"|"senior"|"lead"|null>,
  "ai_summary_card": "<2-3 punchy sentences, max 15 words each, for display on a job card>",
  "ai_summary_bullets": ["<bullet, max 20 words>", ...]
}

Rules:
- Salary: convert hourly/monthly to annual. If only one bound given, set the other to null.
- required_skills: canonical names only (e.g. "Python", "React", "AWS"). Max 10.
- ai_summary_card: 2-3 sentences. Each max 15 words. No fluff.
- ai_summary_bullets: 5-7 items. Each max 20 words. Cover requirements, perks, and tech stack.
- Do not guess or infer what is not stated. Return null when uncertain.
"""


def _build_user_prompt(job: Job) -> str:
    desc = (job.description or "")[:_MAX_DESC_CHARS]
    parts = [
        f"Title: {job.title}",
        f"Company: {job.company or 'Unknown'}",
        f"Location: {job.location or 'Not specified'}",
    ]
    if job.salary:
        parts.append(f"Salary (raw): {job.salary}")
    if desc:
        parts.append(f"\nDescription:\n{desc}")
    parts.append("\nReturn the JSON object now:")
    return "\n".join(parts)


def _extract_json(raw: str) -> Optional[dict]:
    raw = raw.strip()
    fenced = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", raw, re.DOTALL)
    if fenced:
        raw = fenced.group(1)
    else:
        brace = re.search(r"\{.*\}", raw, re.DOTALL)
        if brace:
            raw = brace.group(0)
    try:
        data = json.loads(raw)
        return data if isinstance(data, dict) else None
    except (json.JSONDecodeError, ValueError):
        return None


def _validate(data: dict) -> dict:
    result: dict = {}

    # salary_min / salary_max
    for field in ("salary_min", "salary_max"):
        val = data.get(field)
        if val is not None:
            try:
                v = int(val)
                if 10_000 <= v <= 10_000_000:
                    result[field] = v
                else:
                    result[field] = None
            except (TypeError, ValueError):
                result[field] = None
        else:
            result[field] = None

    # Ensure salary_max >= salary_min when both present
    if result.get("salary_min") and result.get("salary_max"):
        if result["salary_max"] < result["salary_min"]:
            result["salary_max"] = None

    # salary_text
    val = data.get("salary_text")
    result["salary_text"] = str(val).strip() if isinstance(val, str) and val.strip() else None

    # remote
    val = data.get("remote")
    result["remote"] = val if isinstance(val, bool) else None

    # work_mode
    val = data.get("work_mode")
    result["work_mode"] = val.lower() if isinstance(val, str) and val.lower() in _VALID_WORK_MODES else None

    # employment_type
    val = data.get("employment_type")
    result["employment_type"] = val.lower() if isinstance(val, str) and val.lower() in _VALID_EMPLOYMENT_TYPES else None

    # experience_min_years
    val = data.get("experience_min_years")
    if val is not None:
        try:
            v = int(val)
            result["experience_min_years"] = v if 0 <= v <= 40 else None
        except (TypeError, ValueError):
            result["experience_min_years"] = None
    else:
        result["experience_min_years"] = None

    # seniority
    val = data.get("seniority")
    result["seniority"] = val.lower() if isinstance(val, str) and val.lower() in _VALID_SENIORITY else None

    # visa_sponsorship
    val = data.get("visa_sponsorship")
    result["visa_sponsorship"] = val if isinstance(val, bool) else None

    # required_skills
    val = data.get("required_skills")
    if isinstance(val, list):
        result["required_skills"] = [str(s) for s in val if s][:10]
    else:
        result["required_skills"] = []

    # industry
    val = data.get("industry")
    result["industry"] = val if isinstance(val, str) and val in _VALID_INDUSTRIES else None

    # experience_level
    val = data.get("experience_level")
    result["experience_level"] = val.lower() if isinstance(val, str) and val.lower() in _VALID_EXPERIENCE_LEVELS else None

    # ai_summary_card
    val = data.get("ai_summary_card")
    result["ai_summary_card"] = str(val).strip() if isinstance(val, str) and val.strip() else None

    # ai_summary_bullets
    val = data.get("ai_summary_bullets")
    if isinstance(val, list) and 5 <= len(val) <= 7:
        result["ai_summary_bullets"] = [str(b) for b in val if b]
    elif isinstance(val, list) and len(val) > 0:
        # Accept it even outside 5-7 if non-empty, just cap at 7
        bullets = [str(b) for b in val if b][:7]
        result["ai_summary_bullets"] = bullets if bullets else None
    else:
        result["ai_summary_bullets"] = None

    return result


async def normalize_job(job: Job, groq_api_key: str) -> Job:
    """Make one Groq call and populate all structured fields on the Job in-place."""
    try:
        from groq import AsyncGroq
    except ImportError:
        logger.warning("groq package not installed — skipping normalization")
        return job

    if not job.title and not job.description:
        return job

    try:
        client = AsyncGroq(api_key=groq_api_key, timeout=30.0)
        response = await client.chat.completions.create(
            model=_MODEL,
            messages=[
                {"role": "system", "content": _SYSTEM_PROMPT},
                {"role": "user", "content": _build_user_prompt(job)},
            ],
            temperature=0,
            max_tokens=512,
        )
        raw = response.choices[0].message.content
        data = _extract_json(raw)
        if not data:
            logger.warning("normalize_job: bad JSON for '%s' at '%s'", job.title, job.company)
            return job

        fields = _validate(data)

        # Apply extracted fields
        job.salary_min = fields["salary_min"]
        job.salary_max = fields["salary_max"]
        if fields.get("salary_text") and not job.salary:
            job.salary = fields["salary_text"]
        if fields["remote"] is not None:
            job.remote = fields["remote"]
        if fields["work_mode"] is not None:
            job.work_mode = fields["work_mode"]
        if fields["employment_type"] is not None:
            job.employment_type = fields["employment_type"]
        if fields["experience_min_years"] is not None:
            job.experience_min_years = fields["experience_min_years"]
        job.seniority = fields["seniority"]
        # visa_sponsorship: Groq wins; fall back to visa tag signal if Groq returned null
        if fields["visa_sponsorship"] is not None:
            job.visa_sponsorship = fields["visa_sponsorship"]
        elif job.visa_sponsorship is None and job.tags:
            if "visa_friendly" in job.tags:
                job.visa_sponsorship = True
            elif "visa_no_sponsorship" in job.tags:
                job.visa_sponsorship = False
        if fields["required_skills"]:
            job.required_skills = fields["required_skills"]
        if fields["industry"] is not None:
            job.industry = fields["industry"]
        if fields["experience_level"] is not None:
            job.experience_level = fields["experience_level"]
        job.ai_summary_card = fields["ai_summary_card"]
        job.ai_summary_bullets = fields["ai_summary_bullets"]
        job.normalized_at = datetime.now(timezone.utc).isoformat()

    except Exception as exc:
        logger.warning("normalize_job failed for '%s': %s", job.title, exc)

    return job


async def normalize_jobs_batch(jobs: List[Job], groq_api_key: Optional[str]) -> List[Job]:
    """Normalize a list of jobs sequentially (concurrency=1 for Groq free tier)."""
    if not groq_api_key:
        logger.debug("Groq API key not set — skipping normalization")
        return jobs

    eligible = [j for j in jobs if j.description]
    skip_count = len(jobs) - len(eligible)
    if skip_count:
        logger.debug("Skipping %d jobs without descriptions", skip_count)
    if not eligible:
        return jobs

    logger.info("Normalizing %d jobs via Groq (%d skipped, no description)", len(eligible), skip_count)
    normalized = failed = 0

    for job in eligible:
        try:
            await normalize_job(job, groq_api_key)
            normalized += 1
        except Exception as exc:
            logger.warning("normalize_jobs_batch: unexpected error for '%s': %s", job.title, exc)
            failed += 1
        await asyncio.sleep(2)  # Groq free tier: ~30 req/min

    logger.info("Normalization done: %d normalized, %d failed", normalized, failed)
    return jobs
