"""
Resume upload, extraction, and LLM-based optimization.

PDF text extraction uses pypdf.
LLM optimization uses Groq (imported lazily; raises HTTP 503 if not installed).
"""
from __future__ import annotations

import io
import json
import logging
import re
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

_DEFAULT_GROQ_MODEL = "llama-3.1-8b-instant"
_DEFAULT_TIMEOUT_SECONDS = 60


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------

class ResumeOptimizeError(Exception):
    """Raised when resume optimization fails (provider not configured, API error, etc.)."""
    pass


# ---------------------------------------------------------------------------
# PDF extraction
# ---------------------------------------------------------------------------

def extract_text_from_pdf(file_bytes: bytes) -> str:
    """Extract plain text from a PDF file.

    Args:
        file_bytes: Raw bytes of the PDF file.

    Returns:
        Extracted text content.

    Raises:
        ValueError: If the PDF is encrypted or has no extractable text layer.
    """
    from pypdf import PdfReader

    reader = PdfReader(io.BytesIO(file_bytes))

    if reader.is_encrypted:
        raise ValueError("PDF is encrypted and cannot be read")

    parts: List[str] = []
    for page in reader.pages:
        text = page.extract_text() or ""
        if text.strip():
            parts.append(text)

    if not parts:
        raise ValueError(
            "No text could be extracted from the PDF. "
            "It may be an image-only or scanned document without a text layer."
        )

    return "\n".join(parts)


# ---------------------------------------------------------------------------
# Groq LLM helper
# ---------------------------------------------------------------------------

async def _call_groq_resume(
    *,
    system_prompt: str,
    user_prompt: str,
    api_key: str,
    model: str,
    timeout: float,
    max_tokens: int = 4096,
) -> str:
    """Call Groq API for resume optimization.

    Raises:
        ResumeOptimizeError: If groq package is not installed or the API call fails.
    """
    try:
        from groq import AsyncGroq
    except ImportError:
        raise ResumeOptimizeError(
            "groq package is not installed. Install it with: pip install groq"
        )

    client = AsyncGroq(api_key=api_key, timeout=timeout)
    response = await client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        temperature=0,
        max_tokens=max_tokens,
    )
    return response.choices[0].message.content


# ---------------------------------------------------------------------------
# JSON parse helper
# ---------------------------------------------------------------------------

def _parse_json_response(raw: str, expected_type: type) -> Any:
    """Parse a JSON response from an LLM, stripping markdown fences if present.

    Args:
        raw: Raw LLM response string.
        expected_type: Expected type (list or dict).

    Returns:
        Parsed JSON value of the expected type.

    Raises:
        ResumeOptimizeError: If parsing fails or type doesn't match.
    """
    text = raw.strip()

    # Strip markdown fences
    fenced = re.search(r"```(?:json)?\s*([\s\S]*?)\s*```", text)
    if fenced:
        text = fenced.group(1).strip()

    # Find outermost [ ] or { }
    if expected_type is list:
        match = re.search(r"\[[\s\S]*\]", text)
    else:
        match = re.search(r"\{[\s\S]*\}", text)

    if match:
        text = match.group(0)

    try:
        parsed = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ResumeOptimizeError(f"LLM returned invalid JSON: {exc}") from exc

    if not isinstance(parsed, expected_type):
        raise ResumeOptimizeError(
            f"Expected {expected_type.__name__} from LLM, got {type(parsed).__name__}"
        )

    return parsed


# ---------------------------------------------------------------------------
# Job summary
# ---------------------------------------------------------------------------

async def summarize_job(
    *,
    job_id: str,
    job_title: str,
    job_company: str,
    job_description: str,
    tags: List[str],
    llm_config: Optional[Dict] = None,
) -> dict:
    """Generate AI summaries of a job posting using a single Groq call.

    Returns:
        Dict with keys: job_id, ai_summary_card (str), ai_summary_detail (dict with
        summary_short, summary_bullets, attention_tags).

    Raises:
        ResumeOptimizeError: If Groq is not configured or the call fails.
    """
    cfg = llm_config or {}
    api_key = cfg.get("groq_api_key") or None
    if not api_key:
        raise ResumeOptimizeError("Groq API key is not configured.")

    model = cfg.get("groq_model") or _DEFAULT_GROQ_MODEL
    timeout = float(cfg.get("timeout", _DEFAULT_TIMEOUT_SECONDS))

    description_trimmed = job_description[:4000]

    system_prompt = (
        "You are a sharp technical recruiter briefing a candidate on a role. "
        "Given a job posting, return ONLY a valid JSON object — no markdown fences, no explanation. "
        "The object must have exactly these keys:\n"
        '{"card": string, "detail": {"summary_short": string, "summary_bullets": [string], "attention_tags": [string]}}.\n'
        "card: Capture the essence of the job in EXACTLY 3 sentences. "
        "Each sentence MUST be under 12 words. Plain text, no markdown. "
        "Focus on: what the role does, what makes it interesting, and one key requirement or perk.\n"
        "detail.summary_short: 2-3 sentences. Lead with what makes this role stand out. "
        "Mention the team or product if known. End with the ideal candidate profile in one line.\n"
        "detail.summary_bullets: exactly 5 bullets, max 12 words each. Cover these in order:\n"
        "  1. Core responsibility — what you'll actually do day-to-day\n"
        "  2. Must-have skill or experience\n"
        "  3. Tech stack or tools mentioned\n"
        "  4. Compensation, equity, or perks signal (if mentioned; otherwise note 'Not disclosed')\n"
        "  5. Growth opportunity or team culture signal\n"
        "detail.attention_tags: 3-5 tags. Use ONLY from this palette when clearly supported by the description: "
        "'Remote', 'Hybrid', 'On-site', 'Visa Friendly', 'No Visa', "
        "'Early Stage', 'Series A-C', 'Public Co', 'FAANG', "
        "'AI/ML', 'Crypto/Web3', 'Fintech', 'Healthcare', "
        "'Junior OK', 'Senior+', 'Lead/Staff', 'IC Only', 'People Manager', "
        "'High Comp', 'Equity Heavy', 'Good WLB'. "
        "Pick only tags with clear evidence in the posting."
    )
    user_prompt = (
        f"Job Title: {job_title}\n"
        f"Company: {job_company}\n"
        f"Existing tags: {', '.join(tags) if tags else 'none'}\n\n"
        f"Job Description:\n{description_trimmed}\n\n"
        "Return the JSON summary now:"
    )

    raw = await _call_groq_resume(
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        api_key=api_key,
        model=model,
        timeout=timeout,
        max_tokens=1024,
    )

    parsed = _parse_json_response(raw, dict)

    card = str(parsed.get("card") or "")
    detail_raw = parsed.get("detail") or {}
    if not isinstance(detail_raw, dict):
        detail_raw = {}

    detail = {
        "summary_short": str(detail_raw.get("summary_short") or ""),
        "summary_bullets": [str(b) for b in (detail_raw.get("summary_bullets") or [])],
        "attention_tags": [str(t) for t in (detail_raw.get("attention_tags") or [])],
    }

    return {
        "job_id": job_id,
        "ai_summary_card": card,
        "ai_summary_detail": detail,
        # Legacy fields for backward compatibility
        "summary_short": detail["summary_short"],
        "summary_bullets": detail["summary_bullets"],
        "attention_tags": detail["attention_tags"],
    }


# ---------------------------------------------------------------------------
# Resume analysis + skill extraction
# ---------------------------------------------------------------------------

_CRITIQUE_PROMPTS: Dict[str, str] = {
    "light": (
        "Be encouraging. Lead with strengths. Limit gaps to 2-3 and frame them as "
        "growth opportunities, not failures."
    ),
    "balanced": (
        "Be honest and direct. Give real strengths and real weaknesses in equal measure. "
        "Don't sugarcoat, but don't be harsh."
    ),
    "hardcore": (
        "Be ruthlessly honest. A hiring manager at Google is reviewing this — what would "
        "they reject it for? Every gap is a reason for a 'no'. The candidate wants the hard truth."
    ),
}


async def analyze_resume(
    *,
    resume_text: str,
    critique_level: str = "balanced",
    llm_config: Optional[Dict] = None,
) -> dict:
    """Analyze a resume generally (not tied to a specific job).

    Returns:
        Dict with keys: score (0-100), headline, strengths, gaps, priority_actions.

    Raises:
        ResumeOptimizeError: If Groq is not configured or the call fails.
    """
    cfg = llm_config or {}
    api_key = cfg.get("groq_api_key") or None
    if not api_key:
        raise ResumeOptimizeError("Groq API key is not configured.")

    model = cfg.get("groq_model") or _DEFAULT_GROQ_MODEL
    timeout = float(cfg.get("timeout", _DEFAULT_TIMEOUT_SECONDS))

    level = critique_level if critique_level in _CRITIQUE_PROMPTS else "balanced"
    critique_instruction = _CRITIQUE_PROMPTS[level]
    resume_trimmed = resume_text[:6000]

    system_prompt = (
        "You are an elite resume reviewer who has screened 10,000+ resumes "
        "for top tech companies. "
        f"{critique_instruction} "
        "Score the resume 0-100 based on: clarity, impact quantification, "
        "keyword optimization, structure, and overall hiring signal strength.\n"
        "Return ONLY a valid JSON object — no markdown fences, no explanation. "
        "The object must have exactly these keys: "
        '{"score": integer 0-100, "headline": string, "strengths": [string], '
        '"gaps": [string], "priority_actions": [string]}.\n'
        "score: overall resume quality 0-100. Be calibrated: "
        "90+ = ready for FAANG/top startup interviews as-is, "
        "70-89 = solid but needs targeted improvements, "
        "50-69 = has potential but significant gaps, "
        "<50 = needs major rework.\n"
        "headline: one punchy sentence capturing the resume's current state.\n"
        "strengths: 3-5 genuine strengths. Be specific — not 'good experience' "
        "but 'Strong quantified backend impact (40% latency reduction, 2M requests)'.\n"
        "gaps: 3-5 weaknesses. Each MUST follow this format: "
        "\"[What's wrong] — [Why it matters] — [What the reader assumes]\" "
        "Example: \"No metrics on team leadership — hiring managers can't gauge scope — "
        "they'll assume you managed 2 people, not 12\"\n"
        "priority_actions: 3-5 concrete fixes, most impactful first. Each MUST follow: "
        "\"[Specific change] — [Why this works] — [Expected effect]\" "
        "Example: \"Add 'Reduced P95 latency from 800ms to 120ms' to your Stripe bullet — "
        "proves system optimization skill — moves you from 'maybe' to 'phone screen' pile\""
    )
    user_prompt = (
        f"Resume:\n{resume_trimmed}\n\n"
        "Return the JSON analysis now:"
    )

    raw = await _call_groq_resume(
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        api_key=api_key,
        model=model,
        timeout=timeout,
        max_tokens=1024,
    )

    parsed = _parse_json_response(raw, dict)

    return {
        "score": int(parsed.get("score") or 0),
        "headline": str(parsed.get("headline") or ""),
        "strengths": [str(s) for s in (parsed.get("strengths") or [])],
        "gaps": [str(g) for g in (parsed.get("gaps") or [])],
        "priority_actions": [str(a) for a in (parsed.get("priority_actions") or [])],
    }


async def extract_resume_profile(
    *,
    resume_text: str,
    llm_config: Optional[Dict] = None,
) -> dict:
    """Extract skills and years of experience from a resume using Groq.

    Returns:
        Dict with keys: skills (list of str), experience_years (int or None).
        Returns empty skills list and None experience_years on failure (non-fatal).
    """
    cfg = llm_config or {}
    api_key = cfg.get("groq_api_key") or None
    if not api_key:
        return {"skills": [], "experience_years": None}

    model = cfg.get("groq_model") or _DEFAULT_GROQ_MODEL
    timeout = float(cfg.get("timeout", _DEFAULT_TIMEOUT_SECONDS))
    resume_trimmed = resume_text[:4000]

    system_prompt = (
        "You are a resume parser. Extract structured profile data from the resume. "
        "Return ONLY a valid JSON object — no markdown fences, no explanation. "
        "The object must have exactly these keys: "
        '{"skills": [string], "experience_years": integer or null}. '
        "skills: list of technical skills, tools, frameworks, and languages (lowercase, deduplicated). "
        "experience_years: total years of professional experience as a single integer, or null if unclear."
    )
    user_prompt = (
        f"Resume:\n{resume_trimmed}\n\n"
        "Return the JSON profile now:"
    )

    try:
        raw = await _call_groq_resume(
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            api_key=api_key,
            model=model,
            timeout=timeout,
            max_tokens=512,
        )
        parsed = _parse_json_response(raw, dict)
        skills = [str(s).strip().lower() for s in (parsed.get("skills") or []) if s]
        raw_years = parsed.get("experience_years")
        experience_years = int(raw_years) if raw_years is not None else None
        return {"skills": skills[:50], "experience_years": experience_years}
    except Exception:
        # Non-fatal — upload still succeeds, skills just won't be available for match scoring
        return {"skills": [], "experience_years": None}


# ---------------------------------------------------------------------------
# Mode-specific handlers
# ---------------------------------------------------------------------------

async def _optimize_bullets(
    *,
    resume_text: str,
    job_title: str,
    job_company: str,
    job_description: str,
    required_skills: List[str],
    api_key: str,
    model: str,
    timeout: float,
) -> dict:
    system_prompt = (
        "You are a top-tier career strategist who has gotten candidates into FAANG, "
        "top startups, and Fortune 500 companies.\n\n"
        "Your task: rewrite resume bullet points to maximize this candidate's chances "
        "for the specific role.\n\n"
        "Strategy:\n"
        "1. Read the job description and identify the 3-5 capabilities the hiring manager "
        "cares about most.\n"
        "2. For each resume bullet, ask: 'Does this prove one of those capabilities?'\n"
        "3. Rewrite bullets to: lead with measurable impact, mirror the JD's language "
        "naturally, and demonstrate the exact competencies the role demands.\n\n"
        "Rules:\n"
        "- Never fabricate experience. Only reframe and quantify what's already there.\n"
        "- Transform vague bullets ('Worked on backend systems') into impact-driven ones "
        "('Reduced API latency 40% by redesigning caching layer serving 2M daily requests').\n"
        "- If a bullet is irrelevant to this role, suggest replacing it with a more relevant "
        "experience from the resume, or flag it as 'Consider removing — doesn't support this application'.\n"
        "- Each rewrite must feel natural, not keyword-stuffed.\n\n"
        "Return ONLY a valid JSON array — no markdown fences, no explanation. "
        "Each element: {\"original\": string, \"improved\": string, \"reason\": string}. "
        "reason: Explain what hiring signal this rewrite strengthens "
        "(e.g. 'Proves system design at scale — their #1 requirement')."
    )
    user_prompt = (
        f"Job Title: {job_title}\n"
        f"Company: {job_company}\n"
        f"Required Skills: {', '.join(required_skills)}\n\n"
        f"Job Description:\n{job_description}\n\n"
        f"Resume:\n{resume_text}\n\n"
        "Return the JSON array of bullet point improvements now:"
    )

    raw = await _call_groq_resume(
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        api_key=api_key,
        model=model,
        timeout=timeout,
    )

    suggestions = _parse_json_response(raw, list)
    return {"suggestions": suggestions}


async def _optimize_overview(
    *,
    resume_text: str,
    job_title: str,
    job_company: str,
    job_description: str,
    required_skills: List[str],
    api_key: str,
    model: str,
    timeout: float,
) -> dict:
    system_prompt = (
        "You are a top-tier career strategist. Rewrite the candidate's professional summary "
        "to be a perfect elevator pitch for this specific role.\n\n"
        "Strategy:\n"
        "1. Open with the candidate's strongest credential that maps to the role's #1 need.\n"
        "2. In 2-3 sentences, connect their experience arc to what this company is building.\n"
        "3. Close with a forward-looking statement that signals motivation for THIS role specifically.\n"
        "4. Naturally weave in 3-5 keywords from the job description without sounding forced.\n\n"
        "Rules:\n"
        "- Never fabricate credentials. Work only with what's in the resume.\n"
        "- The summary should make the hiring manager think 'this person gets what we need'.\n"
        "- Avoid generic filler ('passionate professional', 'team player', 'results-driven'). "
        "Every word must earn its place.\n"
        "- If no summary exists, write one from scratch based on the resume content.\n\n"
        "Return ONLY a valid JSON object — no markdown fences, no explanation. "
        "Keys: {\"original_summary\": string, \"optimized_summary\": string, \"key_changes\": [string]}. "
        "key_changes: 3-5 specific changes made and why each strengthens the application."
    )
    user_prompt = (
        f"Job Title: {job_title}\n"
        f"Company: {job_company}\n"
        f"Required Skills: {', '.join(required_skills)}\n\n"
        f"Job Description:\n{job_description}\n\n"
        f"Resume:\n{resume_text}\n\n"
        "Return the JSON object with the improved professional summary now:"
    )

    raw = await _call_groq_resume(
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        api_key=api_key,
        model=model,
        timeout=timeout,
    )

    suggestions = _parse_json_response(raw, dict)
    return {"suggestions": suggestions}


async def _optimize_full_rewrite(
    *,
    resume_text: str,
    job_title: str,
    job_company: str,
    job_description: str,
    required_skills: List[str],
    api_key: str,
    model: str,
    timeout: float,
) -> dict:
    system_prompt = (
        "You are a top-tier career strategist who writes resumes that get interviews. "
        "Rewrite this entire resume to be laser-targeted for the specific role.\n\n"
        "Strategy:\n"
        "1. Analyze the job description. Identify the top 5 capabilities the hiring manager "
        "will screen for in the first 6 seconds of reading.\n"
        "2. Restructure the resume so those capabilities appear above the fold.\n"
        "3. For every experience entry: lead with the most relevant achievement, "
        "quantify impact, and mirror the JD's language naturally.\n"
        "4. Remove or condense experience that doesn't support this application.\n"
        "5. Add a Skills section that exactly matches the JD's technical requirements "
        "(only skills the candidate actually has).\n\n"
        "Rules:\n"
        "- Preserve all real experience, education, and dates. Never fabricate.\n"
        "- Use strong action verbs: Architected, Spearheaded, Reduced, Scaled, Launched.\n"
        "- Every bullet should answer: 'So what? Why does this matter for THIS role?'\n"
        "- Keep it to 1 page if the candidate has <7 years experience, 2 pages max otherwise.\n"
        "- Format: clean plaintext with clear section headers.\n\n"
        "Output only the rewritten resume — no JSON, no markdown fences, no commentary."
    )
    user_prompt = (
        f"Job Title: {job_title}\n"
        f"Company: {job_company}\n"
        f"Required Skills: {', '.join(required_skills)}\n\n"
        f"Job Description:\n{job_description}\n\n"
        f"Original Resume:\n{resume_text}\n\n"
        "Rewrite the resume now:"
    )

    raw = await _call_groq_resume(
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        api_key=api_key,
        model=model,
        timeout=timeout,
    )

    return {"optimized_resume": raw.strip()}


# ---------------------------------------------------------------------------
# Public dispatch
# ---------------------------------------------------------------------------

async def optimize_resume(
    *,
    resume_text: str,
    job_title: str,
    job_company: str,
    job_description: str,
    required_skills: List[str],
    mode: str,
    llm_config: Optional[Dict] = None,
) -> dict:
    """Optimize a resume against a job posting using an LLM.

    Args:
        resume_text: Extracted plain text of the resume.
        job_title: Title of the target job.
        job_company: Company name for the target job.
        job_description: Full job description text.
        required_skills: List of required skills from the job.
        mode: One of "bullets", "overview", or "full_rewrite".
        llm_config: Dict with keys: groq_api_key, groq_model, timeout.
                    Typically Config().llm_parser.

    Returns:
        Dict with mode-specific keys (see module docstring).

    Raises:
        ResumeOptimizeError: If LLM is not configured, not installed, or the API call fails.
        ValueError: If mode is invalid.
    """
    cfg = llm_config or {}
    api_key = cfg.get("groq_api_key") or None
    if not api_key:
        raise ResumeOptimizeError(
            "Groq API key is not configured. Set groq_api_key in llm_parser config."
        )

    model = cfg.get("groq_model") or _DEFAULT_GROQ_MODEL
    timeout = float(cfg.get("timeout", _DEFAULT_TIMEOUT_SECONDS))

    # Truncate inputs to avoid exceeding context limits
    resume_text = resume_text[:6000]
    job_description = job_description[:3000]
    required_skills = required_skills[:30]

    kwargs = dict(
        resume_text=resume_text,
        job_title=job_title or "",
        job_company=job_company or "",
        job_description=job_description,
        required_skills=required_skills,
        api_key=api_key,
        model=model,
        timeout=timeout,
    )

    if mode == "bullets":
        return await _optimize_bullets(**kwargs)
    elif mode == "overview":
        return await _optimize_overview(**kwargs)
    elif mode == "full_rewrite":
        return await _optimize_full_rewrite(**kwargs)
    else:
        raise ValueError(f"Invalid mode: {mode!r}. Must be 'bullets', 'overview', or 'full_rewrite'.")
