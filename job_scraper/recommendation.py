"""
Deterministic recommendation scoring for jobs.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable, Optional


@dataclass
class MatchProfile:
    experience_years: Optional[int] = None
    skills: Optional[list[str]] = None
    industries: Optional[list[str]] = None
    work_mode: Optional[str] = None


def _normalize_list(values: Optional[Iterable[str]]) -> list[str]:
    if not values:
        return []
    out: list[str] = []
    seen = set()
    for value in values:
        cleaned = str(value or "").strip().lower()
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        out.append(cleaned)
    return out


def _parse_posted_date(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    raw = value.strip()
    if not raw:
        return None
    try:
        if raw.endswith("Z"):
            raw = raw[:-1] + "+00:00"
        dt = datetime.fromisoformat(raw)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _score_recency(posted_date: Optional[str]) -> tuple[int, str]:
    dt = _parse_posted_date(posted_date)
    if dt is None:
        return (14, "Posted date is missing; treated as moderately recent")
    age_days = max(0, int((datetime.now(timezone.utc) - dt).total_seconds() // 86400))
    if age_days <= 3:
        return (25, "Fresh posting from the last 3 days")
    if age_days <= 7:
        return (22, "Recently posted this week")
    if age_days <= 14:
        return (18, "Posted within the last 2 weeks")
    if age_days <= 30:
        return (12, "Posted within the last month")
    return (6, "Older posting; verify if role is still active")


def _score_skills(required_skills: Optional[list[str]], profile_skills: list[str]) -> tuple[int, str]:
    req = _normalize_list(required_skills or [])
    prof = _normalize_list(profile_skills)
    if not req and not prof:
        return (10, "No explicit skill inventory to compare")
    if not req:
        return (12, "Job has no explicit skill list; profile can still fit")
    if not prof:
        return (4, "Profile skill set was not provided")

    req_set = set(req)
    prof_set = set(prof)
    overlap = req_set.intersection(prof_set)
    ratio = len(overlap) / max(1, len(req_set))
    score = int(round(ratio * 30))
    score = max(0, min(30, score))
    if overlap:
        matched = ", ".join(sorted(list(overlap))[:3])
        return (score, f"Skill overlap: {matched}")
    return (score, "No direct skill overlap found")


def _score_experience(
    profile_years: Optional[int],
    min_years: Optional[int],
    max_years: Optional[int],
    experience_level: Optional[str],
) -> tuple[int, str]:
    level = (experience_level or "unknown").strip().lower()
    if profile_years is None and level == "unknown" and min_years is None and max_years is None:
        return (10, "Experience requirements are not explicit")
    if profile_years is None:
        return (8, "Profile years are missing; scored with neutral baseline")

    if min_years is not None and profile_years < min_years:
        gap = min_years - profile_years
        score = max(0, 12 - (gap * 4))
        return (score, f"Profile is below minimum experience by ~{gap} years")
    if max_years is not None and profile_years > max_years + 2:
        return (13, "Profile is above typical years range; may be over-level")
    if min_years is not None and profile_years >= min_years:
        return (20, "Profile years fit stated experience range")

    if level == "entry" and profile_years <= 2:
        return (19, "Profile aligns with entry-level scope")
    if level == "mid" and 2 <= profile_years <= 6:
        return (19, "Profile aligns with mid-level scope")
    if level == "senior" and profile_years >= 5:
        return (19, "Profile aligns with senior scope")
    if level == "lead" and profile_years >= 8:
        return (18, "Profile aligns with lead-level scope")

    return (12, "Experience alignment is partial")


def _score_industry(job_industry: Optional[str], profile_industries: list[str]) -> tuple[int, str]:
    job_value = (job_industry or "").strip().lower()
    profile_values = _normalize_list(profile_industries)
    if not job_value:
        return (6, "Industry is unspecified")
    if not profile_values:
        return (8, f"Industry is {job_industry}; profile industries not provided")
    if job_value in set(profile_values):
        return (15, f"Industry match: {job_industry}")
    return (5, f"Industry mismatch ({job_industry})")


def _score_work_mode(job_work_mode: Optional[str], profile_work_mode: Optional[str]) -> tuple[int, str]:
    job_mode = (job_work_mode or "unknown").strip().lower()
    profile_mode = (profile_work_mode or "").strip().lower()
    if not profile_mode:
        return (3, "Work mode preference not provided")
    if job_mode == profile_mode:
        return (5, f"Work mode aligned ({job_mode})")
    if job_mode == "unknown":
        return (2, "Job work mode is unknown")
    return (0, f"Work mode mismatch (job={job_mode}, profile={profile_mode})")


def _score_compensation(salary: Optional[str]) -> tuple[int, str]:
    if salary and str(salary).strip():
        return (5, "Compensation details are visible")
    return (2, "Compensation details are not listed")


def fit_band(score: int) -> str:
    if score >= 80:
        return "strong"
    if score >= 60:
        return "good"
    if score >= 40:
        return "moderate"
    return "weak"


def score_job(
    *,
    posted_date: Optional[str],
    required_skills: Optional[list[str]],
    experience_min_years: Optional[int],
    experience_max_years: Optional[int],
    experience_level: Optional[str],
    industry: Optional[str],
    work_mode: Optional[str],
    salary: Optional[str],
    profile: MatchProfile,
) -> dict:
    recency_score, recency_reason = _score_recency(posted_date)
    skills_score, skills_reason = _score_skills(required_skills, _normalize_list(profile.skills))
    experience_score, experience_reason = _score_experience(
        profile.experience_years,
        experience_min_years,
        experience_max_years,
        experience_level,
    )
    industry_score, industry_reason = _score_industry(industry, _normalize_list(profile.industries))
    work_mode_score, work_mode_reason = _score_work_mode(work_mode, profile.work_mode)
    comp_score, comp_reason = _score_compensation(salary)

    score = recency_score + skills_score + experience_score + industry_score + work_mode_score + comp_score
    score = max(0, min(100, score))

    reasons = [recency_reason, skills_reason, experience_reason, industry_reason, work_mode_reason, comp_reason]
    reasons = [item for item in reasons if item][:4]

    gaps = []
    if skills_score < 12:
        gaps.append("Skill overlap is limited")
    if experience_score < 10:
        gaps.append("Experience requirements may be above profile")
    if industry_score <= 6:
        gaps.append("Industry alignment is weak")
    if work_mode_score == 0:
        gaps.append("Work mode preference mismatches")

    return {
        "score": score,
        "fit_band": fit_band(score),
        "reasons": reasons,
        "gaps": gaps[:3],
        "breakdown": {
            "recency": recency_score,
            "skills_overlap": skills_score,
            "experience_fit": experience_score,
            "industry_fit": industry_score,
            "work_mode_fit": work_mode_score,
            "compensation_visibility": comp_score,
        },
    }
