"""
Deterministic enrichment helpers for job intelligence fields.
"""
from __future__ import annotations

import re
from datetime import datetime
from typing import Iterable, Optional, Tuple

from .models import Job


YEARS_RANGE_RE = re.compile(
    r"(?P<min>\d{1,2})\s*(?:\+|plus)?\s*(?:-|to)?\s*(?P<max>\d{1,2})?\s*(?:years?|yrs?)",
    re.IGNORECASE,
)
AT_LEAST_YEARS_RE = re.compile(r"(?:at least|min(?:imum)?)[^\d]{0,8}(?P<min>\d{1,2})\s*(?:years?|yrs?)", re.IGNORECASE)

SENIORITY_PATTERNS = [
    ("entry", re.compile(r"\b(intern|internship|entry[-\s]?level|junior|jr\.)\b", re.IGNORECASE)),
    ("mid", re.compile(r"\b(mid|intermediate)\b", re.IGNORECASE)),
    ("senior", re.compile(r"\b(senior|sr\.?)\b", re.IGNORECASE)),
    ("lead", re.compile(r"\b(staff|principal|lead|manager|director|head of)\b", re.IGNORECASE)),
]

SKILL_ALIASES = {
    "python": "Python",
    "py": "Python",
    "javascript": "JavaScript",
    "js": "JavaScript",
    "typescript": "TypeScript",
    "ts": "TypeScript",
    "react": "React",
    "reactjs": "React",
    "node": "Node.js",
    "nodejs": "Node.js",
    "java": "Java",
    "c#": "C#",
    ".net": ".NET",
    "golang": "Go",
    "go": "Go",
    "rust": "Rust",
    "sql": "SQL",
    "postgres": "PostgreSQL",
    "postgresql": "PostgreSQL",
    "mysql": "MySQL",
    "mongodb": "MongoDB",
    "redis": "Redis",
    "aws": "AWS",
    "azure": "Azure",
    "gcp": "GCP",
    "docker": "Docker",
    "kubernetes": "Kubernetes",
    "k8s": "Kubernetes",
    "terraform": "Terraform",
    "spark": "Apache Spark",
    "airflow": "Airflow",
    "machine learning": "Machine Learning",
    "ml": "Machine Learning",
    "artificial intelligence": "Artificial Intelligence",
    "ai": "Artificial Intelligence",
    "nlp": "NLP",
    "llm": "LLM",
    "pytorch": "PyTorch",
    "tensorflow": "TensorFlow",
    "tableau": "Tableau",
}

INDUSTRY_BY_CATEGORY = {
    "engineering": "Technology",
    "ai / ml": "Technology",
    "data": "Technology",
    "product": "Technology",
    "it": "Technology",
    "finance": "Finance",
    "accounting": "Finance",
    "bank": "Finance",
    "health": "Healthcare",
    "medical": "Healthcare",
    "pharma": "Healthcare",
    "education": "Education",
    "edtech": "Education",
    "government": "Government",
    "federal": "Government",
    "legal": "Legal",
    "sales": "Sales",
    "marketing": "Marketing",
    "retail": "Retail",
    "logistics": "Logistics",
    "supply chain": "Logistics",
    "manufacturing": "Manufacturing",
}

INDUSTRY_KEYWORDS = {
    "Technology": ["software", "cloud", "platform", "data", "developer", "engineering", "saas"],
    "Finance": ["trading", "fintech", "investment", "bank", "capital", "wealth"],
    "Healthcare": ["clinical", "hospital", "patient", "biotech", "medical"],
    "Education": ["curriculum", "school", "learning", "education", "student"],
    "Government": ["federal", "public sector", "agency", "clearance"],
    "Retail": ["store", "merchandise", "retail", "ecommerce"],
    "Logistics": ["supply chain", "warehouse", "distribution", "shipment"],
    "Manufacturing": ["factory", "production", "plant", "industrial"],
}


def _normalize_text(value: Optional[str]) -> str:
    return (value or "").strip()


def _normalize_token(value: str) -> str:
    return re.sub(r"[^a-z0-9+#.\s]", "", value.lower()).strip()


def _dedupe_keep_order(items: Iterable[str], limit: int) -> list[str]:
    out: list[str] = []
    seen = set()
    for raw in items:
        cleaned = raw.strip()
        if not cleaned:
            continue
        key = cleaned.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(cleaned)
        if len(out) >= limit:
            break
    return out


def _infer_experience_level_from_years(min_years: Optional[int], max_years: Optional[int]) -> str:
    pivot = max_years if max_years is not None else min_years
    if pivot is None:
        return "unknown"
    if pivot <= 1:
        return "entry"
    if pivot <= 4:
        return "mid"
    if pivot <= 7:
        return "senior"
    return "lead"


def extract_experience(title: Optional[str], description: Optional[str]) -> Tuple[str, Optional[int], Optional[int]]:
    """
    Extract normalized experience fields.

    Precedence:
    1. Explicit year ranges from title+description.
    2. Explicit "at least N years".
    3. Seniority keywords.
    """
    text = f"{_normalize_text(title)} {_normalize_text(description)}".strip()
    if not text:
        return ("unknown", None, None)

    min_years: Optional[int] = None
    max_years: Optional[int] = None

    for m in YEARS_RANGE_RE.finditer(text):
        m_min = int(m.group("min"))
        m_max_raw = m.group("max")
        m_max = int(m_max_raw) if m_max_raw else None
        if min_years is None or m_min < min_years:
            min_years = m_min
        if m_max is not None:
            if max_years is None or m_max > max_years:
                max_years = m_max
        else:
            if max_years is None:
                max_years = m_min

    if min_years is None:
        atleast = AT_LEAST_YEARS_RE.search(text)
        if atleast:
            min_years = int(atleast.group("min"))
            max_years = min_years

    if min_years is not None:
        level = _infer_experience_level_from_years(min_years, max_years)
        return (level, min_years, max_years)

    for level, pattern in SENIORITY_PATTERNS:
        if pattern.search(text):
            return (level, None, None)

    return ("unknown", None, None)


def _skills_from_text(text: str) -> list[str]:
    normalized = _normalize_token(text)
    found: list[str] = []
    for alias, canonical in SKILL_ALIASES.items():
        probe = f" {alias} "
        haystack = f" {normalized} "
        if probe in haystack:
            found.append(canonical)
    return found


def extract_required_skills(
    title: Optional[str],
    description: Optional[str],
    source_skills: Optional[list[str]],
    source_tags: Optional[list[str]],
    limit: int = 12,
) -> list[str]:
    combined_tokens: list[str] = []
    for bucket in (source_skills or []):
        token = _normalize_token(str(bucket))
        if token in SKILL_ALIASES:
            combined_tokens.append(SKILL_ALIASES[token])
        elif token:
            combined_tokens.append(str(bucket).strip())
    for bucket in (source_tags or []):
        token = _normalize_token(str(bucket))
        if token in SKILL_ALIASES:
            combined_tokens.append(SKILL_ALIASES[token])

    text = f"{_normalize_text(title)} {_normalize_text(description)}"
    combined_tokens.extend(_skills_from_text(text))
    return _dedupe_keep_order(combined_tokens, limit=limit)


def extract_industry(
    category: Optional[str],
    title: Optional[str],
    description: Optional[str],
) -> Tuple[str, float]:
    category_norm = _normalize_token(category or "")
    if category_norm:
        for key, industry in INDUSTRY_BY_CATEGORY.items():
            if key in category_norm:
                return (industry, 0.95)

    text = _normalize_token(f"{_normalize_text(title)} {_normalize_text(description)}")
    if not text:
        return ("Other", 0.3)

    best_industry = "Other"
    best_hits = 0
    for industry, keywords in INDUSTRY_KEYWORDS.items():
        hits = sum(1 for kw in keywords if kw in text)
        if hits > best_hits:
            best_hits = hits
            best_industry = industry

    if best_hits == 0:
        return ("Other", 0.35)
    if best_hits == 1:
        return (best_industry, 0.55)
    if best_hits == 2:
        return (best_industry, 0.72)
    return (best_industry, 0.86)


def derive_work_mode(remote: Optional[bool], location: Optional[str], title: Optional[str], description: Optional[str]) -> str:
    if remote is True:
        return "remote"
    text = _normalize_token(f"{_normalize_text(location)} {_normalize_text(title)} {_normalize_text(description)}")
    if "hybrid" in text:
        return "hybrid"
    if remote is False or "on site" in text or "onsite" in text:
        return "onsite"
    if "remote" in text:
        return "remote"
    return "unknown"


def build_role_pop_reasons(
    *,
    salary: Optional[str],
    work_mode: str,
    visa_friendly: Optional[bool],
    experience_level: str,
    required_skills: list[str],
    industry: str,
    location: Optional[str],
) -> list[str]:
    reasons: list[str] = []
    reasons.append(
        f"Comp range is visible ({salary})" if salary else "Comp is not listed, so negotiate on scope"
    )
    if work_mode == "remote":
        reasons.append("Remote setup supports flexible execution")
    elif work_mode == "hybrid":
        reasons.append("Hybrid setup balances focus and collaboration")
    elif work_mode == "onsite":
        reasons.append("On-site setup favors high-touch team loops")
    else:
        reasons.append("Work mode is not explicit; confirm in recruiter screen")

    if visa_friendly is True:
        reasons.append("Visa sponsorship signals look positive")
    elif visa_friendly is False:
        reasons.append("Visa policy appears restrictive; verify before applying")
    else:
        reasons.append("Visa policy is unclear; ask in first outreach")

    skill_slice = required_skills[:2]
    if skill_slice:
        reasons.append(f"Skill signal matches {', '.join(skill_slice)}")
    else:
        location_text = _normalize_text(location) or "broad geography"
        reasons.append(f"Industry fit in {industry}; hiring focus includes {location_text}")

    # Exactly 4 items
    return _dedupe_keep_order(reasons, limit=4)[:4]


def enrich_job(job: Job, enrichment_version: int = 1) -> Job:
    level, exp_min, exp_max = extract_experience(job.title, job.description)
    skills = extract_required_skills(job.title, job.description, job.skills, job.tags)
    industry, industry_confidence = extract_industry(job.category, job.title, job.description)
    work_mode = derive_work_mode(job.remote, job.location, job.title, job.description)

    tags = [str(t).strip().lower() for t in (job.tags or []) if str(t).strip()]
    visa_friendly: Optional[bool] = None
    if "visa_friendly" in tags:
        visa_friendly = True
    elif "visa_no_sponsorship" in tags:
        visa_friendly = False

    role_pop = build_role_pop_reasons(
        salary=job.salary,
        work_mode=work_mode,
        visa_friendly=visa_friendly,
        experience_level=level,
        required_skills=skills,
        industry=industry,
        location=job.location,
    )

    job.experience_level = level
    job.experience_min_years = exp_min
    job.experience_max_years = exp_max
    job.required_skills = skills
    job.industry = industry
    job.industry_confidence = industry_confidence
    job.work_mode = work_mode
    job.role_pop_reasons = role_pop
    job.enrichment_version = enrichment_version
    job.enrichment_updated_at = datetime.utcnow().isoformat()
    return job
