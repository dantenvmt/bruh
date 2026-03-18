"""
Visa / H1B / OPT enrichment.

Goal: add lightweight, best-effort tags so the frontend can filter for "visa-friendly"
jobs without needing a dedicated column/migration.

This is NOT a guarantee of sponsorship. Tagging is heuristic:
- Company-level: match company name against a configured sponsor list.
- Job-level: detect sponsorship/authorization phrases in title/description.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Iterable, List, Optional, Set

from .config import Config
from .models import Job


_CORP_SUFFIXES = {
    "inc",
    "inc.",
    "llc",
    "l.l.c.",
    "ltd",
    "ltd.",
    "corp",
    "corp.",
    "corporation",
    "company",
    "co",
    "co.",
    "gmbh",
    "plc",
}


def _normalize_company(name: str) -> str:
    # Keep it simple and deterministic: lowercase, strip punctuation/extra spaces,
    # drop common suffixes.
    if not name:
        return ""
    text = re.sub(r"[^a-zA-Z0-9\s]+", " ", str(name)).strip().lower()
    parts = [p for p in text.split() if p and p not in _CORP_SUFFIXES]
    return " ".join(parts)


_POSITIVE_PATTERNS = [
    re.compile(r"\b(h-?1b|h1b)\b", re.IGNORECASE),
    re.compile(r"\b(stem\s+opt|opt|cpt)\b", re.IGNORECASE),
    re.compile(r"\bvisa\s+sponsorship\b", re.IGNORECASE),
    re.compile(r"\b(sponsorship\s+available|we\s+sponsor|will\s+sponsor)\b", re.IGNORECASE),
]

_NEGATIVE_PATTERNS = [
    re.compile(r"\b(no\s+visa\s+sponsorship|no\s+sponsorship)\b", re.IGNORECASE),
    re.compile(r"\b(does\s+not\s+sponsor|will\s+not\s+sponsor|cannot\s+sponsor|unable\s+to\s+sponsor)\b", re.IGNORECASE),
    # "without sponsorship" only when paired with explicit refusal context
    re.compile(r"\b(not\s+able\s+to\s+provide\s+sponsorship|sponsorship\s+(is\s+)?not\s+(available|offered|provided))\b", re.IGNORECASE),
    # Common phrasing: "must be authorized to work in the US without sponsorship"
    re.compile(r"\b(authori[sz]ed\s+to\s+work\s+in\s+the\s+u\.?s\.?\s+without\s+(visa\s+)?sponsorship)\b", re.IGNORECASE),
]


def _load_company_list_from_file(path: Optional[str]) -> List[str]:
    if not path:
        return []
    try:
        data = Path(path).read_text(encoding="utf-8")
    except OSError:
        return []
    items = []
    for line in data.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        items.append(line)
    return items


def _get_sponsor_company_keys(cfg: Config) -> Set[str]:
    visa_cfg = cfg.visa or {}
    items: List[str] = []
    items.extend(visa_cfg.get("sponsor_companies") or [])
    items.extend(_load_company_list_from_file(visa_cfg.get("sponsor_companies_file")))
    return {_normalize_company(x) for x in items if _normalize_company(x)}


def _ensure_tags(job: Job) -> List[str]:
    if job.tags is None:
        job.tags = []
    # Ensure list type (some sources may return non-list JSON).
    if not isinstance(job.tags, list):
        job.tags = [str(job.tags)]
    return job.tags


def enrich_jobs_with_visa_tags(jobs: Iterable[Job], cfg: Optional[Config] = None) -> List[Job]:
    """
    Enrich jobs in-place by adding visa-related tags.

    Adds:
      - visa_friendly (heuristic)
      - visa_h1b / visa_opt (keyword mentions)
      - visa_no_sponsorship (negative mention)
      - visa_sponsor_company (company matches sponsor list)
    """
    cfg = cfg or Config()
    visa_cfg = cfg.visa or {}
    if not visa_cfg.get("tagging_enabled", True):
        return list(jobs)

    sponsor_keys = _get_sponsor_company_keys(cfg)

    enriched: List[Job] = []
    for job in jobs:
        tags = _ensure_tags(job)

        text = " ".join([job.title or "", job.description or ""]).strip()
        company_key = _normalize_company(job.company or "")
        sponsor_company = bool(company_key and company_key in sponsor_keys)

        positive = False
        negative = False

        # Keyword tagging
        if text:
            for pattern in _POSITIVE_PATTERNS:
                if pattern.search(text):
                    positive = True
                    break
            for pattern in _NEGATIVE_PATTERNS:
                if pattern.search(text):
                    negative = True
                    break

        # Fine-grained tags
        if re.search(r"\b(h-?1b|h1b)\b", text, re.IGNORECASE):
            tags.append("visa_h1b")
        if re.search(r"\b(stem\s+opt|opt|cpt)\b", text, re.IGNORECASE):
            tags.append("visa_opt")
        if negative:
            tags.append("visa_no_sponsorship")
        if sponsor_company:
            tags.append("visa_sponsor_company")

        # Simple "friendly" heuristic:
        # - if explicitly negative -> not friendly
        # - else if sponsor company or positive mention -> friendly
        visa_friendly = (not negative) and (sponsor_company or positive)
        if visa_friendly:
            tags.append("visa_friendly")

        # De-duplicate tags while preserving order
        seen = set()
        deduped = []
        for t in tags:
            if not t:
                continue
            s = str(t)
            if s in seen:
                continue
            seen.add(s)
            deduped.append(s)
        job.tags = deduped

        enriched.append(job)

    return enriched
