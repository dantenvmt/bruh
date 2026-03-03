"""
CSS selector hint generation for custom career sites.

Uses basic heuristics to detect potential job listing selectors.
These are NOT production-ready - they require human validation.

Strategy:
1. Find repeated container elements with anchor children
2. Score by link density, text patterns, location patterns
3. Return hints with confidence scores
"""

import logging
import re
from datetime import datetime, timezone
from typing import List, Optional, Tuple

from bs4 import BeautifulSoup, Tag

from .types import SelectorHint


logger = logging.getLogger(__name__)

DEFAULT_SELECTOR_MIN_CONFIDENCE = 0.6
DEFAULT_SELECTOR_MIN_JOBS = 3
APPROVED_SELECTOR_STATUSES = {"approved", "manual_approved"}
GENERIC_CONTAINER_SELECTORS = {
    "div",
    "section",
    "article",
    "li",
    "ul",
    "ol",
    "tr",
    "td",
    "span",
    "p",
    "a",
}

# CSS-in-JS and framework-generated class prefixes that are usually unstable.
UNSTABLE_CLASS_PATTERNS = [
    re.compile(r"^css-[a-z0-9]+$", re.IGNORECASE),
    re.compile(r"^mui", re.IGNORECASE),
    re.compile(r"^chakra-", re.IGNORECASE),
    re.compile(r"^emotion-", re.IGNORECASE),
    re.compile(r"^sc-[a-z0-9]+$", re.IGNORECASE),
    re.compile(r"^jsx-\d+$", re.IGNORECASE),
    re.compile(r"^styled__", re.IGNORECASE),
]


# Text patterns that suggest job titles
TITLE_PATTERNS = [
    r"engineer",
    r"developer",
    r"manager",
    r"analyst",
    r"designer",
    r"director",
    r"specialist",
    r"coordinator",
    r"lead",
    r"senior",
    r"junior",
    r"intern",
    r"associate",
    r"consultant",
]

# Text patterns that suggest locations
LOCATION_PATTERNS = [
    r"remote",
    r"hybrid",
    r"on-?site",
    r"new york",
    r"san francisco",
    r"seattle",
    r"austin",
    r"chicago",
    r"boston",
    r"los angeles",
    r"\b[A-Z]{2}\b",  # State abbreviations
    r"\b\d{5}\b",  # ZIP codes
]


def _is_unstable_class(class_name: str) -> bool:
    text = (class_name or "").strip()
    if not text:
        return True
    if len(text) <= 2:
        return True
    return any(pat.match(text) for pat in UNSTABLE_CLASS_PATTERNS)


def _is_generic_selector(selector: Optional[str]) -> bool:
    text = (selector or "").strip().lower()
    if not text:
        return True
    if text in GENERIC_CONTAINER_SELECTORS:
        return True
    return bool(re.fullmatch(r"[a-z][a-z0-9_-]*", text) and text in GENERIC_CONTAINER_SELECTORS)


def _has_unstable_class_token(selector: Optional[str]) -> bool:
    if not selector:
        return False
    classes = re.findall(r"\.([A-Za-z0-9_-]+)", selector)
    return any(_is_unstable_class(cls) for cls in classes)


def _is_selector_stable(selector: Optional[str], *, allow_generic: bool = False) -> bool:
    text = (selector or "").strip()
    if not text:
        return False
    if not allow_generic and _is_generic_selector(text):
        return False
    if _has_unstable_class_token(text):
        return False
    return True


def _hint_note(base_note: str, suffix: str) -> str:
    base = (base_note or "").strip()
    if not base:
        return suffix
    return f"{base} | {suffix}"


def _build_validation_payload(
    *,
    passed: bool,
    reason: str,
    jobs_found: int = 0,
    min_jobs: int = DEFAULT_SELECTOR_MIN_JOBS,
    extraction_mode: Optional[str] = None,
    sample_titles: Optional[list[str]] = None,
) -> dict:
    payload = {
        "passed": passed,
        "reason": reason,
        "jobs_found": int(jobs_found or 0),
        "min_jobs": int(min_jobs),
        "checked_at": datetime.now(timezone.utc).isoformat(),
    }
    if extraction_mode:
        payload["extraction_mode"] = extraction_mode
    if sample_titles:
        payload["sample_titles"] = sample_titles[:3]
    return payload


def assess_selector_hint(
    hint: Optional[SelectorHint],
    *,
    min_confidence: float = DEFAULT_SELECTOR_MIN_CONFIDENCE,
) -> Tuple[bool, str]:
    """
    Structural validation for selector hints before parse-based validation.
    """
    if hint is None:
        return False, "no selector hint generated"
    if not hint.is_valid():
        return False, "missing required selectors (job_container/title/link)"
    if float(hint.confidence or 0.0) < float(min_confidence):
        return False, f"confidence below threshold ({hint.confidence:.2f} < {min_confidence:.2f})"
    if int(hint.sample_count or 0) < DEFAULT_SELECTOR_MIN_JOBS:
        return False, f"sample_count too low ({hint.sample_count})"
    if not _is_selector_stable(hint.job_container, allow_generic=False):
        return False, f"job_container selector is too generic or unstable: {hint.job_container}"
    if not _is_selector_stable(hint.title, allow_generic=True):
        return False, f"title selector is unstable: {hint.title}"
    if not _is_selector_stable(hint.link, allow_generic=True):
        return False, f"link selector is unstable: {hint.link}"
    return True, "ok"


def validate_selector_hints(
    html: str,
    base_url: str,
    hint: Optional[SelectorHint],
    *,
    min_confidence: float = DEFAULT_SELECTOR_MIN_CONFIDENCE,
    min_jobs: int = DEFAULT_SELECTOR_MIN_JOBS,
    extraction_mode: Optional[str] = None,
) -> Tuple[bool, dict]:
    """
    Execute selectors against HTML and verify they extract a sane set of jobs.
    """
    ok, reason = assess_selector_hint(hint, min_confidence=min_confidence)
    if not ok:
        return False, _build_validation_payload(
            passed=False,
            reason=reason,
            jobs_found=0,
            min_jobs=min_jobs,
            extraction_mode=extraction_mode,
        )

    from ..scraping.parsers.css import ParseError, parse

    try:
        parsed = parse(html, hint.to_dict(), base_url)
    except ParseError as exc:
        return False, _build_validation_payload(
            passed=False,
            reason=f"parse validation failed: {exc}",
            jobs_found=0,
            min_jobs=min_jobs,
            extraction_mode=extraction_mode,
        )
    except Exception as exc:
        return False, _build_validation_payload(
            passed=False,
            reason=f"unexpected parse validation error: {exc}",
            jobs_found=0,
            min_jobs=min_jobs,
            extraction_mode=extraction_mode,
        )

    if len(parsed) < int(min_jobs):
        return False, _build_validation_payload(
            passed=False,
            reason=f"too few jobs extracted ({len(parsed)} < {min_jobs})",
            jobs_found=len(parsed),
            min_jobs=min_jobs,
            extraction_mode=extraction_mode,
            sample_titles=[j.title for j in parsed],
        )

    return True, _build_validation_payload(
        passed=True,
        reason="selector validation passed",
        jobs_found=len(parsed),
        min_jobs=min_jobs,
        extraction_mode=extraction_mode,
        sample_titles=[j.title for j in parsed],
    )


def build_selector_hint_record(
    hint: SelectorHint,
    *,
    validation: Optional[dict] = None,
    review_status: str = "proposed",
    extraction_mode: Optional[str] = None,
) -> dict:
    """
    Build JSON payload persisted in scrape_sites.selector_hints.
    """
    record = hint.to_dict()
    record["review_status"] = review_status
    if extraction_mode:
        record["extraction_mode"] = extraction_mode
    if validation is not None:
        record["validation"] = validation
    return record


def selector_hints_ready_for_scrape(
    selector_hints: Optional[dict],
    *,
    selector_confidence: Optional[float] = None,
    min_confidence: float = DEFAULT_SELECTOR_MIN_CONFIDENCE,
    require_approved: bool = True,
) -> Tuple[bool, str]:
    """
    Production gate: only scrape when selectors are high-confidence, validated,
    and (optionally) manually approved.
    """
    if not isinstance(selector_hints, dict) or not selector_hints:
        return False, "selector_hints missing"

    hint = SelectorHint.from_dict(selector_hints)
    ok, reason = assess_selector_hint(hint, min_confidence=min_confidence)
    if not ok:
        return False, reason

    if selector_confidence is not None and float(selector_confidence) < float(min_confidence):
        return False, f"selector_confidence below threshold ({selector_confidence:.2f} < {min_confidence:.2f})"

    validation = selector_hints.get("validation")
    if not isinstance(validation, dict) or not bool(validation.get("passed")):
        return False, "selector validation missing or failed"

    jobs_found = int(validation.get("jobs_found", 0) or 0)
    min_jobs = int(validation.get("min_jobs", DEFAULT_SELECTOR_MIN_JOBS) or DEFAULT_SELECTOR_MIN_JOBS)
    if jobs_found < min_jobs:
        return False, f"validated jobs below minimum ({jobs_found} < {min_jobs})"

    if require_approved:
        status = str(selector_hints.get("review_status", "")).strip().lower()
        if status not in APPROVED_SELECTOR_STATUSES:
            return False, f"review_status '{status or 'unset'}' is not approved"

    return True, "ready"


def get_element_selector(element: Tag) -> str:
    """Generate a CSS selector for an element.

    Args:
        element: BeautifulSoup Tag

    Returns:
        CSS selector string
    """
    parts = [element.name]

    # Add ID if present
    if element.get("id"):
        element_id = str(element["id"]).strip()
        if element_id:
            parts.append(f"#{element_id}")
            return "".join(parts)

    # Prefer stable semantic attributes over CSS classes.
    for attr in ("data-testid", "data-test", "data-qa", "aria-label", "role"):
        attr_value = element.get(attr)
        if not attr_value:
            continue
        text = str(attr_value).strip()
        if not text:
            continue
        safe = text.replace('"', '\\"')
        return f'{element.name}[{attr}="{safe}"]'

    # Add classes
    classes = element.get("class", [])
    if classes:
        # Use first few stable classes.
        meaningful = [c for c in classes if not _is_unstable_class(str(c))]
        meaningful = meaningful[:2]
        for cls in meaningful:
            parts.append(f".{cls}")

    return "".join(parts)


def find_repeated_containers(soup: BeautifulSoup) -> List[Tuple[str, List[Tag]]]:
    """Find container elements that repeat with similar structure.

    Args:
        soup: Parsed HTML

    Returns:
        List of (selector, elements) tuples
    """
    # Find all container elements
    containers = soup.find_all(["div", "li", "article", "section", "tr"])

    # Group by parent and structure
    groups: dict[str, List[Tag]] = {}

    for container in containers:
        parent = container.parent
        if not parent:
            continue

        # Create a structural signature
        children_tags = [c.name for c in container.children if isinstance(c, Tag)]
        has_link = bool(container.find("a", href=True))

        if not has_link:
            continue

        # Key by parent + structure
        parent_selector = get_element_selector(parent) if isinstance(parent, Tag) else "root"
        signature = f"{parent_selector}|{'-'.join(children_tags[:3])}"

        if signature not in groups:
            groups[signature] = []
        groups[signature].append(container)

    # Filter to groups with multiple items (repeated structure)
    return [(sig, elems) for sig, elems in groups.items() if len(elems) >= 3]


def score_container_group(
    containers: List[Tag],
    base_url: str,
) -> Tuple[float, dict]:
    """Score a group of containers as potential job listings.

    Args:
        containers: List of candidate container elements
        base_url: Base URL for resolving links

    Returns:
        Tuple of (score, metadata dict)
    """
    score = 0.0
    metadata = {
        "count": len(containers),
        "has_titles": 0,
        "has_locations": 0,
        "has_links": 0,
        "avg_text_length": 0,
    }

    text_lengths = []

    for container in containers:
        text = container.get_text(strip=True)
        text_lengths.append(len(text))

        # Check for job title patterns
        text_lower = text.lower()
        if any(re.search(p, text_lower) for p in TITLE_PATTERNS):
            metadata["has_titles"] += 1

        # Check for location patterns
        if any(re.search(p, text, re.IGNORECASE) for p in LOCATION_PATTERNS):
            metadata["has_locations"] += 1

        # Check for links
        links = container.find_all("a", href=True)
        if links:
            metadata["has_links"] += 1

    if text_lengths:
        metadata["avg_text_length"] = sum(text_lengths) / len(text_lengths)

    # Calculate score
    # More items = more likely to be a job list
    if metadata["count"] >= 5:
        score += 0.2
    elif metadata["count"] >= 3:
        score += 0.1

    # Title patterns are strong signal
    title_ratio = metadata["has_titles"] / len(containers)
    score += title_ratio * 0.3

    # Location patterns are moderate signal
    location_ratio = metadata["has_locations"] / len(containers)
    score += location_ratio * 0.2

    # Links are required
    link_ratio = metadata["has_links"] / len(containers)
    if link_ratio < 0.5:
        score *= 0.5  # Penalize low link ratio
    else:
        score += link_ratio * 0.2

    # Reasonable text length
    avg_len = metadata["avg_text_length"]
    if 20 <= avg_len <= 500:
        score += 0.1

    return min(score, 1.0), metadata


def extract_selectors_from_container(
    container: Tag,
    base_url: str,
) -> dict:
    """Extract potential selectors from a sample container.

    Args:
        container: Sample job listing container
        base_url: Base URL for resolving links

    Returns:
        Dict with selector hints
    """
    selectors = {}

    # Container selector
    selectors["job_container"] = get_element_selector(container)

    # Find the main link (likely job title link)
    links = container.find_all("a", href=True)
    if links:
        # Prefer links with longer text (more likely to be title)
        links_with_text = [(a, len(a.get_text(strip=True))) for a in links]
        links_with_text.sort(key=lambda x: -x[1])

        main_link = links_with_text[0][0]
        link_selector = get_element_selector(main_link)
        selectors["link"] = link_selector if _is_selector_stable(link_selector, allow_generic=True) else "a[href]"

        # Title is likely the link text or nearby heading
        heading = container.find(["h1", "h2", "h3", "h4", "h5", "h6"])
        if heading:
            heading_selector = get_element_selector(heading)
            selectors["title"] = (
                heading_selector
                if _is_selector_stable(heading_selector, allow_generic=True)
                else heading.name
            )
        else:
            selectors["title"] = selectors["link"]  # Fall back to link text

    # Look for location
    for elem in container.descendants:
        if not isinstance(elem, Tag):
            continue
        text = elem.get_text(strip=True)
        if text and any(re.search(p, text, re.IGNORECASE) for p in LOCATION_PATTERNS):
            candidate = get_element_selector(elem)
            if _is_selector_stable(candidate, allow_generic=True):
                selectors["location"] = candidate
                break

    return selectors


def generate_selector_hints(
    html: str,
    base_url: str,
    min_confidence: float = DEFAULT_SELECTOR_MIN_CONFIDENCE,
) -> Optional[SelectorHint]:
    """Generate CSS selector hints for a custom careers page.

    This uses basic heuristics and is NOT production-ready.
    Results require human validation.

    Args:
        html: HTML content of careers page
        base_url: Base URL for resolving links

    Returns:
        SelectorHint with detected selectors, or None if no candidates found
    """
    soup = BeautifulSoup(html, "html.parser")

    # Find repeated containers
    groups = find_repeated_containers(soup)
    if not groups:
        logger.debug("No repeated containers found")
        return None

    # Score each group
    scored_groups = []
    for signature, containers in groups:
        score, metadata = score_container_group(containers, base_url)
        if score >= min_confidence:
            scored_groups.append((score, signature, containers, metadata))

    if not scored_groups:
        logger.debug(f"No container groups scored above threshold {min_confidence:.2f}")
        return None

    # Take the highest scoring group
    scored_groups.sort(key=lambda x: -x[0])
    best_score, _, best_containers, metadata = scored_groups[0]

    # Extract selectors from first container as sample
    selectors = extract_selectors_from_container(best_containers[0], base_url)

    if not selectors.get("job_container") or not selectors.get("link"):
        return None

    hint = SelectorHint(
        job_container=selectors.get("job_container"),
        title=selectors.get("title"),
        link=selectors.get("link"),
        location=selectors.get("location"),
        confidence=best_score,
        sample_count=len(best_containers),
        notes=f"Auto-detected from {len(best_containers)} items. "
              f"Titles: {metadata['has_titles']}, "
              f"Locations: {metadata['has_locations']}",
    )

    ok, reason = assess_selector_hint(hint, min_confidence=min_confidence)
    if not ok:
        hint.notes = _hint_note(hint.notes, f"rejected: {reason}")
        logger.info(f"Discarded selector hints: {reason}")
        return None

    logger.info(f"Generated selector hints with confidence {best_score:.2f}")
    return hint


class SelectorDetector:
    """Detects CSS selectors for custom career pages."""

    async def detect(
        self,
        html: str,
        base_url: str,
        min_confidence: float = DEFAULT_SELECTOR_MIN_CONFIDENCE,
    ) -> Optional[SelectorHint]:
        """Detect selector hints for a careers page.

        Args:
            html: HTML content
            base_url: Base URL of the page

        Returns:
            SelectorHint or None if detection failed
        """
        return generate_selector_hints(html, base_url, min_confidence=min_confidence)
