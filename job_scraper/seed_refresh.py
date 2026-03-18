"""
Bi-weekly seed refresh: discovers new companies from visa sponsor and Fortune 500
sources, parses them with Groq, and appends genuinely new entries to
data/targets_seed_150.csv.
"""
from __future__ import annotations

import asyncio
import csv
import json
import logging
from pathlib import Path
from typing import Any

import httpx

from .config import Config

logger = logging.getLogger(__name__)

_SOURCES = [
    # --- H1B sponsor databases ---
    {
        "url": "https://www.myvisajobs.com/reports/h1b/",
        "priority": 2,
        "category": "visa_sponsor",
        "label": "MyVisaJobs top H1B sponsors (FY2025)",
    },
    {
        "url": "https://h1bdata.info/topcompanies.php",
        "priority": 2,
        "category": "visa_sponsor",
        "label": "H1BData top filing companies",
    },
    {
        "url": "https://h1bgrader.com/h1b-sponsors",
        "priority": 2,
        "category": "visa_sponsor",
        "label": "H1BGrader sponsor index",
    },
    # --- Fortune / large employer lists ---
    {
        "url": "https://fortune.com/ranking/fortune500/",
        "priority": 3,
        "category": "fortune500",
        "label": "Fortune 500",
    },
]

_TEXT_TRUNCATE = 12_000  # ~3k tokens, leaves room for prompt + response
_GROQ_MODEL = "llama-3.1-8b-instant"

_PROMPT_TEMPLATE = (
    "Extract every company name from the text below. "
    "Return ONLY a JSON array of strings — company names, nothing else. "
    "No explanation, no markdown, no extra keys.\n\n"
    "Text:\n{text}"
)


def _html_to_text(html: str) -> str:
    """Strip HTML tags, scripts, styles → plain text with company names preserved."""
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "nav", "footer", "header", "noscript"]):
        tag.decompose()
    text = soup.get_text(separator=" ", strip=True)
    # Collapse whitespace
    return " ".join(text.split())


def _load_existing_names(csv_path: Path) -> set[str]:
    """Return a set of lowercased company names already in the CSV."""
    if not csv_path.exists():
        return set()
    names: set[str] = set()
    try:
        with open(csv_path, newline="", encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                name = row.get("company_name", "").strip()
                if name:
                    names.add(name.lower())
    except Exception as exc:
        logger.warning("Could not read existing CSV %s: %s", csv_path, exc)
    return names


def _append_to_csv(csv_path: Path, rows: list[dict[str, Any]]) -> None:
    """Append rows to the CSV, creating the file with a header if it does not exist."""
    write_header = not csv_path.exists()
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    with open(csv_path, "a", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=["company_name", "priority", "category"])
        if write_header:
            writer.writeheader()
        writer.writerows(rows)


def _parse_groq_response(content: str) -> list[str]:
    """
    Parse Groq response as a JSON array of strings.
    Drops non-string elements and strips whitespace from valid ones.
    Returns an empty list on parse failure.
    """
    text = content.strip()

    # Strip markdown code fences if present
    if text.startswith("```"):
        lines = text.splitlines()
        # Remove first and last fence lines
        inner = [ln for ln in lines if not ln.startswith("```")]
        text = "\n".join(inner).strip()

    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        logger.warning("Groq response was not valid JSON; skipping. Preview: %.200s", text)
        return []

    if not isinstance(parsed, list):
        logger.warning("Groq response was JSON but not an array; skipping.")
        return []

    result: list[str] = []
    for item in parsed:
        if isinstance(item, str):
            cleaned = item.strip()
            if cleaned:
                result.append(cleaned)
        else:
            logger.debug("Dropping non-string element from Groq array: %r", item)

    return result


async def _refresh_from_source(
    url: str,
    priority: int,
    category: str,
    llm_config: dict[str, Any],
    existing_names: set[str],
) -> list[str]:
    """
    Fetch `url`, send its HTML to Groq, validate the response, and return
    only the company names that are not already in `existing_names`.

    Returns a list of new company name strings (original casing preserved).
    On any failure, logs a warning and returns an empty list.
    """
    try:
        from groq import AsyncGroq  # lazy import — groq is optional
    except ImportError:
        logger.warning("groq package not installed; cannot refresh seeds from %s", url)
        return []

    groq_api_key = llm_config.get("groq_api_key")
    if not groq_api_key:
        logger.warning("No Groq API key configured; cannot refresh from %s", url)
        return []

    # --- Fetch HTML ---
    try:
        async with httpx.AsyncClient(
            timeout=30.0,
            follow_redirects=True,
            headers={"User-Agent": "Mozilla/5.0 (compatible; JobScraper/1.0)"},
        ) as client:
            response = await client.get(url)
            response.raise_for_status()
            html = response.text
    except Exception as exc:
        logger.warning("HTTP fetch failed for %s: %s", url, exc)
        return []

    # Strip HTML → plain text, then truncate to stay within Groq token limits
    text = _html_to_text(html)
    if len(text) > _TEXT_TRUNCATE:
        text = text[:_TEXT_TRUNCATE]

    # --- Call Groq ---
    prompt = _PROMPT_TEMPLATE.format(text=text)
    try:
        client = AsyncGroq(api_key=groq_api_key)
        completion = await client.chat.completions.create(
            model=_GROQ_MODEL,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.0,
        )
        raw_content = completion.choices[0].message.content or ""
    except Exception as exc:
        logger.warning("Groq call failed for %s: %s", url, exc)
        return []

    # --- Parse and deduplicate ---
    names = _parse_groq_response(raw_content)
    logger.info("Groq returned %d company names from %s", len(names), url)

    new_names: list[str] = []
    for name in names:
        if name.lower() not in existing_names:
            new_names.append(name)
            # Update the working set so duplicates within this batch are caught too
            existing_names.add(name.lower())

    return new_names


async def _run_all_sources(data_dir: Path) -> dict[str, Any]:
    """Async implementation: runs both sources and appends results to CSV."""
    cfg = Config()
    llm_config = cfg.llm_parser

    csv_path = data_dir / "targets_seed_150.csv"
    existing_names = _load_existing_names(csv_path)
    logger.info("Loaded %d existing company names from %s", len(existing_names), csv_path)

    total_found = 0
    total_new = 0
    total_existing = 0
    source_results: list[dict[str, Any]] = []

    for source in _SOURCES:
        url: str = source["url"]
        priority: int = source["priority"]
        category: str = source["category"]
        label: str = source["label"]

        logger.info("Refreshing seeds from: %s (%s)", label, url)

        new_names = await _refresh_from_source(
            url=url,
            priority=priority,
            category=category,
            llm_config=llm_config,
            existing_names=existing_names,
        )

        # We can't easily tell "found" vs "existing" from just new_names without
        # a separate pre-dedupe count, so we track what Groq gave back vs what's new.
        found_count = len(new_names)  # post-dedupe count from this source
        total_found += found_count
        total_new += found_count

        if new_names:
            rows = [
                {"company_name": name, "priority": priority, "category": category}
                for name in new_names
            ]
            _append_to_csv(csv_path, rows)
            logger.info(
                "Appended %d new companies from %s (category=%s, priority=%d)",
                len(new_names),
                label,
                category,
                priority,
            )
        else:
            logger.info("No new companies to add from %s", label)

        source_results.append(
            {
                "source": label,
                "url": url,
                "new_added": found_count,
            }
        )

    summary = {
        "total_new_added": total_new,
        "sources": source_results,
        "csv_path": str(csv_path),
    }
    logger.info(
        "Seed refresh complete: %d new companies added total",
        total_new,
    )
    return summary


def _run_post_refresh_pipeline() -> dict[str, Any]:
    """Run discovery pipeline for newly imported seed companies.

    Calls import → resolve URLs → probe ATS sequentially.
    Only processes sites that need work (no URLs, no ATS detected).

    Returns:
        Dict summarising each pipeline step.
    """
    # Import inline to avoid circular imports (run_discovery imports from job_scraper)
    import sys
    sys.path.insert(0, str(Path(__file__).parent.parent))

    from run_discovery import import_seed_companies, resolve_urls, probe_ats

    pipeline_result: dict[str, Any] = {}

    # Step 1: Import seed CSV → scrape_sites
    try:
        inserted = import_seed_companies()
        pipeline_result["imported"] = inserted
        logger.info("Pipeline: imported %d new companies into scrape_sites", inserted)
    except Exception as exc:
        logger.error("Pipeline: import_seed_companies failed: %s", exc)
        pipeline_result["imported"] = 0
        pipeline_result["import_error"] = str(exc)
        return pipeline_result  # No point continuing if import fails

    # Step 2: Resolve URLs for sites missing them
    try:
        resolve_result = resolve_urls(limit=200)
        pipeline_result["resolve"] = resolve_result or {"resolved": 0, "failed": 0}
        logger.info("Pipeline: resolved URLs — %s", resolve_result)
    except Exception as exc:
        logger.error("Pipeline: resolve_urls failed: %s", exc)
        pipeline_result["resolve"] = {"resolved": 0, "failed": 0, "error": str(exc)}

    # Step 3: Probe ATS for sites with URLs but no ATS detected
    try:
        probe_result = probe_ats(limit=200)
        pipeline_result["probe"] = probe_result or {"probed": 0, "ats_counts": {}}
        logger.info("Pipeline: ATS probe — %s", probe_result)
    except Exception as exc:
        logger.error("Pipeline: probe_ats failed: %s", exc)
        pipeline_result["probe"] = {"probed": 0, "ats_counts": {}, "error": str(exc)}

    return pipeline_result


def run_seed_refresh(data_dir: Path | None = None) -> dict[str, Any]:
    """
    Public synchronous entry point.

    Fetches company names from visa sponsor and Fortune 500 sources via Groq,
    deduplicates against the existing CSV, and appends genuinely new entries.
    When new companies are found, automatically runs the discovery pipeline
    (import → resolve URLs → probe ATS).

    Returns a summary dict with keys:
        total_new_added (int), sources (list), csv_path (str),
        pipeline (dict, present when new companies triggered pipeline)
    """
    if data_dir is None:
        data_dir = Path(__file__).parent.parent / "data"

    summary = asyncio.run(_run_all_sources(data_dir))

    # Auto-wire: if new companies were added, run the discovery pipeline
    if summary.get("total_new_added", 0) > 0:
        logger.info(
            "Seed refresh added %d new companies — running discovery pipeline",
            summary["total_new_added"],
        )
        try:
            pipeline_result = _run_post_refresh_pipeline()
            summary["pipeline"] = pipeline_result
        except Exception as exc:
            logger.error("Discovery pipeline failed: %s", exc)
            summary["pipeline"] = {"error": str(exc)}
    else:
        logger.info("No new companies added — skipping discovery pipeline")

    # Always refresh Workday export, even when no new companies were added.
    try:
        import sys
        sys.path.insert(0, str(Path(__file__).parent.parent))
        from run_discovery import export_workday_sites

        workday_result = export_workday_sites(data_dir=data_dir)
        summary["workday_export"] = workday_result
    except Exception as exc:
        logger.error("Workday site export failed: %s", exc)
        summary["workday_export"] = {"exported": 0, "error": str(exc)}

    return summary
