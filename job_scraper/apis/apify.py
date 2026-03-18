"""
Apify integration — runs one or more job-scraping actors on Apify.

Supports multiple actors in a single source so you can scrape Indeed +
LinkedIn + Google Jobs in one scheduled run.

Free-tier budget: $5/month ≈ 16.7 CU.  Default max_items=200 targets
~$4.50-5.00/month across 3 actors daily.  Compute usage is logged per run
so you can tune per-actor caps in config.yaml if costs drift.

Configure via env vars (comma-separated for multiple actors):
    APIFY_API_TOKEN   — Apify API token (required)
    APIFY_ACTOR_IDS   — comma-separated actor IDs (all 3 default actors enabled)
    APIFY_MAX_ITEMS   — global per-actor item cap (default: 200)
    APIFY_COUNTRY     — country code or full name (default: US)

Or in config.yaml:
    apify:
      api_token: "apft_..."
      actors:
        - id: "memo23/apify-indeed-cheerio-keywords-ppr"
          label: "indeed"
          max_items: 200         # per-actor override (optional)
        - id: "worldunboxer/rapid-linkedin-scraper"
          label: "linkedin"
          max_items: 100         # LinkedIn may ignore rows; cap client-side
        - id: "orgupdate/google-jobs-scraper"
          label: "google-jobs"
          max_items: 200
      max_items: 200             # global default
      country: "US"

Requires:
    pip install apify-client
"""
import logging
import time
from typing import Dict, List, Optional, Tuple

from . import BaseJobAPI, BoardResult, TrackedJob
from ..models import Job

logger = logging.getLogger(__name__)

# Map common country codes to the full names expected by most Apify actors.
_COUNTRY_CODE_MAP = {
    "US": "United States",
    "UK": "United Kingdom",
    "GB": "United Kingdom",
    "CA": "Canada",
    "AU": "Australia",
    "DE": "Germany",
    "FR": "France",
    "IN": "India",
    "NL": "Netherlands",
    "SG": "Singapore",
    "AE": "United Arab Emirates",
    "KR": "South Korea",
    "SA": "Saudi Arabia",
    "NZ": "New Zealand",
    "ZA": "South Africa",
    "CZ": "Czech Republic",
    "HK": "Hong Kong",
    "CR": "Costa Rica",
}

# Priority order — first match wins for each target field.
# Covers field names from Indeed, LinkedIn (rapid), Google Jobs, and others.
_TARGET_PRIORITY: List[Tuple[str, List[str]]] = [
    ("title",           ["positionName", "position", "jobTitle", "job_title", "title", "name"]),
    ("company",         ["company", "companyName", "company_name"]),
    ("location",        ["location"]),
    ("salary",          ["salary", "salary_range"]),
    ("url",             ["url", "link", "job_url", "URL"]),
    ("description",     ["description", "job_description"]),
    ("employment_type", ["jobType", "employmentType"]),
    ("posted_date",     ["postedAt", "datePosted", "date", "time_posted", "posted_via"]),
]


# ---------------------------------------------------------------------------
# Per-actor input builders
# ---------------------------------------------------------------------------
# Each actor has its own expected input schema.  We map our generic
# (query, location, country, maxItems) to the actor-specific fields.

def _build_input_indeed(
    query: Optional[str], location: Optional[str],
    country: str, cap: int,
) -> Dict:
    """memo23/apify-indeed-cheerio-keywords-ppr"""
    inp: Dict = {
        "country": country,
        "maxItems": cap,
        "saveOnlyUniqueItems": True,
    }
    if query:
        inp["position"] = query
    if location:
        inp["location"] = location
    return inp


def _build_input_rapid_linkedin(
    query: Optional[str], location: Optional[str],
    country: str, cap: int,
) -> Dict:
    """worldunboxer/rapid-linkedin-scraper — uses 'location' for country,
    'rows' for max items, 'keyword' for search."""
    inp: Dict = {
        "location": country,
        "rows": cap,
    }
    if query:
        inp["keyword"] = query
    return inp


def _build_input_google_jobs(
    query: Optional[str], location: Optional[str],
    country: str, cap: int,
) -> Dict:
    """orgupdate/google-jobs-scraper — uses 'queries', 'countryName', 'maxItems'."""
    inp: Dict = {
        "maxItems": cap,
    }
    if query:
        inp["queries"] = query
    if country:
        inp["countryName"] = country
    if location:
        inp["locationName"] = location
    return inp


def _build_input_generic(
    query: Optional[str], location: Optional[str],
    country: str, cap: int,
) -> Dict:
    """Fallback for unknown actors — send the common fields."""
    inp: Dict = {
        "country": country,
        "maxItems": cap,
        "saveOnlyUniqueItems": True,
    }
    if query:
        inp["position"] = query
    if location:
        inp["location"] = location
    return inp


# Map actor ID → input builder.  Partial matches on the actor name slug.
_ACTOR_INPUT_BUILDERS = {
    "memo23/apify-indeed-cheerio-keywords-ppr": _build_input_indeed,
    "worldunboxer/rapid-linkedin-scraper": _build_input_rapid_linkedin,
    "orgupdate/google-jobs-scraper": _build_input_google_jobs,
}


def _get_input_builder(actor_id: str):
    """Return the input builder for an actor, falling back to generic."""
    return _ACTOR_INPUT_BUILDERS.get(actor_id, _build_input_generic)


# ---------------------------------------------------------------------------
# Actor config parsing
# ---------------------------------------------------------------------------

def _parse_actors_config(actors_raw, actor_ids_csv: Optional[str]) -> List[Dict]:
    """Build a list of actor dicts from config.yaml or env var.

    Returns list of ``{"id": "...", "label": "..."}`` dicts.
    """
    # Prefer structured config.yaml list
    if actors_raw and isinstance(actors_raw, list):
        result = []
        for entry in actors_raw:
            if isinstance(entry, dict) and entry.get("id"):
                actor = {
                    "id": entry["id"],
                    "label": entry.get("label") or entry["id"].split("/")[-1],
                }
                if entry.get("max_items") is not None:
                    actor["max_items"] = int(entry["max_items"])
                result.append(actor)
            elif isinstance(entry, str) and entry.strip():
                result.append({"id": entry.strip(), "label": entry.strip().split("/")[-1]})
        if result:
            return result

    # Fall back to comma-separated env var / single actor_id
    csv = actor_ids_csv or "memo23/apify-indeed-cheerio-keywords-ppr"
    ids = [a.strip() for a in csv.split(",") if a.strip()]
    return [{"id": aid, "label": aid.split("/")[-1]} for aid in ids]


class ApifyAPI(BaseJobAPI):
    """Apify actor-based job scraper — supports multiple actors."""

    def __init__(
        self,
        api_token: Optional[str] = None,
        actor_ids: Optional[str] = None,
        actors: Optional[list] = None,
        max_items: int = 200,
        country: str = "US",
    ):
        super().__init__(name="Apify")
        self.api_token = api_token
        self.actors = _parse_actors_config(actors, actor_ids)
        self.max_items = max_items
        self.country = country

    def is_configured(self) -> bool:
        return bool(self.api_token) and bool(self.actors)

    # ------------------------------------------------------------------
    # search_jobs  (simple interface — merges all actors)
    # ------------------------------------------------------------------

    async def search_jobs(
        self,
        query: Optional[str] = None,
        location: Optional[str] = None,
        max_results: int = 100,
        **kwargs,
    ) -> List[Job]:
        tracked, _ = await self.search_jobs_with_tracking(
            query=query, location=location, max_results=max_results, **kwargs,
        )
        return [t.job for t in tracked]

    # ------------------------------------------------------------------
    # search_jobs_with_tracking  (per-actor board results)
    # ------------------------------------------------------------------

    async def search_jobs_with_tracking(
        self,
        query: Optional[str] = None,
        location: Optional[str] = None,
        max_results: int = 100,
        **kwargs,
    ) -> Tuple[List[TrackedJob], List[BoardResult]]:
        if not self.is_configured():
            logger.warning("Apify: no API token configured — skipping")
            return [], []

        try:
            from apify_client import ApifyClient
        except ImportError:
            logger.error("apify-client not installed — run: pip install apify-client")
            return [], []

        client = ApifyClient(self.api_token)
        cap_per_actor = min(max_results, self.max_items)

        all_tracked: List[TrackedJob] = []
        all_board_results: List[BoardResult] = []

        for actor_cfg in self.actors:
            actor_id = actor_cfg["id"]
            label = actor_cfg["label"]
            # Per-actor cap overrides global cap
            actor_cap = actor_cfg.get("max_items", cap_per_actor)
            start_mono = time.monotonic()

            try:
                jobs = self._run_actor(client, actor_id, label, query, location, actor_cap)
                tracked = [TrackedJob(job=job, board_token=f"apify:{label}") for job in jobs]
                all_tracked.extend(tracked)
                all_board_results.append(BoardResult(
                    source="apify",
                    board_token=f"apify:{label}",
                    jobs_fetched=len(jobs),
                    duration_ms=int((time.monotonic() - start_mono) * 1000),
                ))
            except Exception as exc:
                logger.error("Apify actor %s failed: %s", actor_id, exc)
                all_board_results.append(BoardResult(
                    source="apify",
                    board_token=f"apify:{label}",
                    jobs_fetched=0,
                    error=str(exc),
                    error_code="actor_failed",
                    duration_ms=int((time.monotonic() - start_mono) * 1000),
                ))

        logger.info("Apify: %d actors → %d total jobs", len(self.actors), len(all_tracked))
        return all_tracked, all_board_results

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _run_actor(
        self, client, actor_id: str, label: str,
        query: Optional[str], location: Optional[str], cap: int,
    ) -> List[Job]:
        """Run a single actor synchronously (called from async context)."""
        # Resolve short country codes (e.g. "US") to full names expected by actors
        country_resolved = _COUNTRY_CODE_MAP.get(self.country.upper(), self.country) if self.country else "United States"

        # Build actor-specific input
        builder = _get_input_builder(actor_id)
        run_input = builder(query, location, country_resolved, cap)

        logger.info(
            "Apify: running actor %s [%s] (query=%r, location=%r, max=%d)",
            actor_id, label, query, location, cap,
        )

        run = client.actor(actor_id).call(run_input=run_input)
        if run is None:
            raise RuntimeError(f"Actor {actor_id} run returned None")

        # Log compute usage for budget tracking
        usage = run.get("usage", {})
        cu = usage.get("ACTOR_COMPUTE_UNITS", 0)
        duration_s = run.get("stats", {}).get("runTimeSecs", 0)
        logger.info(
            "Apify: actor %s [%s] finished in %.0fs, %.4f CU (~$%.4f)",
            actor_id, label, duration_s, cu, cu * 0.30,
        )

        dataset_id = run.get("defaultDatasetId")
        if not dataset_id:
            raise RuntimeError(f"Actor {actor_id}: no dataset ID in run result")

        items = list(client.dataset(dataset_id).iterate_items())
        logger.info("Apify: actor %s [%s] returned %d items", actor_id, label, len(items))

        jobs: List[Job] = []
        for item in items:
            job = self._parse_job(item, label)
            if job and job.title:
                jobs.append(job)
            if len(jobs) >= cap:
                break

        return jobs

    def _parse_job(self, item: dict, actor_label: str) -> Optional[Job]:
        """Normalise actor output into a Job model."""
        mapped: Dict[str, object] = {}
        for target, source_keys in _TARGET_PRIORITY:
            for key in source_keys:
                val = item.get(key)
                if val is not None:
                    mapped[target] = val
                    break

        title = mapped.get("title")
        if not title:
            return None

        company = mapped.get("company")

        # employment_type may be a list in some actors
        emp_type = mapped.get("employment_type")
        if isinstance(emp_type, list):
            emp_type = emp_type[0] if emp_type else None

        # Unique ID from the actor's item
        ext_id = item.get("id") or item.get("jobId") or item.get("externalId") or item.get("job_id")

        return Job(
            title=str(title)[:255],
            company=str(company)[:255] if company else "Unknown",
            location=str(mapped.get("location", ""))[:255] or None,
            url=str(mapped.get("url", ""))[:512] or None,
            description=str(mapped.get("description", ""))[:2000] or None,
            salary=str(mapped.get("salary", ""))[:255] or None,
            employment_type=str(emp_type)[:100] if emp_type else None,
            posted_date=str(mapped.get("posted_date", "")) or None,
            source=f"apify:{actor_label}",
            job_id=str(ext_id)[:128] if ext_id else None,
            raw_payload=item,
        )
