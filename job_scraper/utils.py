"""
Utility functions for job scraping
"""
import asyncio
import html
import logging
import os
import random
import re
import time
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from typing import List, Optional

import bleach

from .models import Job

logger = logging.getLogger(__name__)

_REDACT_ENV_VARS = [
    "ADZUNA_APP_ID",
    "ADZUNA_APP_KEY",
    "USAJOBS_API_KEY",
    "USAJOBS_USER_AGENT",
    "CAREERONESTOP_API_KEY",
    "CAREERONESTOP_USER_ID",
    "RAPIDAPI_KEY",
    "THEMUSE_API_KEY",
    "FINDWORK_API_KEY",
    "JOB_SCRAPER_DB_DSN",
    "DATABASE_URL",
    "JOB_SCRAPER_API_KEY",
]

_REDACT_PATTERNS = [
    re.compile(r"(postgresql(?:\+psycopg)?://[^:]+:)([^@]+)(@)"),
    re.compile(r"(postgres://[^:]+:)([^@]+)(@)"),
]


def deduplicate_jobs(jobs: List[Job]) -> List[Job]:
    """
    Remove duplicate jobs based on unique_key

    Args:
        jobs: List of Job objects

    Returns:
        Deduplicated list of jobs
    """
    seen = set()
    unique_jobs = []

    for job in jobs:
        key = build_dedupe_key(job)
        if key not in seen:
            seen.add(key)
            unique_jobs.append(job)
        else:
            logger.debug(f"Duplicate job found: {job.title} at {job.company}")

    logger.info(f"Deduplicated {len(jobs)} jobs down to {len(unique_jobs)}")
    return unique_jobs


def sanitize_html(text: Optional[str]) -> Optional[str]:
    """Sanitize HTML content, stripping all tags to plain text."""
    if not text:
        return text
    # Some sources return HTML as escaped entities (e.g., "&lt;div&gt;...").
    # Unescape first so bleach can strip tags properly.
    return bleach.clean(html.unescape(text), tags=[], strip=True)


def normalize_text(value: Optional[str]) -> str:
    if not value:
        return ""
    return " ".join(value.strip().split())


def normalize_url(url: Optional[str]) -> str:
    if not url:
        return ""
    return normalize_text(url).rstrip("/")


def build_dedupe_key(job: Job) -> str:
    url = normalize_url(job.url)
    title = normalize_text(job.title)
    company = normalize_text(job.company or "")
    if url:
        base = f"{url}|{title}|{company}"
    else:
        base = f"{title}|{company}"
    return base.lower()


_US_STATE_CODES = {
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
    "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
    "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
    "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
    "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
    "DC",
}

_US_STATE_NAMES = {
    "alabama", "alaska", "arizona", "arkansas", "california", "colorado",
    "connecticut", "delaware", "florida", "georgia", "hawaii", "idaho",
    "illinois", "indiana", "iowa", "kansas", "kentucky", "louisiana",
    "maine", "maryland", "massachusetts", "michigan", "minnesota",
    "mississippi", "missouri", "montana", "nebraska", "nevada",
    "new hampshire", "new jersey", "new mexico", "new york",
    "north carolina", "north dakota", "ohio", "oklahoma", "oregon",
    "pennsylvania", "rhode island", "south carolina", "south dakota",
    "tennessee", "texas", "utah", "vermont", "virginia", "washington",
    "west virginia", "wisconsin", "wyoming", "district of columbia",
}

_NON_US_COUNTRY_TOKENS = {
    "united kingdom", "uk", "canada", "germany", "india", "australia",
    "ireland", "france", "spain", "italy", "netherlands", "sweden",
    "norway", "denmark", "finland", "poland", "singapore", "mexico",
    "brazil", "china", "japan", "south africa", "new zealand",
}


def _is_us_location(text: str) -> bool:
    t = text.lower()
    if "united states" in t or "usa" in t or "u.s." in t:
        return True
    for name in _US_STATE_NAMES:
        if name in t:
            return True
    # Check state abbreviations in uppercase word-boundary form
    upper = text.upper()
    for code in _US_STATE_CODES:
        if re.search(rf"\b{code}\b", upper):
            return True
    return False


def _is_non_us_location(text: str) -> bool:
    t = text.lower()
    for token in _NON_US_COUNTRY_TOKENS:
        if token in t:
            return True
    return False


def is_us_job(job: Job) -> bool:
    location = normalize_text(job.location or "")
    if location:
        if _is_non_us_location(location):
            return False
        if _is_us_location(location):
            return True
    # Best-effort for remote jobs without explicit location
    if job.remote:
        return True
    return False


def is_us_job_for_source(job: Job, us_scoped_sources: set[str]) -> bool:
    if (job.source or "").lower() in us_scoped_sources:
        # Treat missing/ambiguous locations as US for US-only sources.
        if not normalize_text(job.location or ""):
            return True
    return is_us_job(job)


def parse_posted_date(posted_date: Optional[str]) -> Optional[datetime]:
    """
    Parse heterogeneous posted_date values into UTC datetime.

    Returns None when date cannot be parsed.
    """
    if posted_date is None:
        return None

    raw = str(posted_date).strip()
    if not raw:
        return None

    lower = raw.lower()
    now = datetime.now(timezone.utc)

    if lower == "today":
        return now
    if lower == "yesterday":
        return now - timedelta(days=1)

    relative_match = re.match(r"^(\d+)\s+(minute|hour|day|week|month|year)s?\s+ago$", lower)
    if relative_match:
        count = int(relative_match.group(1))
        unit = relative_match.group(2)
        delta_map = {
            "minute": timedelta(minutes=count),
            "hour": timedelta(hours=count),
            "day": timedelta(days=count),
            "week": timedelta(weeks=count),
            "month": timedelta(days=30 * count),
            "year": timedelta(days=365 * count),
        }
        return now - delta_map[unit]

    # Numeric timestamp handling (seconds or milliseconds)
    if re.fullmatch(r"\d{10,13}", raw):
        try:
            ts = float(raw)
            if ts > 1e12:
                ts /= 1000.0
            return datetime.fromtimestamp(ts, tz=timezone.utc)
        except (TypeError, ValueError, OSError):
            pass

    # ISO and RFC formats
    try:
        iso = raw.replace("Z", "+00:00")
        dt = datetime.fromisoformat(iso)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except ValueError:
        pass

    try:
        dt = parsedate_to_datetime(raw)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except (TypeError, ValueError):
        pass

    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%m/%d/%Y", "%d-%m-%Y", "%Y-%m-%d %H:%M:%S"):
        try:
            dt = datetime.strptime(raw, fmt).replace(tzinfo=timezone.utc)
            return dt
        except ValueError:
            continue

    return None


def filter_recent_jobs(jobs: List[Job], max_age_days: int) -> tuple[List[Job], int]:
    """
    Remove jobs older than max_age_days when posted_date can be parsed.

    Jobs with unknown posted_date are kept to avoid false negatives.
    """
    if max_age_days <= 0:
        return jobs, 0

    cutoff = datetime.now(timezone.utc) - timedelta(days=max_age_days)
    fresh_jobs: List[Job] = []
    dropped = 0

    for job in jobs:
        posted_at = parse_posted_date(job.posted_date)
        if posted_at is None or posted_at >= cutoff:
            fresh_jobs.append(job)
        else:
            dropped += 1

    return fresh_jobs, dropped


async def retry_async(func, max_retries: int = 3, delay: float = 1.0):
    """
    Retry an async function with exponential backoff

    Args:
        func: Async function to retry
        max_retries: Maximum number of retries
        delay: Initial delay in seconds

    Returns:
        Result of function call
    """
    last_exception = None

    for attempt in range(max_retries):
        try:
            return await func()
        except Exception as e:
            last_exception = e
            if attempt < max_retries - 1:
                wait_time = delay * (2**attempt)
                logger.warning(
                    f"Attempt {attempt + 1}/{max_retries} failed: {e}. "
                    f"Retrying in {wait_time}s..."
                )
                await asyncio.sleep(wait_time)
            else:
                logger.error(f"All {max_retries} attempts failed")

    raise last_exception


class ExponentialBackoff:
    """
    Exponential backoff helper for rate limiting and retries.

    Provides exponential growth with optional jitter and Retry-After header parsing.
    """

    def __init__(self, base_seconds: float = 2.0, max_seconds: float = 300.0, jitter: bool = True):
        """
        Initialize backoff configuration.

        Args:
            base_seconds: Base delay in seconds (default: 2.0)
            max_seconds: Maximum delay cap in seconds (default: 300.0 = 5 minutes)
            jitter: Add random 0-25% jitter to avoid thundering herd (default: True)
        """
        self.base_seconds = base_seconds
        self.max_seconds = max_seconds
        self.jitter = jitter

    def get_delay(self, attempt: int) -> float:
        """
        Calculate delay for given attempt number using exponential backoff.

        Formula: min(base * (2 ^ attempt), max_seconds) + optional jitter

        Args:
            attempt: Attempt number (0-indexed)

        Returns:
            Delay in seconds

        Examples:
            >>> backoff = ExponentialBackoff(base_seconds=2.0, jitter=False)
            >>> backoff.get_delay(0)  # 2 seconds
            >>> backoff.get_delay(1)  # 4 seconds
            >>> backoff.get_delay(2)  # 8 seconds
            >>> backoff.get_delay(3)  # 16 seconds
        """
        # Calculate exponential delay: base * (2 ^ attempt)
        delay = self.base_seconds * (2 ** attempt)

        # Cap at max_seconds
        delay = min(delay, self.max_seconds)

        # Add jitter (random 0-25% of delay)
        if self.jitter:
            jitter_amount = delay * random.uniform(0, 0.25)
            delay += jitter_amount

        return delay

    def parse_retry_after(self, headers: dict) -> Optional[float]:
        """
        Parse Retry-After header from HTTP response.

        Supports both formats:
        - Delay in seconds: "Retry-After: 120"
        - HTTP-date: "Retry-After: Wed, 21 Oct 2026 07:28:00 GMT"

        Args:
            headers: HTTP response headers (dict-like, case-insensitive)

        Returns:
            Delay in seconds, or None if header not present or invalid

        Examples:
            >>> backoff = ExponentialBackoff()
            >>> backoff.parse_retry_after({"Retry-After": "60"})
            60.0
            >>> backoff.parse_retry_after({"retry-after": "Wed, 21 Oct 2026 07:28:00 GMT"})
            # Returns seconds until that datetime
        """
        # Handle case-insensitive header lookup
        retry_after = None
        for key, value in headers.items():
            if key.lower() == "retry-after":
                retry_after = value
                break

        if not retry_after:
            return None

        # Try parsing as integer (seconds)
        try:
            return float(retry_after)
        except (ValueError, TypeError):
            pass

        # Try parsing as HTTP-date
        try:
            retry_datetime = parsedate_to_datetime(retry_after)
            now = time.time()
            retry_timestamp = retry_datetime.timestamp()
            delay = max(0, retry_timestamp - now)
            return delay
        except (ValueError, TypeError, AttributeError):
            pass

        return None


def setup_logging(level: str = "INFO", config: Optional[dict] = None):
    """Setup logging configuration"""
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    _install_log_redaction(config)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)


def _install_log_redaction(config: Optional[dict] = None) -> None:
    secrets = _collect_redact_values(config)
    if not secrets and not _REDACT_PATTERNS:
        return
    logging.getLogger().addFilter(_RedactingFilter(secrets, _REDACT_PATTERNS))


def _collect_redact_values(config: Optional[dict] = None) -> List[str]:
    values = []

    # Collect from environment variables
    for env_name in _REDACT_ENV_VARS:
        val = os.getenv(env_name)
        if val:
            values.append(val)

    # Collect from config/secrets if provided
    if config:
        for section in config.values():
            if isinstance(section, dict):
                for key, val in section.items():
                    if any(k in key.lower() for k in ('key', 'secret', 'password', 'token', 'dsn', 'app_id')):
                        if val and isinstance(val, str) and len(val) > 4:
                            values.append(val)

    # Collect from extra redact values env var
    extra = os.getenv("JOB_SCRAPER_REDACT_VALUES")
    if extra:
        values.extend([v.strip() for v in extra.split(",") if v.strip()])

    # Preserve order while removing duplicates
    seen = set()
    unique_values = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        unique_values.append(value)
    return unique_values


class _RedactingFilter(logging.Filter):
    def __init__(self, secrets: List[str], patterns: List[re.Pattern]):
        super().__init__()
        self._secrets = [s for s in secrets if s]
        self._patterns = patterns

    def filter(self, record: logging.LogRecord) -> bool:
        message = record.getMessage()
        redacted = message

        for secret in self._secrets:
            if secret in redacted:
                redacted = redacted.replace(secret, "[REDACTED]")

        for pattern in self._patterns:
            redacted = pattern.sub(r"\1[REDACTED]\3", redacted)

        if redacted != message:
            record.msg = redacted
            record.args = ()

        return True
