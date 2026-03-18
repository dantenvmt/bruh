"""
FastAPI app for querying stored jobs.
"""
from __future__ import annotations

import base64
import hmac
import json
import os
from collections import Counter
from datetime import datetime, timedelta, timezone
from typing import Any, Optional
from uuid import UUID

from fastapi import APIRouter, Body, Depends, FastAPI, File, HTTPException, Query, Request, UploadFile, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import APIKeyHeader
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address
from sqlalchemy import and_, or_, select, func, text
from starlette.middleware.base import BaseHTTPMiddleware

from ..config import Config
from ..recommendation import MatchProfile, score_job
from ..storage import (
    JobRecord,
    RunRecord,
    RunSourceRecord,
    UserJobEventRecord,
    UserResumeRecord,
    UserSavedJobRecord,
    session_scope,
    get_run_sources,
)
from ..resume import (
    analyze_resume,
    extract_resume_profile,
    extract_text_from_pdf,
    optimize_resume,
    summarize_job,
    ResumeOptimizeError,
)
from ..utils import sanitize_html


API_KEY_HEADER = APIKeyHeader(name="X-API-Key", auto_error=False)
RATE_LIMIT = os.getenv("JOB_SCRAPER_RATE_LIMIT", "60/minute")


def _get_int_env(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        return default


def _get_bool_env(name: str, default: bool = False) -> bool:
    """Parse boolean from environment variable."""
    value = os.getenv(name)
    if value is None:
        return default
    return value.lower() in ("1", "true", "yes", "on")


MAX_QUERY_LENGTH = _get_int_env("JOB_SCRAPER_QUERY_MAX_LEN", 200)
REQUIRE_API_KEY = _get_bool_env("JOB_SCRAPER_REQUIRE_API_KEY", True)
RAW_PAYLOAD_ENABLED = _get_bool_env("JOB_SCRAPER_RAW_PAYLOAD_ENABLED", False)
SANITIZE_HTML = _get_bool_env("JOB_SCRAPER_SANITIZE_HTML", True)
ENABLE_HSTS = _get_bool_env("JOB_SCRAPER_ENABLE_HSTS", False)


def _parse_cors_origins(raw: Optional[str]) -> list[str]:
    if raw is None:
        return [
            "http://localhost:5173",
            "http://127.0.0.1:5173",
        ]
    if not raw.strip():
        return []
    return [item.strip() for item in raw.split(",") if item.strip()]


def _sanitize_like_pattern(value: str) -> str:
    if value is None:
        return ""
    trimmed = value.strip()
    if MAX_QUERY_LENGTH > 0:
        trimmed = trimmed[:MAX_QUERY_LENGTH]
    return trimmed.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def _parse_csv_list(value: Optional[str]) -> list[str]:
    if value is None:
        return []
    text = str(value).strip()
    if not text:
        return []
    items = [part.strip() for part in text.split(",") if part.strip()]
    deduped: list[str] = []
    seen: set[str] = set()
    for item in items:
        key = item.lower()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)
    return deduped


def _normalize_lower_list(values: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        item = str(value or "").strip().lower()
        if not item or item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


def _identity_clause(
    *,
    user_id: Optional[str],
    guest_session_id: Optional[str],
) -> Any:
    if user_id and guest_session_id:
        return or_(
            UserSavedJobRecord.user_id == user_id,
            UserSavedJobRecord.guest_session_id == guest_session_id,
        )
    if user_id:
        return UserSavedJobRecord.user_id == user_id
    return UserSavedJobRecord.guest_session_id == guest_session_id


def _event_identity_clause(
    *,
    user_id: Optional[str],
    guest_session_id: Optional[str],
) -> Any:
    if user_id and guest_session_id:
        return or_(
            UserJobEventRecord.user_id == user_id,
            UserJobEventRecord.guest_session_id == guest_session_id,
        )
    if user_id:
        return UserJobEventRecord.user_id == user_id
    return UserJobEventRecord.guest_session_id == guest_session_id


def _validate_identity(user_id: Optional[str], guest_session_id: Optional[str]) -> None:
    if not user_id and not guest_session_id:
        raise HTTPException(
            status_code=400,
            detail="Either user_id or guest_session_id is required",
        )


def _as_int(value: Optional[Any]) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _to_utc_naive(dt: datetime) -> datetime:
    """
    Convert a datetime to naive UTC.

    Database columns are stored as timezone-naive UTC timestamps.
    """
    if dt.tzinfo is None:
        return dt
    return dt.astimezone(timezone.utc).replace(tzinfo=None)


def _format_utc(dt: datetime) -> str:
    """Format a datetime as an ISO string with a trailing Z (UTC)."""
    aware = dt if dt.tzinfo is not None else dt.replace(tzinfo=timezone.utc)
    return aware.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _encode_cursor(updated_at: datetime, job_id: UUID) -> str:
    """Encode cursor as base64 JSON for stable pagination."""
    payload = {
        "updated_at": _format_utc(updated_at),
        "id": str(job_id),
    }
    return base64.urlsafe_b64encode(json.dumps(payload).encode()).decode()


def _decode_cursor(cursor: str) -> tuple[datetime, UUID]:
    """Decode cursor from base64 JSON."""
    try:
        payload = json.loads(base64.urlsafe_b64decode(cursor.encode()))
        # Handle both naive and aware datetimes
        updated_at_str = payload["updated_at"]
        if updated_at_str.endswith("Z"):
            updated_at_str = updated_at_str[:-1] + "+00:00"
        updated_at = _to_utc_naive(datetime.fromisoformat(updated_at_str))
        return (updated_at, UUID(payload["id"]))
    except Exception as e:
        raise HTTPException(status_code=400, detail="Invalid cursor format")


def _require_api_key(api_key: Optional[str] = Depends(API_KEY_HEADER)) -> Optional[str]:
    expected = os.getenv("JOB_SCRAPER_API_KEY")
    if not expected:
        return None
    if not api_key or not hmac.compare_digest(api_key, expected):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Invalid API key")
    return api_key


class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    """Middleware to add security headers to all responses."""

    async def dispatch(self, request: Request, call_next):
        response = await call_next(request)

        # Add security headers
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
        response.headers["Content-Security-Policy"] = "default-src 'self'"
        response.headers["Permissions-Policy"] = "camera=(), microphone=(), geolocation=()"

        # Add HSTS if enabled
        if ENABLE_HSTS:
            response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"

        return response


_DISABLE_DOCS = _get_bool_env("JOB_SCRAPER_DISABLE_DOCS", False)

app = FastAPI(
    title="Multi-API Job Aggregator",
    docs_url=None if _DISABLE_DOCS else "/docs",
    redoc_url=None if _DISABLE_DOCS else "/redoc",
    openapi_url=None if _DISABLE_DOCS else "/openapi.json",
)

# Module-level Config singleton — avoids re-reading YAML/env on every request
_config: Optional[Config] = None


def _get_config() -> Config:
    global _config
    if _config is None:
        _config = Config()
    return _config
_storage_uri = os.getenv("REDIS_URL") or "memory://"
limiter = Limiter(key_func=get_remote_address, storage_uri=_storage_uri)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

cors_origins = _parse_cors_origins(os.getenv("JOB_SCRAPER_CORS_ORIGINS"))
if cors_origins:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=cors_origins,
        allow_methods=["GET", "POST", "DELETE", "OPTIONS"],
        allow_headers=["X-API-Key", "Content-Type"],
    )

# Add security headers middleware
app.add_middleware(SecurityHeadersMiddleware)

# Versioned router — all business endpoints live under /api/v1
router = APIRouter(prefix="/api/v1")


@app.on_event("startup")
def validate_startup_config():
    """Validate required configuration on startup."""
    if REQUIRE_API_KEY:
        api_key = os.getenv("JOB_SCRAPER_API_KEY")
        if not api_key or not api_key.strip():
            raise RuntimeError(
                "JOB_SCRAPER_REQUIRE_API_KEY is set but JOB_SCRAPER_API_KEY is not configured. "
                "Please set JOB_SCRAPER_API_KEY or disable JOB_SCRAPER_REQUIRE_API_KEY."
            )


def _job_to_dict(job: JobRecord, include_raw: bool = False) -> dict:
    # Sanitize HTML in description if enabled
    description = job.description
    if SANITIZE_HTML and description:
        description = sanitize_html(description)

    # Build human-readable experience range
    exp_min = job.experience_min_years
    exp_max = job.experience_max_years
    exp_level = (job.experience_level or "").strip()
    if exp_min is not None and exp_max is not None:
        experience_range = f"{exp_min}-{exp_max} yrs"
    elif exp_min is not None:
        experience_range = f"{exp_min}+ yrs"
    elif exp_max is not None:
        experience_range = f"Up to {exp_max} yrs"
    elif exp_level:
        experience_range = exp_level.capitalize()
    else:
        experience_range = "Not specified"

    data = {
        "id": str(job.id),
        "dedupe_key": job.dedupe_key,  # Stable identifier for saved jobs
        "source": job.source,
        "source_job_id": job.source_job_id,
        "title": job.title,
        "company": job.company,
        "location": job.location,
        "url": job.url,
        "description": description,
        "salary": job.salary,
        "employment_type": job.employment_type,
        "posted_date": job.posted_date,
        "remote": job.remote,
        "category": job.category,
        "tags": job.tags,
        "skills": job.skills,
        "experience_level": job.experience_level,
        "experience_min_years": job.experience_min_years,
        "experience_max_years": job.experience_max_years,
        "experience_range": experience_range,
        "required_skills": job.required_skills,
        "industry": job.industry,
        "industry_confidence": job.industry_confidence,
        "work_mode": job.work_mode,
        "role_pop_reasons": job.role_pop_reasons,
        "enrichment_version": job.enrichment_version,
        "enrichment_updated_at": job.enrichment_updated_at.isoformat() if job.enrichment_updated_at else None,
        "ai_summary_card": job.ai_summary_card,
        "ai_summary_detail": job.ai_summary_detail or (
            {
                "summary_short": job.ai_summary_card,
                "summary_bullets": job.ai_summary_bullets or [],
                "attention_tags": [],
            }
            if job.ai_summary_card or job.ai_summary_bullets
            else None
        ),
        "ai_summarized_at": job.ai_summarized_at.isoformat() if job.ai_summarized_at else None,
        "created_at": job.created_at.isoformat() if job.created_at else None,
        "updated_at": job.updated_at.isoformat() if job.updated_at else None,
    }
    if include_raw:
        data["raw_payload"] = job.raw_payload
    return data


def _run_source_to_dict(rs: RunSourceRecord) -> dict:
    """Convert RunSourceRecord to dict for API response."""
    return {
        "id": str(rs.id),
        "source": rs.source,
        "source_target": rs.source_target,
        "jobs_fetched": rs.jobs_fetched,
        "jobs_after_dedupe": rs.jobs_after_dedupe,
        "error_message": rs.error_message,
        "error_code": rs.error_code,
        "request_duration_ms": rs.request_duration_ms,
        "created_at": rs.created_at.isoformat() if rs.created_at else None,
    }


def _apply_job_filters(
    stmt,
    *,
    q: Optional[str] = None,
    location: Optional[str] = None,
    source: Optional[str] = None,
    remote: Optional[bool] = None,
    visa: Optional[bool] = None,
    experience_level: Optional[str] = None,
    exp_min_years: Optional[int] = None,
    exp_max_years: Optional[int] = None,
    industry: Optional[str] = None,
    work_mode: Optional[str] = None,
    required_skill: Optional[str] = None,
    salary_visible: Optional[bool] = None,
    posted_within_days: Optional[int] = None,
    active_only: bool = True,
    stale_after_days: int = 7,
):
    if q:
        safe_q = _sanitize_like_pattern(q)
        like = f"%{safe_q}%"
        stmt = stmt.where(
            or_(
                JobRecord.title.ilike(like, escape="\\"),
                JobRecord.company.ilike(like, escape="\\"),
                JobRecord.description.ilike(like, escape="\\"),
            )
        )
    if location:
        safe_location = _sanitize_like_pattern(location)
        stmt = stmt.where(JobRecord.location.ilike(f"%{safe_location}%", escape="\\"))
    if source:
        stmt = stmt.where(JobRecord.source == source)
    else:
        # Exclude thin jobs (no description AND no location) so sparse
        # custom-scraper entries don't bury richer API-sourced results.
        stmt = stmt.where(
            or_(
                JobRecord.description.is_not(None),
                JobRecord.location.is_not(None),
            )
        )
    if remote is not None:
        stmt = stmt.where(JobRecord.remote == remote)
    if visa is not None:
        if visa is True:
            stmt = stmt.where(JobRecord.tags.contains(["visa_friendly"]))
        else:
            stmt = stmt.where(
                or_(
                    JobRecord.tags.is_(None),
                    ~JobRecord.tags.contains(["visa_friendly"]),
                )
            )
    if experience_level:
        stmt = stmt.where(func.lower(JobRecord.experience_level) == experience_level.strip().lower())
    if exp_min_years is not None:
        stmt = stmt.where(
            or_(
                JobRecord.experience_min_years.is_(None),
                JobRecord.experience_min_years >= exp_min_years,
            )
        )
    if exp_max_years is not None:
        stmt = stmt.where(
            or_(
                JobRecord.experience_max_years.is_(None),
                JobRecord.experience_max_years <= exp_max_years,
            )
        )
    if industry:
        safe_industry = _sanitize_like_pattern(industry)
        stmt = stmt.where(JobRecord.industry.ilike(f"%{safe_industry}%", escape="\\"))
    if work_mode:
        stmt = stmt.where(func.lower(JobRecord.work_mode) == work_mode.strip().lower())
    if required_skill:
        normalized_skill = required_skill.strip().lower()
        if normalized_skill:
            stmt = stmt.where(JobRecord.required_skills.contains([normalized_skill]))
    if salary_visible is True:
        stmt = stmt.where(and_(JobRecord.salary.is_not(None), JobRecord.salary != ""))
    elif salary_visible is False:
        stmt = stmt.where(or_(JobRecord.salary.is_(None), JobRecord.salary == ""))
    if posted_within_days is not None:
        cutoff = datetime.utcnow() - timedelta(days=max(0, posted_within_days))
        stmt = stmt.where(JobRecord.updated_at >= cutoff)
    if active_only:
        stale_cutoff = datetime.utcnow() - timedelta(days=max(1, stale_after_days))
        stmt = stmt.where(JobRecord.last_seen_at >= stale_cutoff)
    return stmt


def _score_job_record(job: JobRecord, profile: MatchProfile) -> dict:
    result = score_job(
        posted_date=job.posted_date,
        required_skills=(job.required_skills or job.skills or []),
        experience_min_years=_as_int(job.experience_min_years),
        experience_max_years=_as_int(job.experience_max_years),
        experience_level=job.experience_level,
        industry=job.industry,
        work_mode=job.work_mode,
        salary=job.salary,
        profile=profile,
    )
    return result


def _record_job_event(
    session,
    *,
    job_id: UUID,
    event_type: str,
    user_id: Optional[str] = None,
    guest_session_id: Optional[str] = None,
    surface: Optional[str] = None,
    event_metadata: Optional[dict] = None,
    occurred_at: Optional[datetime] = None,
) -> None:
    session.add(
        UserJobEventRecord(
            job_id=job_id,
            user_id=user_id,
            guest_session_id=guest_session_id,
            event_type=event_type,
            surface=surface,
            event_metadata=event_metadata,
            occurred_at=occurred_at or datetime.utcnow(),
        )
    )


def _saved_row_to_dict(saved: UserSavedJobRecord, job: Optional[JobRecord]) -> dict:
    return {
        "id": str(saved.id),
        "job_id": str(saved.job_id),
        "user_id": saved.user_id,
        "guest_session_id": saved.guest_session_id,
        "saved_at": saved.saved_at.isoformat() if saved.saved_at else None,
        "updated_at": saved.updated_at.isoformat() if saved.updated_at else None,
        "is_active": bool(saved.is_active),
        "job": _job_to_dict(job) if job is not None else None,
    }


@app.get("/health")
def health() -> dict:
    cfg = _get_config()
    result: dict = {"status": "ok"}
    if cfg.db_dsn:
        try:
            with session_scope(cfg.db_dsn) as session:
                session.execute(text("SELECT 1"))
            result["db"] = "connected"
        except Exception:
            result["status"] = "degraded"
            result["db"] = "unreachable"
    return result


@router.get("/jobs")
@limiter.limit(RATE_LIMIT)
def list_jobs(
    request: Request,
    q: Optional[str] = None,
    location: Optional[str] = None,
    source: Optional[str] = None,
    remote: Optional[bool] = None,
    visa: Optional[bool] = None,
    experience_level: Optional[str] = None,
    exp_min_years: Optional[int] = Query(None, ge=0, le=50),
    exp_max_years: Optional[int] = Query(None, ge=0, le=60),
    industry: Optional[str] = None,
    work_mode: Optional[str] = None,
    required_skill: Optional[str] = None,
    salary_visible: Optional[bool] = None,
    posted_within_days: Optional[int] = Query(None, ge=1, le=3650),
    active_only: bool = Query(True, description="Exclude jobs not seen recently (dead jobs filter)"),
    stale_after_days: int = Query(7, ge=1, le=60, description="Jobs not seen within this many days are considered stale"),
    limit: int = Query(20, ge=1, le=100),
    cursor: Optional[str] = None,
    as_of: Optional[str] = None,
    # No api_key - public endpoint for job listings
) -> dict:
    """
    List jobs with cursor-based pagination for stable session feeds.

    - `cursor`: Opaque cursor from previous response's `next_cursor`
    - `as_of`: ISO timestamp to freeze the result set (pass back from first response)

    Response includes `next_cursor` and `as_of` for subsequent requests.
    """
    cfg = _get_config()
    if not cfg.db_dsn:
        raise HTTPException(status_code=500, detail="DB not configured")
    if exp_min_years is not None and exp_max_years is not None and exp_min_years > exp_max_years:
        raise HTTPException(status_code=400, detail="exp_min_years cannot exceed exp_max_years")

    # Parse or create as_of timestamp for snapshot boundary
    if as_of:
        try:
            as_of_str = as_of.replace("Z", "+00:00") if as_of.endswith("Z") else as_of
            as_of_dt = _to_utc_naive(datetime.fromisoformat(as_of_str))
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid as_of timestamp")
    else:
        as_of_dt = datetime.utcnow()

    with session_scope(cfg.db_dsn) as session:
        # Stable sort: (updated_at DESC, id DESC)
        stmt = select(JobRecord).order_by(
            JobRecord.updated_at.desc(),
            JobRecord.id.desc(),
        )

        # Apply snapshot boundary - only jobs updated at or before as_of
        stmt = stmt.where(JobRecord.updated_at <= as_of_dt)

        # Decode offset cursor (int offset into the interleaved pool)
        offset = 0
        if cursor:
            try:
                payload = json.loads(base64.urlsafe_b64decode(cursor.encode()))
                offset = int(payload.get("offset", 0))
            except Exception:
                raise HTTPException(status_code=400, detail="Invalid cursor format")

        # Apply filters
        stmt = _apply_job_filters(
            stmt,
            q=q,
            location=location,
            source=source,
            remote=remote,
            visa=visa,
            experience_level=experience_level,
            exp_min_years=exp_min_years,
            exp_max_years=exp_max_years,
            industry=industry,
            work_mode=work_mode,
            required_skill=required_skill,
            salary_visible=salary_visible,
            posted_within_days=posted_within_days,
            active_only=active_only,
            stale_after_days=stale_after_days,
        )

        # Fetch a large pool, interleave by company across all pages.
        # Pool is capped at 500; as_of snapshot keeps it stable across pages.
        pool_size = 500
        stmt = stmt.limit(pool_size)
        raw = list(session.execute(stmt).scalars().all())

        # Round-robin interleave by company across the entire pool
        from collections import defaultdict
        buckets: dict = defaultdict(list)
        for job in raw:
            buckets[job.company or ""].append(job)
        interleaved: list = []
        while any(buckets.values()):
            for key in list(buckets.keys()):
                if buckets[key]:
                    interleaved.append(buckets[key].pop(0))
                else:
                    del buckets[key]

        page = interleaved[offset: offset + limit]
        has_more = (offset + limit) < len(interleaved)
        jobs = page

        next_cursor = None
        if has_more:
            next_offset = offset + limit
            next_cursor = base64.urlsafe_b64encode(
                json.dumps({"offset": next_offset}).encode()
            ).decode()

        return {
            "items": [_job_to_dict(j) for j in jobs],
            "next_cursor": next_cursor,
            "as_of": _format_utc(as_of_dt),
            "has_more": has_more,
        }


@router.get("/jobs/recommended")
@limiter.limit(RATE_LIMIT)
def list_jobs_recommended(
    request: Request,
    q: Optional[str] = None,
    location: Optional[str] = None,
    source: Optional[str] = None,
    remote: Optional[bool] = None,
    visa: Optional[bool] = None,
    experience_level: Optional[str] = None,
    exp_min_years: Optional[int] = Query(None, ge=0, le=50),
    exp_max_years: Optional[int] = Query(None, ge=0, le=60),
    industry: Optional[str] = None,
    work_mode: Optional[str] = None,
    required_skill: Optional[str] = None,
    salary_visible: Optional[bool] = None,
    posted_within_days: Optional[int] = Query(None, ge=1, le=3650),
    active_only: bool = Query(True, description="Exclude jobs not seen recently (dead jobs filter)"),
    stale_after_days: int = Query(7, ge=1, le=60, description="Jobs not seen within this many days are considered stale"),
    profile_experience_years: Optional[int] = Query(None, ge=0, le=70),
    profile_skills: Optional[str] = None,
    profile_industries: Optional[str] = None,
    profile_work_mode: Optional[str] = None,
    user_id: Optional[str] = None,
    guest_session_id: Optional[str] = None,
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
    as_of: Optional[str] = None,
) -> dict:
    cfg = _get_config()
    if not cfg.db_dsn:
        raise HTTPException(status_code=500, detail="DB not configured")
    if exp_min_years is not None and exp_max_years is not None and exp_min_years > exp_max_years:
        raise HTTPException(status_code=400, detail="exp_min_years cannot exceed exp_max_years")

    if as_of:
        try:
            as_of_str = as_of.replace("Z", "+00:00") if as_of.endswith("Z") else as_of
            as_of_dt = _to_utc_naive(datetime.fromisoformat(as_of_str))
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid as_of timestamp")
    else:
        as_of_dt = datetime.utcnow()

    profile = MatchProfile(
        experience_years=profile_experience_years,
        skills=_parse_csv_list(profile_skills),
        industries=_parse_csv_list(profile_industries),
        work_mode=(profile_work_mode or "").strip().lower() or None,
    )

    # Optional identity parameters are accepted for future personalization.
    _ = user_id
    _ = guest_session_id

    pool_size = int(cfg.recommendation.get("pool_size", 1500))
    pool_size = max(limit + offset, min(max(pool_size, 100), 5000))

    with session_scope(cfg.db_dsn) as session:
        stmt = select(JobRecord).order_by(JobRecord.updated_at.desc(), JobRecord.id.desc())
        stmt = stmt.where(JobRecord.updated_at <= as_of_dt)
        stmt = _apply_job_filters(
            stmt,
            q=q,
            location=location,
            source=source,
            remote=remote,
            visa=visa,
            experience_level=experience_level,
            exp_min_years=exp_min_years,
            exp_max_years=exp_max_years,
            industry=industry,
            work_mode=work_mode,
            required_skill=required_skill,
            salary_visible=salary_visible,
            posted_within_days=posted_within_days,
            active_only=active_only,
            stale_after_days=stale_after_days,
        )
        stmt = stmt.limit(pool_size)
        jobs = list(session.execute(stmt).scalars().all())

    scored_rows: list[dict[str, Any]] = []
    for job in jobs:
        scored = _score_job_record(job, profile)
        item = _job_to_dict(job)
        item["recommendation_score"] = scored["score"]
        item["recommendation_reasons"] = scored["reasons"]
        item["match_breakdown"] = scored["breakdown"]
        item["fit_band"] = scored["fit_band"]
        item["gaps"] = scored["gaps"]
        scored_rows.append(
            {
                "sort_score": scored["score"],
                "updated_at": job.updated_at or datetime.min,
                "id": str(job.id),
                "item": item,
            }
        )

    scored_rows.sort(
        key=lambda x: (
            x["sort_score"],
            x["updated_at"] if isinstance(x["updated_at"], datetime) else datetime(1970, 1, 1),
            x["id"],
        ),
        reverse=True,
    )

    total_candidates = len(scored_rows)
    paged = scored_rows[offset : offset + limit]
    has_more = (offset + limit) < total_candidates
    next_offset = (offset + limit) if has_more else None

    return {
        "items": [row["item"] for row in paged],
        "limit": limit,
        "offset": offset,
        "next_offset": next_offset,
        "has_more": has_more,
        "total_candidates": total_candidates,
        "as_of": _format_utc(as_of_dt),
    }


@router.post("/jobs/{job_id}/match")
@limiter.limit(RATE_LIMIT)
def match_job(
    job_id: UUID,
    request: Request,
    payload: dict = Body(...),
) -> dict:
    cfg = _get_config()
    if not cfg.db_dsn:
        raise HTTPException(status_code=500, detail="DB not configured")

    skills = payload.get("profile_skills") or []
    if len(skills) > 100:
        raise HTTPException(status_code=400, detail="profile_skills exceeds maximum of 100 items")
    industries = payload.get("profile_industries") or []
    if len(industries) > 50:
        raise HTTPException(status_code=400, detail="profile_industries exceeds maximum of 50 items")

    profile = MatchProfile(
        experience_years=_as_int(payload.get("profile_experience_years")),
        skills=skills,
        industries=industries,
        work_mode=(str(payload.get("profile_work_mode") or "").strip().lower() or None),
    )

    with session_scope(cfg.db_dsn) as session:
        job = session.query(JobRecord).filter(JobRecord.id == job_id).first()
        if job is None:
            raise HTTPException(status_code=404, detail=f"Job {job_id} not found")

        scored = _score_job_record(job, profile)
        return {
            "job_id": str(job.id),
            "match_score": scored["score"],
            "fit_band": scored["fit_band"],
            "breakdown": scored["breakdown"],
            "reasons": scored["reasons"],
            "gaps": scored["gaps"],
        }


@router.get("/jobs/raw")
@limiter.limit(RATE_LIMIT)
def list_jobs_raw(
    request: Request,
    q: Optional[str] = None,
    location: Optional[str] = None,
    source: Optional[str] = None,
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    api_key: Optional[str] = Depends(_require_api_key),
) -> dict:
    """
    List jobs with raw payload data included.

    This endpoint is disabled by default for security reasons.
    Enable via JOB_SCRAPER_RAW_PAYLOAD_ENABLED=true environment variable.
    """
    if not RAW_PAYLOAD_ENABLED:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Raw payload endpoint is disabled. Set JOB_SCRAPER_RAW_PAYLOAD_ENABLED=true to enable.",
        )
    cfg = _get_config()
    if not cfg.db_dsn:
        raise HTTPException(status_code=500, detail="DB not configured")
    with session_scope(cfg.db_dsn) as session:
        stmt = select(JobRecord).order_by(JobRecord.updated_at.desc())
        if q:
            safe_q = _sanitize_like_pattern(q)
            like = f"%{safe_q}%"
            stmt = stmt.where(
                or_(
                    JobRecord.title.ilike(like, escape="\\"),
                    JobRecord.company.ilike(like, escape="\\"),
                    JobRecord.description.ilike(like, escape="\\"),
                )
            )
        if location:
            safe_location = _sanitize_like_pattern(location)
            stmt = stmt.where(JobRecord.location.ilike(f"%{safe_location}%", escape="\\"))
        if source:
            stmt = stmt.where(JobRecord.source == source)

        stmt = stmt.limit(limit).offset(offset)
        jobs = session.execute(stmt).scalars().all()
        return {"items": [_job_to_dict(j, include_raw=True) for j in jobs], "limit": limit, "offset": offset}


@router.get("/runs")
@limiter.limit(RATE_LIMIT)
def list_runs(
    request: Request,
    limit: int = Query(20, ge=1, le=200),
    api_key: Optional[str] = Depends(_require_api_key),
) -> dict:
    cfg = _get_config()
    if not cfg.db_dsn:
        raise HTTPException(status_code=500, detail="DB not configured")
    with session_scope(cfg.db_dsn) as session:
        stmt = select(RunRecord).order_by(RunRecord.started_at.desc()).limit(limit)
        runs = session.execute(stmt).scalars().all()
        items = []
        for r in runs:
            items.append(
                {
                    "id": str(r.id),
                    "started_at": r.started_at.isoformat() if r.started_at else None,
                    "ended_at": r.ended_at.isoformat() if r.ended_at else None,
                    "status": r.status,
                    "sources": r.sources,
                    "total_jobs": r.total_jobs,
                }
            )
        return {"items": items, "limit": limit}


@router.get("/runs/{run_id}/sources")
@limiter.limit(RATE_LIMIT)
def get_run_sources_endpoint(
    run_id: UUID,
    request: Request,
    limit: int = Query(100, ge=1, le=200),
    offset: int = Query(0, ge=0),
    api_key: Optional[str] = Depends(_require_api_key),
) -> dict:
    """
    Get all source results for a specific run.

    Returns paginated list of source fetch results with job counts, errors, and timing data.
    """
    cfg = _get_config()
    if not cfg.db_dsn:
        raise HTTPException(status_code=500, detail="DB not configured")

    # Check if run exists
    with session_scope(cfg.db_dsn) as session:
        run_exists = session.query(RunRecord).filter(RunRecord.id == run_id).first()
        if not run_exists:
            raise HTTPException(status_code=404, detail=f"Run {run_id} not found")

    # Get source results
    all_sources = get_run_sources(cfg.db_dsn, run_id)
    total = len(all_sources)

    # Apply pagination
    paginated_sources = all_sources[offset : offset + limit]

    return {
        "items": [_run_source_to_dict(rs) for rs in paginated_sources],
        "total": total,
        "limit": limit,
        "offset": offset,
    }


@router.get("/runs/{run_id}/errors")
@limiter.limit(RATE_LIMIT)
def get_run_errors_endpoint(
    run_id: UUID,
    request: Request,
    api_key: Optional[str] = Depends(_require_api_key),
) -> dict:
    """
    Get error summary for a specific run.

    Returns all sources with errors, grouped by source and error_code.
    """
    cfg = _get_config()
    if not cfg.db_dsn:
        raise HTTPException(status_code=500, detail="DB not configured")

    # Check if run exists
    with session_scope(cfg.db_dsn) as session:
        run_exists = session.query(RunRecord).filter(RunRecord.id == run_id).first()
        if not run_exists:
            raise HTTPException(status_code=404, detail=f"Run {run_id} not found")

    # Single query - build summary inline, no second round-trip to the DB.
    all_sources = get_run_sources(cfg.db_dsn, run_id)
    error_sources = [rs for rs in all_sources if rs.error_message]

    by_source: dict = {}
    by_error_code: dict = {}
    for rs in all_sources:
        src = rs.source
        if src not in by_source:
            by_source[src] = {"total": 0, "successful": 0, "failed": 0, "errors": []}
        by_source[src]["total"] += 1
        if rs.error_message:
            by_source[src]["failed"] += 1
            by_source[src]["errors"].append({
                "target": rs.source_target,
                "error_code": rs.error_code,
                "message": rs.error_message,
            })
        else:
            by_source[src]["successful"] += 1
        if rs.error_code:
            if rs.error_code not in by_error_code:
                by_error_code[rs.error_code] = {"count": 0, "sources": []}
            by_error_code[rs.error_code]["count"] += 1
            by_error_code[rs.error_code]["sources"].append({
                "source": rs.source,
                "target": rs.source_target,
            })

    return {
        "items": [_run_source_to_dict(rs) for rs in error_sources],
        "total": len(error_sources),
        "by_source": by_source,
        "by_error_code": by_error_code,
    }


@router.post("/analytics/events")
@limiter.limit("100/minute")
def post_analytics_events(
    request: Request,
    payload: Any = Body(...),
    api_key: Optional[str] = Depends(_require_api_key),
) -> dict:
    cfg = _get_config()
    if not cfg.db_dsn:
        raise HTTPException(status_code=500, detail="DB not configured")

    max_batch = int(cfg.analytics.get("max_batch", 50))

    if isinstance(payload, list):
        events = payload
    elif isinstance(payload, dict):
        events = payload.get("events", [])
    else:
        raise HTTPException(status_code=400, detail="Payload must be an object with 'events' or a list")

    if not isinstance(events, list):
        raise HTTPException(status_code=400, detail="'events' must be a list")
    if len(events) > max_batch:
        raise HTTPException(
            status_code=400,
            detail=f"Batch size exceeds limit ({len(events)} > {max_batch})",
        )
    if not events:
        return {"accepted": 0, "max_batch": max_batch}

    normalized_events: list[dict[str, Any]] = []
    job_ids: set[UUID] = set()

    for idx, event in enumerate(events):
        if not isinstance(event, dict):
            raise HTTPException(status_code=400, detail=f"Event at index {idx} must be an object")

        raw_job_id = event.get("job_id")
        raw_user_id = (event.get("user_id") or None)
        raw_guest = (event.get("guest_session_id") or None)
        event_type = str(event.get("event_type") or "").strip().lower()
        surface = (str(event.get("surface") or "").strip() or None)
        metadata = event.get("event_metadata")
        raw_occurred_at = event.get("occurred_at")

        if not event_type:
            raise HTTPException(status_code=400, detail=f"Event at index {idx} missing event_type")
        if len(event_type) > 50:
            raise HTTPException(status_code=400, detail=f"Event at index {idx} has event_type > 50 chars")
        if surface and len(surface) > 50:
            raise HTTPException(status_code=400, detail=f"Event at index {idx} has surface > 50 chars")
        if metadata is not None and not isinstance(metadata, dict):
            raise HTTPException(status_code=400, detail=f"Event at index {idx} event_metadata must be an object")
        if raw_user_id and len(str(raw_user_id)) > 128:
            raise HTTPException(status_code=400, detail=f"Event at index {idx} user_id exceeds 128 chars")
        if raw_guest and len(str(raw_guest)) > 128:
            raise HTTPException(status_code=400, detail=f"Event at index {idx} guest_session_id exceeds 128 chars")
        if not raw_user_id and not raw_guest:
            raise HTTPException(
                status_code=400,
                detail=f"Event at index {idx} requires user_id or guest_session_id",
            )

        try:
            job_id = UUID(str(raw_job_id))
        except Exception:
            raise HTTPException(status_code=400, detail=f"Event at index {idx} has invalid job_id")

        occurred_at = datetime.utcnow()
        if raw_occurred_at:
            try:
                text = str(raw_occurred_at)
                text = text.replace("Z", "+00:00") if text.endswith("Z") else text
                occurred_at = _to_utc_naive(datetime.fromisoformat(text))
            except Exception:
                raise HTTPException(status_code=400, detail=f"Event at index {idx} has invalid occurred_at")

        job_ids.add(job_id)
        normalized_events.append(
            {
                "job_id": job_id,
                "user_id": raw_user_id,
                "guest_session_id": raw_guest,
                "event_type": event_type,
                "surface": surface,
                "event_metadata": metadata,
                "occurred_at": occurred_at,
            }
        )

    with session_scope(cfg.db_dsn) as session:
        existing_job_ids = {
            row[0]
            for row in session.execute(select(JobRecord.id).where(JobRecord.id.in_(list(job_ids)))).all()
        }
        missing = [str(job_id) for job_id in job_ids if job_id not in existing_job_ids]
        if missing:
            raise HTTPException(status_code=404, detail=f"Unknown job_id(s): {', '.join(missing[:5])}")

        for event in normalized_events:
            _record_job_event(
                session,
                job_id=event["job_id"],
                user_id=event["user_id"],
                guest_session_id=event["guest_session_id"],
                event_type=event["event_type"],
                surface=event["surface"],
                event_metadata=event["event_metadata"],
                occurred_at=event["occurred_at"],
            )

    return {
        "accepted": len(normalized_events),
        "max_batch": max_batch,
    }


@router.get("/analytics/jobs/{job_id}")
@limiter.limit(RATE_LIMIT)
def get_job_analytics(
    job_id: UUID,
    request: Request,
    window_days: int = Query(30, ge=1, le=365),
) -> dict:
    cfg = _get_config()
    if not cfg.db_dsn:
        raise HTTPException(status_code=500, detail="DB not configured")

    cutoff = datetime.utcnow() - timedelta(days=window_days)

    with session_scope(cfg.db_dsn) as session:
        job = session.query(JobRecord).filter(JobRecord.id == job_id).first()
        if job is None:
            raise HTTPException(status_code=404, detail=f"Job {job_id} not found")

        events = (
            session.query(UserJobEventRecord)
            .filter(
                UserJobEventRecord.job_id == job_id,
                UserJobEventRecord.occurred_at >= cutoff,
            )
            .all()
        )

    counts = Counter((ev.event_type or "").strip().lower() for ev in events if ev.event_type)
    unique_users = len({ev.user_id for ev in events if ev.user_id})
    unique_sessions = len({ev.guest_session_id for ev in events if ev.guest_session_id})

    viewed = (
        counts.get("view", 0)
        + counts.get("open", 0)
        + counts.get("impression", 0)
        + counts.get("tile_view", 0)
    )
    saves = counts.get("save", 0)
    applies = counts.get("apply", 0) + counts.get("applied", 0) + counts.get("click_apply", 0)
    base = viewed if viewed > 0 else max(1, sum(counts.values()))

    return {
        "job_id": str(job_id),
        "window_days": window_days,
        "total_events": sum(counts.values()),
        "event_counts": dict(counts),
        "unique_users": unique_users,
        "unique_sessions": unique_sessions,
        "save_count": saves,
        "apply_count": applies,
        "save_rate": round(saves / base, 4),
        "apply_rate": round(applies / base, 4),
    }


@router.get("/analytics/users/{user_id}")
@limiter.limit(RATE_LIMIT)
def get_user_analytics(
    user_id: str,
    request: Request,
    window_days: int = Query(30, ge=1, le=365),
    api_key: Optional[str] = Depends(_require_api_key),
) -> dict:
    cfg = _get_config()
    if not cfg.db_dsn:
        raise HTTPException(status_code=500, detail="DB not configured")

    cutoff = datetime.utcnow() - timedelta(days=window_days)

    with session_scope(cfg.db_dsn) as session:
        events = (
            session.query(UserJobEventRecord)
            .filter(
                UserJobEventRecord.user_id == user_id,
                UserJobEventRecord.occurred_at >= cutoff,
            )
            .all()
        )

        if not events:
            return {
                "user_id": user_id,
                "window_days": window_days,
                "total_events": 0,
                "distinct_jobs": 0,
                "event_counts": {},
                "top_skills": [],
                "top_industries": [],
                "top_sources": [],
                "active_saved_jobs": 0,
            }

        event_counts = Counter((ev.event_type or "").strip().lower() for ev in events if ev.event_type)
        job_weights = Counter(ev.job_id for ev in events if ev.job_id)
        job_ids = list(job_weights.keys())

        jobs = session.query(JobRecord).filter(JobRecord.id.in_(job_ids)).all() if job_ids else []
        jobs_by_id = {job.id: job for job in jobs}

        skill_counter: Counter[str] = Counter()
        industry_counter: Counter[str] = Counter()
        source_counter: Counter[str] = Counter()

        for jid, weight in job_weights.items():
            job = jobs_by_id.get(jid)
            if job is None:
                continue
            skills = _normalize_lower_list(
                [str(x) for x in ((job.required_skills or job.skills or [])) if x]
            )
            for skill in skills:
                skill_counter[skill] += int(weight)
            if job.industry:
                industry_counter[job.industry] += int(weight)
            if job.source:
                source_counter[job.source] += int(weight)

        active_saved_jobs = (
            session.query(UserSavedJobRecord)
            .filter(
                UserSavedJobRecord.user_id == user_id,
                UserSavedJobRecord.is_active == True,
            )
            .count()
        )

    return {
        "user_id": user_id,
        "window_days": window_days,
        "total_events": sum(event_counts.values()),
        "distinct_jobs": len(job_weights),
        "event_counts": dict(event_counts),
        "top_skills": [{"skill": name, "count": count} for name, count in skill_counter.most_common(10)],
        "top_industries": [{"industry": name, "count": count} for name, count in industry_counter.most_common(10)],
        "top_sources": [{"source": name, "count": count} for name, count in source_counter.most_common(10)],
        "active_saved_jobs": active_saved_jobs,
    }


@router.get("/saved-jobs")
@limiter.limit(RATE_LIMIT)
def list_saved_jobs(
    request: Request,
    user_id: Optional[str] = None,
    guest_session_id: Optional[str] = None,
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
) -> dict:
    cfg = _get_config()
    if not cfg.db_dsn:
        raise HTTPException(status_code=500, detail="DB not configured")
    _validate_identity(user_id, guest_session_id)

    with session_scope(cfg.db_dsn) as session:
        base_query = (
            session.query(UserSavedJobRecord)
            .filter(
                UserSavedJobRecord.is_active == True,
                _identity_clause(user_id=user_id, guest_session_id=guest_session_id),
            )
            .order_by(UserSavedJobRecord.updated_at.desc())
        )
        total = base_query.count()
        rows = base_query.limit(limit).offset(offset).all()

        job_ids = [row.job_id for row in rows]
        jobs = session.query(JobRecord).filter(JobRecord.id.in_(job_ids)).all() if job_ids else []
        jobs_by_id = {job.id: job for job in jobs}

    items = [_saved_row_to_dict(row, jobs_by_id.get(row.job_id)) for row in rows]
    return {
        "items": items,
        "total": total,
        "limit": limit,
        "offset": offset,
    }


@router.post("/saved-jobs")
@limiter.limit(RATE_LIMIT)
def save_job(
    request: Request,
    payload: dict = Body(...),
) -> dict:
    cfg = _get_config()
    if not cfg.db_dsn:
        raise HTTPException(status_code=500, detail="DB not configured")

    try:
        job_id = UUID(str(payload.get("job_id")))
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid job_id")

    user_id = payload.get("user_id")
    guest_session_id = payload.get("guest_session_id")
    _validate_identity(user_id, guest_session_id)

    surface = (str(payload.get("surface") or "").strip() or "saved_jobs")
    metadata = payload.get("event_metadata") if isinstance(payload.get("event_metadata"), dict) else None

    with session_scope(cfg.db_dsn) as session:
        job = session.query(JobRecord).filter(JobRecord.id == job_id).first()
        if job is None:
            raise HTTPException(status_code=404, detail=f"Job {job_id} not found")

        query = session.query(UserSavedJobRecord).filter(UserSavedJobRecord.job_id == job_id)
        if user_id is not None:
            query = query.filter(UserSavedJobRecord.user_id == user_id)
        else:
            query = query.filter(UserSavedJobRecord.user_id.is_(None))
        if guest_session_id is not None:
            query = query.filter(UserSavedJobRecord.guest_session_id == guest_session_id)
        else:
            query = query.filter(UserSavedJobRecord.guest_session_id.is_(None))

        existing = query.first()
        now = datetime.utcnow()
        if existing:
            existing.is_active = True
            existing.updated_at = now
            if existing.saved_at is None:
                existing.saved_at = now
            saved_row = existing
        else:
            saved_row = UserSavedJobRecord(
                job_id=job_id,
                user_id=user_id,
                guest_session_id=guest_session_id,
                saved_at=now,
                updated_at=now,
                is_active=True,
            )
            session.add(saved_row)

        _record_job_event(
            session,
            job_id=job_id,
            user_id=user_id,
            guest_session_id=guest_session_id,
            event_type="save",
            surface=surface,
            event_metadata=metadata,
        )
        session.flush()

    return {
        "item": _saved_row_to_dict(saved_row, job),
    }


@router.delete("/saved-jobs/{job_id}")
@limiter.limit(RATE_LIMIT)
def unsave_job(
    job_id: UUID,
    request: Request,
    user_id: Optional[str] = None,
    guest_session_id: Optional[str] = None,
) -> dict:
    cfg = _get_config()
    if not cfg.db_dsn:
        raise HTTPException(status_code=500, detail="DB not configured")
    _validate_identity(user_id, guest_session_id)

    with session_scope(cfg.db_dsn) as session:
        row = (
            session.query(UserSavedJobRecord)
            .filter(
                UserSavedJobRecord.job_id == job_id,
                UserSavedJobRecord.is_active == True,
                _identity_clause(user_id=user_id, guest_session_id=guest_session_id),
            )
            .first()
        )
        if row is None:
            return {"removed": False}

        row.is_active = False
        row.updated_at = datetime.utcnow()

        _record_job_event(
            session,
            job_id=job_id,
            user_id=user_id,
            guest_session_id=guest_session_id,
            event_type="unsave",
            surface="saved_jobs",
            event_metadata=None,
        )

    return {"removed": True}


def _validate_user_id(user_id: Optional[str]) -> str:
    """Validate and return the user_id; raises 400 if empty or too long."""
    if not user_id or not user_id.strip():
        raise HTTPException(status_code=400, detail="user_id is required")
    if len(user_id) > 128:
        raise HTTPException(status_code=400, detail="user_id exceeds 128 characters")
    return user_id.strip()


@router.post("/resume")
@limiter.limit(RATE_LIMIT)
async def upload_resume(
    request: Request,
    user_id: Optional[str] = Query(None),
    file: UploadFile = File(...),
) -> dict:
    """Upload a PDF resume for a user. Replaces any previously active resume."""
    cfg = _get_config()
    if not cfg.db_dsn:
        raise HTTPException(status_code=500, detail="DB not configured")

    uid = _validate_user_id(user_id)

    # Content-type guard
    if file.content_type != "application/pdf":
        raise HTTPException(
            status_code=415,
            detail=f"Only PDF files are accepted (got {file.content_type!r})",
        )

    # Size cap: 5 MB
    MAX_SIZE = 5 * 1024 * 1024
    file_bytes = await file.read()
    if len(file_bytes) > MAX_SIZE:
        raise HTTPException(
            status_code=413,
            detail=f"File too large ({len(file_bytes)} bytes). Maximum is 5 MB.",
        )

    # Extract text
    try:
        raw_text = extract_text_from_pdf(file_bytes)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))

    # Extract skills + experience from resume (non-fatal if Groq not configured)
    profile = await extract_resume_profile(
        resume_text=raw_text,
        llm_config=cfg.llm_parser,
    )

    with session_scope(cfg.db_dsn) as session:
        # Soft-deactivate prior resumes
        session.query(UserResumeRecord).filter(
            UserResumeRecord.user_id == uid,
            UserResumeRecord.is_active == True,
        ).update({"is_active": False})

        resume = UserResumeRecord(
            user_id=uid,
            raw_text=raw_text,
            filename=file.filename,
            extracted_skills=profile["skills"] or None,
            extracted_experience_years=profile["experience_years"],
            is_active=True,
        )
        session.add(resume)
        session.flush()

        return {
            "resume_id": str(resume.id),
            "user_id": resume.user_id,
            "extracted_text_preview": raw_text[:200],
            "filename": resume.filename,
            "extracted_skills": profile["skills"],
            "extracted_experience_years": profile["experience_years"],
            "skills_extracted": len(profile["skills"]) > 0,
            "created_at": resume.created_at.isoformat() if resume.created_at else None,
        }


@router.get("/resume")
@limiter.limit(RATE_LIMIT)
def get_resume(
    request: Request,
    user_id: Optional[str] = Query(None),
) -> dict:
    """Get the most recent active resume for a user."""
    cfg = _get_config()
    if not cfg.db_dsn:
        raise HTTPException(status_code=500, detail="DB not configured")

    uid = _validate_user_id(user_id)

    with session_scope(cfg.db_dsn) as session:
        resume = (
            session.query(UserResumeRecord)
            .filter(
                UserResumeRecord.user_id == uid,
                UserResumeRecord.is_active == True,
            )
            .order_by(UserResumeRecord.created_at.desc())
            .first()
        )
        if resume is None:
            raise HTTPException(status_code=404, detail="No active resume found for this user")

        return {
            "resume_id": str(resume.id),
            "user_id": resume.user_id,
            "raw_text": resume.raw_text,
            "filename": resume.filename,
            "created_at": resume.created_at.isoformat() if resume.created_at else None,
            "updated_at": resume.updated_at.isoformat() if resume.updated_at else None,
        }


@router.post("/jobs/{job_id}/optimize-resume")
@limiter.limit(RATE_LIMIT)
async def optimize_resume_for_job(
    job_id: UUID,
    request: Request,
    user_id: Optional[str] = Query(None),
    payload: dict = Body(...),
) -> dict:
    """Optimize a user's resume against a specific job using an LLM.

    Modes: "bullets" | "overview" | "full_rewrite"
    """
    cfg = _get_config()
    if not cfg.db_dsn:
        raise HTTPException(status_code=500, detail="DB not configured")

    uid = _validate_user_id(user_id)

    mode = str(payload.get("mode") or "").strip()
    if mode not in ("bullets", "overview", "full_rewrite"):
        raise HTTPException(
            status_code=400,
            detail="mode must be one of: 'bullets', 'overview', 'full_rewrite'",
        )

    # Fetch job and resume in one session, snapshot to locals before closing
    with session_scope(cfg.db_dsn) as session:
        job = session.query(JobRecord).filter(JobRecord.id == job_id).first()
        if job is None:
            raise HTTPException(status_code=404, detail=f"Job {job_id} not found")

        resume = (
            session.query(UserResumeRecord)
            .filter(
                UserResumeRecord.user_id == uid,
                UserResumeRecord.is_active == True,
            )
            .order_by(UserResumeRecord.created_at.desc())
            .first()
        )
        if resume is None:
            raise HTTPException(status_code=404, detail="No active resume found for this user")

        # Snapshot fields before session closes
        job_title = job.title or ""
        job_company = job.company or ""
        job_description = job.description or ""
        required_skills = list(job.required_skills or [])
        resume_text = resume.raw_text

    # Call LLM outside the DB session
    try:
        result = await optimize_resume(
            resume_text=resume_text,
            job_title=job_title,
            job_company=job_company,
            job_description=job_description,
            required_skills=required_skills,
            mode=mode,
            llm_config=cfg.llm_parser,
        )
    except ResumeOptimizeError as exc:
        msg = str(exc)
        if "not installed" in msg or "not configured" in msg:
            raise HTTPException(status_code=503, detail=msg)
        raise HTTPException(status_code=502, detail=f"LLM API error: {msg}")

    response: dict = {"job_id": str(job_id), "mode": mode}
    response.update(result)
    return response


@router.post("/jobs/{job_id}/summary")
@limiter.limit(RATE_LIMIT)
async def get_job_summary(
    job_id: UUID,
    request: Request,
) -> dict:
    """Generate (or return cached) an AI summary of a job description using Groq.

    On first call: calls Groq, stores result in DB, returns result.
    On subsequent calls: returns stored summary instantly from DB.
    Returns 503 if Groq is not configured and no cached summary exists.
    """
    cfg = _get_config()
    if not cfg.db_dsn:
        raise HTTPException(status_code=500, detail="DB not configured")

    with session_scope(cfg.db_dsn) as session:
        job = session.query(JobRecord).filter(JobRecord.id == job_id).first()
        if job is None:
            raise HTTPException(status_code=404, detail=f"Job {job_id} not found")

        # Return stored summary if already generated
        if job.ai_summarized_at is not None:
            return {
                "job_id": str(job_id),
                "ai_summary_card": job.ai_summary_card,
                "ai_summary_detail": job.ai_summary_detail,
                # Legacy fields
                "summary_short": (job.ai_summary_detail or {}).get("summary_short", ""),
                "summary_bullets": (job.ai_summary_detail or {}).get("summary_bullets", []),
                "attention_tags": (job.ai_summary_detail or {}).get("attention_tags", []),
                "cached": True,
            }

        # Snapshot before session closes
        job_title = job.title or ""
        job_company = job.company or ""
        job_description = job.description or ""
        tags = list(job.tags or [])

    # Call Groq outside DB session
    try:
        result = await summarize_job(
            job_id=str(job_id),
            job_title=job_title,
            job_company=job_company,
            job_description=job_description,
            tags=tags,
            llm_config=cfg.llm_parser,
        )
    except ResumeOptimizeError as exc:
        msg = str(exc)
        if "not installed" in msg or "not configured" in msg:
            raise HTTPException(status_code=503, detail=msg)
        raise HTTPException(status_code=502, detail=f"LLM API error: {msg}")

    # Persist summaries back to the job row
    with session_scope(cfg.db_dsn) as session:
        session.query(JobRecord).filter(JobRecord.id == job_id).update(
            {
                "ai_summary_card": result["ai_summary_card"],
                "ai_summary_detail": result["ai_summary_detail"],
                "ai_summarized_at": datetime.utcnow(),
            }
        )

    result["cached"] = False
    return result


@router.post("/resume/analyze")
@limiter.limit(RATE_LIMIT)
async def analyze_resume_endpoint(
    request: Request,
    user_id: Optional[str] = Query(None),
    payload: dict = Body(...),
) -> dict:
    """Analyze a user's stored resume generally (not tied to a specific job).

    Body: {"critique_level": "light"|"balanced"|"hardcore"}
    Returns score, headline, strengths, gaps, priority_actions.
    """
    cfg = _get_config()
    if not cfg.db_dsn:
        raise HTTPException(status_code=500, detail="DB not configured")

    uid = _validate_user_id(user_id)

    critique_level = str(payload.get("critique_level") or "balanced").strip()
    if critique_level not in ("light", "balanced", "hardcore"):
        raise HTTPException(
            status_code=400,
            detail="critique_level must be 'light', 'balanced', or 'hardcore'",
        )

    with session_scope(cfg.db_dsn) as session:
        resume = (
            session.query(UserResumeRecord)
            .filter(
                UserResumeRecord.user_id == uid,
                UserResumeRecord.is_active == True,
            )
            .order_by(UserResumeRecord.created_at.desc())
            .first()
        )
        if resume is None:
            raise HTTPException(status_code=404, detail="No active resume found for this user")
        resume_text = resume.raw_text

    try:
        result = await analyze_resume(
            resume_text=resume_text,
            critique_level=critique_level,
            llm_config=cfg.llm_parser,
        )
    except ResumeOptimizeError as exc:
        msg = str(exc)
        if "not installed" in msg or "not configured" in msg:
            raise HTTPException(status_code=503, detail=msg)
        raise HTTPException(status_code=502, detail=f"LLM API error: {msg}")

    return result


@router.get("/resume/match-profile")
@limiter.limit(RATE_LIMIT)
def get_resume_match_profile(
    request: Request,
    user_id: Optional[str] = Query(None),
) -> dict:
    """Return the extracted skills and experience years from the user's stored resume.

    Used by the frontend to feed the match scorer without requiring manual input.
    Returns skills_extracted: false and empty skills if profile was not extracted
    (e.g. Groq was not configured at upload time).
    """
    cfg = _get_config()
    if not cfg.db_dsn:
        raise HTTPException(status_code=500, detail="DB not configured")

    uid = _validate_user_id(user_id)

    with session_scope(cfg.db_dsn) as session:
        resume = (
            session.query(UserResumeRecord)
            .filter(
                UserResumeRecord.user_id == uid,
                UserResumeRecord.is_active == True,
            )
            .order_by(UserResumeRecord.created_at.desc())
            .first()
        )
        if resume is None:
            raise HTTPException(status_code=404, detail="No active resume found for this user")

        skills = list(resume.extracted_skills or [])
        experience_years = resume.extracted_experience_years

        return {
            "user_id": uid,
            "skills": skills,
            "experience_years": experience_years,
            "skills_extracted": len(skills) > 0,
        }


# Register the versioned router — all routes are now accessible under /api/v1
app.include_router(router)
