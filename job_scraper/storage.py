"""
Postgres storage layer for runs and jobs.
"""
from __future__ import annotations

import os
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta
from typing import Iterable, List, Optional

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    Column,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    create_engine,
    text,
    func,
    select,
    delete,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID, insert as pg_insert
from sqlalchemy.orm import declarative_base, sessionmaker, Session

from .models import Job as JobModel
from .utils import build_dedupe_key, normalize_text, normalize_url


Base = declarative_base()


class RunRecord(Base):
    __tablename__ = "runs"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    started_at = Column(DateTime(timezone=False), nullable=False, default=datetime.utcnow)
    ended_at = Column(DateTime(timezone=False), nullable=True)
    status = Column(String(32), nullable=False, default="running")
    sources = Column(JSONB, nullable=True)
    total_jobs = Column(Integer, nullable=False, default=0)


class JobRecord(Base):
    __tablename__ = "jobs"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    dedupe_key = Column(Text, nullable=False, unique=True, index=True)
    source = Column(String(64), nullable=True)
    source_job_id = Column(String(128), nullable=True)
    title = Column(Text, nullable=False)
    company = Column(Text, nullable=True)
    location = Column(Text, nullable=True)
    url = Column(Text, nullable=True)
    description = Column(Text, nullable=True)
    salary = Column(Text, nullable=True)
    employment_type = Column(Text, nullable=True)
    posted_date = Column(Text, nullable=True)
    remote = Column(Boolean, nullable=True)
    category = Column(Text, nullable=True)
    tags = Column(JSONB, nullable=True)
    skills = Column(JSONB, nullable=True)
    experience_level = Column(String(32), nullable=True)
    experience_min_years = Column(Integer, nullable=True)
    experience_max_years = Column(Integer, nullable=True)
    required_skills = Column(JSONB, nullable=True)
    industry = Column(String(64), nullable=True)
    industry_confidence = Column(Float, nullable=True)
    work_mode = Column(String(32), nullable=True)
    role_pop_reasons = Column(JSONB, nullable=True)
    enrichment_version = Column(Integer, nullable=True)
    enrichment_updated_at = Column(DateTime(timezone=False), nullable=True)
    raw_payload = Column(JSONB, nullable=True)
    ai_summary_card = Column(Text, nullable=True)
    ai_summary_detail = Column(JSONB, nullable=True)
    ai_summarized_at = Column(DateTime(timezone=False), nullable=True)
    salary_min = Column(Integer, nullable=True)
    salary_max = Column(Integer, nullable=True)
    seniority = Column(String(32), nullable=True)
    visa_sponsorship = Column(Boolean, nullable=True)
    ai_summary_bullets = Column(JSONB, nullable=True)
    normalized_at = Column(DateTime(timezone=False), nullable=True)
    created_at = Column(DateTime(timezone=False), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=False), server_default=func.now(), onupdate=func.now(), nullable=False)
    last_seen_at = Column(DateTime(timezone=False), server_default=func.now(), nullable=False)

    __table_args__ = (
        Index("idx_jobs_source", "source"),
        Index("idx_jobs_remote_partial", "remote", postgresql_where=text("remote IS TRUE")),
        Index("idx_jobs_updated_at_desc", updated_at.desc()),
        Index("idx_jobs_cursor_pagination", updated_at.desc(), "id"),
        Index("idx_jobs_created_at", "created_at"),
        Index("idx_jobs_last_seen_at", "last_seen_at"),
        Index("idx_jobs_experience_level", "experience_level"),
        Index("idx_jobs_industry", "industry"),
        Index("idx_jobs_work_mode", "work_mode"),
        Index("idx_jobs_required_skills_gin", "required_skills", postgresql_using="gin"),
        Index("idx_jobs_role_pop_reasons_gin", "role_pop_reasons", postgresql_using="gin"),
    )


class JobSeenRecord(Base):
    __tablename__ = "job_seen"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    run_id = Column(UUID(as_uuid=True), ForeignKey("runs.id", ondelete="CASCADE"), index=True, nullable=False)
    job_id = Column(UUID(as_uuid=True), ForeignKey("jobs.id", ondelete="CASCADE"), index=True, nullable=False)
    seen_at = Column(DateTime(timezone=False), nullable=False, default=datetime.utcnow)

    __table_args__ = (UniqueConstraint("run_id", "job_id", name="uq_run_job_seen"),)


class RunSourceRecord(Base):
    """Track per-source, per-target results for each ingestion run."""
    __tablename__ = "run_sources"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    run_id = Column(UUID(as_uuid=True), ForeignKey("runs.id", ondelete="CASCADE"), index=True, nullable=False)
    source = Column(String(64), nullable=False)
    source_target = Column(String(256), nullable=True)
    jobs_fetched = Column(Integer, default=0)
    jobs_after_dedupe = Column(Integer, default=0)
    error_message = Column(Text, nullable=True)
    error_code = Column(String(32), nullable=True)
    request_duration_ms = Column(Integer, nullable=True)
    created_at = Column(DateTime(timezone=False), server_default=func.now(), nullable=False)

    __table_args__ = (
        Index("idx_run_sources_source_target", "source", "source_target"),
    )


class SourceErrorRecord(Base):
    __tablename__ = "source_errors"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    run_id = Column(UUID(as_uuid=True), ForeignKey("runs.id", ondelete="CASCADE"), index=True, nullable=False)
    source = Column(String(64), nullable=True)
    message = Column(Text, nullable=False)
    payload = Column(JSONB, nullable=True)
    created_at = Column(DateTime(timezone=False), nullable=False, default=datetime.utcnow)


class UserJobEventRecord(Base):
    __tablename__ = "user_job_events"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    job_id = Column(UUID(as_uuid=True), ForeignKey("jobs.id", ondelete="CASCADE"), index=True, nullable=False)
    user_id = Column(String(128), nullable=True, index=True)
    guest_session_id = Column(String(128), nullable=True, index=True)
    event_type = Column(String(50), nullable=False)
    surface = Column(String(50), nullable=True)
    event_metadata = Column(JSONB, nullable=True)
    occurred_at = Column(DateTime(timezone=False), nullable=False, default=datetime.utcnow, index=True)

    __table_args__ = (
        CheckConstraint(
            "(user_id IS NOT NULL) OR (guest_session_id IS NOT NULL)",
            name="ck_user_job_events_identity_required",
        ),
        Index("idx_user_job_events_job_occurred", "job_id", "occurred_at"),
        Index("idx_user_job_events_user_occurred", "user_id", "occurred_at"),
    )


class UserSavedJobRecord(Base):
    __tablename__ = "user_saved_jobs"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    job_id = Column(UUID(as_uuid=True), ForeignKey("jobs.id", ondelete="CASCADE"), index=True, nullable=False)
    user_id = Column(String(128), nullable=True, index=True)
    guest_session_id = Column(String(128), nullable=True, index=True)
    saved_at = Column(DateTime(timezone=False), nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime(timezone=False), nullable=False, default=datetime.utcnow)
    is_active = Column(Boolean, nullable=False, default=True, index=True)

    __table_args__ = (
        CheckConstraint(
            "(user_id IS NOT NULL) OR (guest_session_id IS NOT NULL)",
            name="ck_user_saved_jobs_identity_required",
        ),
    )


class UserResumeRecord(Base):
    __tablename__ = "user_resumes"

    id                       = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id                  = Column(String(128), nullable=False, index=True)
    raw_text                 = Column(Text, nullable=False)
    filename                 = Column(String(256), nullable=True)
    extracted_skills         = Column(JSONB, nullable=True)
    extracted_experience_years = Column(Integer, nullable=True)
    created_at               = Column(DateTime(timezone=False), server_default=func.now(), nullable=False)
    updated_at               = Column(DateTime(timezone=False), server_default=func.now(), onupdate=func.now(), nullable=False)
    is_active                = Column(Boolean, nullable=False, default=True)

    __table_args__ = (
        Index("idx_user_resumes_user_id", "user_id"),
        Index("idx_user_resumes_user_id_created", "user_id", "created_at"),
    )


_ENGINE_CACHE: dict[str, object] = {}
_SESSION_CACHE: dict[str, sessionmaker] = {}


def _get_int_env(name: str, default: int) -> int:
    """Parse integer from environment variable with fallback."""
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        return default


def _get_bool_env(name: str, default: bool) -> bool:
    """Parse boolean from environment variable with fallback."""
    value = os.getenv(name)
    if value is None:
        return default
    return value.lower() in ("true", "1", "yes", "on")


# Security configuration
STORE_RAW_PAYLOAD = _get_bool_env("JOB_SCRAPER_STORE_RAW_PAYLOAD", False)
SKIP_DSN_TLS_CHECK = _get_bool_env("JOB_SCRAPER_SKIP_DSN_TLS_CHECK", False)


def _validate_dsn_security(dsn: str) -> None:
    """Validate that non-localhost DSNs use SSL/TLS."""
    if SKIP_DSN_TLS_CHECK:
        return
    from urllib.parse import urlparse, parse_qs
    parsed = urlparse(dsn)
    host = parsed.hostname or ""
    # Skip for local development
    if host in ("localhost", "127.0.0.1", "::1", "db"):
        return
    query_params = parse_qs(parsed.query)
    sslmode = query_params.get("sslmode", [None])[0]
    if sslmode not in ("require", "verify-ca", "verify-full"):
        raise RuntimeError(
            f"Database connection to '{host}' must use sslmode=require or higher. "
            f"Current sslmode: {sslmode or 'none'}. "
            "Set JOB_SCRAPER_SKIP_DSN_TLS_CHECK=true to bypass (testing only)."
        )


def _get_engine(dsn: str):
    """
    Get or create a SQLAlchemy engine with configurable pool settings.

    Pool settings via environment variables:
    - JOB_SCRAPER_POOL_SIZE: Connection pool size (default: 20)
    - JOB_SCRAPER_POOL_MAX_OVERFLOW: Max connections beyond pool_size (default: 30)
    - JOB_SCRAPER_POOL_TIMEOUT: Seconds to wait for connection (default: 30)
    - JOB_SCRAPER_POOL_RECYCLE: Seconds before recycling connections (default: 1800)
    """
    _validate_dsn_security(dsn)

    if dsn not in _ENGINE_CACHE:
        pool_size = _get_int_env("JOB_SCRAPER_POOL_SIZE", 20)
        max_overflow = _get_int_env("JOB_SCRAPER_POOL_MAX_OVERFLOW", 30)
        pool_timeout = _get_int_env("JOB_SCRAPER_POOL_TIMEOUT", 30)
        pool_recycle = _get_int_env("JOB_SCRAPER_POOL_RECYCLE", 1800)

        _ENGINE_CACHE[dsn] = create_engine(
            dsn,
            pool_pre_ping=True,
            pool_size=pool_size,
            max_overflow=max_overflow,
            pool_timeout=pool_timeout,
            pool_recycle=pool_recycle,
        )
    return _ENGINE_CACHE[dsn]


def _get_sessionmaker(dsn: str) -> sessionmaker:
    if dsn not in _SESSION_CACHE:
        _SESSION_CACHE[dsn] = sessionmaker(bind=_get_engine(dsn), expire_on_commit=False)
    return _SESSION_CACHE[dsn]


def get_session(dsn: str) -> Session:
    return _get_sessionmaker(dsn)()


@contextmanager
def session_scope(dsn: str):
    """
    Context manager for database sessions with automatic commit/rollback.

    Usage:
        with session_scope(dsn) as session:
            session.add(record)
            # Commit happens automatically on success
            # Rollback happens automatically on exception
    """
    session = get_session(dsn)
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def init_db(dsn: str) -> None:
    engine = _get_engine(dsn)
    Base.metadata.create_all(engine)


def start_run(dsn: str, sources: Optional[List[str]] = None) -> uuid.UUID:
    with session_scope(dsn) as session:
        rec = RunRecord(sources=sources or [])
        session.add(rec)
        session.flush()  # Ensure ID is generated before commit
        return rec.id


def finish_run(dsn: str, run_id: uuid.UUID, total_jobs: int, status: str = "success") -> None:
    with session_scope(dsn) as session:
        session.query(RunRecord).filter(RunRecord.id == run_id).update(
            {"ended_at": datetime.utcnow(), "status": status, "total_jobs": total_jobs}
        )


def record_error(dsn: str, run_id: uuid.UUID, source: Optional[str], message: str, payload: Optional[dict] = None) -> None:
    with session_scope(dsn) as session:
        rec = SourceErrorRecord(run_id=run_id, source=source, message=message, payload=payload)
        session.add(rec)


def upsert_jobs(dsn: str, run_id: Optional[uuid.UUID], jobs: Iterable[JobModel], batch_size: int = 100) -> int:
    """
    Upsert jobs to database in batches.

    Args:
        dsn: Database connection string
        run_id: Optional run ID to associate jobs with
        jobs: Iterable of Job models to upsert
        batch_size: Number of jobs per batch (default 100, Supabase has query size limits)

    Returns:
        Total number of jobs upserted
    """
    rows = []
    for job in jobs:
        title = normalize_text(job.title)
        company = normalize_text(job.company or "")
        # Truncate source_job_id to fit varchar(128) column
        source_job_id = job.job_id[:128] if job.job_id and len(job.job_id) > 128 else job.job_id
        rows.append(
            {
                "dedupe_key": build_dedupe_key(job),
                "source": job.source,
                "source_job_id": source_job_id,
                "title": title,
                "company": company or None,
                "location": normalize_text(job.location or "") or None,
                "url": normalize_url(job.url) or None,
                "description": job.description,
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
                "required_skills": job.required_skills,
                "industry": job.industry,
                "industry_confidence": job.industry_confidence,
                "work_mode": job.work_mode,
                "role_pop_reasons": job.role_pop_reasons,
                "enrichment_version": job.enrichment_version,
                "enrichment_updated_at": datetime.utcnow() if job.enrichment_version is not None else None,
                "raw_payload": job.raw_payload if STORE_RAW_PAYLOAD else None,
                "salary_min": job.salary_min,
                "salary_max": job.salary_max,
                "seniority": job.seniority,
                "visa_sponsorship": job.visa_sponsorship,
                "ai_summary_card": job.ai_summary_card,
                "ai_summary_bullets": job.ai_summary_bullets,
                "normalized_at": datetime.fromisoformat(job.normalized_at.replace("Z", "+00:00")).replace(tzinfo=None) if job.normalized_at else None,
            }
        )

    if not rows:
        return 0

    total_job_ids = []

    with session_scope(dsn) as session:
        # Process in batches to avoid Supabase query size limits
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i + batch_size]

            stmt = pg_insert(JobRecord).values(batch)
            update_cols = {
                "source": stmt.excluded.source,
                "source_job_id": stmt.excluded.source_job_id,
                "title": stmt.excluded.title,
                "company": stmt.excluded.company,
                "location": func.coalesce(func.nullif(stmt.excluded.location, ''), JobRecord.location),
                "url": stmt.excluded.url,
                "description": func.coalesce(func.nullif(stmt.excluded.description, ''), JobRecord.description),
                "salary": func.coalesce(func.nullif(stmt.excluded.salary, ''), JobRecord.salary),
                "employment_type": stmt.excluded.employment_type,
                "posted_date": stmt.excluded.posted_date,
                "remote": stmt.excluded.remote,
                "category": stmt.excluded.category,
                "tags": stmt.excluded.tags,
                "skills": stmt.excluded.skills,
                "experience_level": stmt.excluded.experience_level,
                "experience_min_years": stmt.excluded.experience_min_years,
                "experience_max_years": stmt.excluded.experience_max_years,
                "required_skills": stmt.excluded.required_skills,
                "industry": stmt.excluded.industry,
                "industry_confidence": stmt.excluded.industry_confidence,
                "work_mode": stmt.excluded.work_mode,
                "role_pop_reasons": stmt.excluded.role_pop_reasons,
                "enrichment_version": stmt.excluded.enrichment_version,
                "enrichment_updated_at": stmt.excluded.enrichment_updated_at,
                "raw_payload": stmt.excluded.raw_payload if STORE_RAW_PAYLOAD else None,
                "salary_min": func.coalesce(stmt.excluded.salary_min, JobRecord.salary_min),
                "salary_max": func.coalesce(stmt.excluded.salary_max, JobRecord.salary_max),
                "seniority": func.coalesce(stmt.excluded.seniority, JobRecord.seniority),
                "visa_sponsorship": func.coalesce(stmt.excluded.visa_sponsorship, JobRecord.visa_sponsorship),
                "ai_summary_card": func.coalesce(func.nullif(stmt.excluded.ai_summary_card, ''), JobRecord.ai_summary_card),
                "ai_summary_bullets": func.coalesce(stmt.excluded.ai_summary_bullets, JobRecord.ai_summary_bullets),
                "normalized_at": func.coalesce(stmt.excluded.normalized_at, JobRecord.normalized_at),
                "updated_at": func.now(),
                "last_seen_at": func.now(),
            }
            stmt = stmt.on_conflict_do_update(index_elements=[JobRecord.dedupe_key], set_=update_cols)
            stmt = stmt.returning(JobRecord.id)
            result = session.execute(stmt)
            batch_ids = [row[0] for row in result.fetchall()]
            total_job_ids.extend(batch_ids)

        if run_id and total_job_ids:
            # Also batch the job_seen inserts
            for i in range(0, len(total_job_ids), batch_size):
                batch_ids = total_job_ids[i:i + batch_size]
                seen_rows = [{"run_id": run_id, "job_id": job_id} for job_id in batch_ids]
                seen_stmt = pg_insert(JobSeenRecord).values(seen_rows)
                seen_stmt = seen_stmt.on_conflict_do_nothing(index_elements=["run_id", "job_id"])
                session.execute(seen_stmt)

        return len(total_job_ids)


def purge_old_runs(dsn: str, retention_days: int) -> int:
    cutoff = datetime.utcnow() - timedelta(days=retention_days)
    with session_scope(dsn) as session:
        # CASCADE handles job_seen and source_errors via FK constraints
        result = session.execute(
            delete(RunRecord).where(RunRecord.started_at < cutoff)
        )
        runs_deleted = result.rowcount
        # Remove jobs not seen within retention period
        session.execute(
            delete(JobRecord).where(JobRecord.last_seen_at < cutoff)
        )
        return runs_deleted


def scrub_existing_raw_payloads(dsn: str) -> int:
    """Scrub all existing raw_payload data. Returns count of affected rows."""
    with session_scope(dsn) as session:
        result = session.execute(
            text("UPDATE jobs SET raw_payload = NULL WHERE raw_payload IS NOT NULL")
        )
        return result.rowcount


def record_source_result(
    dsn: str,
    run_id: uuid.UUID,
    source: str,
    source_target: Optional[str],
    jobs_fetched: int,
    jobs_after_dedupe: int = 0,
    error_message: Optional[str] = None,
    error_code: Optional[str] = None,
    duration_ms: int = 0,
) -> None:
    """Record the result of fetching from a specific source/target."""
    with session_scope(dsn) as session:
        rec = RunSourceRecord(
            run_id=run_id,
            source=source,
            source_target=source_target,
            jobs_fetched=jobs_fetched,
            jobs_after_dedupe=jobs_after_dedupe,
            error_message=error_message,
            error_code=error_code,
            request_duration_ms=duration_ms,
        )
        session.add(rec)


def record_source_results_bulk(
    dsn: str,
    run_id: uuid.UUID,
    board_results: List,
    jobs_stored_map: dict,
) -> None:
    """Bulk insert source results using a single INSERT statement."""
    rows = []
    for board_result in board_results:
        board_key = (board_result.source, board_result.board_token)
        rows.append(
            {
                "run_id": run_id,
                "source": board_result.source,
                "source_target": board_result.board_token if board_result.board_token != board_result.source else None,
                "jobs_fetched": board_result.jobs_fetched,
                "jobs_after_dedupe": jobs_stored_map.get(board_key, 0),
                "error_message": board_result.error,
                "error_code": board_result.error_code,
                "request_duration_ms": board_result.duration_ms,
            }
        )

    if not rows:
        return

    with session_scope(dsn) as session:
        session.execute(pg_insert(RunSourceRecord), rows)


def get_run_sources(dsn: str, run_id: uuid.UUID) -> List[RunSourceRecord]:
    """Get all source results for a given run."""
    with session_scope(dsn) as session:
        results = session.query(RunSourceRecord).filter(RunSourceRecord.run_id == run_id).all()
        # Detach from session to avoid lazy-load issues after session closes
        session.expunge_all()
        return results


def get_source_error_summary(dsn: str, run_id: uuid.UUID) -> dict:
    """Get summary of errors grouped by source and error_code."""
    with session_scope(dsn) as session:
        # Get all source records for this run
        results = session.query(RunSourceRecord).filter(RunSourceRecord.run_id == run_id).all()

        # Build summary structure
        summary = {
            "total_sources": len(results),
            "successful": 0,
            "failed": 0,
            "by_source": {},
            "by_error_code": {},
        }

        for rec in results:
            # Count successes vs failures
            if rec.error_message:
                summary["failed"] += 1
            else:
                summary["successful"] += 1

            # Group by source
            if rec.source not in summary["by_source"]:
                summary["by_source"][rec.source] = {
                    "total": 0,
                    "successful": 0,
                    "failed": 0,
                    "errors": [],
                }
            summary["by_source"][rec.source]["total"] += 1
            if rec.error_message:
                summary["by_source"][rec.source]["failed"] += 1
                summary["by_source"][rec.source]["errors"].append({
                    "target": rec.source_target,
                    "error_code": rec.error_code,
                    "message": rec.error_message,
                })
            else:
                summary["by_source"][rec.source]["successful"] += 1

            # Group by error_code
            if rec.error_code:
                if rec.error_code not in summary["by_error_code"]:
                    summary["by_error_code"][rec.error_code] = {
                        "count": 0,
                        "sources": [],
                    }
                summary["by_error_code"][rec.error_code]["count"] += 1
                summary["by_error_code"][rec.error_code]["sources"].append({
                    "source": rec.source,
                    "target": rec.source_target,
                })

        return summary
