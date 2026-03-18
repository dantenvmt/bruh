"""
Thin async Redis cache helper with graceful degradation.

If REDIS_URL is not set, all cache operations silently no-op.
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
from typing import Any, Optional

logger = logging.getLogger(__name__)

_client = None  # module-level singleton


async def get_redis():
    """Return Redis async client or None if REDIS_URL is not set / unavailable."""
    global _client
    if _client is not None:
        return _client
    url = os.getenv("REDIS_URL", "")
    if not url:
        return None
    try:
        import redis.asyncio as aioredis  # type: ignore[import]
        _client = aioredis.from_url(url, decode_responses=True)
        return _client
    except Exception as exc:
        logger.warning("Redis unavailable: %s", exc)
        return None


async def get_cache(key: str) -> Optional[Any]:
    """Return cached value for key, or None on miss / error."""
    r = await get_redis()
    if r is None:
        return None
    try:
        raw = await r.get(key)
        return json.loads(raw) if raw else None
    except Exception as exc:
        logger.debug("Cache get error: %s", exc)
        return None


async def set_cache(key: str, value: Any, ttl: int) -> None:
    """Store value in cache with TTL seconds. Silently no-ops on error."""
    r = await get_redis()
    if r is None:
        return
    try:
        await r.setex(key, ttl, json.dumps(value))
    except Exception as exc:
        logger.debug("Cache set error: %s", exc)


def content_key(*parts) -> str:
    """Build a stable cache key from arbitrary parts.

    Handles bytes efficiently by hashing them directly instead of
    converting to a massive str() representation.
    """
    h = hashlib.sha256()
    for i, p in enumerate(parts):
        if i > 0:
            h.update(b"|")
        if isinstance(p, bytes):
            h.update(p)
        else:
            h.update(str(p).encode())
    return "job_scraper:" + h.hexdigest()[:24]
