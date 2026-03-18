"""
Shared helpers for exporting discovered Workday sites into YAML config.
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Iterable

import httpx
import yaml

from ..apis.workday import WorkdaySite, parse_workday_url

logger = logging.getLogger(__name__)

_JSON_HEADERS = {
    "Content-Type": "application/json",
    "Accept": "application/json",
}

_VALIDATION_BODY = {
    "appliedFacets": {},
    "limit": 1,
    "offset": 0,
    "searchText": "",
}


def _resolve_site_from_url(
    url: str,
    *,
    client: httpx.Client,
    timeout: float,
) -> WorkdaySite | None:
    parsed = parse_workday_url(url)
    if parsed is not None:
        return parsed

    try:
        resp = client.get(url, follow_redirects=True, timeout=timeout)
    except Exception:
        return None
    return parse_workday_url(str(resp.url))


def _acquire_csrf(client: httpx.Client, host: str, timeout: float) -> str | None:
    try:
        resp = client.get(
            f"https://{host}",
            follow_redirects=True,
            timeout=timeout,
            headers={"User-Agent": "Mozilla/5.0"},
        )
    except Exception:
        return None

    for cookie in resp.cookies.jar:
        if cookie.name == "CALYPSO_CSRF_TOKEN":
            return cookie.value
    return None


def _validate_site(
    site: WorkdaySite,
    *,
    client: httpx.Client,
    timeout: float,
    csrf_cache: dict[str, str],
) -> bool:
    url = f"{site.api_base}/jobs"
    headers = dict(_JSON_HEADERS)

    token = csrf_cache.get(site.host)
    if token:
        headers["CALYPSO_CSRF_TOKEN"] = token

    try:
        resp = client.post(url, json=_VALIDATION_BODY, headers=headers, timeout=timeout)
    except Exception:
        return False

    if resp.status_code in (401, 403):
        token = csrf_cache.get(site.host) or _acquire_csrf(client, site.host, timeout)
        if token:
            csrf_cache[site.host] = token
            headers["CALYPSO_CSRF_TOKEN"] = token
            try:
                resp = client.post(url, json=_VALIDATION_BODY, headers=headers, timeout=timeout)
            except Exception:
                return False

    if resp.status_code != 200:
        return False

    try:
        data = resp.json() if resp.content else {}
    except ValueError:
        return False
    if not isinstance(data, dict):
        return False
    return ("jobPostings" in data) or ("total" in data)


def collect_workday_sites(
    careers_urls: Iterable[str],
    *,
    timeout: float = 15.0,
    validate: bool = True,
) -> tuple[list[dict], dict]:
    """
    Parse + dedupe Workday sites from raw careers URLs.

    When *validate* is True, each parsed site is validated against its CXS
    endpoint before inclusion.
    """
    seen: set[tuple[str, str, str]] = set()
    sites: list[dict] = []
    stats = {
        "input_urls": 0,
        "resolved": 0,
        "validated": 0,
        "rejected": 0,
    }
    csrf_cache: dict[str, str] = {}

    with httpx.Client(follow_redirects=True) as client:
        for raw_url in careers_urls:
            url = (raw_url or "").strip()
            if not url:
                continue
            stats["input_urls"] += 1

            parsed = _resolve_site_from_url(url, client=client, timeout=timeout)
            if parsed is None:
                stats["rejected"] += 1
                continue
            stats["resolved"] += 1

            if validate and not _validate_site(
                parsed, client=client, timeout=timeout, csrf_cache=csrf_cache
            ):
                stats["rejected"] += 1
                continue
            if validate:
                stats["validated"] += 1

            key = (parsed.host.lower(), parsed.tenant.lower(), parsed.site.lower())
            if key in seen:
                continue
            seen.add(key)
            sites.append(
                {
                    "host": parsed.host.lower(),
                    "tenant": parsed.tenant,
                    "site": parsed.site,
                }
            )

    sites.sort(key=lambda row: (row["host"], row["tenant"].lower(), row["site"].lower()))
    return sites, stats


def export_workday_sites_to_yaml(
    careers_urls: Iterable[str],
    output_path: Path,
    *,
    timeout: float = 15.0,
    validate: bool = True,
) -> dict:
    """
    Export Workday sites to YAML with atomic write.
    """
    sites, stats = collect_workday_sites(
        careers_urls,
        timeout=timeout,
        validate=validate,
    )
    payload = {"workday": {"sites": sites}}

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = output_path.with_suffix(output_path.suffix + ".tmp")
    with open(tmp_path, "w", encoding="utf-8") as handle:
        yaml.safe_dump(payload, handle, sort_keys=False)
    tmp_path.replace(output_path)

    result = {
        "exported": len(sites),
        "path": str(output_path),
        "stats": stats,
    }
    logger.info("Exported %d Workday sites to %s", len(sites), output_path)
    return result
