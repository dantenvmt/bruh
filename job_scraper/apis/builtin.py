"""
Built In multi-city jobs integration.

Built In does not expose a stable public API. This adapter performs lightweight
HTML extraction from each configured city/jobs board page.
"""
from __future__ import annotations

import asyncio
import html
import json
import logging
import re
import time
from typing import Dict, List, Optional
from urllib.parse import urljoin, urlparse

import httpx

from . import BaseJobAPI
from ..models import Job

logger = logging.getLogger(__name__)


class BuiltInAPI(BaseJobAPI):
    """Built In jobs scraper."""

    DEFAULT_DOMAINS = [
        "https://www.builtinnyc.com",
        "https://www.builtinsf.com",
        "https://www.builtinseattle.com",
        "https://www.builtinaustin.com",
        "https://www.builtinchicago.org",
        "https://www.builtinla.com",
        "https://www.builtinboston.com",
        "https://www.builtincolorado.com",
        "https://www.builtinwashingtondc.com",
        "https://www.builtinatlanta.com",
        "https://www.builtinportland.com",
    ]

    CITY_LABELS = {
        "builtinnyc": "New York, NY",
        "builtinsf": "San Francisco, CA",
        "builtinseattle": "Seattle, WA",
        "builtinaustin": "Austin, TX",
        "builtinchicago": "Chicago, IL",
        "builtinla": "Los Angeles, CA",
        "builtinboston": "Boston, MA",
        "builtincolorado": "Colorado, US",
        "builtinwashingtondc": "Washington, DC",
        "builtinatlanta": "Atlanta, GA",
        "builtinportland": "Portland, OR",
    }

    CARD_PATTERN = re.compile(
        r'<div id="job-card-(?P<job_id>\d+)".*?'
        r'<a(?=[^>]*data-id="company-title")[^>]*><span>(?P<company>.*?)</span>.*?'
        r'<a(?=[^>]*data-id="job-card-title")(?=[^>]*href="(?P<href>/job/[^"]+)")[^>]*>(?P<title>.*?)</a>',
        re.IGNORECASE | re.DOTALL,
    )
    PUBLISHED_PATTERN = re.compile(
        r"'id':(?P<job_id>\d+),'published_date':'(?P<published>[^']+)'",
        re.IGNORECASE,
    )
    JSON_LD_PATTERN = re.compile(
        r'<script[^>]*type="application/ld(?:\+|&#x2B;)json"[^>]*>(?P<payload>.*?)</script>',
        re.IGNORECASE | re.DOTALL,
    )

    def __init__(
        self,
        domains: Optional[List[str]] = None,
        max_pages: int = 5,
        requests_per_minute: int = 60,
    ):
        super().__init__(name="BuiltIn")
        self.domains = self._normalize_domains(domains or self.DEFAULT_DOMAINS)
        self.max_pages = max(1, int(max_pages or 5))
        self.requests_per_minute = max(1, int(requests_per_minute or 60))
        self._rate_lock = asyncio.Lock()
        self._last_request_time = 0.0

    def is_configured(self) -> bool:
        return bool(self.domains)

    async def search_jobs(
        self,
        query: Optional[str] = None,
        location: Optional[str] = None,
        max_results: int = 100,
        **kwargs,
    ) -> List[Job]:
        needle_query = (query or "").strip().lower()
        needle_location = (location or "").strip().lower()

        jobs: List[Job] = []
        seen_urls = set()

        async with httpx.AsyncClient(timeout=30.0) as client:
            for domain in self.domains:
                city_label = self._city_label_for_domain(domain)
                for page in range(1, self.max_pages + 1):
                    page_url = f"{domain}/jobs"
                    if page > 1:
                        page_url = f"{page_url}?page={page}"

                    page_html = await self._fetch_page(client, page_url)
                    if not page_html:
                        break

                    parsed_jobs = self._extract_jobs(page_html, domain, city_label)
                    if not parsed_jobs:
                        break

                    added_on_page = 0
                    for job in parsed_jobs:
                        if job.url in seen_urls:
                            continue
                        if needle_query and not self._matches_query(job, needle_query):
                            continue
                        if needle_location and needle_location not in (job.location or "").lower():
                            continue

                        seen_urls.add(job.url)
                        jobs.append(job)
                        added_on_page += 1

                        if len(jobs) >= max_results:
                            logger.info(f"BuiltIn reached max_results ({max_results})")
                            return jobs

                    if added_on_page == 0:
                        break

        logger.info(f"BuiltIn returned {len(jobs)} jobs")
        return jobs

    async def _fetch_page(self, client: httpx.AsyncClient, url: str) -> Optional[str]:
        await self._wait_for_slot()
        try:
            response = await client.get(url, follow_redirects=True)
            if response.status_code != 200:
                return None
            return response.text
        except Exception as exc:
            logger.debug(f"BuiltIn fetch failed for {url}: {exc}")
            return None

    def _extract_jobs(self, html_text: str, domain: str, city_label: str) -> List[Job]:
        published_dates = self._extract_published_dates(html_text)
        descriptions = self._extract_descriptions(html_text)
        jobs: List[Job] = []

        for match in self.CARD_PATTERN.finditer(html_text):
            job_id = match.group("job_id")
            href = html.unescape(match.group("href") or "").strip()
            title = self._clean_text(match.group("title"))
            company = self._clean_text(match.group("company"))
            if not href or not title or not company:
                continue

            url = urljoin(domain, href)
            description = descriptions.get(url)
            remote = False
            if "remote" in title.lower() or (description and "remote" in description.lower()):
                remote = True

            jobs.append(
                Job(
                    title=title,
                    company=company,
                    location=city_label,
                    url=url,
                    description=description,
                    salary=None,
                    employment_type=None,
                    posted_date=published_dates.get(job_id),
                    source="BuiltIn",
                    job_id=job_id,
                    category=None,
                    tags=None,
                    skills=None,
                    remote=remote,
                    raw_payload={"job_id": job_id, "href": href, "domain": domain},
                )
            )

        return jobs

    def _extract_published_dates(self, html_text: str) -> Dict[str, str]:
        mapping: Dict[str, str] = {}
        for match in self.PUBLISHED_PATTERN.finditer(html_text):
            mapping[match.group("job_id")] = match.group("published")
        return mapping

    def _extract_descriptions(self, html_text: str) -> Dict[str, str]:
        descriptions: Dict[str, str] = {}
        for match in self.JSON_LD_PATTERN.finditer(html_text):
            payload = html.unescape(match.group("payload") or "").strip()
            if not payload:
                continue
            try:
                data = json.loads(payload)
            except json.JSONDecodeError:
                continue

            graph = data.get("@graph") if isinstance(data, dict) else None
            if not isinstance(graph, list):
                continue

            for node in graph:
                if not isinstance(node, dict) or node.get("@type") != "ItemList":
                    continue
                entries = node.get("itemListElement") or []
                if not isinstance(entries, list):
                    continue
                for entry in entries:
                    if not isinstance(entry, dict):
                        continue
                    url = entry.get("url")
                    description = entry.get("description")
                    if isinstance(url, str) and isinstance(description, str) and description.strip():
                        descriptions[url] = description.strip()
        return descriptions

    def _normalize_domains(self, domains: List[str]) -> List[str]:
        normalized = []
        seen = set()
        for value in domains:
            raw = (value or "").strip()
            if not raw:
                continue
            if not raw.startswith("http://") and not raw.startswith("https://"):
                raw = f"https://{raw}"
            raw = raw.rstrip("/")
            if raw in seen:
                continue
            normalized.append(raw)
            seen.add(raw)
        return normalized

    def _city_label_for_domain(self, domain: str) -> str:
        hostname = urlparse(domain).hostname or ""
        if hostname.startswith("www."):
            hostname = hostname[4:]
        city_key = hostname.split(".", 1)[0].lower()
        return self.CITY_LABELS.get(city_key, city_key.replace("builtin", "BuiltIn ").strip())

    def _clean_text(self, value: Optional[str]) -> str:
        if not value:
            return ""
        text = re.sub(r"<[^>]+>", "", value)
        text = html.unescape(text)
        return " ".join(text.split()).strip()

    def _matches_query(self, job: Job, needle: str) -> bool:
        haystack = " ".join(
            [
                job.title or "",
                job.company or "",
                job.location or "",
                job.description or "",
            ]
        ).lower()
        return needle in haystack

    async def _wait_for_slot(self) -> None:
        min_interval = 60.0 / self.requests_per_minute if self.requests_per_minute > 0 else 0.0
        if min_interval <= 0:
            return

        async with self._rate_lock:
            now = time.monotonic()
            elapsed = now - self._last_request_time
            wait_seconds = max(0.0, min_interval - elapsed)
            if wait_seconds > 0:
                await asyncio.sleep(wait_seconds)
            self._last_request_time = time.monotonic()
