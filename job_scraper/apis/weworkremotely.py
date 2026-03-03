"""
We Work Remotely RSS integration.
"""
from __future__ import annotations

import logging
from html import unescape
from typing import List, Optional
from xml.etree import ElementTree

import httpx

from . import BaseJobAPI
from ..models import Job

logger = logging.getLogger(__name__)


class WeWorkRemotelyAPI(BaseJobAPI):
    """Client for We Work Remotely RSS feed."""

    DEFAULT_BASE_URL = "https://weworkremotely.com/remote-jobs.rss"

    def __init__(self, base_url: Optional[str] = None):
        super().__init__(name="WeWorkRemotely")
        self.base_url = base_url or self.DEFAULT_BASE_URL

    def is_configured(self) -> bool:
        return True

    async def search_jobs(
        self,
        query: Optional[str] = None,
        location: Optional[str] = None,
        max_results: int = 100,
        **kwargs,
    ) -> List[Job]:
        params = {}
        if query:
            params["term"] = query

        jobs: List[Job] = []
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.get(self.base_url, params=params)
                response.raise_for_status()
                items = self._parse_feed(response.text)

                for item in items:
                    job = self._parse_job(item)
                    if not job:
                        continue
                    if query and not self._matches_query(job, query):
                        continue
                    if location and not self._matches_location(job, location):
                        continue

                    jobs.append(job)
                    if len(jobs) >= max_results:
                        break

        except Exception as exc:
            logger.error(f"WeWorkRemotely error: {exc}")

        logger.info(f"WeWorkRemotely returned {len(jobs)} jobs")
        return jobs

    def _parse_feed(self, xml_text: str) -> List[dict]:
        items: List[dict] = []
        try:
            root = ElementTree.fromstring(xml_text)
        except ElementTree.ParseError as exc:
            logger.error(f"WeWorkRemotely RSS parse error: {exc}")
            return items

        channel = root.find("channel")
        if channel is None:
            return items

        for item in channel.findall("item"):
            entry = {}
            for child in item:
                tag_name = child.tag.rsplit("}", 1)[-1]
                entry[tag_name] = (child.text or "").strip()
            items.append(entry)
        return items

    def _parse_job(self, item: dict) -> Optional[Job]:
        title = unescape(item.get("title") or "").strip()
        if not title:
            return None

        company = "Unknown"
        job_title = title
        if ":" in title:
            split = title.split(":", 1)
            company = split[0].strip() or company
            job_title = split[1].strip() or job_title

        region = item.get("region") or ""
        state = item.get("state") or ""
        country = item.get("country") or ""
        location_parts = [part for part in (region, state, country) if part]
        location = ", ".join(location_parts) if location_parts else "Remote"

        raw_skills = item.get("skills") or ""
        skills = [s.strip() for s in raw_skills.split(",") if s.strip()] if raw_skills else None

        return Job(
            title=job_title,
            company=company,
            location=location,
            url=item.get("link") or item.get("guid"),
            description=item.get("description"),
            salary=None,
            employment_type=item.get("type"),
            posted_date=item.get("pubDate"),
            source="WeWorkRemotely",
            job_id=item.get("guid") or item.get("link"),
            category=item.get("category"),
            tags=skills,
            skills=skills,
            remote=True,
            raw_payload=item,
        )

    def _matches_query(self, job: Job, query: str) -> bool:
        needle = query.lower()
        haystack = " ".join(
            [
                job.title or "",
                job.company or "",
                job.description or "",
                " ".join(job.tags or []),
            ]
        ).lower()
        return needle in haystack

    def _matches_location(self, job: Job, location: str) -> bool:
        needle = location.lower()
        return needle in (job.location or "").lower()
