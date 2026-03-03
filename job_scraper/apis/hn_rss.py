"""
Hacker News RSS job feed integration.

Uses the public HN RSS feed (https://hnrss.org/jobs).
No authentication required.
"""
import logging
from html import unescape
from typing import List, Optional
from xml.etree import ElementTree

import httpx

from . import BaseJobAPI
from ..models import Job

logger = logging.getLogger(__name__)


class HNRSSAPI(BaseJobAPI):
    """HN RSS job feed client."""

    DEFAULT_BASE_URL = "https://hnrss.org/jobs"

    def __init__(self, base_url: Optional[str] = None):
        super().__init__(name="HN RSS")
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
            params["q"] = query

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

                    if query and not self._match_query(job, item, query):
                        continue

                    if location and not self._match_location(job, location):
                        continue

                    jobs.append(job)
                    if len(jobs) >= max_results:
                        break

            logger.info(f"HN RSS returned {len(jobs)} jobs")
            return jobs
        except Exception as exc:
            logger.error(f"HN RSS error: {exc}")
            return jobs

    def _parse_feed(self, xml_text: str) -> List[dict]:
        items: List[dict] = []
        try:
            root = ElementTree.fromstring(xml_text)
        except ElementTree.ParseError as exc:
            logger.error(f"HN RSS parse error: {exc}")
            return items

        channel = root.find("channel")
        if channel is None:
            return items

        for item in channel.findall("item"):
            title = unescape(item.findtext("title") or "").strip()
            link = (item.findtext("link") or "").strip()
            description = unescape(item.findtext("description") or "").strip()
            pub_date = (item.findtext("pubDate") or "").strip()
            guid = (item.findtext("guid") or "").strip()
            categories = [c.text.strip() for c in item.findall("category") if c.text]
            items.append(
                {
                    "title": title,
                    "link": link,
                    "description": description,
                    "pub_date": pub_date,
                    "guid": guid,
                    "categories": categories,
                }
            )

        return items

    def _parse_job(self, item: dict) -> Optional[Job]:
        title = item.get("title") or ""
        if not title:
            return None

        role, company, location = self._split_title(title)
        description = item.get("description") or None
        link = item.get("link") or None
        guid = item.get("guid") or None
        posted = item.get("pub_date") or None

        remote = False
        if "remote" in title.lower() or (description and "remote" in description.lower()):
            remote = True

        return Job(
            title=role,
            company=company or "Unknown",
            location=location,
            url=link,
            description=description,
            salary=None,
            employment_type=None,
            posted_date=posted,
            source="HN RSS",
            job_id=guid or link,
            category=None,
            tags=item.get("categories") or None,
            skills=None,
            remote=remote,
            raw_payload=item,
        )

    def _split_title(self, title: str) -> tuple[str, Optional[str], Optional[str]]:
        parts = [p.strip() for p in title.split("|") if p.strip()]
        if len(parts) >= 2:
            company = parts[0]
            role = parts[1]
            location = None
            if len(parts) >= 3:
                location = " | ".join(parts[2:])
            return role, company, location
        return title, None, None

    def _match_query(self, job: Job, item: dict, query: str) -> bool:
        needle = query.lower()
        haystack = " ".join(
            [
                job.title or "",
                job.company or "",
                job.location or "",
                item.get("description") or "",
            ]
        ).lower()
        return needle in haystack

    def _match_location(self, job: Job, location: str) -> bool:
        needle = location.lower()
        haystack = " ".join([job.location or "", job.title or ""]).lower()
        return needle in haystack
