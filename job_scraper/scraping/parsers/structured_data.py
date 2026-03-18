"""
Structured data parser for extracting job listings from JSON-LD / Schema.org markup.

Many companies embed <script type="application/ld+json"> with JobPosting schema
for SEO (Google for Jobs compliance).  This data is machine-readable by design,
stable (companies don't remove it without breaking their search rankings), and
richer than what CSS or LLM parsers typically extract.

Extracts:
  - title, url, location from JobPosting objects
  - Handles both single objects and @graph arrays
  - Resolves relative URLs against base_url
"""
import json
import logging
import re
from typing import List, Optional
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from .css import _is_valid_job, _dedupe_jobs
from ..types import RawScrapedJob

logger = logging.getLogger(__name__)


def _extract_jsonld_blocks(html: str) -> List[dict]:
    """Find and parse all <script type="application/ld+json"> blocks."""
    soup = BeautifulSoup(html, "html.parser")
    blocks: List[dict] = []
    for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
        text = script.string
        if not text:
            continue
        try:
            data = json.loads(text)
            if isinstance(data, list):
                blocks.extend(d for d in data if isinstance(d, dict))
            elif isinstance(data, dict):
                blocks.append(data)
        except (json.JSONDecodeError, ValueError):
            continue
    return blocks


def _find_job_postings(blocks: List[dict]) -> List[dict]:
    """Walk JSON-LD blocks and collect all JobPosting objects."""
    postings: List[dict] = []

    for block in blocks:
        obj_type = block.get("@type", "")
        # Normalize to list for multi-type (rare but valid)
        types = obj_type if isinstance(obj_type, list) else [obj_type]

        if "JobPosting" in types:
            postings.append(block)

        # Handle @graph arrays (common in WordPress / Yoast SEO)
        graph = block.get("@graph")
        if isinstance(graph, list):
            for node in graph:
                if isinstance(node, dict):
                    node_type = node.get("@type", "")
                    node_types = node_type if isinstance(node_type, list) else [node_type]
                    if "JobPosting" in node_types:
                        postings.append(node)

        # Handle itemListElement containing JobPostings
        item_list = block.get("itemListElement")
        if isinstance(item_list, list):
            for entry in item_list:
                if isinstance(entry, dict):
                    item = entry.get("item", entry)
                    if isinstance(item, dict):
                        item_type = item.get("@type", "")
                        item_types = item_type if isinstance(item_type, list) else [item_type]
                        if "JobPosting" in item_types:
                            postings.append(item)

    return postings


def _extract_location(posting: dict) -> Optional[str]:
    """Extract location string from a JobPosting's jobLocation field."""
    loc = posting.get("jobLocation")
    if isinstance(loc, str):
        return loc.strip() or None
    if isinstance(loc, dict):
        address = loc.get("address")
        if isinstance(address, str):
            return address.strip() or None
        if isinstance(address, dict):
            parts = []
            for key in ("addressLocality", "addressRegion", "addressCountry"):
                val = address.get(key)
                if isinstance(val, str) and val.strip():
                    parts.append(val.strip())
            return ", ".join(parts) if parts else None
        # Fall back to name
        name = loc.get("name")
        if isinstance(name, str) and name.strip():
            return name.strip()
    if isinstance(loc, list) and loc:
        # Multiple locations — take first
        return _extract_location({"jobLocation": loc[0]})
    # Check applicantLocationRequirements for remote
    remote_reqs = posting.get("applicantLocationRequirements")
    if remote_reqs:
        return "Remote"
    if posting.get("jobLocationType") == "TELECOMMUTE":
        return "Remote"
    return None


def _extract_url(posting: dict, base_url: str) -> Optional[str]:
    """Extract the best URL from a JobPosting."""
    for key in ("url", "sameAs", "mainEntityOfPage"):
        val = posting.get(key)
        if isinstance(val, str) and val.strip():
            url = val.strip()
            if not url.startswith("http"):
                url = urljoin(base_url, url)
            return url
        if isinstance(val, dict):
            # mainEntityOfPage can be {"@id": "url"}
            url = val.get("@id") or val.get("url")
            if isinstance(url, str) and url.strip():
                url = url.strip()
                if not url.startswith("http"):
                    url = urljoin(base_url, url)
                return url
    # directApply link
    apply = posting.get("directApply")
    if isinstance(apply, str) and apply.startswith("http"):
        return apply
    return None


def parse_structured_data(html: str, base_url: str) -> List[RawScrapedJob]:
    """Extract job listings from JSON-LD structured data in HTML.

    Args:
        html: Raw HTML content.
        base_url: The URL the HTML was fetched from (for relative URL resolution).

    Returns:
        List of RawScrapedJob objects. Empty list if no structured data found.
    """
    blocks = _extract_jsonld_blocks(html)
    if not blocks:
        return []

    postings = _find_job_postings(blocks)
    if not postings:
        return []

    jobs: List[RawScrapedJob] = []
    for posting in postings:
        title = posting.get("title") or posting.get("name") or ""
        if isinstance(title, str):
            title = title.strip()
        if not title:
            continue

        url = _extract_url(posting, base_url)
        if not url:
            # Fall back to base_url with job ID fragment
            identifier = posting.get("identifier")
            if isinstance(identifier, dict):
                val = identifier.get("value")
                if val:
                    url = f"{base_url}#job-{val}"
            if not url:
                continue

        location = _extract_location(posting)

        job = RawScrapedJob(title=title, url=url, location=location)
        if _is_valid_job(job, base_url):
            jobs.append(job)

    jobs = _dedupe_jobs(jobs)
    if jobs:
        logger.info("Structured data (JSON-LD) extracted %d jobs from %s", len(jobs), base_url)
    return jobs
