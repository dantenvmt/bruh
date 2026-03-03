"""
Text-based parser for extracting job listings from plain text or markdown.

Phase A stub - not implemented yet.
"""
from typing import List
from job_scraper.models import Job


def extract_from_text(text: str, source_url: str) -> List[Job]:
    """
    Extract job listings from plain text or markdown content.

    Args:
        text: Plain text or markdown content containing job listings
        source_url: URL of the source page

    Returns:
        List of Job objects extracted from the text

    Raises:
        NotImplementedError: Phase A stub - not implemented yet
    """
    raise NotImplementedError("Text extraction not implemented in Phase A")
