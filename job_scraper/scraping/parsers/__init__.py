"""
Parser modules for extracting job listings from HTML.
"""
from .css import parse_with_selectors, ParseError
from .text import extract_from_text

__all__ = ["parse_with_selectors", "ParseError", "extract_from_text"]
