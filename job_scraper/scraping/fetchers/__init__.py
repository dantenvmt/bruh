"""
Fetcher modules for retrieving HTML content from career pages.
"""
from .static import fetch_static
from .browser import fetch_with_browser, close_browser

__all__ = ["fetch_static", "fetch_with_browser", "close_browser"]
