"""
Shared Playwright page interaction helpers.

Used by both NetworkSpy (for network capture) and the browser fetcher
(for HTML extraction) so JS-heavy, lazy-loaded pages are fully rendered
before content is read.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any

logger = logging.getLogger(__name__)


async def scroll_page(page: Any) -> None:
    """Scroll down in steps to trigger lazy-loaded content, then back to top."""
    try:
        for _ in range(6):
            await page.evaluate("window.scrollBy(0, window.innerHeight * 0.8)")
            await asyncio.sleep(0.4)
        # Scroll back to top — some sites re-fetch on scroll-up
        await page.evaluate("window.scrollTo(0, 0)")
    except Exception as exc:
        logger.debug("Scroll failed (non-fatal): %s", exc)


async def click_load_more(page: Any) -> None:
    """Click common 'Load More' / 'Next' pagination triggers once."""
    selectors = [
        "button:has-text('Load more')",
        "button:has-text('Show more')",
        "button:has-text('More jobs')",
        "button:has-text('See more jobs')",
        "button:has-text('View more')",
        "[data-automation='pagination-next']",
        "[aria-label='Next page']",
        "[aria-label='Load more']",
        "a:has-text('Next')",
        ".load-more",
        "#load-more",
        "[class*='loadMore']",
        "[class*='load-more']",
        "[class*='show-more']",
    ]
    for sel in selectors:
        try:
            btn = page.locator(sel).first
            if await btn.is_visible(timeout=800):
                await btn.click()
                await asyncio.sleep(1.5)
                logger.debug("Clicked load-more selector: %s", sel)
                break
        except Exception:
            continue
