"""
Browser-based fetcher using Playwright for JavaScript-heavy pages.
"""
import asyncio
import logging
from typing import Optional, Tuple

logger = logging.getLogger(__name__)

# Lazy-loaded Playwright resources
_playwright = None
_browser = None


async def _get_browser():
    """Lazy-initialize Playwright browser (Chromium)."""
    global _playwright, _browser

    if _browser is None:
        try:
            from playwright.async_api import async_playwright
        except ImportError:
            raise ImportError(
                "Playwright is required for JS-rendered pages. "
                "Install with: pip install playwright && playwright install chromium"
            )

        _playwright = await async_playwright().start()
        _browser = await _playwright.chromium.launch(
            headless=True,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--disable-dev-shm-usage",
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-gpu",
                "--disable-extensions",
            ],
        )
        logger.info("Playwright browser initialized")

    return _browser


async def fetch_with_browser(
    url: str,
    timeout: float = 30.0,
    wait_for_selector: Optional[str] = None,
    wait_for_network_idle: bool = True,
) -> Tuple[Optional[str], Optional[str]]:
    """
    Fetch page HTML using Playwright (with JavaScript rendering).

    Args:
        url: The careers page URL to fetch
        timeout: Maximum wait time in seconds
        wait_for_selector: Optional CSS selector to wait for before extracting HTML
        wait_for_network_idle: Whether to wait for network to be idle

    Returns:
        Tuple of (html_content, error_message)
        - On success: (html_string, None)
        - On failure: (None, error_description)
    """
    context = None

    try:
        browser = await _get_browser()

        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1920, "height": 1080},
            java_script_enabled=True,
            locale="en-US",
            timezone_id="America/New_York",
        )

        page = await context.new_page()

        # Navigate to page
        wait_until = "networkidle" if wait_for_network_idle else "domcontentloaded"
        await page.goto(url, timeout=timeout * 1000, wait_until=wait_until)

        # Wait for specific selector if provided
        if wait_for_selector:
            try:
                await page.wait_for_selector(wait_for_selector, timeout=10000)
            except Exception:
                logger.warning(f"Selector '{wait_for_selector}' not found on {url}")

        # Small delay to let dynamic content settle
        await asyncio.sleep(0.5)

        # Extract full HTML
        html = await page.content()
        return html, None

    except Exception as e:
        error_msg = str(e)
        if "Timeout" in error_msg:
            error_msg = "timeout"
        elif "net::ERR" in error_msg:
            error_msg = "connection_error"
        logger.error(f"Playwright fetch failed for {url}: {error_msg}")
        return None, error_msg

    finally:
        if context:
            await context.close()


async def close_browser() -> None:
    """Clean up Playwright resources. Call on application shutdown."""
    global _playwright, _browser

    if _browser:
        try:
            await _browser.close()
        except Exception as e:
            logger.warning(f"Error closing browser: {e}")
        _browser = None

    if _playwright:
        try:
            await _playwright.stop()
        except Exception as e:
            logger.warning(f"Error stopping playwright: {e}")
        _playwright = None

    logger.info("Playwright browser closed")
