"""
Browser-based fetcher using Playwright for JavaScript-heavy pages.
"""
import asyncio
import json
import logging
import random
from urllib.parse import urlparse
from typing import Any, List, Optional, Tuple

logger = logging.getLogger(__name__)

# Randomize context fingerprint pool
_VIEWPORTS = [
    {"width": 1920, "height": 1080},
    {"width": 1440, "height": 900},
    {"width": 1366, "height": 768},
    {"width": 1536, "height": 864},
]
_TIMEZONES = [
    "America/New_York",
    "America/Chicago",
    "America/Los_Angeles",
    "America/Denver",
]
_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
]

# Injected before any page script runs — hides automation signals
_STEALTH_SCRIPT = """
Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']});
window.chrome = {runtime: {}};
"""

# Lazy-loaded Playwright resources
_playwright = None
_browser = None
_browser_loop = None


async def _get_browser():
    """Lazy-initialize Playwright browser (Chromium).

    The browser is bound to the event loop that created it.  APScheduler may
    recycle loops between runs, so we detect loop changes and re-initialise.
    """
    global _playwright, _browser, _browser_loop

    current_loop = asyncio.get_event_loop()
    if _browser is not None and _browser_loop is not current_loop:
        # Event loop changed — old handles are invalid, tear down.
        logger.info("Event loop changed, re-initialising Playwright browser")
        await close_browser()

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
                "--disable-infobars",
                "--disable-notifications",
                "--disable-popup-blocking",
                "--window-size=1920,1080",
                "--lang=en-US",
            ],
        )
        _browser_loop = current_loop
        logger.info("Playwright browser initialized")

    return _browser


async def fetch_with_browser(
    url: str,
    timeout: float = 30.0,
    wait_for_selector: Optional[str] = None,
    wait_for_network_idle: bool = True,
    capture_network: bool = False,
    capture_screenshot: bool = False,
    skip_interactions: bool = False,
) -> Tuple[Optional[str], Optional[str], Optional[List[Any]], Optional[bytes]]:
    """
    Fetch page HTML using Playwright (with JavaScript rendering).

    Args:
        url: The careers page URL to fetch
        timeout: Maximum wait time in seconds
        wait_for_selector: Optional CSS selector to wait for before extracting HTML
        wait_for_network_idle: Whether to wait for network to be idle
        capture_network: When True, attach request/response listeners and
            return captured XHR/Fetch calls as the third tuple element.
        capture_screenshot: When True, capture a full-page PNG screenshot
            returned as the fourth tuple element.

    Returns:
        Tuple of (html_content, error_message, captured_calls, screenshot_png)
        - On success: (html_string, None, [CapturedCall, ...] or None, png_bytes or None)
        - On failure: (None, error_description, None, None)
    """
    context = None

    try:
        browser = await _get_browser()

        ua = random.choice(_USER_AGENTS)
        viewport = random.choice(_VIEWPORTS)
        timezone_id = random.choice(_TIMEZONES)
        context = await browser.new_context(
            user_agent=ua,
            viewport=viewport,
            java_script_enabled=True,
            locale="en-US",
            timezone_id=timezone_id,
            color_scheme="light",
            extra_http_headers={
                "Accept-Language": "en-US,en;q=0.9",
                "Accept-Encoding": "gzip, deflate, br",
            },
        )
        await context.add_init_script(_STEALTH_SCRIPT)

        page = await context.new_page()

        # --- Optional network capture (same pattern as NetworkSpy._capture) ---
        captured_calls: Optional[List[Any]] = None
        if capture_network:
            from .network_spy import CapturedCall

            captured_calls = []
            pending: dict[str, Any] = {}
            _capture_types = ("xhr", "fetch")

            async def _on_request(req: Any) -> None:
                if req.resource_type not in _capture_types:
                    return
                call = CapturedCall(
                    method=req.method,
                    url=req.url,
                    resource_type=req.resource_type,
                    request_headers=dict(req.headers),
                    request_post_data=req.post_data,
                )
                pending[req.url] = call

            async def _on_response(resp: Any) -> None:
                call = pending.pop(resp.url, None)
                if call is None:
                    return
                call.response_status = resp.status
                call.response_headers = dict(resp.headers)
                try:
                    body = await resp.body()
                    call.response_body = body
                    call.response_text = body.decode("utf-8", errors="replace")
                    if call.is_json:
                        call.response_json = json.loads(call.response_text)
                except Exception as exc:
                    call.error = str(exc)
                    logger.debug("Could not read response body for %s: %s", resp.url, exc)
                captured_calls.append(call)

            page.on("request", _on_request)
            page.on("response", _on_response)

        # Warm-up: visit homepage first so the site sees a returning visitor
        # with cookies/session state rather than a cold direct hit on careers URL.
        parsed = urlparse(url)
        homepage = f"{parsed.scheme}://{parsed.netloc}"
        if homepage and homepage != url:
            try:
                await page.goto(homepage, timeout=15_000, wait_until="domcontentloaded")
                await asyncio.sleep(random.uniform(0.5, 1.0))
                from ._interactions import scroll_page
                await scroll_page(page)
                await asyncio.sleep(random.uniform(0.5, 1.0))
            except Exception:
                pass  # warm-up is best-effort; proceed regardless

        # Navigate to target page
        wait_until = "networkidle" if wait_for_network_idle else "domcontentloaded"
        await page.goto(url, timeout=timeout * 1000, wait_until=wait_until)

        # Wait for specific selector if provided
        if wait_for_selector:
            try:
                await page.wait_for_selector(wait_for_selector, timeout=10000)
            except Exception:
                logger.warning(f"Selector '{wait_for_selector}' not found on {url}")

        # Scroll and click load-more so lazy-loaded content is visible
        # before we hand the HTML to parsers.  Skipped for detail-page
        # fetches where we only need the static rendered content.
        if not skip_interactions:
            from ._interactions import scroll_page, click_load_more
            await scroll_page(page)
            await click_load_more(page)

        # Settle after interactions — longer when capturing to catch late XHR
        settle_time = 1.5 if capture_network else 1.0
        await asyncio.sleep(settle_time)

        # Extra networkidle wait when capturing (match NetworkSpy behavior)
        if capture_network:
            try:
                await page.wait_for_load_state("networkidle", timeout=5000)
            except Exception:
                pass

        # Capture screenshot before extracting HTML (page is fully rendered)
        screenshot_png: Optional[bytes] = None
        if capture_screenshot:
            try:
                screenshot_png = await page.screenshot(type="png", full_page=True)
                # Groq vision limit: ~4MB base64 → 3.5MB raw PNG guard
                if screenshot_png and len(screenshot_png) > 3_500_000:
                    logger.warning(
                        "Screenshot too large (%.1fMB) for %s, skipping",
                        len(screenshot_png) / 1_000_000,
                        url,
                    )
                    screenshot_png = None
            except Exception as exc:
                logger.debug("Screenshot capture failed for %s: %s", url, exc)

        # Extract full HTML
        html = await page.content()
        if not html or not html.strip():
            logger.warning("Browser fetch returned empty body for %s", url)
            return None, "empty_response", None, None
        return html, None, captured_calls, screenshot_png

    except Exception as e:
        error_msg = str(e)
        if "Timeout" in error_msg:
            error_msg = "timeout"
        elif "net::ERR" in error_msg:
            error_msg = "connection_error"
        logger.error(f"Playwright fetch failed for {url}: {error_msg}")
        return None, error_msg, None, None

    finally:
        if context:
            await context.close()


async def close_browser() -> None:
    """Clean up Playwright resources. Call on application shutdown."""
    global _playwright, _browser, _browser_loop

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

    _browser_loop = None
    logger.info("Playwright browser closed")
