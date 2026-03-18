"""
Static HTML fetcher using httpx (no JavaScript rendering).
"""
import logging
import random
from typing import Optional, Tuple

import httpx

logger = logging.getLogger(__name__)

# Realistic browser user agents — rotated per request
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4.1 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
]

# Rotate Accept-Language slightly to vary fingerprint
_ACCEPT_LANGUAGES = [
    "en-US,en;q=0.9",
    "en-US,en;q=0.8,es;q=0.5",
    "en-US,en;q=0.9,fr;q=0.4",
    "en-GB,en;q=0.9,en-US;q=0.8",
    "en-US,en;q=0.7",
]


async def fetch_static(url: str, timeout: float = 30.0) -> Tuple[Optional[str], Optional[str]]:
    """
    Fetch page HTML using httpx (no JavaScript rendering).

    Args:
        url: The careers page URL to fetch
        timeout: Request timeout in seconds

    Returns:
        Tuple of (html_content, error_message)
        - On success: (html_string, None)
        - On failure: (None, error_description)
    """
    ua = random.choice(USER_AGENTS)
    # Derive a plausible Sec-CH-UA from the UA string
    is_chrome = "Chrome/" in ua
    is_firefox = "Firefox/" in ua
    sec_ch_ua = (
        '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"'
        if is_chrome else
        '"Firefox";v="125", "Not-A.Brand";v="99"'
    )

    headers = {
        "User-Agent": ua,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/png,*/*;q=0.8",
        "Accept-Language": random.choice(_ACCEPT_LANGUAGES),
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Cache-Control": "max-age=0",
    }
    if is_chrome:
        headers["Sec-CH-UA"] = sec_ch_ua
        headers["Sec-CH-UA-Mobile"] = "?0"
        headers["Sec-CH-UA-Platform"] = '"Windows"' if "Windows" in ua else '"macOS"' if "Macintosh" in ua else '"Linux"'

    try:
        async with httpx.AsyncClient(
            timeout=timeout,
            follow_redirects=True,
            http2=False,  # Disabled: h2 package not always installed
        ) as client:
            response = await client.get(url, headers=headers)
            response.raise_for_status()
            html = response.text
            if not html or not html.strip():
                logger.warning("Static fetch returned empty body for %s", url)
                return None, "empty_response"
            return html, None

    except httpx.HTTPStatusError as e:
        error_msg = f"HTTP {e.response.status_code}"
        logger.warning(f"Static fetch failed for {url}: {error_msg}")
        return None, error_msg

    except httpx.TimeoutException:
        logger.warning(f"Static fetch timeout for {url}")
        return None, "timeout"

    except httpx.ConnectError as e:
        logger.warning(f"Static fetch connection error for {url}: {e}")
        return None, "connection_error"

    except Exception as e:
        logger.error(f"Static fetch unexpected error for {url}: {e}")
        return None, str(e)
