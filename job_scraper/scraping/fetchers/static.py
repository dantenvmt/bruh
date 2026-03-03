"""
Static HTML fetcher using httpx (no JavaScript rendering).
"""
import logging
import random
from typing import Optional, Tuple

import httpx

logger = logging.getLogger(__name__)

# Rotate user agents to reduce bot detection
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
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
    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate, br",
        "DNT": "1",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Cache-Control": "max-age=0",
    }

    try:
        async with httpx.AsyncClient(
            timeout=timeout,
            follow_redirects=True,
            http2=False,  # Disabled: h2 package not always installed
        ) as client:
            response = await client.get(url, headers=headers)
            response.raise_for_status()
            return response.text, None

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
