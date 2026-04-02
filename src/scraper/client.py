"""Async HTTP client for t.me/s/* with rate limiting and 429 backoff."""

from __future__ import annotations

import asyncio
import logging
import random
from typing import Optional

import httpx

from src.config.settings import ScraperSettings

logger = logging.getLogger(__name__)


class ChannelNotFoundError(Exception):
    """Raised when the Telegram channel is not found (HTTP 404)."""

    def __init__(self, channel_username: str) -> None:
        self.channel_username = channel_username
        super().__init__(f"Channel not found: {channel_username}")


class RateLimitExhaustedError(Exception):
    """Raised when retries are exhausted after repeated 429 responses."""

    def __init__(self, channel_username: str, retries: int) -> None:
        self.channel_username = channel_username
        self.retries = retries
        super().__init__(
            f"Rate limit exhausted for {channel_username} after {retries} retries"
        )


class TelegramClient:
    """Async HTTP client for fetching Telegram channel pages via t.me/s/*.

    Features:
    - Configurable rate limiting (requests per second)
    - Exponential backoff with jitter on HTTP 429
    - Custom User-Agent header
    - Context-manager support
    """

    def __init__(
        self,
        settings: Optional[ScraperSettings] = None,
        *,
        rate_limit_per_sec: float = 1.0,
        user_agent: str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    ) -> None:
        if settings is not None:
            self._rate_limit_per_sec = settings.rate_limit_per_sec
            self._user_agent = settings.user_agent
        else:
            self._rate_limit_per_sec = rate_limit_per_sec
            self._user_agent = user_agent

        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(connect=30.0, read=60.0, write=30.0, pool=30.0),
            headers={"User-Agent": self._user_agent},
            follow_redirects=True,
        )
        self._min_interval = 1.0 / self._rate_limit_per_sec if self._rate_limit_per_sec > 0 else 0
        self._last_request_time: float = 0.0
        self._max_retries = 5

    async def _rate_limit(self) -> None:
        """Sleep to respect the configured rate limit."""
        if self._min_interval <= 0:
            return
        now = asyncio.get_event_loop().time()
        elapsed = now - self._last_request_time
        if elapsed < self._min_interval:
            await asyncio.sleep(self._min_interval - elapsed)
        self._last_request_time = asyncio.get_event_loop().time()

    async def fetch_page(
        self, channel_username: str, before: Optional[int] = None
    ) -> tuple[str, int]:
        """Fetch a channel page from t.me/s/*.

        Args:
            channel_username: Telegram channel username (without @).
            before: If set, fetch posts older than this post_id (pagination).

        Returns:
            Tuple of (html_text, status_code).

        Raises:
            ChannelNotFoundError: On HTTP 404.
            RateLimitExhaustedError: When all retries are consumed on 429.
            httpx.HTTPStatusError: On other non-2xx responses.
        """
        url = f"https://t.me/s/{channel_username}"
        if before is not None:
            url = f"{url}?before={before}"

        backoff = 1.0
        max_backoff = 30.0

        for attempt in range(self._max_retries + 1):
            await self._rate_limit()

            try:
                response = await self._client.get(url)
            except httpx.TransportError as exc:
                if attempt == self._max_retries:
                    raise
                logger.warning(
                    "Transport error fetching %s (attempt %d): %s",
                    url,
                    attempt + 1,
                    exc,
                )
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, max_backoff)
                continue

            if response.status_code == 200:
                return response.text, 200

            if response.status_code == 404:
                raise ChannelNotFoundError(channel_username)

            if response.status_code == 429:
                if attempt == self._max_retries:
                    raise RateLimitExhaustedError(channel_username, self._max_retries)
                jitter = backoff * random.uniform(0.8, 1.2)
                logger.warning(
                    "HTTP 429 for %s, backing off %.1fs (attempt %d/%d)",
                    channel_username,
                    jitter,
                    attempt + 1,
                    self._max_retries,
                )
                await asyncio.sleep(jitter)
                backoff = min(backoff * 2, max_backoff)
                continue

            # Other error — raise
            response.raise_for_status()
            # Shouldn't reach here, but just in case
            return response.text, response.status_code

        # Should not be reachable, but safety fallback
        raise RateLimitExhaustedError(channel_username, self._max_retries)

    async def close(self) -> None:
        """Close the underlying HTTP client."""
        await self._client.aclose()

    async def __aenter__(self) -> "TelegramClient":
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.close()
