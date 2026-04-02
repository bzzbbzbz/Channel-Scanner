"""Scraper service — orchestrates channel scraping via TelegramClient + parser."""

from __future__ import annotations

import logging
from typing import Optional

from src.scraper.client import ChannelNotFoundError, RateLimitExhaustedError, TelegramClient
from src.scraper.parser import ParsedPost, parse_page

logger = logging.getLogger(__name__)


class ScraperService:
    """Orchestrate scraping of Telegram channels.

    Uses a :class:`TelegramClient` for HTTP requests and :func:`parse_page`
    for HTML parsing. Returns :class:`ParsedPost` objects without any
    database dependency — storage is handled by the repository layer.
    """

    def __init__(self, client: TelegramClient) -> None:
        self._client = client

    async def scrape_channel(
        self,
        channel_username: str,
        max_posts: int = 20,
    ) -> list[ParsedPost]:
        """Scrape posts from a Telegram channel.

        Fetches pages sequentially, following pagination until *max_posts*
        are collected or there are no more pages.

        Args:
            channel_username: Telegram channel username (without @).
            max_posts: Maximum number of posts to collect.

        Returns:
            List of :class:`ParsedPost` objects. Empty on 404 or error.
        """
        all_posts: list[ParsedPost] = []
        before: Optional[int] = None

        try:
            while len(all_posts) < max_posts:
                html, _ = await self._client.fetch_page(channel_username, before=before)
                posts, next_url = parse_page(html)

                if not posts:
                    break

                all_posts.extend(posts)

                if next_url is None:
                    break

                # Extract the 'before' parameter from next_url for the next request
                before = self._extract_before(next_url)
                if before is None:
                    break

        except ChannelNotFoundError:
            logger.warning("Channel not found: %s — skipping", channel_username)
            return []
        except RateLimitExhaustedError:
            logger.error(
                "Rate limit exhausted for %s — returning %d posts collected so far",
                channel_username,
                len(all_posts),
            )
            return all_posts

        # Trim to max_posts
        return all_posts[:max_posts]

    @staticmethod
    def _extract_before(url: str) -> Optional[int]:
        """Extract the ``before`` parameter from a pagination URL.

        Accepts both relative (``?before=99``) and absolute URLs.
        """
        import re

        match = re.search(r"[?&]before=(\d+)", url)
        if match:
            return int(match.group(1))
        return None
