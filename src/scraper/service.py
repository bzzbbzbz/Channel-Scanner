"""Scraper service — orchestrates channel scraping via TelegramClient + parser."""

from __future__ import annotations

import logging
from datetime import datetime
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

    async def scrape_channel_period(
        self,
        channel_username: str,
        period_start: datetime,
        period_end: datetime,
        max_posts: int = 100,
    ) -> list[ParsedPost]:
        """Fetch only posts in a period, stopping once pagination reaches its start."""
        matched: list[ParsedPost] = []
        before: Optional[int] = None

        try:
            while len(matched) < max_posts:
                html, _ = await self._client.fetch_page(channel_username, before=before)
                posts, next_url = parse_page(html)
                if not posts:
                    break

                reached_period_start = False
                for post in posts:
                    published_at = datetime.fromisoformat(post.datetime.replace("Z", "+00:00"))
                    if period_start <= published_at < period_end:
                        matched.append(post)
                        if len(matched) >= max_posts:
                            return matched
                    if published_at < period_start:
                        reached_period_start = True

                if reached_period_start or next_url is None:
                    break
                before = self._extract_before(next_url)
                if before is None:
                    break
        except ChannelNotFoundError:
            logger.warning("Channel not found: %s — skipping", channel_username)
        except RateLimitExhaustedError:
            logger.error("Rate limit exhausted for %s — returning %d matching posts", channel_username, len(matched))

        return matched

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
