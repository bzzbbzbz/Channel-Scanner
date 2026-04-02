"""Scheduler jobs — periodic scraping via APScheduler."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from sqlalchemy.ext.asyncio import async_sessionmaker

from src.repository.channel import ChannelRepository
from src.repository.post import PostRepository
from src.scraper.client import ChannelNotFoundError, TelegramClient
from src.scraper.service import ScraperService

from src.config.settings import Settings

logger = logging.getLogger(__name__)


async def scraping_job(
    session_factory: async_sessionmaker,
    client: TelegramClient,
    max_posts: int = 20,
) -> None:
    """Scrape all active channels and store new posts.

    Iterates active channels sequentially (per CONTEXT decision),
    scraping each and storing posts with deduplication.

    Args:
        session_factory: SQLAlchemy async session factory.
        client: Telegram HTTP client for fetching channel pages.
        max_posts: Maximum posts to scrape per channel.
    """
    total_inserted = 0
    channels_scraped = 0

    async with session_factory() as session:
        channel_repo = ChannelRepository(session)
        post_repo = PostRepository(session)

        channels = await channel_repo.get_active_channels()

        if not channels:
            logger.info("No active channels to scrape")
            return

        # Snapshot channel data to avoid expired-attribute issues after rollback
        channel_data = [
            {"id": ch.id, "username": ch.username or ""}
            for ch in channels
        ]

        for ch_info in channel_data:
            ch_id = ch_info["id"]
            ch_username = ch_info["username"]
            try:
                service = ScraperService(client)
                posts = await service.scrape_channel(
                    ch_username,
                    max_posts=max_posts,
                )

                if posts:
                    inserted = await post_repo.upsert_posts(ch_id, posts)
                    total_inserted += inserted

                await channel_repo.mark_scraped(ch_id)
                await session.commit()
                channels_scraped += 1

                logger.info(
                    "Scraped channel %s: %d posts found, %d new",
                    ch_username,
                    len(posts),
                    inserted if posts else 0,
                )

            except ChannelNotFoundError:
                await channel_repo.mark_error(
                    ch_id, "Channel not found or private"
                )
                await session.commit()
                logger.warning(
                    "Channel %s not found — marked as error",
                    ch_username,
                )

            except Exception:
                logger.exception(
                    "Error scraping channel %s — skipping",
                    ch_username,
                )
                await session.rollback()

    logger.info(
        "Scraping complete: %d channels scraped, %d new posts",
        channels_scraped,
        total_inserted,
    )


def create_scheduler(
    settings: Settings,
    session_factory: async_sessionmaker,
    client: TelegramClient,
) -> AsyncIOScheduler:
    """Create and configure an AsyncIOScheduler with a scraping job.

    Args:
        settings: Application settings (provides interval_minutes).
        session_factory: SQLAlchemy async session factory.
        client: Telegram HTTP client.

    Returns:
        Configured AsyncIOScheduler (not yet started).
    """
    scheduler = AsyncIOScheduler()

    scheduler.add_job(
        scraping_job,
        trigger=IntervalTrigger(minutes=settings.scheduler.interval_minutes),
        id="scraping_job",
        name="Periodic channel scraping",
        kwargs={
            "session_factory": session_factory,
            "client": client,
            "max_posts": settings.scraper.max_posts,
        },
        misfire_grace_time=60,
        coalesce=True,
    )

    logger.info(
        "Scheduler configured: interval=%d minutes",
        settings.scheduler.interval_minutes,
    )

    return scheduler
