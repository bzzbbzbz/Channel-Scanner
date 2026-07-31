"""Scheduler jobs — periodic scraping via APScheduler."""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from sqlalchemy.ext.asyncio import async_sessionmaker

from src.assistant.memory import AssistantMemoryService
from src.digest.service import DigestService
from src.llm import OpenRouterClient, OpenRouterModelPool
from src.knowledge.service import KnowledgeService
from src.repository.llm_usage import build_usage_recorder
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


async def digest_delivery_job(
    session_factory: async_sessionmaker,
    bot_token: str,
    llm_settings,
    model_pool: OpenRouterModelPool | None = None,
    memory_service: AssistantMemoryService | None = None,
) -> None:
    """Deliver scheduled digests to users through the Bot API."""
    delivered_users = await DigestService(
        session_factory,
        bot_token,
        llm_settings=llm_settings,
        model_pool=model_pool,
        memory_service=memory_service,
    ).run_once(
        now=datetime.now(timezone.utc),
    )
    logger.info("Digest delivery cycle complete: %d users served", delivered_users)


async def llm_model_refresh_job(session_factory: async_sessionmaker, llm_settings, model_pool: OpenRouterModelPool) -> None:
    """Refresh OpenRouter model metadata for runtime model selection."""
    if not llm_settings.openrouter_api_key:
        logger.info("LLM model refresh skipped because OPENROUTER_API_KEY is empty")
        return
    client = OpenRouterClient(
        api_key=llm_settings.openrouter_api_key,
        base_url=llm_settings.openrouter_base_url,
        timeout_seconds=llm_settings.timeout_seconds,
        telemetry_recorder=build_usage_recorder(session_factory),
    )
    try:
        await model_pool.refresh_if_due(client, force=True)
    finally:
        await client.close()


async def knowledge_index_job(session_factory: async_sessionmaker, knowledge_service: KnowledgeService) -> None:
    """Retry bounded failed knowledge work and index newly scraped catalog posts."""
    attempted, completed = await knowledge_service.retry_failed_indexing()
    logger.info("Knowledge indexing cycle complete: attempted=%d completed=%d", attempted, completed)


def create_scheduler(
    settings: Settings,
    session_factory: async_sessionmaker,
    client: TelegramClient,
    model_pool: OpenRouterModelPool | None = None,
    memory_service: AssistantMemoryService | None = None,
    knowledge_service: KnowledgeService | None = None,
) -> AsyncIOScheduler:
    """Create and configure an AsyncIOScheduler with a scraping job.

    Args:
        settings: Application settings (provides interval_minutes).
        session_factory: SQLAlchemy async session factory.
        client: Telegram HTTP client.
        model_pool: Optional shared OpenRouter model pool.

    Returns:
        Configured AsyncIOScheduler (not yet started).
    """
    scheduler = AsyncIOScheduler()
    memory_service = memory_service or (AssistantMemoryService(settings.memory, settings.llm) if settings.bot.token else None)

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

    if settings.bot.token:
        scheduler.add_job(
            digest_delivery_job,
            trigger=IntervalTrigger(minutes=settings.scheduler.interval_minutes),
            id="digest_delivery_job",
            name="Scheduled digest delivery",
            kwargs={
                "session_factory": session_factory,
                "bot_token": settings.bot.token,
                "llm_settings": settings.llm,
                "model_pool": model_pool,
                "memory_service": memory_service,
            },
            misfire_grace_time=60,
            coalesce=True,
        )
    else:
        logger.warning("Digest delivery job not scheduled because BOT_TOKEN is empty")

    if model_pool is not None and settings.llm.openrouter_api_key:
        scheduler.add_job(
            llm_model_refresh_job,
            trigger=IntervalTrigger(hours=1),
            id="llm_model_refresh_job",
            name="Refresh OpenRouter free model pool",
            kwargs={"session_factory": session_factory, "llm_settings": settings.llm, "model_pool": model_pool},
            misfire_grace_time=60,
            coalesce=True,
        )

    if knowledge_service is not None and settings.knowledge.enabled:
        scheduler.add_job(
            knowledge_index_job,
            trigger=IntervalTrigger(hours=settings.knowledge.sync_interval_hours),
            id="knowledge_index_job",
            name="Index approved knowledge channels",
            kwargs={"session_factory": session_factory, "knowledge_service": knowledge_service},
            misfire_grace_time=300,
            coalesce=True,
        )

    logger.info(
        "Scheduler configured: interval=%d minutes",
        settings.scheduler.interval_minutes,
    )

    return scheduler
