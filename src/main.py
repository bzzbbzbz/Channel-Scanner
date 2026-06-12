"""Application entry point — boots scheduler, DB, and runs forever."""

from __future__ import annotations

import asyncio
import logging
import signal
import sys

from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from src.assistant.memory import AssistantMemoryService
from src.bot.runtime import BotRuntime
from src.config.settings import Settings, get_settings
from src.llm import OpenRouterModelPool
from src.scraper.client import TelegramClient
from src.scheduler.jobs import create_scheduler

logger = logging.getLogger(__name__)


def run() -> None:
    """Run the async application entry point."""
    asyncio.run(main())


async def main() -> None:
    """Start the Telegram parser bot.

    1. Load settings from config.toml + env vars
    2. Create async DB engine + session factory
    3. Create Telegram HTTP client
    4. Configure and start APScheduler with scraping job
    5. Run forever until interrupted
    """
    settings = get_settings()

    # Configure logging
    log_level = getattr(logging, settings.logging.level.upper(), logging.INFO)
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stdout,
    )

    logger.info("Starting Telegram Parser Bot")

    # --- Database ---
    engine = create_async_engine(
        settings.database.url,
        pool_size=settings.database.pool_size,
        pool_recycle=settings.database.pool_recycle,
    )
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    logger.info("Database engine created: %s", settings.database.url.split("@")[-1] if "@" in settings.database.url else "(local)")

    # --- Telegram client ---
    client = TelegramClient(settings.scraper)
    model_pool = OpenRouterModelPool(settings.llm)
    memory_service = AssistantMemoryService(settings.memory, settings.llm)

    # --- Scheduler ---
    if settings.scheduler.enabled:
        scheduler = create_scheduler(settings, session_factory, client, model_pool, memory_service)
        scheduler.start()
        logger.info(
            "Scheduler started — scraping every %d minutes",
            settings.scheduler.interval_minutes,
        )
    else:
        logger.info("Scheduler disabled in config")
        scheduler = None

    # --- Telegram Bot ---
    bot_runtime = None
    if settings.bot.enabled and settings.bot.polling and settings.bot.token:
        bot_runtime = BotRuntime(settings, session_factory, client, model_pool, memory_service)
        await bot_runtime.start()
    elif settings.bot.enabled and settings.bot.polling:
        logger.warning("Bot polling enabled but BOT_TOKEN is empty; bot runtime skipped")
    else:
        logger.info("Bot runtime disabled in config")

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError:
            # Fallback for platforms without loop signal handlers.
            pass

    try:
        logger.info("Bot started — press Ctrl+C to stop")
        await stop_event.wait()
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt received — shutting down")
    finally:
        if bot_runtime is not None:
            await bot_runtime.shutdown()
        if scheduler is not None:
            scheduler.shutdown(wait=False)
            logger.info("Scheduler shut down")
        await client.close()
        logger.info("Telegram client closed")
        await engine.dispose()
        logger.info("Database engine disposed")


if __name__ == "__main__":
    run()
