"""Integration tests for the scheduler and scraping job.

Uses mocks for HTTP calls — no real Telegram requests.
Tests verify job logic: channel iteration, error marking, dedup.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Optional
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from src.models.channel import Channel, ChannelStatus
from src.llm import OpenRouterModelPool
from src.repository.channel import ChannelRepository
from src.repository.post import PostRepository
from src.scraper.parser import ParsedPost
from src.scheduler.jobs import create_scheduler, digest_delivery_job, knowledge_index_job, scraping_job
from src.config.settings import BotSettings, LlmSettings, Settings, SchedulerSettings, ScraperSettings


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_post(post_id: int = 1, content: str = "Hello") -> ParsedPost:
    return ParsedPost(
        post_id=post_id,
        channel_username="testch",
        content=content,
        datetime="2026-01-15T12:00:00+00:00",
    )


async def _seed_channel(
    session: AsyncSession,
    telegram_id: int = 100,
    username: str = "testch",
    status: ChannelStatus = ChannelStatus.ACTIVE,
) -> Channel:
    channel = Channel(
        telegram_id=telegram_id,
        username=username,
        name="Test Channel",
        status=status,
    )
    session.add(channel)
    await session.flush()
    return channel


# ---------------------------------------------------------------------------
# scraping_job tests
# ---------------------------------------------------------------------------


class TestScrapingJob:
    """Tests for the scraping_job coroutine."""

    async def test_scraping_job_iterates_active_channels(
        self, engine: AsyncEngine
    ) -> None:
        """scraping_job scrapes each active channel and stores posts."""
        session_factory = async_sessionmaker(engine, expire_on_commit=False)

        # Seed channels
        async with session_factory() as session:
            ch1 = await _seed_channel(session, telegram_id=100, username="ch1")
            ch2 = await _seed_channel(session, telegram_id=200, username="ch2")
            await session.commit()

        # Mock TelegramClient + ScraperService
        mock_client = AsyncMock()
        mock_posts_ch1 = [_make_post(1), _make_post(2)]
        mock_posts_ch2 = [_make_post(10)]

        with patch("src.scheduler.jobs.ScraperService") as MockService:
            svc_instance = AsyncMock()
            svc_instance.scrape_channel = AsyncMock(
                side_effect=[mock_posts_ch1, mock_posts_ch2]
            )
            MockService.return_value = svc_instance

            await scraping_job(session_factory, mock_client, max_posts=20)

        # Verify posts stored
        async with session_factory() as session:
            post_repo = PostRepository(session)
            count1 = await post_repo.count_posts(ch1.id)
            count2 = await post_repo.count_posts(ch2.id)
            assert count1 == 2
            assert count2 == 1

    async def test_scraping_job_marks_error_on_channel_not_found(
        self, engine: AsyncEngine
    ) -> None:
        """scraping_job marks channel as error when ChannelNotFoundError is raised."""
        session_factory = async_sessionmaker(engine, expire_on_commit=False)

        async with session_factory() as session:
            await _seed_channel(session, telegram_id=300, username="notfound")
            await session.commit()

        mock_client = AsyncMock()

        with patch("src.scheduler.jobs.ScraperService") as MockService:
            from src.scraper.client import ChannelNotFoundError

            svc_instance = AsyncMock()
            svc_instance.scrape_channel = AsyncMock(
                side_effect=ChannelNotFoundError("notfound")
            )
            MockService.return_value = svc_instance

            await scraping_job(session_factory, mock_client, max_posts=20)

        # Verify channel marked as error
        async with session_factory() as session:
            channel_repo = ChannelRepository(session)
            refreshed = await channel_repo.get_by_telegram_id(300)
            assert refreshed is not None
            assert refreshed.status == ChannelStatus.ERROR

    async def test_scraping_job_handles_empty_channel_list(
        self, engine: AsyncEngine
    ) -> None:
        """scraping_job handles gracefully when there are no active channels."""
        session_factory = async_sessionmaker(engine, expire_on_commit=False)
        mock_client = AsyncMock()

        # No channels seeded — should complete without error
        await scraping_job(session_factory, mock_client, max_posts=20)

    async def test_scraping_job_handles_generic_exception(
        self, engine: AsyncEngine
    ) -> None:
        """scraping_job continues to next channel on unexpected exception."""
        session_factory = async_sessionmaker(engine, expire_on_commit=False)

        async with session_factory() as session:
            ch1 = await _seed_channel(session, telegram_id=400, username="bad_ch")
            ch2 = await _seed_channel(session, telegram_id=500, username="good_ch")
            await session.commit()

        mock_client = AsyncMock()

        with patch("src.scheduler.jobs.ScraperService") as MockService:
            svc_instance = AsyncMock()
            svc_instance.scrape_channel = AsyncMock(
                side_effect=[RuntimeError("unexpected"), [_make_post(99)]]
            )
            MockService.return_value = svc_instance

            await scraping_job(session_factory, mock_client, max_posts=20)

        # ch2 should have posts stored
        async with session_factory() as session:
            post_repo = PostRepository(session)
            count2 = await post_repo.count_posts(ch2.id)
            assert count2 == 1


# ---------------------------------------------------------------------------
# create_scheduler tests
# ---------------------------------------------------------------------------


class TestCreateScheduler:
    """Tests for create_scheduler configuration."""

    def test_create_scheduler_configures_correct_interval(self) -> None:
        """create_scheduler sets the interval from settings."""
        settings = Settings(
            scheduler=SchedulerSettings(interval_minutes=10),
            scraper=ScraperSettings(max_posts=50),
            bot=BotSettings(token="token"),
        )
        mock_session_factory = MagicMock()
        mock_client = MagicMock()

        scheduler = create_scheduler(settings, mock_session_factory, mock_client)

        # Check the job was added with correct configuration
        jobs = scheduler.get_jobs()
        assert len(jobs) == 2
        job = scheduler.get_job("scraping_job")

        assert job is not None
        assert job.id == "scraping_job"
        assert job.name == "Periodic channel scraping"
        assert job.misfire_grace_time == 60
        assert job.coalesce is True

        # Check interval trigger
        trigger = job.trigger
        assert hasattr(trigger, "interval")
        # IntervalTrigger stores timedelta
        assert trigger.interval.total_seconds() == 600  # 10 minutes

        digest_job = scheduler.get_job("digest_delivery_job")
        assert digest_job is not None
        assert digest_job.name == "Scheduled digest delivery"

    def test_create_scheduler_skips_digest_job_without_token(self) -> None:
        settings = Settings(
            scheduler=SchedulerSettings(interval_minutes=10),
            scraper=ScraperSettings(max_posts=50),
            bot=BotSettings(token=""),
        )
        scheduler = create_scheduler(settings, MagicMock(), MagicMock())

        assert scheduler.get_job("scraping_job") is not None
        assert scheduler.get_job("digest_delivery_job") is None

    def test_create_scheduler_adds_hourly_model_refresh_when_pool_and_key_exist(self) -> None:
        settings = Settings(
            scheduler=SchedulerSettings(interval_minutes=10),
            scraper=ScraperSettings(max_posts=50),
            bot=BotSettings(token="token"),
            llm=LlmSettings(OPENROUTER_API_KEY="key"),
        )
        pool = OpenRouterModelPool(settings.llm)

        scheduler = create_scheduler(settings, MagicMock(), MagicMock(), pool)

        refresh_job = scheduler.get_job("llm_model_refresh_job")
        assert refresh_job is not None
        assert refresh_job.trigger.interval.total_seconds() == 3600


@pytest.mark.asyncio
async def test_digest_delivery_job_invokes_service() -> None:
    mock_service = AsyncMock()
    mock_service.run_once = AsyncMock(return_value=2)

    with patch("src.scheduler.jobs.DigestService", return_value=mock_service) as service_cls:
        await digest_delivery_job(MagicMock(), "token", LlmSettings())

    service_cls.assert_called_once()
    mock_service.run_once.assert_awaited_once()


@pytest.mark.asyncio
async def test_digest_delivery_job_passes_model_pool() -> None:
    mock_service = AsyncMock()
    mock_service.run_once = AsyncMock(return_value=1)
    llm_settings = LlmSettings()
    pool = OpenRouterModelPool(llm_settings)

    with patch("src.scheduler.jobs.DigestService", return_value=mock_service) as service_cls:
        await digest_delivery_job(MagicMock(), "token", llm_settings, pool)

    assert service_cls.call_args.kwargs["model_pool"] is pool
    mock_service.run_once.assert_awaited_once()


@pytest.mark.asyncio
async def test_knowledge_index_job_retries_pending_and_failed_work() -> None:
    knowledge_service = MagicMock()
    knowledge_service.retry_failed_indexing = AsyncMock(return_value=(3, 2))

    await knowledge_index_job(MagicMock(), knowledge_service)

    knowledge_service.retry_failed_indexing.assert_awaited_once_with()
