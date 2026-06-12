"""Opt-in real Telegram E2E coverage for an allowlisted shared group chat."""

from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass
from datetime import datetime, timezone

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine

import src.models  # noqa: F401
from src.bot.runtime import BotRuntime
from src.config.settings import BotSettings, LlmSettings, SchedulerSettings, ScraperSettings, Settings
from src.digest.service import DigestService
from src.models.base import Base
from src.models.channel import Channel, ChannelStatus
from src.models.subscription import Subscription
from src.models.user import DeliveryFrequency, DigestFormat, SummaryMode
from src.repository.digest_delivery import DigestDeliveryRepository
from src.repository.post import PostRepository
from src.repository.subscription import SubscriptionRepository
from src.repository.user import UserRepository
from src.scraper.parser import ParsedPost
from tests.e2e.harness import ProductBotSender, RealTelegramChatHarness


class FakeScraperClient:
    async def fetch_page(self, channel_username: str, before: int | None = None) -> tuple[str, int]:
        del channel_username, before
        return "<html></html>", 200


@dataclass(slots=True)
class RealTelegramEnv:
    product_token: str
    tester_token: str
    chat_id: int


def _load_real_telegram_env() -> RealTelegramEnv:
    product_token = os.getenv("BOT_TOKEN") or os.getenv("TELEGRAM_TOKEN")
    tester_token = os.getenv("E2E_TELEGRAM_TOKEN")
    chat_id_raw = os.getenv("E2E_CHAT_ID")
    if not product_token:
        pytest.skip("Real Telegram E2E skipped: BOT_TOKEN/TELEGRAM_TOKEN is not configured")
    if not tester_token:
        pytest.skip("Real Telegram E2E skipped: E2E_TELEGRAM_TOKEN is not configured")
    if not chat_id_raw:
        pytest.skip("Real Telegram E2E skipped: E2E_CHAT_ID is not configured")
    return RealTelegramEnv(
        product_token=product_token,
        tester_token=tester_token,
        chat_id=int(chat_id_raw),
    )


def _make_post(post_id: int, content: str, dt: str, username: str = "digestch") -> ParsedPost:
    return ParsedPost(post_id=post_id, channel_username=username, content=content, datetime=dt)


async def _seed_subscription(session: AsyncSession, user_id: int) -> Subscription:
    subscription = Subscription(
        user_id=user_id,
        name="E2E AI",
        digest_format=DigestFormat.SHORT,
        summary_mode=SummaryMode.BRIEF,
        frequency=DeliveryFrequency.HOURLY,
        enabled=True,
    )
    session.add(subscription)
    await session.flush()
    return subscription


async def _seed_channel(session: AsyncSession, username: str = "digestch", telegram_id: int = 123) -> Channel:
    channel = Channel(telegram_id=telegram_id, username=username, name="Digest Channel", status=ChannelStatus.ACTIVE)
    session.add(channel)
    await session.flush()
    return channel


@pytest_asyncio.fixture
async def e2e_engine(tmp_path) -> AsyncEngine:
    db_path = tmp_path / "real-telegram-e2e.sqlite"
    engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}", echo=False)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield engine
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
    await engine.dispose()


@pytest_asyncio.fixture
async def running_real_runtime(e2e_engine: AsyncEngine):
    env = _load_real_telegram_env()
    session_factory = async_sessionmaker(e2e_engine, expire_on_commit=False)
    settings = Settings(
        scheduler=SchedulerSettings(enabled=False),
        scraper=ScraperSettings(),
        llm=LlmSettings(),
        bot=BotSettings(
            token=env.product_token,
            enabled=True,
            polling=True,
            set_commands_on_startup=False,
            drop_pending_updates=True,
            e2e_allowed_chat_id=env.chat_id,
        ),
    )
    harness = RealTelegramChatHarness(env.product_token, env.tester_token, env.chat_id)
    runtime = BotRuntime(settings, session_factory, FakeScraperClient())
    runtime._bot = harness.product_bot
    await runtime.start()
    await asyncio.sleep(2.0)
    try:
        yield harness, session_factory, env
    finally:
        await runtime.shutdown()
        await harness.close(close_product_bot=False)


@pytest.mark.asyncio
@pytest.mark.e2e
async def test_allowlisted_group_start_and_digest_delivery(running_real_runtime) -> None:
    harness, session_factory, env = running_real_runtime

    start_index = len(harness.product_session.calls)
    await harness.send_tester_message("/start")
    welcome = await harness.wait_for_product_call(
        api_method="sendMessage",
        contains_text="Telegram Parser Bot v1",
        after_index=start_index,
    )

    tester_id = await harness.tester_id()
    async with session_factory() as session:
        user = await UserRepository(session).get_by_telegram_user_id(tester_id)
        assert user is not None
        assert user.chat_id == env.chat_id
        assert user.chat_type in {"group", "supergroup"}

        subscription = await _seed_subscription(session, user.id)
        channel = await _seed_channel(session)
        await SubscriptionRepository(session).add_channel(
            subscription.id,
            channel.id,
            subscribed_at=datetime(2026, 5, 23, 9, 0, tzinfo=timezone.utc),
        )
        await PostRepository(session).upsert_posts(
            channel.id,
            [_make_post(1, "hello from real telegram e2e", "2026-05-23T10:00:00+00:00")],
        )
        await session.commit()

    delivery_index = len(harness.product_session.calls)
    delivered = await DigestService(
        session_factory,
        bot_token=env.product_token,
        llm_settings=LlmSettings(),
        sender=ProductBotSender(harness.product_bot),
    ).run_once(now=datetime(2026, 5, 23, 10, 30, tzinfo=timezone.utc))

    assert delivered == 1
    digest = await harness.wait_for_product_call(
        api_method="sendMessage",
        contains_text="hello from real telegram e2e",
        after_index=delivery_index,
    )
    assert int(digest.payload["chat_id"]) == env.chat_id
    assert int(welcome.payload["chat_id"]) == env.chat_id

    async with session_factory() as session:
        user = await UserRepository(session).get_by_telegram_user_id(tester_id)
        assert user is not None
        subscriptions = await SubscriptionRepository(session).list_for_user(user.id)
        assert len(subscriptions) == 1
        delivered_post_ids = await DigestDeliveryRepository(session).list_delivered_post_ids_for_subscription(subscriptions[0].id)
        assert delivered_post_ids == [1]
