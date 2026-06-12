"""Integration tests for bot-facing user and subscription flows."""

from __future__ import annotations

import pytest
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

from src.bot.service import BotService, TelegramIdentity
from src.config.settings import BotSettings
from src.models.user import DeliveryFrequency, DigestFormat, SummaryMode
from src.scraper.client import ChannelNotFoundError


class FakeScraperClient:
    """Small scraper client stub for bot service tests."""

    def __init__(self, missing: set[str] | None = None) -> None:
        self.missing = missing or set()

    async def fetch_page(self, channel_username: str, before: int | None = None) -> tuple[str, int]:
        del before
        if channel_username in self.missing:
            raise ChannelNotFoundError(channel_username)
        return "<html></html>", 200


@pytest.mark.asyncio
async def test_bot_service_registers_user_and_manages_named_subscriptions(engine: AsyncEngine) -> None:
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    service = BotService(session_factory, FakeScraperClient(), BotSettings(default_timezone="Europe/Berlin"))

    user = await service.ensure_user(
        TelegramIdentity(
            telegram_user_id=1001,
            chat_id=2001,
            chat_type="private",
            username="alice",
            first_name="Alice",
            last_name=None,
            language_code="en",
        )
    )

    assert user.telegram_user_id == 1001
    assert user.timezone == "Europe/Berlin"
    assert user.language == "en"

    user = await service.update_timezone(1001, "UTC+3")
    assert user.timezone == "UTC+3"

    first = await service.create_subscription(1001, "AI")
    second = await service.create_subscription(1001, "Business")

    assert [item.name for item in await service.list_subscriptions(1001)] == ["AI", "Business"]

    first = await service.update_subscription_digest_format(1001, first.id, DigestFormat.SUMMARY)
    first = await service.update_subscription_summary_mode(1001, first.id, SummaryMode.DETAILED)
    first = await service.update_subscription_custom_prompt(1001, first.id, "Summarize for founders")
    first = await service.update_subscription_frequency(1001, first.id, DeliveryFrequency.HOURLY)

    assert first.digest_format == DigestFormat.SUMMARY
    assert first.summary_mode == SummaryMode.CUSTOM
    assert first.custom_prompt == "Summarize for founders"
    assert first.frequency == DeliveryFrequency.HOURLY

    second = await service.toggle_subscription_enabled(1001, second.id)
    assert second.enabled is False


@pytest.mark.asyncio
async def test_bot_service_manages_channels_per_subscription(engine: AsyncEngine) -> None:
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    service = BotService(session_factory, FakeScraperClient(), BotSettings(default_timezone="UTC"))

    await service.ensure_user(
        TelegramIdentity(
            telegram_user_id=1002,
            chat_id=2002,
            chat_type="private",
            username="bob",
            first_name="Bob",
            last_name=None,
            language_code="ru",
        )
    )

    ai = await service.create_subscription(1002, "AI")
    biz = await service.create_subscription(1002, "Бизнес")

    result = await service.subscribe_many(1002, ai.id, "@example_channel, https://t.me/durov")
    assert result.added == ["@example_channel", "@durov"]

    result = await service.subscribe_many(1002, biz.id, "@example_channel")
    assert result.added == ["@example_channel"]

    ai_channels = await service.list_channels(1002, ai.id)
    biz_channels = await service.list_channels(1002, biz.id)
    assert [channel.username for channel in ai_channels] == ["durov", "example_channel"]
    assert [channel.username for channel in biz_channels] == ["example_channel"]

    remove = await service.unsubscribe_many(1002, ai.id, "@durov")
    assert remove.removed == ["@durov"]
    assert [channel.username for channel in await service.list_channels(1002, ai.id)] == ["example_channel"]


@pytest.mark.asyncio
async def test_bot_service_marks_invalid_and_missing_channels(engine: AsyncEngine) -> None:
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    service = BotService(
        session_factory,
        FakeScraperClient(missing={"missingchannel"}),
        BotSettings(default_timezone="UTC"),
    )

    await service.ensure_user(
        TelegramIdentity(
            telegram_user_id=1003,
            chat_id=2003,
            chat_type="private",
            username="carol",
            first_name="Carol",
            last_name=None,
            language_code="ru",
        )
    )
    subscription = await service.create_subscription(1003, "AI")

    result = await service.subscribe_many(1003, subscription.id, "not a channel, @missingchannel")

    assert result.invalid == ["not a channel"]
    assert result.not_found == ["@missingchannel"]
