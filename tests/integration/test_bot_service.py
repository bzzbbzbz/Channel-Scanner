"""Integration tests for bot-facing user and subscription flows."""

from __future__ import annotations

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

from src.bot.service import BotService, ProductLimitExceededError, TelegramIdentity, format_subscriptions_text
from src.config.settings import BotSettings
from src.models.subscription import SubscriptionChannel
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
    assert first.digest_format == DigestFormat.SUMMARY

    first = await service.update_subscription_digest_format(1001, first.id, DigestFormat.SHORT)
    assert first.digest_format == DigestFormat.SUMMARY
    first = await service.update_subscription_summary_mode(1001, first.id, SummaryMode.DETAILED)
    first = await service.update_subscription_custom_prompt(1001, first.id, "Summarize for founders")
    first = await service.update_subscription_filter_prompt(1001, first.id, "Skip ads")
    first = await service.update_subscription_frequency(1001, first.id, DeliveryFrequency.HOURLY)

    assert first.digest_format == DigestFormat.SUMMARY
    assert first.summary_mode == SummaryMode.CUSTOM
    assert first.custom_prompt == "Summarize for founders"
    assert first.filter_prompt == "Skip ads"
    assert first.frequency == DeliveryFrequency.HOURLY

    first = await service.reset_subscription_prompts(1001, first.id)
    assert first.digest_format == DigestFormat.SUMMARY
    assert first.summary_mode == SummaryMode.BRIEF
    assert first.custom_prompt is None
    assert first.filter_prompt is None

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

    subscriptions_text = format_subscriptions_text(
        await service.list_subscriptions(1002),
        "ru",
        max_subscriptions=5,
        max_channels=10,
    )
    assert subscriptions_text == "Подписки (2/5)\n\n1. AI [вкл] - 2/10 каналов\n2. Бизнес [вкл] - 1/10 каналов"

    remove = await service.unsubscribe_many(1002, ai.id, "@durov")
    assert remove.removed == ["@durov"]
    assert [channel.username for channel in await service.list_channels(1002, ai.id)] == ["example_channel"]


@pytest.mark.asyncio
async def test_format_subscriptions_text_shows_empty_subscription_limit(engine: AsyncEngine) -> None:
    del engine

    assert format_subscriptions_text([], "en", max_subscriptions=5, max_channels=10) == (
        "Subscriptions (0/5)\n\nNo subscriptions yet. Create your first one."
    )


@pytest.mark.asyncio
async def test_bot_service_enforces_subscription_limit(engine: AsyncEngine) -> None:
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    service = BotService(
        session_factory,
        FakeScraperClient(),
        BotSettings(default_timezone="UTC", max_subscriptions_per_user=2),
    )

    await service.ensure_user(
        TelegramIdentity(
            telegram_user_id=1010,
            chat_id=2010,
            chat_type="private",
            username="limited",
            first_name="Limited",
            last_name=None,
            language_code="en",
        )
    )
    await service.create_subscription(1010, "One")
    await service.create_subscription(1010, "Two")

    with pytest.raises(ProductLimitExceededError) as exc_info:
        await service.create_subscription(1010, "Three")

    assert exc_info.value.code == "max_subscriptions_per_user"
    assert exc_info.value.limit == 2
    assert [item.name for item in await service.list_subscriptions(1010)] == ["One", "Two"]


@pytest.mark.asyncio
async def test_bot_service_enforces_preset_subscription_limit(engine: AsyncEngine) -> None:
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    service = BotService(
        session_factory,
        FakeScraperClient(),
        BotSettings(default_timezone="UTC", max_subscriptions_per_user=1),
    )

    await service.ensure_user(
        TelegramIdentity(
            telegram_user_id=1011,
            chat_id=2011,
            chat_type="private",
            username="presetlimited",
            first_name="Preset",
            last_name=None,
            language_code="ru",
        )
    )
    await service.create_subscription(1011, "Новости")

    with pytest.raises(ProductLimitExceededError) as exc_info:
        await service.create_subscription_from_preset(1011, "news")

    assert exc_info.value.code == "max_subscriptions_per_user"
    assert [item.name for item in await service.list_subscriptions(1011)] == ["Новости"]


@pytest.mark.asyncio
async def test_bot_service_enforces_channel_limit_per_subscription(engine: AsyncEngine) -> None:
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    service = BotService(
        session_factory,
        FakeScraperClient(),
        BotSettings(default_timezone="UTC", max_channels_per_subscription=2),
    )

    await service.ensure_user(
        TelegramIdentity(
            telegram_user_id=1012,
            chat_id=2012,
            chat_type="private",
            username="channellimited",
            first_name="Channel",
            last_name=None,
            language_code="en",
        )
    )
    subscription = await service.create_subscription(1012, "AI")

    result = await service.subscribe_many(1012, subscription.id, "@alpha, @beta, @gamma")

    assert result.added == ["@alpha", "@beta"]
    assert result.limit_exceeded == ["@gamma"]
    assert [channel.username for channel in await service.list_channels(1012, subscription.id)] == ["alpha", "beta"]

    result = await service.subscribe_many(1012, subscription.id, "@alpha, @delta")
    assert result.already_subscribed == ["@alpha"]
    assert result.limit_exceeded == ["@delta"]


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


@pytest.mark.asyncio
async def test_bot_service_deletes_subscription_with_channels(engine: AsyncEngine) -> None:
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    service = BotService(session_factory, FakeScraperClient(), BotSettings(default_timezone="UTC"))

    await service.ensure_user(
        TelegramIdentity(
            telegram_user_id=1006,
            chat_id=2006,
            chat_type="private",
            username="frank",
            first_name="Frank",
            last_name=None,
            language_code="ru",
        )
    )
    subscription = await service.create_subscription(1006, "Новости")
    await service.subscribe_many(1006, subscription.id, "@rbc_news")

    deleted = await service.delete_subscription(1006, subscription.id)

    assert deleted is True
    assert await service.get_subscription(1006, subscription.id) is None
    async with session_factory() as session:
        result = await session.execute(select(SubscriptionChannel).where(SubscriptionChannel.subscription_id == subscription.id))
        assert result.scalars().all() == []


@pytest.mark.asyncio
async def test_bot_service_creates_subscription_from_preset_with_unique_name(engine: AsyncEngine) -> None:
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    service = BotService(
        session_factory,
        FakeScraperClient(missing={"kommersant"}),
        BotSettings(default_timezone="UTC"),
    )

    await service.ensure_user(
        TelegramIdentity(
            telegram_user_id=1004,
            chat_id=2004,
            chat_type="private",
            username="dave",
            first_name="Dave",
            last_name=None,
            language_code="ru",
        )
    )
    await service.create_subscription(1004, "Новости")

    result = await service.create_subscription_from_preset(1004, "news")

    assert result.subscription is not None
    assert result.subscription.name == "Новости 2"
    assert result.subscription.digest_format == DigestFormat.SUMMARY
    assert result.subscription.summary_mode == SummaryMode.BRIEF
    assert result.subscription.notification_cron == "0 */4 * * *"
    assert result.subscription.enabled is True
    assert result.added == ["@rbc_news", "@tass_agency", "@rian_ru"]
    assert result.not_found == ["@kommersant"]

    channels = await service.list_channels(1004, result.subscription.id)
    assert [channel.username for channel in channels] == ["rbc_news", "rian_ru", "tass_agency"]


@pytest.mark.asyncio
async def test_bot_service_does_not_create_preset_subscription_when_all_channels_unavailable(engine: AsyncEngine) -> None:
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    service = BotService(
        session_factory,
        FakeScraperClient(missing={"rbc_news", "kommersant", "tass_agency", "rian_ru"}),
        BotSettings(default_timezone="UTC"),
    )

    await service.ensure_user(
        TelegramIdentity(
            telegram_user_id=1005,
            chat_id=2005,
            chat_type="private",
            username="erin",
            first_name="Erin",
            last_name=None,
            language_code="ru",
        )
    )

    result = await service.create_subscription_from_preset(1005, "news")

    assert result.subscription is None
    assert result.added == []
    assert result.not_found == ["@rbc_news", "@kommersant", "@tass_agency", "@rian_ru"]
    assert await service.list_subscriptions(1005) == []
