"""Integration coverage for assistant product tools."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

from src.assistant.tools import AssistantToolRegistry
from src.bot.service import BotService, TelegramIdentity
from src.config.settings import BotSettings, LlmSettings
from src.models.channel import Channel, ChannelStatus
from src.models.user import DigestFormat, SummaryMode
from src.repository.digest_delivery import DigestDeliveryRepository
from src.repository.post import PostRepository
from src.repository.subscription import SubscriptionRepository
from src.scraper.parser import ParsedPost


class FakeScraperClient:
    async def fetch_page(self, channel_username: str, before: int | None = None) -> tuple[str, int]:
        del channel_username, before
        return "<html></html>", 200


async def _make_service_and_user(engine: AsyncEngine, *, telegram_user_id: int, language_code: str):
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    service = BotService(session_factory, FakeScraperClient(), BotSettings(default_timezone="UTC"))
    user = await service.ensure_user(
        TelegramIdentity(
            telegram_user_id=telegram_user_id,
            chat_id=telegram_user_id + 1000,
            chat_type="private",
            username="agent",
            first_name="Agent",
            last_name=None,
            language_code=language_code,
        )
    )
    registry = AssistantToolRegistry(session_factory, FakeScraperClient(), service)
    return session_factory, service, user, registry


@pytest.mark.asyncio
async def test_assistant_set_notification_persists_validated_cron(engine: AsyncEngine) -> None:
    session_factory, service, user, registry = await _make_service_and_user(engine, telegram_user_id=5001, language_code="ru")
    subscription = await service.create_subscription(user.telegram_user_id, "AI")

    result = await registry.execute("setNotification", {"subscription_id": subscription.id, "cron": "0 10 * * *"}, user)

    assert result.system_message == "Время уведомлений в подписке <b>AI</b> установлено: каждый день в 10:00."
    async with session_factory() as session:
        stored = await SubscriptionRepository(session).get_for_user(user.id, subscription.id)
        assert stored is not None
        assert stored.notification_cron == "0 10 * * *"


@pytest.mark.asyncio
async def test_assistant_get_subscriptions_handles_interval_hour_cron(engine: AsyncEngine) -> None:
    session_factory, service, user, registry = await _make_service_and_user(engine, telegram_user_id=5005, language_code="ru")
    subscription = await service.create_subscription(user.telegram_user_id, "AI")
    await service.update_subscription_notification_cron(user.telegram_user_id, subscription.id, "0 */3 * * *")

    result = await registry.execute("getSubscriptions", {}, user)

    assert result.payload["subscriptions"][0]["notification_cron"] == "0 */3 * * *"
    assert result.payload["subscriptions"][0]["schedule_label"] == "каждые 3 часа"


@pytest.mark.asyncio
async def test_assistant_create_subscription_requires_confirmation(engine: AsyncEngine) -> None:
    _, service, user, registry = await _make_service_and_user(engine, telegram_user_id=5002, language_code="en")

    result = await registry.execute("createSubscription", {"name": "AI", "confirmed": False}, user)

    assert result.payload == {"error": "creation_requires_explicit_confirmation"}
    assert await service.list_subscriptions(user.telegram_user_id) == []


@pytest.mark.asyncio
async def test_assistant_create_subscription_returns_limit_error(engine: AsyncEngine) -> None:
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    service = BotService(
        session_factory,
        FakeScraperClient(),
        BotSettings(default_timezone="UTC", max_subscriptions_per_user=1),
    )
    user = await service.ensure_user(
        TelegramIdentity(
            telegram_user_id=5006,
            chat_id=6006,
            chat_type="private",
            username="agent",
            first_name="Agent",
            last_name=None,
            language_code="en",
        )
    )
    registry = AssistantToolRegistry(session_factory, FakeScraperClient(), service)
    await service.create_subscription(user.telegram_user_id, "One")

    result = await registry.execute("createSubscription", {"name": "Two", "confirmed": True}, user)

    assert result.payload == {"error": "max_subscriptions_per_user", "limit": 1}
    assert result.system_message == "Limit reached: you can create up to 1 subscriptions."
    assert [item.name for item in await service.list_subscriptions(user.telegram_user_id)] == ["One"]


@pytest.mark.asyncio
async def test_assistant_add_channels_returns_limit_details(engine: AsyncEngine) -> None:
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    service = BotService(
        session_factory,
        FakeScraperClient(),
        BotSettings(default_timezone="UTC", max_channels_per_subscription=1),
    )
    user = await service.ensure_user(
        TelegramIdentity(
            telegram_user_id=5007,
            chat_id=6007,
            chat_type="private",
            username="agent",
            first_name="Agent",
            last_name=None,
            language_code="ru",
        )
    )
    registry = AssistantToolRegistry(session_factory, FakeScraperClient(), service)
    subscription = await service.create_subscription(user.telegram_user_id, "AI")

    result = await registry.execute("addChannels", {"subscription_id": subscription.id, "channels": "@alpha, @beta"}, user)

    assert result.payload["added"] == ["@alpha"]
    assert result.payload["limit_exceeded"] == ["@beta"]
    assert result.system_message == "Каналы в подписке <b>AI</b> обновлены. Лимит: в одной подписке можно хранить не больше 1 каналов."


@pytest.mark.asyncio
async def test_assistant_tool_system_messages_are_russian_and_html_safe(engine: AsyncEngine) -> None:
    _, service, user, registry = await _make_service_and_user(engine, telegram_user_id=5003, language_code="ru")
    subscription = await service.create_subscription(user.telegram_user_id, "Новости")

    create_result = await registry.execute("createSubscription", {"name": "AI & ML", "confirmed": True}, user)
    digest_result = await registry.execute("setDigestFormat", {"subscription_id": subscription.id, "format": "summary"}, user)
    filter_result = await registry.execute("setFilterPrompt", {"subscription_id": subscription.id, "prompt": "Не пропускай рекламу"}, user)
    prompt_result = await registry.execute("setSummaryPrompt", {"subscription_id": subscription.id, "prompt": "Пиши кратко"}, user)
    reset_result = await registry.execute("resetPrompts", {"subscription_id": subscription.id}, user)
    channels_result = await registry.execute("addChannels", {"subscription_id": subscription.id, "channels": "@durov"}, user)
    remove_result = await registry.execute("removeChannels", {"subscription_id": subscription.id, "channels": "@durov"}, user)

    assert create_result.system_message == "Подписка <b>AI &amp; ML</b> создана."
    assert digest_result.system_message == "Формат дайджеста в подписке <b>Новости</b> обновлён: Пересказ (Кратко)."
    assert filter_result.system_message == "Инструкция для AI-фильтра в подписке <b>Новости</b> обновлена."
    assert prompt_result.system_message == "Инструкция для пересказа в подписке <b>Новости</b> обновлена."
    assert reset_result.system_message == "Промпты AI-фильтра и AI-пересказа в подписке <b>Новости</b> сброшены по умолчанию."
    assert channels_result.system_message == "Каналы в подписке <b>Новости</b> обновлены."
    assert remove_result.system_message == "Каналы в подписке <b>Новости</b> обновлены."


@pytest.mark.asyncio
async def test_assistant_tool_system_messages_are_english_and_html_safe(engine: AsyncEngine) -> None:
    _, service, user, registry = await _make_service_and_user(engine, telegram_user_id=5004, language_code="en")
    subscription = await service.create_subscription(user.telegram_user_id, "News")

    create_result = await registry.execute("createSubscription", {"name": "AI & ML", "confirmed": True}, user)
    notification_result = await registry.execute("setNotification", {"subscription_id": subscription.id, "cron": "0 10 * * *"}, user)
    digest_result = await registry.execute("setDigestFormat", {"subscription_id": subscription.id, "format": "summary"}, user)
    filter_result = await registry.execute("setFilterPrompt", {"subscription_id": subscription.id, "prompt": "Skip ads"}, user)
    prompt_result = await registry.execute("setSummaryPrompt", {"subscription_id": subscription.id, "prompt": "Keep it short"}, user)
    reset_result = await registry.execute("resetPrompts", {"subscription_id": subscription.id}, user)

    assert create_result.system_message == "Subscription <b>AI &amp; ML</b> created."
    assert notification_result.system_message == "Notification schedule for subscription <b>News</b> set: daily at 10:00."
    assert digest_result.system_message == "Digest format for subscription <b>News</b> updated: Summary (Brief)."
    assert filter_result.system_message == "AI filter instructions for subscription <b>News</b> updated."
    assert prompt_result.system_message == "Summary instructions for subscription <b>News</b> updated."
    assert reset_result.system_message == "AI filter and summary prompts for subscription <b>News</b> reset to defaults."


@pytest.mark.asyncio
async def test_assistant_debug_digest_prompts_replays_candidate_prompts_without_persisting_delivery(engine: AsyncEngine) -> None:
    session_factory, service, user, _ = await _make_service_and_user(engine, telegram_user_id=5008, language_code="en")
    subscription = await service.create_subscription(user.telegram_user_id, "AI")
    registry = AssistantToolRegistry(
        session_factory,
        FakeScraperClient(),
        service,
        llm_settings=LlmSettings(OPENROUTER_API_KEY="test-key"),
    )

    async with session_factory() as session:
        stored = await SubscriptionRepository(session).get_for_user(user.id, subscription.id)
        assert stored is not None
        stored.digest_format = DigestFormat.SUMMARY
        stored.summary_mode = SummaryMode.CUSTOM
        stored.filter_prompt = "Existing filter"
        stored.custom_prompt = "Existing summary"
        channel = Channel(telegram_id=8008, username="aichannel", name="AI channel", status=ChannelStatus.ACTIVE)
        session.add(channel)
        await session.flush()
        await SubscriptionRepository(session).add_channel(
            stored.id,
            channel.id,
            subscribed_at=datetime(2026, 4, 26, 10, 0, tzinfo=timezone.utc),
        )
        await PostRepository(session).upsert_posts(
            channel.id,
            [
                ParsedPost(1, "aichannel", "historical post", "2026-04-26T09:00:00+00:00"),
                ParsedPost(2, "aichannel", "new model release", "2026-04-26T10:30:00+00:00"),
                ParsedPost(3, "aichannel", "later post", "2026-04-26T12:00:00+00:00"),
            ],
        )
        selected = await DigestDeliveryRepository(session).get_posts_for_subscription_period(
            stored.id,
            datetime(2026, 4, 26, 10, 0, tzinfo=timezone.utc),
            datetime(2026, 4, 26, 11, 0, tzinfo=timezone.utc),
        )
        assert [item.telegram_post_id for item in selected] == [2]
        selected_post_id = selected[0].post_db_id
        await session.commit()

    filter_json = json.dumps({"included_post_ids": [selected_post_id], "skipped_posts": []})
    digest_json = json.dumps(
        {"topics": [{"title": "Models", "summary": "A new model was released.", "source_post_ids": [selected_post_id]}]},
    )
    with patch("src.digest.service.OpenRouterClient.generate_summary", new=AsyncMock(side_effect=[filter_json, digest_json])) as generate:
        result = await registry.execute(
            "debugDigestPrompts",
            {
                "subscription_id": subscription.id,
                "period_start": "2026-04-26T10:00:00+00:00",
                "period_end": "2026-04-26T11:00:00+00:00",
                "filter_prompt": "Only include model releases",
                "summary_prompt": "Write decision-ready summaries",
            },
            user,
        )

    assert result.payload["post_count"] == 1
    assert "A new model was released." in result.payload["digest_messages"][0]
    assert result.payload["post_outcomes"] == {selected_post_id: {"status": "delivered", "skip_reason": None}}
    assert "Only include model releases" in generate.await_args_list[0].args[1]
    assert "Existing filter" not in generate.await_args_list[0].args[1]
    assert "Write decision-ready summaries" in generate.await_args_list[1].args[1]
    assert "Existing summary" not in generate.await_args_list[1].args[1]

    async with session_factory() as session:
        stored = await SubscriptionRepository(session).get_for_user(user.id, subscription.id)
        assert stored is not None
        assert stored.last_digest_at is None
        assert await DigestDeliveryRepository(session).list_delivered_post_ids_for_subscription(subscription.id) == []


@pytest.mark.asyncio
async def test_assistant_debug_digest_prompts_rejects_invalid_period(engine: AsyncEngine) -> None:
    _, service, user, registry = await _make_service_and_user(engine, telegram_user_id=5009, language_code="en")
    subscription = await service.create_subscription(user.telegram_user_id, "AI")

    result = await registry.execute(
        "debugDigestPrompts",
        {
            "subscription_id": subscription.id,
            "period_start": "2026-04-26T11:00:00+00:00",
            "period_end": "2026-04-26T10:00:00+00:00",
            "filter_prompt": "Include AI news",
            "summary_prompt": "Write a concise digest",
        },
        user,
    )

    assert result.payload == {"error": "invalid_period", "detail": "period_start must be before period_end"}


@pytest.mark.asyncio
async def test_assistant_generates_and_reuses_on_demand_digest_without_delivery_state(engine: AsyncEngine) -> None:
    session_factory, service, user, _ = await _make_service_and_user(engine, telegram_user_id=5011, language_code="en")
    subscription = await service.create_subscription(user.telegram_user_id, "AI")
    registry = AssistantToolRegistry(
        session_factory,
        FakeScraperClient(),
        service,
        llm_settings=LlmSettings(OPENROUTER_API_KEY="test-key"),
    )

    async with session_factory() as session:
        stored = await SubscriptionRepository(session).get_for_user(user.id, subscription.id)
        assert stored is not None
        channel = Channel(telegram_id=8011, username="aichannel", name="AI channel", status=ChannelStatus.ACTIVE)
        session.add(channel)
        await session.flush()
        await SubscriptionRepository(session).add_channel(
            stored.id,
            channel.id,
            subscribed_at=datetime(2026, 4, 26, 10, 0, tzinfo=timezone.utc),
        )
        await PostRepository(session).upsert_posts(
            channel.id,
            [ParsedPost(1, "aichannel", "new model release", "2026-04-26T10:30:00+00:00")],
        )
        selected = await DigestDeliveryRepository(session).get_posts_for_subscription_period(
            stored.id,
            datetime(2026, 4, 26, 10, 0, tzinfo=timezone.utc),
            datetime(2026, 4, 26, 11, 0, tzinfo=timezone.utc),
        )
        assert len(selected) == 1
        post_id = selected[0].post_db_id
        await session.commit()

    filter_json = json.dumps({"included_post_ids": [post_id], "skipped_posts": []})
    digest_json = json.dumps(
        {"topics": [{"title": "Models", "summary": "A new model was released.", "source_post_ids": [post_id]}]},
    )
    arguments = {
        "subscription_id": subscription.id,
        "period_start": "2026-04-26T10:00:00+00:00",
        "period_end": "2026-04-26T11:00:00+00:00",
    }
    with patch("src.assistant.tools.ScraperService.scrape_channel_period", new=AsyncMock(return_value=[])) as scrape, patch(
        "src.digest.service.OpenRouterClient.generate_summary",
        new=AsyncMock(side_effect=[filter_json, digest_json]),
    ) as generate:
        first = await registry.execute("generateOnDemandDigest", arguments, user)
        repeated = await registry.execute("generateOnDemandDigest", arguments, user)

    assert first.payload["cached"] is False
    assert repeated.payload["cached"] is True
    assert first.additional_system_messages == repeated.additional_system_messages
    assert "A new model was released." in first.additional_system_messages[0]
    assert scrape.await_count == 1
    assert generate.await_count == 2

    async with session_factory() as session:
        stored = await SubscriptionRepository(session).get_for_user(user.id, subscription.id)
        assert stored is not None
        assert stored.last_digest_at is None
        delivery_repo = DigestDeliveryRepository(session)
        assert await delivery_repo.list_delivered_post_ids_for_subscription(subscription.id) == []
        stats = await delivery_repo.get_processing_stats_for_period(
            user.id,
            subscription.id,
            datetime(2026, 4, 26, 10, 0, tzinfo=timezone.utc),
            datetime(2026, 4, 26, 11, 0, tzinfo=timezone.utc),
        )
        assert stats.run_count == 0


@pytest.mark.asyncio
async def test_assistant_get_digest_processing_logs_aggregates_user_subscription_period(engine: AsyncEngine) -> None:
    session_factory, service, user, registry = await _make_service_and_user(engine, telegram_user_id=5010, language_code="en")
    subscription = await service.create_subscription(user.telegram_user_id, "AI")

    async with session_factory() as session:
        repo = DigestDeliveryRepository(session)
        await repo.record_processing_log(
            user.id,
            subscription.id,
            found_count=3,
            filtered_count=1,
            included_count=2,
            completed_at=datetime(2026, 4, 26, 10, 0, tzinfo=timezone.utc),
        )
        await repo.record_processing_log(
            user.id,
            subscription.id,
            found_count=0,
            filtered_count=0,
            included_count=0,
            completed_at=datetime(2026, 4, 26, 11, 0, tzinfo=timezone.utc),
        )
        await session.commit()

    result = await registry.execute(
        "getDigestProcessingLogs",
        {
            "subscription_id": subscription.id,
            "period_start": "2026-04-26T09:00:00+00:00",
            "period_end": "2026-04-26T12:00:00+00:00",
        },
        user,
    )

    assert result.payload == {
        "subscription_id": subscription.id,
        "period_start": "2026-04-26T09:00:00+00:00",
        "period_end": "2026-04-26T12:00:00+00:00",
        "completed_run_count": 2,
        "found_count": 3,
        "filtered_count": 1,
        "included_count": 2,
    }
