"""Integration coverage for assistant product tools."""

from __future__ import annotations

import pytest
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

from src.assistant.tools import AssistantToolRegistry
from src.bot.service import BotService, TelegramIdentity
from src.config.settings import BotSettings
from src.repository.subscription import SubscriptionRepository


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
