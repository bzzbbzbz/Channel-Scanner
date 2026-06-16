"""Unit tests for callback routing in the bot runtime."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from aiogram import Bot, Dispatcher
from aiogram.enums import ChatAction, ParseMode
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import CallbackQuery, Message, Update

from src.assistant.service import AssistantTurnResult
from src.bot.runtime import _format_keyboard, build_router, is_supported_chat
from src.models.subscription import Subscription
from src.models.user import DeliveryFrequency, DigestFormat, SummaryMode, User


def _make_user() -> User:
    return User(
        telegram_user_id=100,
        chat_id=200,
        chat_type="private",
        timezone="UTC",
        language="ru",
    )


def _make_subscription() -> Subscription:
    return Subscription(
        id=1,
        user_id=1,
        name="AI",
        digest_format=DigestFormat.SUMMARY,
        summary_mode=SummaryMode.BRIEF,
        frequency=DeliveryFrequency.HOURLY,
        enabled=True,
    )


def test_format_keyboard_shows_prompt_actions_without_legacy_format_buttons() -> None:
    subscription = Subscription(
        id=1,
        user_id=1,
        name="AI",
        digest_format=DigestFormat.SHORT,
        summary_mode=SummaryMode.BRIEF,
        frequency=DeliveryFrequency.DAILY,
        enabled=True,
    )

    keyboard = _format_keyboard(subscription, "ru")

    assert [row[0].text for row in keyboard.inline_keyboard] == [
        "Изменить фильтр",
        "Изменить пересказ",
        "По умолчанию",
        "Назад",
    ]


def test_is_supported_chat_allows_private_and_allowlisted_e2e_group() -> None:
    assert is_supported_chat("private", 1) is True
    assert is_supported_chat("supergroup", -1001) is False
    assert is_supported_chat("supergroup", -1001, -1001) is True


def _make_callback_update(data: str) -> Update:
    payload = {
        "update_id": 1,
        "callback_query": {
            "id": "cb-1",
            "chat_instance": "instance-1",
            "data": data,
            "from": {
                "id": 100,
                "is_bot": False,
                "first_name": "Test",
                "language_code": "ru",
            },
            "message": {
                "message_id": 10,
                "date": int(datetime.now(timezone.utc).timestamp()),
                "chat": {"id": 200, "type": "private"},
                "from": {
                    "id": 999,
                    "is_bot": True,
                    "first_name": "Bot",
                },
                "text": "screen",
            },
        },
    }
    return Update.model_validate(payload)


def _make_message_update(text: str) -> Update:
    payload = {
        "update_id": 2,
        "message": {
            "message_id": 20,
            "date": int(datetime.now(timezone.utc).timestamp()),
            "chat": {"id": 200, "type": "private"},
            "from": {
                "id": 100,
                "is_bot": False,
                "first_name": "Test",
                "language_code": "ru",
            },
            "text": text,
        },
    }
    return Update.model_validate(payload)


class FakeAssistant:
    async def handle_message(self, user: User, text: str) -> AssistantTurnResult:
        del user, text
        await asyncio.sleep(0.01)
        return AssistantTurnResult(reply_text="**Готово**", system_messages=["**Системно**"])


def _answer_text(call) -> str:
    for arg in call.args:
        if isinstance(arg, str):
            return arg
    return str(call.kwargs.get("text", ""))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("callback_data", "expected_method", "unexpected_method"),
    [
        ("subscription:frequency:1", "get_subscription", "update_subscription_frequency"),
        ("subscription:frequency:set:1:daily", "update_subscription_frequency", "get_subscription"),
        ("subscription:format:1", "get_subscription", "update_subscription_digest_format"),
        ("subscription:prompts:reset:1", "reset_subscription_prompts", "get_subscription"),
    ],
)
async def test_subscription_callbacks_route_to_expected_handler(
    monkeypatch: pytest.MonkeyPatch,
    callback_data: str,
    expected_method: str,
    unexpected_method: str,
) -> None:
    user = _make_user()
    subscription = _make_subscription()
    service = SimpleNamespace(
        ensure_user=AsyncMock(return_value=user),
        get_user=AsyncMock(return_value=user),
        get_subscription=AsyncMock(return_value=subscription),
        update_subscription_frequency=AsyncMock(return_value=subscription),
        update_subscription_digest_format=AsyncMock(return_value=subscription),
        update_subscription_summary_mode=AsyncMock(return_value=subscription),
        reset_subscription_prompts=AsyncMock(return_value=subscription),
    )

    callback_answer = AsyncMock(return_value=True)
    message_edit_text = AsyncMock(return_value=True)
    monkeypatch.setattr(CallbackQuery, "answer", callback_answer)
    monkeypatch.setattr(Message, "edit_text", message_edit_text)

    dispatcher = Dispatcher(storage=MemoryStorage())
    dispatcher.include_router(build_router(service))
    bot = Bot("123:TEST")

    try:
        await dispatcher.feed_update(bot, _make_callback_update(callback_data))
    finally:
        await bot.session.close()

    assert getattr(service, expected_method).await_count == 1
    assert getattr(service, unexpected_method).await_count == 0
    assert message_edit_text.await_count == 1
    assert callback_answer.await_count == 1
    if callback_data == "subscription:format:1":
        assert message_edit_text.await_args.kwargs["parse_mode"] == ParseMode.HTML
        assert "Промпт для AI-фильтра" in message_edit_text.await_args.args[0]


@pytest.mark.asyncio
async def test_assistant_reply_shows_typing_and_uses_html_parse_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    user = _make_user()
    service = SimpleNamespace(ensure_user=AsyncMock(return_value=user))

    send_chat_action = AsyncMock(return_value=True)
    message_answer = AsyncMock(return_value=True)
    monkeypatch.setattr(Bot, "send_chat_action", send_chat_action)
    monkeypatch.setattr(Message, "answer", message_answer)

    dispatcher = Dispatcher(storage=MemoryStorage())
    dispatcher.include_router(build_router(service, assistant_service=FakeAssistant()))
    bot = Bot("123:TEST")

    try:
        await dispatcher.feed_update(bot, _make_message_update("сделай уведомления"))
    finally:
        await bot.session.close()

    assert send_chat_action.await_count >= 1
    assert send_chat_action.await_args.kwargs["chat_id"] == 200
    assert send_chat_action.await_args.kwargs["action"] == ChatAction.TYPING
    assert [_answer_text(call) for call in message_answer.await_args_list] == ["<b>Системно</b>", "<b>Готово</b>"]
    assert all(call.kwargs["parse_mode"] == ParseMode.HTML for call in message_answer.await_args_list)


@pytest.mark.asyncio
async def test_start_command_explains_capabilities_and_natural_language(monkeypatch: pytest.MonkeyPatch) -> None:
    user = _make_user()
    service = SimpleNamespace(ensure_user=AsyncMock(return_value=user))

    message_answer = AsyncMock(return_value=True)
    monkeypatch.setattr(Message, "answer", message_answer)

    dispatcher = Dispatcher(storage=MemoryStorage())
    dispatcher.include_router(build_router(service))
    bot = Bot("123:TEST")

    try:
        await dispatcher.feed_update(bot, _make_message_update("/start"))
    finally:
        await bot.session.close()

    text = _answer_text(message_answer.await_args)
    assert "собирать новые посты" in text
    assert "AI-фильтр" in text
    assert "промпт AI-пересказа" in text
    assert "обычным человеческим языком" in text
    assert "Добавь канал @example" in text
