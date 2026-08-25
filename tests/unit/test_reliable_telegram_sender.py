from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from aiogram.exceptions import (
    TelegramBadRequest,
    TelegramNetworkError,
    TelegramRetryAfter,
    TelegramServerError,
)

from src.reliability.telegram_sender import (
    AiogramReliableTelegramSender,
    DeliveryErrorKind,
    classify_delivery_error,
)


@pytest.mark.asyncio
async def test_aiogram_reliable_sender_returns_telegram_message_id() -> None:
    bot = MagicMock()
    bot.send_message = AsyncMock(return_value=SimpleNamespace(message_id=77))
    bot.session.close = AsyncMock()
    with patch("src.reliability.telegram_sender.Bot", return_value=bot):
        sender = AiogramReliableTelegramSender("123:token")
        assert await sender.send_message(1, "digest", "HTML") == 77
        await sender.close()
    bot.send_message.assert_awaited_once()
    bot.session.close.assert_awaited_once()


def test_aiogram_reliable_sender_can_use_isolated_bot_api_endpoint() -> None:
    with patch("src.reliability.telegram_sender.AiohttpSession") as session_type, patch(
        "src.reliability.telegram_sender.Bot"
    ) as bot_type:
        AiogramReliableTelegramSender(
            "123:token",
            api_base_url="http://fake-telegram:8081",
            allow_isolated_e2e=True,
        )

    api = session_type.call_args.kwargs["api"]
    assert api.base == "http://fake-telegram:8081/bot{token}/{method}"
    bot_type.assert_called_once_with(token="123:token", session=session_type.return_value)


@pytest.mark.parametrize(
    ("url", "allowed"),
    [
        ("http://fake-telegram:8081", False),
        ("https://attacker.example", True),
        ("http://127.0.0.1:8081", True),
    ],
)
def test_aiogram_reliable_sender_fails_closed_for_untrusted_bot_api(url: str, allowed: bool) -> None:
    with pytest.raises(ValueError, match="forbidden"):
        AiogramReliableTelegramSender("123:token", api_base_url=url, allow_isolated_e2e=allowed)


def test_delivery_error_classifier_distinguishes_transient_permanent_and_ambiguous() -> None:
    method = MagicMock()
    retry = classify_delivery_error(TelegramRetryAfter(method, "retry", retry_after=9))
    server = classify_delivery_error(TelegramServerError(method, "server"))
    permanent = classify_delivery_error(TelegramBadRequest(method, "bad html"))
    network = classify_delivery_error(TelegramNetworkError(method, "connection lost"))

    assert (retry.kind, retry.retry_after_seconds) == (DeliveryErrorKind.TRANSIENT, 9)
    assert server.kind == DeliveryErrorKind.TRANSIENT
    assert permanent.kind == DeliveryErrorKind.PERMANENT
    assert network.kind == DeliveryErrorKind.AMBIGUOUS
