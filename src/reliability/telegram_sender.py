"""Reliable Telegram sender and content-free delivery error classification."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Protocol

from aiogram import Bot
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.client.telegram import PRODUCTION, TelegramAPIServer
from aiogram.enums import ParseMode
from aiogram.exceptions import (
    TelegramBadRequest,
    TelegramForbiddenError,
    TelegramNetworkError,
    TelegramRetryAfter,
    TelegramServerError,
    TelegramUnauthorizedError,
)
from src.reliability.e2e_faults import ISOLATED_BOT_API_URL


class ReliableTelegramSender(Protocol):
    """Sender used only by the reliable delivery worker."""

    async def send_message(self, chat_id: int, text: str, parse_mode: str | None = None) -> int:
        """Send one persisted part and return Telegram's message identifier."""

    async def close(self) -> None:
        """Release sender resources."""


class AiogramReliableTelegramSender:
    """aiogram Bot API adapter that preserves Telegram's acknowledgement ID."""

    def __init__(
        self,
        token: str,
        *,
        api_base_url: str = "https://api.telegram.org",
        allow_isolated_e2e: bool = False,
    ) -> None:
        if not token:
            raise ValueError("BOT_TOKEN is required for reliable Telegram delivery")
        if api_base_url == "https://api.telegram.org":
            api = PRODUCTION
        elif allow_isolated_e2e and api_base_url == ISOLATED_BOT_API_URL:
            api = TelegramAPIServer.from_base(api_base_url)
        else:
            raise ValueError("Custom Bot API endpoints are forbidden outside the isolated stage-6 E2E")
        self._bot = Bot(token=token, session=AiohttpSession(api=api))

    async def send_message(self, chat_id: int, text: str, parse_mode: str | None = None) -> int:
        message = await self._bot.send_message(
            chat_id=chat_id,
            text=text,
            parse_mode=parse_mode or ParseMode.HTML,
            disable_web_page_preview=True,
        )
        return message.message_id

    async def close(self) -> None:
        await self._bot.session.close()


class DeliveryErrorKind(str, Enum):
    TRANSIENT = "transient"
    PERMANENT = "permanent"
    AMBIGUOUS = "ambiguous"


@dataclass(frozen=True, slots=True)
class ClassifiedDeliveryError:
    kind: DeliveryErrorKind
    code: str
    retry_after_seconds: float | None = None


def classify_delivery_error(exc: BaseException) -> ClassifiedDeliveryError:
    """Classify an error without persisting provider response content."""
    if isinstance(exc, TelegramRetryAfter):
        return ClassifiedDeliveryError(
            DeliveryErrorKind.TRANSIENT,
            "TelegramRetryAfter",
            max(0.0, float(exc.retry_after)),
        )
    if isinstance(exc, (TimeoutError, TelegramNetworkError, ConnectionError, OSError)):
        return ClassifiedDeliveryError(DeliveryErrorKind.AMBIGUOUS, type(exc).__name__[:128])
    if isinstance(exc, TelegramServerError):
        return ClassifiedDeliveryError(DeliveryErrorKind.TRANSIENT, type(exc).__name__[:128])
    if isinstance(exc, (TelegramBadRequest, TelegramForbiddenError, TelegramUnauthorizedError)):
        return ClassifiedDeliveryError(DeliveryErrorKind.PERMANENT, type(exc).__name__[:128])
    if isinstance(exc, (ValueError, TypeError)):
        return ClassifiedDeliveryError(DeliveryErrorKind.PERMANENT, type(exc).__name__[:128])
    return ClassifiedDeliveryError(DeliveryErrorKind.PERMANENT, type(exc).__name__[:128])
