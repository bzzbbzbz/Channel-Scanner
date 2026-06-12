"""Helpers for opt-in real Telegram E2E tests."""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Any

from aiogram import Bot
from aiogram.client.session.aiohttp import AiohttpSession


@dataclass(slots=True)
class RecordedTelegramCall:
    api_method: str
    payload: dict[str, Any]
    response: Any
    captured_at: float


class RecordingAiohttpSession(AiohttpSession):
    """Pass through to the real Bot API and keep request/response history."""

    def __init__(self) -> None:
        super().__init__()
        self.calls: list[RecordedTelegramCall] = []

    async def make_request(self, bot: Bot, method, timeout: int | None = None):
        response = await super().make_request(bot, method, timeout=timeout)
        payload = method.model_dump(exclude_none=True, exclude_defaults=True, mode="python")
        self.calls.append(
            RecordedTelegramCall(
                api_method=method.__api_method__,
                payload=payload,
                response=response,
                captured_at=time.monotonic(),
            )
        )
        return response


class ProductBotSender:
    """Digest sender that reuses the real product bot instance."""

    def __init__(self, bot: Bot) -> None:
        self._bot = bot

    async def send_message(self, chat_id: int, text: str, parse_mode: str | None = None) -> None:
        await self._bot.send_message(
            chat_id=chat_id,
            text=text,
            parse_mode=parse_mode,
            disable_web_page_preview=True,
        )

    async def close(self) -> None:
        return None


class RealTelegramChatHarness:
    """Drive the product bot from a dedicated tester bot."""

    def __init__(self, product_token: str, tester_token: str, chat_id: int) -> None:
        self.chat_id = chat_id
        self.product_session = RecordingAiohttpSession()
        self.product_bot = Bot(product_token, session=self.product_session)
        self.tester_bot = Bot(tester_token)

    async def tester_id(self) -> int:
        return (await self.tester_bot.get_me()).id

    async def send_tester_message(self, text: str, reply_to_message_id: int | None = None):
        return await self.tester_bot.send_message(
            self.chat_id,
            text,
            reply_to_message_id=reply_to_message_id,
        )

    async def wait_for_product_call(
        self,
        *,
        api_method: str,
        contains_text: str,
        after_index: int = 0,
        timeout: float = 30.0,
    ) -> RecordedTelegramCall:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            for call in self.product_session.calls[after_index:]:
                if call.api_method != api_method:
                    continue
                if contains_text in str(call.payload.get("text", "")):
                    return call
            await asyncio.sleep(0.5)
        raise TimeoutError(
            f"Timed out waiting for product bot {api_method} containing {contains_text!r}"
        )

    async def close(self, *, close_product_bot: bool = True) -> None:
        await self.tester_bot.session.close()
        if close_product_bot:
            await self.product_bot.session.close()
