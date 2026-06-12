"""E2E coverage for natural-language assistant flows through the real bot runtime.

Opt-in suite — requires:
  BOT_TOKEN / TELEGRAM_TOKEN
  E2E_TELEGRAM_TOKEN
  E2E_CHAT_ID
  OPENROUTER_API_KEY

Starts the real BotRuntime with a real OpenRouter LLM and drives the
product bot through a dedicated tester bot in the shared E2E group chat.
"""

from __future__ import annotations

import asyncio
import os
import time
from dataclasses import dataclass

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine

import src.models  # noqa: F401
from src.bot.runtime import BotRuntime
from src.config.settings import (
    AssistantSettings,
    BotSettings,
    LlmSettings,
    MemorySettings,
    SchedulerSettings,
    ScraperSettings,
    Settings,
)
from src.models.base import Base
from src.repository.subscription import SubscriptionRepository
from src.repository.user import UserRepository
from src.scraper.client import TelegramClient
from tests.e2e.harness import RecordedTelegramCall, RealTelegramChatHarness


@dataclass(slots=True)
class E2EEnv:
    product_token: str
    tester_token: str
    chat_id: int


def _load_env() -> E2EEnv:
    product_token = os.getenv("BOT_TOKEN") or os.getenv("TELEGRAM_TOKEN")
    tester_token = os.getenv("E2E_TELEGRAM_TOKEN")
    chat_id_raw = os.getenv("E2E_CHAT_ID")
    api_key = os.getenv("OPENROUTER_API_KEY")

    if not product_token:
        pytest.skip("BOT_TOKEN/TELEGRAM_TOKEN is not configured")
    if not tester_token:
        pytest.skip("E2E_TELEGRAM_TOKEN is not configured")
    if not chat_id_raw:
        pytest.skip("E2E_CHAT_ID is not configured")
    if not api_key:
        pytest.skip("OPENROUTER_API_KEY is not configured")

    return E2EEnv(product_token=product_token, tester_token=tester_token, chat_id=int(chat_id_raw))


async def _wait_for_send_message(
    harness: RealTelegramChatHarness,
    *,
    contains_text: str,
    after_index: int = 0,
    timeout: float = 60.0,
) -> RecordedTelegramCall:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        for call in harness.product_session.calls[after_index:]:
            if call.api_method != "sendMessage":
                continue
            if contains_text in str(call.payload.get("text", "")):
                return call
        await asyncio.sleep(0.5)
    raise TimeoutError(f"No sendMessage with {contains_text!r} after index {after_index}")


async def _assistant_turn(
    harness: RealTelegramChatHarness,
    text: str,
    *,
    after_index: int | None = None,
    timeout: float = 120.0,
) -> list[RecordedTelegramCall]:
    """Send a user message and wait for the assistant to finish its full turn.

    1. Wait for the first sendMessage from the product bot
    2. Then wait for 3 seconds of no new calls (turn complete)
    Returns all new sendMessage calls made during the turn.
    """
    if after_index is None:
        after_index = len(harness.product_session.calls)

    await harness.send_tester_message(text)

    deadline = time.monotonic() + timeout
    new_calls_snapshot: list[RecordedTelegramCall] = []
    stable_loops = 0

    while time.monotonic() < deadline:
        current = [c for c in harness.product_session.calls[after_index:] if c.api_method == "sendMessage"]
        if len(current) > len(new_calls_snapshot):
            new_calls_snapshot = current
            stable_loops = 0
        else:
            if len(new_calls_snapshot) > 0:
                stable_loops += 1
        if stable_loops >= 6:
            break
        await asyncio.sleep(0.5)
    else:
        total_new = len(harness.product_session.calls) - after_index
        raise TimeoutError(f"Assistant did not finish turn for: {text!r} (total new calls: {total_new})")

    return new_calls_snapshot


def _response_texts(calls: list[RecordedTelegramCall]) -> list[str]:
    return [str(c.payload.get("text", "")) for c in calls]


def _is_asking(text: str) -> bool:
    return text.strip().endswith("?") or any(w in text.lower() for w in ("хотите", "подтвердите", "согласны"))


def _has_tool_confirmation(calls: list[RecordedTelegramCall]) -> bool:
    """Check if any call looks like a deterministic tool system message."""
    keywords = ["Подписка", "Каналы в подписке", "Формат дайджеста", "Время уведомлений", "Инструкция"]
    for c in calls:
        text = str(c.payload.get("text", ""))
        if any(kw in text for kw in keywords):
            return True
    return False


@pytest_asyncio.fixture(scope="module")
async def e2e_engine():
    engine = create_async_engine("sqlite+aiosqlite:////tmp/e2e-nl-assistant.sqlite", echo=False)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield engine
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
    await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.e2e
async def test_nl_assistant_sets_up_and_disables_subscription(e2e_engine: AsyncEngine) -> None:
    """Real-user flow: set up a subscription via NL, then disable it via NL."""
    env = _load_env()
    session_factory = async_sessionmaker(e2e_engine, expire_on_commit=False)
    scraper = TelegramClient(ScraperSettings())

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
        assistant=AssistantSettings(enabled=True, history_limit=30, max_tool_rounds=5),
        memory=MemorySettings(enabled=False),
    )

    harness = RealTelegramChatHarness(env.product_token, env.tester_token, env.chat_id)
    runtime = BotRuntime(settings, session_factory, scraper)
    runtime._bot = harness.product_bot
    await runtime.start()
    await asyncio.sleep(2.0)

    try:
        tester_id = await harness.tester_id()

        # ── Phase 1: Register via /start ──
        start_index = len(harness.product_session.calls)
        await harness.send_tester_message("/start")
        await _wait_for_send_message(harness, contains_text="Telegram Parser Bot", after_index=start_index)

        async with session_factory() as session:
            user = await UserRepository(session).get_by_telegram_user_id(tester_id)
            assert user is not None
            assert user.language in ("ru", "en")

        # ── Phase 2: User asks assistant to set everything up ──
        turn_index = len(harness.product_session.calls)
        replies = await _assistant_turn(
            harness,
            "настрой подписку DurovDaily: канал @durov, ежедневно в 9 утра, формат пересказ кратко",
            after_index=turn_index,
        )

        # Handle clarification questions (LLM might ask for confirmation)
        max_rounds = 3
        for _ in range(max_rounds):
            if not any(_is_asking(t) for t in _response_texts(replies)):
                break
            turn_index = len(harness.product_session.calls)
            replies = await _assistant_turn(harness, "да, всё верно", after_index=turn_index)

            # Verify DB: subscription created with channel, daily 9:00, summary format
        async with session_factory() as session:
            user = await UserRepository(session).get_by_telegram_user_id(tester_id)
            assert user is not None
            subs = await SubscriptionRepository(session).list_for_user(user.id)
            durov = [s for s in subs if "DurovDaily" in s.name]
            assert len(durov) == 1, f"Expected 1 DurovDaily sub among {[s.name for s in subs]}, replies: {_response_texts(replies)}"
            sub = durov[0]
            assert sub.enabled is True
            assert sub.digest_format.value in ("summary", "short"), f"Unexpected digest_format: {sub.digest_format.value}"
            assert len(sub.channel_links) >= 1, "Should have at least one channel"

        # ── Phase 3: User asks to disable the subscription via NL ──
        disable_index = len(harness.product_session.calls)
        replies = await _assistant_turn(harness, "отключи подписку DurovDaily", after_index=disable_index)

        for _ in range(max_rounds):
            if not any(_is_asking(t) for t in _response_texts(replies)):
                break
            disable_index = len(harness.product_session.calls)
            replies = await _assistant_turn(harness, "да", after_index=disable_index)

        # Verify DB: subscription is now disabled
        async with session_factory() as session:
            user = await UserRepository(session).get_by_telegram_user_id(tester_id)
            assert user is not None
            subs = await SubscriptionRepository(session).list_for_user(user.id)
            durov = [s for s in subs if "DurovDaily" in s.name]
            assert len(durov) == 1
            assert durov[0].enabled is False, "Subscription should be disabled"

    finally:
        # Safety: disable any remaining enabled subscriptions
        async with session_factory() as session:
            user = await UserRepository(session).get_by_telegram_user_id(tester_id)
            if user is not None:
                for s in await SubscriptionRepository(session).list_for_user(user.id):
                    if s.enabled:
                        await SubscriptionRepository(session).update_enabled(s, False)
                await session.commit()
        await runtime.shutdown()
        await harness.close(close_product_bot=False)
