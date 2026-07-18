"""Integration tests for digest selection, delivery, and deduplication."""

from __future__ import annotations

from datetime import datetime, timezone
import json

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from src.config.settings import LlmSettings
from src.digest.service import DigestService
from src.models.channel import Channel, ChannelStatus
from src.models.digest_delivery import DigestDelivery
from src.models.digest_processing_log import DigestProcessingLog
from src.models.subscription import Subscription
from src.models.user import DeliveryFrequency, DigestFormat, SummaryMode, User
from src.repository.digest_delivery import DeliveredSummary, DigestDeliveryRepository
from src.repository.chat_message import ChatMessageRepository
from src.repository.post import PostRepository
from src.repository.subscription import SubscriptionRepository
from src.repository.user import UserRepository
from src.scraper.parser import ParsedPost, parse_page


class FakeDigestSender:
    """Collect sent messages for assertions."""

    def __init__(self) -> None:
        self.messages: list[tuple[int, str]] = []

    async def send_message(self, chat_id: int, text: str, parse_mode: str | None = None) -> None:
        self.messages.append((chat_id, text))

    async def close(self) -> None:
        return None


REPLY_PAGE_HTML = """
<html><body>
<div class="tgme_widget_message" data-post="bankrollo/61957">
  <div class="tgme_widget_message_bubble">
    <a class="tgme_widget_message_reply user-color-default" href="https://t.me/bankrollo/61954">
      <div class="tgme_widget_message_text js-message_reply_text" dir="auto">
        Трамп объявил о сделке между США и Ираном. Мирный договор согласован всеми сторонами.
      </div>
    </a>
    <div class="tgme_widget_message_text js-message_text" dir="auto">
      Израиль и Иран отрицают существование мирной сделки — израильский канал N12.
      <a href="https://t.me/bankrollo" target="_blank">@bankrollo</a>
    </div>
    <time datetime="2026-06-11T20:06:00+00:00">20:06</time>
    <span class="tgme_widget_message_views">38.9K</span>
    <a class="tgme_widget_message_date" href="https://t.me/bankrollo/61957">20:06</a>
  </div>
</div>
</body></html>
"""


def _make_post(post_id: int, content: str, dt: str, username: str = "digestch") -> ParsedPost:
    return ParsedPost(post_id=post_id, channel_username=username, content=content, datetime=dt)


async def _seed_user(session: AsyncSession, **overrides: object) -> User:
    params = {
        "telegram_user_id": 111,
        "chat_id": 222,
        "chat_type": "private",
        "timezone": "UTC",
        "language": "ru",
    }
    params.update(overrides)
    user = User(**params)
    session.add(user)
    await session.flush()
    return user


async def _seed_subscription(session: AsyncSession, user_id: int, name: str = "AI", **overrides: object) -> Subscription:
    params = {
        "user_id": user_id,
        "name": name,
        "digest_format": DigestFormat.SHORT,
        "summary_mode": SummaryMode.BRIEF,
        "frequency": DeliveryFrequency.DAILY,
        "enabled": True,
    }
    params.update(overrides)
    subscription = Subscription(**params)
    session.add(subscription)
    await session.flush()
    return subscription


async def _seed_channel(session: AsyncSession, username: str = "digestch", telegram_id: int = 123) -> Channel:
    channel = Channel(telegram_id=telegram_id, username=username, name="Digest Channel", status=ChannelStatus.ACTIVE)
    session.add(channel)
    await session.flush()
    return channel


@pytest.mark.asyncio
async def test_digest_repository_selects_only_subscription_posts(session: AsyncSession) -> None:
    user = await _seed_user(session)
    first = await _seed_subscription(session, user.id, name="AI")
    second = await _seed_subscription(session, user.id, name="Business")
    channel = await _seed_channel(session)
    other_channel = await _seed_channel(session, username="otherch", telegram_id=456)

    repo = SubscriptionRepository(session)
    await repo.add_channel(first.id, channel.id, subscribed_at=datetime(2026, 4, 26, 9, 0, tzinfo=timezone.utc))
    await repo.add_channel(second.id, other_channel.id, subscribed_at=datetime(2026, 4, 26, 9, 0, tzinfo=timezone.utc))
    await PostRepository(session).upsert_posts(channel.id, [_make_post(1, "first", "2026-04-26T10:00:00+00:00")])
    await PostRepository(session).upsert_posts(other_channel.id, [_make_post(2, "second", "2026-04-26T11:00:00+00:00", username="otherch")])
    await session.commit()

    delivery_repo = DigestDeliveryRepository(session)
    pending = await delivery_repo.get_pending_posts_for_subscription(first.id)
    assert [item.telegram_post_id for item in pending] == [1]

    await delivery_repo.mark_posts_delivered(
        user.id,
        first.id,
        [DeliveredSummary(pending[0].post_db_id, "first", "short", None, None)],
        datetime.now(timezone.utc),
    )
    await session.commit()

    assert await delivery_repo.get_pending_posts_for_subscription(first.id) == []
    assert [item.telegram_post_id for item in await delivery_repo.get_pending_posts_for_subscription(second.id)] == [2]


@pytest.mark.asyncio
async def test_digest_service_delivers_per_subscription(engine: AsyncEngine) -> None:
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    sender = FakeDigestSender()

    async with session_factory() as session:
        user = await _seed_user(session)
        ai = await _seed_subscription(session, user.id, name="AI", frequency=DeliveryFrequency.HOURLY)
        business = await _seed_subscription(session, user.id, name="Business", frequency=DeliveryFrequency.HOURLY)
        shared_channel = await _seed_channel(session)
        second_channel = await _seed_channel(session, username="bizch", telegram_id=999)
        repo = SubscriptionRepository(session)
        await repo.add_channel(ai.id, shared_channel.id, subscribed_at=datetime(2026, 4, 26, 9, 0, tzinfo=timezone.utc))
        await repo.add_channel(business.id, shared_channel.id, subscribed_at=datetime(2026, 4, 26, 9, 0, tzinfo=timezone.utc))
        await repo.add_channel(business.id, second_channel.id, subscribed_at=datetime(2026, 4, 26, 9, 0, tzinfo=timezone.utc))
        await PostRepository(session).upsert_posts(shared_channel.id, [_make_post(1, "hello world", "2026-04-26T10:00:00+00:00")])
        await PostRepository(session).upsert_posts(second_channel.id, [_make_post(2, "biz post", "2026-04-26T10:05:00+00:00", username="bizch")])
        await session.commit()

    service = DigestService(session_factory, bot_token="test-token", llm_settings=LlmSettings(), sender=sender)
    delivered = await service.run_once(now=datetime(2026, 4, 26, 10, 30, tzinfo=timezone.utc))

    assert delivered == 2
    assert len(sender.messages) == 2
    assert any("hello world" in message[1] and "biz post" not in message[1] for message in sender.messages)
    assert any("hello world" in message[1] and "biz post" in message[1] for message in sender.messages)

    async with session_factory() as session:
        user = await UserRepository(session).get_by_telegram_user_id(111)
        subscriptions = await SubscriptionRepository(session).list_for_user(user.id)
        delivered_ai = await DigestDeliveryRepository(session).list_delivered_post_ids_for_subscription(subscriptions[0].id)
        delivered_business = await DigestDeliveryRepository(session).list_delivered_post_ids_for_subscription(subscriptions[1].id)
        assert delivered_ai == [1]
        assert delivered_business == [1, 2]
        digest_messages = await ChatMessageRepository(session).list_recent_digests(user.id, limit=10)
        assert len(digest_messages) == 2
        assert any("hello world" in message.text for message in digest_messages)
        logs = (await session.execute(select(DigestProcessingLog).order_by(DigestProcessingLog.subscription_id))).scalars().all()
        assert [(log.found_count, log.filtered_count, log.included_count) for log in logs] == [(1, 0, 1), (2, 0, 2)]


@pytest.mark.asyncio
async def test_digest_service_records_empty_processing_runs(engine: AsyncEngine) -> None:
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    sender = FakeDigestSender()

    async with session_factory() as session:
        user = await _seed_user(session)
        subscription = await _seed_subscription(session, user.id, frequency=DeliveryFrequency.HOURLY)
        await session.commit()

    sent_at = datetime(2026, 4, 26, 10, 30, tzinfo=timezone.utc)
    service = DigestService(session_factory, bot_token="test-token", llm_settings=LlmSettings(), sender=sender)

    assert await service.run_once(now=sent_at) == 0
    assert sender.messages == []

    async with session_factory() as session:
        logs = (await session.execute(select(DigestProcessingLog))).scalars().all()
        assert [(log.subscription_id, log.found_count, log.filtered_count, log.included_count) for log in logs] == [
            (subscription.id, 0, 0, 0),
        ]
        assert logs[0].completed_at.replace(tzinfo=timezone.utc) == sent_at
        stored = await SubscriptionRepository(session).get_by_id(subscription.id)
        assert stored is not None
        assert stored.last_digest_at is not None
        assert stored.last_digest_at.replace(tzinfo=timezone.utc) == sent_at


@pytest.mark.asyncio
async def test_digest_service_falls_back_to_200_chars_when_summary_models_fail(engine: AsyncEngine) -> None:
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    sender = FakeDigestSender()

    async with session_factory() as session:
        user = await _seed_user(session)
        subscription = await _seed_subscription(
            session,
            user.id,
            frequency=DeliveryFrequency.HOURLY,
            digest_format=DigestFormat.SUMMARY,
            summary_mode=SummaryMode.BRIEF,
        )
        channel = await _seed_channel(session)
        await SubscriptionRepository(session).add_channel(
            subscription.id,
            channel.id,
            subscribed_at=datetime(2026, 4, 26, 9, 0, tzinfo=timezone.utc),
        )
        await PostRepository(session).upsert_posts(
            channel.id,
            [
                _make_post(1, "x" * 300, "2026-04-26T10:00:00+00:00"),
                _make_post(2, "y" * 300, "2026-04-26T10:05:00+00:00"),
            ],
        )
        await session.commit()

    service = DigestService(
        session_factory,
        bot_token="test-token",
        llm_settings=LlmSettings(OPENROUTER_API_KEY="test-key"),
        sender=sender,
    )

    from unittest.mock import AsyncMock, patch

    with patch("src.digest.service.OpenRouterClient.generate_summary", new=AsyncMock(side_effect=RuntimeError("boom"))):
        delivered = await service.run_once(now=datetime(2026, 4, 26, 10, 30, tzinfo=timezone.utc))

    assert delivered == 1
    assert len(sender.messages) == 1
    assert "x" * 50 in sender.messages[0][1]
    assert "y" * 50 in sender.messages[0][1]
    assert sender.messages[0][1].count('href="https://t.me/digestch/') == 2


@pytest.mark.asyncio
async def test_digest_service_skips_empty_summary_posts_before_llm(engine: AsyncEngine) -> None:
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    sender = FakeDigestSender()

    async with session_factory() as session:
        user = await _seed_user(session)
        subscription = await _seed_subscription(
            session,
            user.id,
            frequency=DeliveryFrequency.HOURLY,
            digest_format=DigestFormat.SUMMARY,
            summary_mode=SummaryMode.BRIEF,
        )
        channel = await _seed_channel(session)
        await SubscriptionRepository(session).add_channel(
            subscription.id,
            channel.id,
            subscribed_at=datetime(2026, 4, 26, 9, 0, tzinfo=timezone.utc),
        )
        await PostRepository(session).upsert_posts(channel.id, [_make_post(1, "   ", "2026-04-26T10:00:00+00:00")])
        await session.commit()

    service = DigestService(
        session_factory,
        bot_token="test-token",
        llm_settings=LlmSettings(OPENROUTER_API_KEY="test-key"),
        sender=sender,
    )

    from unittest.mock import AsyncMock, patch

    generate = AsyncMock(return_value="should not be called")
    with patch("src.digest.service.OpenRouterClient.generate_summary", new=generate):
        delivered = await service.run_once(now=datetime(2026, 4, 26, 10, 30, tzinfo=timezone.utc))

    assert delivered == 1
    generate.assert_not_awaited()
    assert len(sender.messages) == 1
    assert "пустые" in sender.messages[0][1]

    async with session_factory() as session:
        rows = (await session.execute(select(DigestDelivery))).scalars().all()
        assert [(row.post_id, row.status, row.skip_reason) for row in rows] == [
            (1, "skipped", "empty post content"),
        ]
        assert await DigestDeliveryRepository(session).get_pending_posts_for_subscription(subscription.id) == []
        logs = (await session.execute(select(DigestProcessingLog))).scalars().all()
        assert [(log.found_count, log.filtered_count, log.included_count) for log in logs] == [(1, 1, 0)]


@pytest.mark.asyncio
async def test_digest_summary_uses_reply_post_body_not_parent_context(engine: AsyncEngine) -> None:
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    sender = FakeDigestSender()
    posts, _ = parse_page(REPLY_PAGE_HTML)
    assert len(posts) == 1
    assert "Израиль и Иран отрицают" in posts[0].content
    assert "Трамп объявил" not in posts[0].content

    async with session_factory() as session:
        user = await _seed_user(session)
        subscription = await _seed_subscription(
            session,
            user.id,
            frequency=DeliveryFrequency.HOURLY,
            digest_format=DigestFormat.SUMMARY,
            summary_mode=SummaryMode.BRIEF,
        )
        channel = await _seed_channel(session, username="bankrollo")
        await SubscriptionRepository(session).add_channel(
            subscription.id,
            channel.id,
            subscribed_at=datetime(2026, 6, 11, 19, 0, tzinfo=timezone.utc),
        )
        await PostRepository(session).upsert_posts(channel.id, posts)
        await session.commit()

    captured_prompt = ""

    async def fake_generate_summary(self, model: str, system_prompt: str, post_text: str, **kwargs: object) -> str:  # noqa: ANN001
        del kwargs
        nonlocal captured_prompt
        captured_prompt = system_prompt
        assert post_text == ""
        assert "Израиль и Иран отрицают" in system_prompt
        assert "Трамп объявил" not in system_prompt
        return "Израиль и Иран отрицают существование мирной сделки."

    service = DigestService(
        session_factory,
        bot_token="test-token",
        llm_settings=LlmSettings(OPENROUTER_API_KEY="test-key"),
        sender=sender,
    )

    from unittest.mock import patch

    with patch("src.digest.service.OpenRouterClient.generate_summary", new=fake_generate_summary):
        delivered = await service.run_once(now=datetime(2026, 6, 11, 20, 30, tzinfo=timezone.utc))

    assert delivered == 1
    assert "Израиль и Иран отрицают" in captured_prompt
    assert len(sender.messages) == 1
    assert "Израиль и Иран отрицают" in sender.messages[0][1]
    assert "Трамп объявил" not in sender.messages[0][1]


@pytest.mark.asyncio
async def test_digest_service_persists_skipped_filtered_posts(engine: AsyncEngine) -> None:
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    sender = FakeDigestSender()

    async with session_factory() as session:
        user = await _seed_user(session)
        subscription = await _seed_subscription(
            session,
            user.id,
            frequency=DeliveryFrequency.HOURLY,
            digest_format=DigestFormat.SUMMARY,
            summary_mode=SummaryMode.BRIEF,
        )
        channel = await _seed_channel(session)
        await SubscriptionRepository(session).add_channel(
            subscription.id,
            channel.id,
            subscribed_at=datetime(2026, 4, 26, 9, 0, tzinfo=timezone.utc),
        )
        await PostRepository(session).upsert_posts(
            channel.id,
            [
                _make_post(1, "важная новость про ИИ", "2026-04-26T10:00:00+00:00"),
                _make_post(2, "рекламный промокод", "2026-04-26T10:05:00+00:00"),
            ],
        )
        await session.commit()

    filter_json = json.dumps(
        {"included_post_ids": [1], "skipped_posts": [{"post_id": 2, "reason": "реклама"}]},
        ensure_ascii=False,
    )
    digest_json = json.dumps(
        {"topics": [{"title": "ИИ", "summary": "Важная новость про ИИ.", "source_post_ids": [1]}]},
        ensure_ascii=False,
    )
    service = DigestService(
        session_factory,
        bot_token="test-token",
        llm_settings=LlmSettings(OPENROUTER_API_KEY="test-key"),
        sender=sender,
    )

    from unittest.mock import AsyncMock, patch

    with patch("src.digest.service.OpenRouterClient.generate_summary", new=AsyncMock(side_effect=[filter_json, digest_json])):
        delivered = await service.run_once(now=datetime(2026, 4, 26, 10, 30, tzinfo=timezone.utc))

    assert delivered == 1
    assert len(sender.messages) == 1
    assert "Важная новость" in sender.messages[0][1]
    assert "промокод" not in sender.messages[0][1]

    async with session_factory() as session:
        rows = (await session.execute(select(DigestDelivery).order_by(DigestDelivery.post_id))).scalars().all()
        assert [(row.post_id, row.status, row.skip_reason) for row in rows] == [
            (1, "delivered", None),
            (2, "skipped", "реклама"),
        ]
        assert await DigestDeliveryRepository(session).get_pending_posts_for_subscription(subscription.id) == []
        logs = (await session.execute(select(DigestProcessingLog))).scalars().all()
        assert [(log.found_count, log.filtered_count, log.included_count) for log in logs] == [(2, 1, 1)]
