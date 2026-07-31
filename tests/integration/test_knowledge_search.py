"""Integration coverage for canonical lexical knowledge search and subscription scope."""

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker

from src.config.settings import KnowledgeSettings, LlmSettings
from src.knowledge.importer import parse_official_export
from src.knowledge.service import KnowledgeService
from src.models.channel import Channel
from src.models.knowledge import EnrichmentStatus, IndexStatus, KnowledgeChannel, KnowledgeChannelState, KnowledgeDocument, KnowledgeRepresentation, RepresentationType
from src.models.post import Post
from src.models.subscription import Subscription, SubscriptionChannel
from src.models.user import User
from src.repository.digest_delivery import DigestDeliveryRepository
from src.knowledge.repository import KnowledgeRepository


@pytest.mark.asyncio
async def test_subscription_knowledge_search_enforces_membership_and_baseline(engine) -> None:
    now = datetime.now(timezone.utc)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    async with session_factory() as session:
        user = User(telegram_user_id=701, chat_id=701, language="ru")
        included = Channel(username="included")
        excluded = Channel(username="excluded")
        session.add_all([user, included, excluded])
        await session.flush()
        subscription = Subscription(user_id=user.id, name="AI")
        session.add(subscription)
        await session.flush()
        session.add(SubscriptionChannel(subscription_id=subscription.id, channel_id=included.id, subscribed_at=now))
        session.add_all([
            Post(channel_id=included.id, post_id=1, content="RAG before baseline", datetime=now - timedelta(days=1)),
            Post(channel_id=included.id, post_id=2, content="RAG after baseline", datetime=now + timedelta(seconds=1)),
            Post(channel_id=excluded.id, post_id=3, content="RAG outside subscription", datetime=now + timedelta(seconds=1)),
            KnowledgeChannel(channel_id=included.id, state=KnowledgeChannelState.READY),
        ])
        await session.commit()

    service = KnowledgeService(session_factory, KnowledgeSettings(), LlmSettings())
    result = await service.search(user, scope_type="subscription", scope_id=subscription.id, question="RAG")

    assert result.mode == "normal"
    assert result.source_post_ids == [2]
    assert "https://t.me/included/2" in result.rendered_html
    assert "outside" not in result.rendered_html


def test_official_export_parser_keeps_public_text_messages_only() -> None:
    raw = b'''{"messages":[{"id":1,"type":"message","date":"2026-01-01T10:00:00+00:00","text":"one"},{"id":2,"type":"service","date":"2026-01-01T10:01:00+00:00","text":"ignored"},{"id":"bad","type":"message","date":"2026-01-01T10:02:00+00:00","text":"ignored"}]}'''

    posts = parse_official_export(raw, 10000)

    assert [(post.telegram_post_id, post.content) for post in posts] == [(1, "one")]


def test_official_export_parser_uses_unix_time_for_naive_desktop_dates() -> None:
    raw = b'''{"messages":[{"id":1,"type":"message","date":"2026-01-01T10:00:00","date_unixtime":"1767261600","text":["one",{"type":"link","text":" https://example.test"}]}]}'''

    post = parse_official_export(raw, 10000)[0]

    assert post.published_at.tzinfo is not None
    assert post.content == "one https://example.test"


@pytest.mark.asyncio
async def test_admin_import_persists_canonical_posts_when_enrichment_is_unavailable(engine) -> None:
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    async with session_factory() as session:
        channel = Channel(username="catalog")
        session.add(channel)
        await session.flush()
        session.add(KnowledgeChannel(channel_id=channel.id, state=KnowledgeChannelState.PENDING_IMPORT))
        await session.commit()

    service = KnowledgeService(
        session_factory,
        KnowledgeSettings(administrator_telegram_ids=[900]),
        LlmSettings(),
    )
    raw = b'''{"messages":[{"id":10,"type":"message","date":"2026-01-01T10:00:00+00:00","text":"Canonical imported RAG post"}]}'''
    import_id = await service.queue_import(900, "@catalog", "result.json", raw)
    await service.process_import(import_id, raw)

    async with session_factory() as session:
        stored = (await session.execute(select(Post).where(Post.post_id == 10))).scalar_one()
        entry = (await session.execute(select(KnowledgeChannel))).scalar_one()
    assert stored.content == "Canonical imported RAG post"
    assert entry.state == KnowledgeChannelState.READY


@pytest.mark.asyncio
async def test_knowledge_backfill_is_skipped_for_existing_subscription_digests(engine) -> None:
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    async with session_factory() as session:
        user = User(telegram_user_id=901, chat_id=901)
        channel = Channel(username="catalog")
        session.add_all([user, channel])
        await session.flush()
        subscription = Subscription(user_id=user.id, name="AI")
        session.add(subscription)
        await session.flush()
        session.add(SubscriptionChannel(subscription_id=subscription.id, channel_id=channel.id, subscribed_at=datetime(2025, 1, 1, tzinfo=timezone.utc)))
        post = Post(channel_id=channel.id, post_id=1, content="Imported history", datetime=datetime(2025, 2, 1, tzinfo=timezone.utc))
        session.add(post)
        await session.flush()

        skipped = await KnowledgeRepository(session).mark_import_backfill_skipped(channel.id, [post.id])
        await session.commit()

        assert skipped == 1
        assert await DigestDeliveryRepository(session).get_pending_posts_for_subscription(subscription.id) == []


@pytest.mark.asyncio
async def test_knowledge_retry_reprocesses_failed_enrichment_and_vectors_within_limit(engine) -> None:
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    async with session_factory() as session:
        channel = Channel(username="catalog")
        session.add(channel)
        await session.flush()
        catalog = KnowledgeChannel(channel_id=channel.id, state=KnowledgeChannelState.READY)
        failed_enrichment = Post(channel_id=channel.id, post_id=1, content="retry enrichment", datetime=datetime.now(timezone.utc))
        failed_vector = Post(channel_id=channel.id, post_id=2, content="retry vector", datetime=datetime.now(timezone.utc))
        exhausted = Post(channel_id=channel.id, post_id=3, content="do not retry", datetime=datetime.now(timezone.utc))
        session.add_all([catalog, failed_enrichment, failed_vector, exhausted])
        await session.flush()
        vector_document = KnowledgeDocument(post_id=failed_vector.id, source_content_hash="a" * 64, enrichment_status=EnrichmentStatus.READY)
        session.add_all([
            KnowledgeDocument(post_id=failed_enrichment.id, source_content_hash="b" * 64, enrichment_status=EnrichmentStatus.FAILED, enrichment_attempts=1),
            vector_document,
            KnowledgeDocument(post_id=exhausted.id, source_content_hash="c" * 64, enrichment_status=EnrichmentStatus.FAILED, enrichment_attempts=3),
        ])
        await session.flush()
        session.add(KnowledgeRepresentation(
            knowledge_document_id=vector_document.id,
            post_id=failed_vector.id,
            representation_type=RepresentationType.FULL,
            text="retry vector",
            text_hash="d" * 64,
            token_count=2,
            qdrant_point_id="e" * 64,
            index_version=1,
            index_status=IndexStatus.FAILED,
            index_attempts=1,
        ))
        await session.commit()

    service = KnowledgeService(session_factory, KnowledgeSettings(max_retry_attempts=3), LlmSettings())
    service.index_post = AsyncMock(return_value=True)

    attempted, completed = await service.retry_failed_indexing("catalog")

    assert (attempted, completed) == (2, 2)
    assert {call.args[0] for call in service.index_post.await_args_list} == {failed_enrichment.id, failed_vector.id}
    async with session_factory() as session:
        refreshed = (await session.execute(select(KnowledgeChannel).where(KnowledgeChannel.id == catalog.id))).scalar_one()
    assert refreshed.post_count == 3
    assert refreshed.representation_count == 0
