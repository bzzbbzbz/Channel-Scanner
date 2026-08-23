"""Integration coverage for the read-only admin dashboard and its aggregates."""

from datetime import datetime, timedelta, timezone

import httpx
import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker

from src.admin.app import create_admin_app
from src.admin.passwords import hash_password
from src.admin.service import AdminDashboardService
from src.config.settings import AdminSettings, KnowledgeSettings
from src.models.channel import Channel, ChannelStatus
from src.models.chat_message import ChatMessage
from src.models.digest_delivery import DigestDelivery
from src.models.digest_processing_log import DigestProcessingLog
from src.models.llm_usage import LlmUsage
from src.models.knowledge import KnowledgeChannel, KnowledgeChannelState, KnowledgeEvaluationRun
from src.models.post import Post
from src.models.subscription import Subscription
from src.models.user import User


@pytest.mark.asyncio
async def test_admin_dashboard_requires_login_and_returns_aggregates(engine) -> None:
    now = datetime.now(timezone.utc)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    async with session_factory() as session:
        user = User(telegram_user_id=101, chat_id=101, created_at=now - timedelta(hours=2))
        channel = Channel(username="news", status=ChannelStatus.ERROR, last_error="source unavailable", created_at=now - timedelta(hours=2))
        session.add_all([user, channel])
        await session.flush()
        knowledge_channel = KnowledgeChannel(channel_id=channel.id, state=KnowledgeChannelState.READY)
        session.add(knowledge_channel)
        await session.flush()
        subscription = Subscription(user_id=user.id, name="News", created_at=now - timedelta(hours=2))
        post = Post(channel_id=channel.id, post_id=1, content="News", datetime=now, created_at=now - timedelta(hours=1))
        session.add_all([subscription, post])
        await session.flush()
        session.add_all(
            [
                ChatMessage(user_id=user.id, chat_id=user.chat_id, role="user", text="hello", created_at=now - timedelta(minutes=30)),
                DigestDelivery(user_id=user.id, subscription_id=subscription.id, post_id=post.id, status="delivered", delivered_at=now - timedelta(minutes=20)),
                DigestProcessingLog(user_id=user.id, subscription_id=subscription.id, found_count=2, filtered_count=1, included_count=1, completed_at=now - timedelta(minutes=20)),
                LlmUsage(model="test/model", use_case="summary", status="success", total_tokens=42, cost="0.012345", created_at=now - timedelta(minutes=10)),
                KnowledgeEvaluationRun(
                    knowledge_channel_id=knowledge_channel.id,
                    index_version=1,
                    dataset_hash="a" * 64,
                    mode="hybrid_parent_rrf@5",
                    recall_at_k=0.8,
                    mrr=0.7,
                    ndcg=0.75,
                    duplicate_source_share=0.1,
                    p50_latency_ms=160,
                    p95_latency_ms=240,
                    p99_latency_ms=300,
                    latency_ms=120,
                    p50_retrieval_latency_ms=40,
                    p95_retrieval_latency_ms=60,
                    p99_retrieval_latency_ms=80,
                    retrieval_latency_ms=45,
                    p50_answer_generation_ms=110,
                    p95_answer_generation_ms=180,
                    p99_answer_generation_ms=220,
                    answer_generation_ms=125,
                    context_tokens=300,
                    cost=None,
                    created_at=now - timedelta(minutes=5),
                ),
            ]
        )
        await session.commit()

    settings = AdminSettings(
        enabled=True,
        username="admin",
        password_hash=hash_password("correct horse battery staple"),
        session_secret="test-secret",
        secure_cookies=False,
    )
    app = create_admin_app(
        settings,
        session_factory,
        KnowledgeSettings(
            rag_rollout_enabled=True,
            rag_canary_telegram_ids=[101],
            rag_configuration_id="bl21-rerank-v1",
        ),
    )
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver", follow_redirects=False) as client:
        unauthenticated = await client.get("/admin/api/metrics")
        assert unauthenticated.status_code == 401
        rejected = await client.post("/admin/login", data={"username": "admin", "password": "wrong"})
        assert rejected.headers["location"].startswith("/admin/login?error=invalid")
        accepted = await client.post("/admin/login", data={"username": "admin", "password": "correct horse battery staple"})
        assert accepted.headers["location"] == "/admin"
        response = await client.get("/admin/api/metrics?period=24h")

    assert response.status_code == 200
    payload = response.json()
    assert payload["range"]["bucket"] == "hour"
    assert len(payload["daily"]) >= 24
    assert payload["overview"]["users_total"] == 1
    assert payload["overview"]["active_users"] == 1
    assert payload["overview"]["delivered"] == 1
    assert payload["overview"]["processing"] == {"found": 2, "filtered": 1, "included": 1}
    assert payload["overview"]["llm_tokens"] == 42
    assert payload["models"] == [
        {"model": "test/model", "calls": 1, "tokens": 42, "cost": 0.012345, "cost_available": True, "use_cases": ["summary"]}
    ]
    assert payload["errors"][0]["component"] == "@news"
    assert payload["knowledge"]["evaluations"][0]["channel"] == "news"
    assert payload["knowledge"]["evaluations"][0]["recall_at_k"] == 0.8
    assert payload["knowledge"]["evaluations"][0]["p50_answer_generation_ms"] == 110
    assert payload["knowledge"]["active_configuration"]["status"] == "canary"
    assert payload["knowledge"]["active_configuration"]["id"] == "bl21-rerank-v1"
    assert "101" not in response.text


@pytest.mark.asyncio
async def test_admin_dashboard_marks_configuration_as_all_users(engine) -> None:
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    settings = AdminSettings(
        enabled=True,
        username="admin",
        password_hash=hash_password("correct horse battery staple"),
        session_secret="test-secret",
        secure_cookies=False,
    )
    app = create_admin_app(
        settings,
        session_factory,
        KnowledgeSettings(
            rag_rollout_enabled=True,
            rag_enabled_for_all_users=True,
            rag_canary_telegram_ids=[101],
            rag_configuration_id="bl21-rerank-v1",
        ),
    )
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver", follow_redirects=False) as client:
        await client.post("/admin/login", data={"username": "admin", "password": "correct horse battery staple"})
        response = await client.get("/admin/api/metrics?period=24h")

    assert response.status_code == 200
    payload = response.json()
    assert payload["knowledge"]["active_configuration"]["status"] == "all_users"
    assert payload["knowledge"]["active_configuration"]["id"] == "bl21-rerank-v1"
    assert "101" not in response.text


@pytest.mark.asyncio
async def test_admin_dashboard_validates_custom_period(engine) -> None:
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    settings = AdminSettings(
        enabled=True,
        username="admin",
        password_hash=hash_password("password"),
        session_secret="test-secret",
        secure_cookies=False,
    )
    app = create_admin_app(settings, session_factory)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        await client.post("/admin/login", data={"username": "admin", "password": "password"})
        response = await client.get("/admin/api/metrics?period=custom&start=2026-07-20T00:00:00Z&end=2026-07-19T00:00:00Z")

    assert response.status_code == 422


@pytest.mark.asyncio
async def test_admin_dashboard_all_period_starts_at_first_event(engine) -> None:
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    first_event = datetime(2026, 7, 10, 9, 30, tzinfo=timezone.utc)
    async with session_factory() as session:
        session.add(User(telegram_user_id=202, chat_id=202, created_at=first_event))
        await session.commit()

    settings = AdminSettings(
        enabled=True,
        username="admin",
        password_hash=hash_password("password"),
        session_secret="test-secret",
        secure_cookies=False,
    )
    app = create_admin_app(settings, session_factory)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        await client.post("/admin/login", data={"username": "admin", "password": "password"})
        response = await client.get("/admin/api/metrics?period=all")

    assert response.status_code == 200
    assert response.json()["range"]["start"] == first_event.isoformat()
