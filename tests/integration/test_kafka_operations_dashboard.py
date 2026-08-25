"""Authenticated content-free Kafka operations dashboard API coverage."""

from datetime import datetime, timedelta, timezone
from uuid import uuid4

import httpx
import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker

from src.admin.app import create_admin_app
from src.admin.passwords import hash_password
from src.admin.service import AdminDashboardService
from src.config.settings import AdminSettings, KafkaSettings, MemorySettings, ReliableDeliverySettings
from src.models.outbox_event import OutboxEvent
from src.models.reliability_role_heartbeat import ReliabilityRoleHeartbeat


class StubProbe:
    async def probe(self):
        return {
            "broker": {"status": "available", "latency_ms": 4, "error_code": None},
            "topics": [
                {
                    "name": name,
                    "status": "available",
                    "partitions": 1,
                    "replication_factor": 1,
                    "drift": False,
                }
                for name in (
                    "tpb.digest.run.requested.v1",
                    "tpb.telegram.delivery.requested.v1",
                    "tpb.digest.run.requested.dlq.v1",
                    "tpb.telegram.delivery.requested.dlq.v1",
                )
            ],
            "consumer_groups": [
                {
                    "group_id": group,
                    "topic": topic,
                    "status": "inactive",
                    "lag": None,
                    "error_code": None,
                }
                for group, topic in (
                    ("digest-renderer-v1", "tpb.digest.run.requested.v1"),
                    ("telegram-delivery-v1", "tpb.telegram.delivery.requested.v1"),
                )
            ],
        }


@pytest.mark.asyncio
async def test_kafka_operations_classifies_stale_roles_and_exposes_only_safe_data(engine) -> None:
    now = datetime.now(timezone.utc)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    secret = "PRIVATE POST AND CHAT 998877"
    async with session_factory() as session:
        session.add_all(
            [
                ReliabilityRoleHeartbeat(
                    role="scheduler",
                    instance_id=str(uuid4()),
                    state="starting",
                    started_at=now - timedelta(minutes=2),
                    heartbeat_at=now - timedelta(seconds=10),
                    updated_at=now - timedelta(seconds=10),
                ),
                ReliabilityRoleHeartbeat(
                    role="outbox-relay",
                    instance_id=str(uuid4()),
                    state="ready",
                    started_at=now - timedelta(minutes=2),
                    heartbeat_at=now - timedelta(seconds=31),
                    updated_at=now - timedelta(seconds=31),
                ),
                ReliabilityRoleHeartbeat(
                    role="digest-worker",
                    instance_id=str(uuid4()),
                    state="stopped",
                    started_at=now - timedelta(minutes=2),
                    heartbeat_at=now - timedelta(seconds=40),
                    stopped_at=now - timedelta(seconds=40),
                    updated_at=now - timedelta(seconds=40),
                ),
                ReliabilityRoleHeartbeat(
                    role="telegram-delivery-worker",
                    instance_id=str(uuid4()),
                    state="failed",
                    started_at=now - timedelta(minutes=2),
                    heartbeat_at=now - timedelta(seconds=5),
                    stopped_at=now - timedelta(seconds=5),
                    last_error_code="RuntimeError",
                    updated_at=now - timedelta(seconds=5),
                ),
                OutboxEvent(
                    event_id=uuid4(),
                    correlation_id=uuid4(),
                    event_type="digest.run.requested",
                    event_version=1,
                    occurred_at=now,
                    aggregate_type="digest_run",
                    aggregate_id=str(uuid4()),
                    attempt=1,
                    generation=1,
                    topic="tpb.digest.run.requested.v1",
                    event_key="safe-reference",
                    payload={"forbidden": secret},
                    state="pending",
                    publication_attempt_count=2,
                    next_attempt_at=now,
                    last_error="TimeoutError",
                    created_at=now - timedelta(minutes=1),
                    updated_at=now,
                ),
            ]
        )
        await session.commit()

    service = AdminDashboardService(
        session_factory,
        kafka_settings=KafkaSettings(enabled=True),
        reliable_settings=ReliableDeliverySettings(),
        memory_settings=MemorySettings(enabled=True),
        kafka_probe=StubProbe(),
    )
    snapshot = await service.kafka_operations(now)
    assert [role["health"] for role in snapshot["roles"]] == ["starting", "stale", "stopped", "failed"]
    assert [role["age_seconds"] for role in snapshot["roles"]] == [10, 31, 40, 5]

    async with session_factory() as session:
        scheduler = await session.get(ReliabilityRoleHeartbeat, "scheduler")
        assert scheduler is not None
        scheduler.state = "ready"
        await session.commit()
    healthy = await service.kafka_operations(now)
    assert healthy["roles"][0]["health"] == "healthy"

    admin = AdminSettings(
        enabled=True,
        username="admin",
        password_hash=hash_password("password"),
        session_secret="test-secret",
        secure_cookies=False,
    )
    app = create_admin_app(
        admin,
        session_factory,
        kafka_settings=KafkaSettings(enabled=True),
        reliable_settings=ReliableDeliverySettings(subscription_ids=[123, 456]),
        memory_settings=MemorySettings(enabled=True),
        kafka_probe=StubProbe(),
    )
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        unauthenticated = await client.get("/admin/api/kafka/operations")
        await client.post("/admin/login", data={"username": "admin", "password": "password"})
        response = await client.get("/admin/api/kafka/operations")

    assert unauthenticated.status_code == 401
    assert response.status_code == 200
    payload = response.json()
    assert payload["mode"] == {
        "kafka_enabled": True,
        "reliable_enabled": False,
        "reliable_all_subscriptions": False,
        "reliable_subscription_count": 2,
        "memory_enabled": True,
        "delivery_path": "legacy",
    }
    assert len(payload["topics"]) == 4
    assert all("name" in topic and "topic" not in topic for topic in payload["topics"])
    assert all("group_id" in group and "group" not in group for group in payload["consumer_groups"])
    assert all("health" in role and "age_seconds" in role and "status" not in role for role in payload["roles"])
    for queue_name in ("unpublished_outbox", "pending_retries", "expired_leases", "open_dead_letters"):
        assert "count" in payload["queues"][queue_name]
        assert "oldest_age_seconds" in payload["queues"][queue_name]
    assert payload["recent_errors"][0]["error_code"] in {"TimeoutError", "RuntimeError"}
    assert len(payload["recent_errors"]) <= 20
    assert secret not in response.text
    assert "payload" not in response.text
    assert "subscription_ids" not in response.text


@pytest.mark.asyncio
async def test_kafka_operations_api_returns_safe_broker_error_instead_of_500(engine) -> None:
    class FailedProbe:
        async def probe(self):
            raise ConnectionError("broker details must not escape")

    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    admin = AdminSettings(
        enabled=True,
        username="admin",
        password_hash=hash_password("password"),
        session_secret="test-secret",
        secure_cookies=False,
    )
    app = create_admin_app(admin, session_factory, kafka_probe=FailedProbe())
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        await client.post("/admin/login", data={"username": "admin", "password": "password"})
        response = await client.get("/admin/api/kafka/operations")

    assert response.status_code == 200
    assert response.json()["broker"]["error_code"] == "ConnectionError"
    assert [role["health"] for role in response.json()["roles"]] == ["missing"] * 4
    assert "broker details" not in response.text


@pytest.mark.asyncio
async def test_kafka_operations_does_not_start_probe_when_database_snapshot_fails() -> None:
    calls = 0

    class CountingProbe:
        async def probe(self):
            nonlocal calls
            calls += 1
            return {}

    class FailedSession:
        async def __aenter__(self):
            raise RuntimeError("database unavailable")

        async def __aexit__(self, exc_type, exc, traceback):
            return False

    service = AdminDashboardService(lambda: FailedSession(), kafka_probe=CountingProbe())

    with pytest.raises(RuntimeError, match="database unavailable"):
        await service.kafka_operations()
    assert calls == 0
