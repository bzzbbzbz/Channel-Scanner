"""Ephemeral dashboard server used only by Playwright browser regression tests."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import uvicorn
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

import src.models  # noqa: F401
from src.admin.app import create_admin_app
from src.admin.passwords import hash_password
from src.config.settings import AdminSettings, KafkaSettings, MemorySettings, ReliableDeliverySettings
from src.models.base import Base
from src.models.outbox_event import OutboxEvent
from src.models.reliability_role_heartbeat import ReliabilityRoleHeartbeat


class RepresentativeKafkaProbe:
    async def probe(self):
        return {
            "broker": {"status": "healthy", "latency_ms": 12, "error_code": None},
            "topics": [
                {
                    "name": "digest.schedule.v1",
                    "status": "healthy",
                    "partitions": 3,
                    "replication_factor": 1,
                    "drift": False,
                },
                {
                    "name": "digest.render.v1",
                    "status": "healthy",
                    "partitions": 3,
                    "replication_factor": 1,
                    "drift": False,
                },
            ],
            "consumer_groups": [
                {
                    "group_id": "digest-renderer-v1",
                    "topic": "digest.schedule.v1",
                    "status": "inactive",
                    "lag": 0,
                    "error_code": None,
                }
            ],
        }


async def create_test_app():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(timezone.utc)
    async with session_factory() as session:
        session.add_all(
            [
                ReliabilityRoleHeartbeat(
                    role="outbox-relay",
                    instance_id="browser-outbox-relay",
                    state="ready",
                    started_at=now - timedelta(minutes=20),
                    heartbeat_at=now,
                    updated_at=now,
                ),
                OutboxEvent(
                    event_id=uuid4(),
                    correlation_id=uuid4(),
                    event_type="digest.run.requested",
                    event_version=1,
                    occurred_at=now - timedelta(minutes=10),
                    aggregate_type="digest_run",
                    aggregate_id=str(uuid4()),
                    attempt=1,
                    generation=1,
                    topic="digest.schedule.v1",
                    event_key="browser-pending",
                    payload={"event_version": 1},
                    state="pending",
                    publication_attempt_count=2,
                    next_attempt_at=now + timedelta(minutes=1),
                    last_error="PublishTimeout",
                    created_at=now - timedelta(minutes=10),
                    updated_at=now - timedelta(minutes=5),
                ),
                OutboxEvent(
                    event_id=uuid4(),
                    correlation_id=uuid4(),
                    event_type="digest.run.requested",
                    event_version=1,
                    occurred_at=now - timedelta(minutes=20),
                    aggregate_type="digest_run",
                    aggregate_id=str(uuid4()),
                    attempt=1,
                    generation=1,
                    topic="digest.schedule.v1",
                    event_key="browser-expired",
                    payload={"event_version": 1},
                    state="publishing",
                    lease_owner="browser-relay",
                    lease_until=now - timedelta(minutes=1),
                    publication_attempt_count=0,
                    next_attempt_at=now,
                    created_at=now - timedelta(minutes=20),
                    updated_at=now - timedelta(minutes=1),
                ),
            ]
        )
        await session.commit()
    app = create_admin_app(
        AdminSettings(
            enabled=True,
            username="admin",
            password_hash=hash_password("playwright-password"),
            session_secret="playwright-session-secret",
            secure_cookies=False,
        ),
        session_factory,
        kafka_settings=KafkaSettings(enabled=True),
        reliable_settings=ReliableDeliverySettings(),
        memory_settings=MemorySettings(enabled=True),
        kafka_probe=RepresentativeKafkaProbe(),
    )
    assert sum(getattr(route, "path", None) == "/admin/api/kafka/operations" for route in app.routes) == 1
    return app


if __name__ == "__main__":
    uvicorn.run(asyncio.run(create_test_app()), host="127.0.0.1", port=4173, log_level="warning")
