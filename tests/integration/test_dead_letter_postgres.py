"""Opt-in real PostgreSQL race coverage for BL-22 stage 5."""

from __future__ import annotations

import asyncio
import os
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

import src.models  # noqa: F401
from src.models.base import Base
from src.models.dead_letter import DeadLetterReplay
from src.models.outbox_event import OutboxEvent
from src.models.reliable_digest import DigestRun
from src.models.subscription import Subscription
from src.models.user import User
from src.repository.dead_letter import DeadLetterRepository
from src.repository.outbox import OutboxRepository
from src.reliability.dead_letter_replay import DeadLetterReplayService


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_concurrent_replay_key_creates_one_generation_and_outbox() -> None:
    url = os.getenv("BL22_POSTGRES_TEST_URL")
    if not url:
        pytest.skip("BL22_POSTGRES_TEST_URL is required for isolated PostgreSQL concurrency coverage")
    if not url.startswith("postgresql+asyncpg://"):
        pytest.fail("BL22_POSTGRES_TEST_URL must use postgresql+asyncpg")

    schema = f"bl22_stage5_{uuid4().hex}"
    admin_engine = create_async_engine(url)
    engine = None
    try:
        async with admin_engine.begin() as connection:
            await connection.execute(text(f'CREATE SCHEMA "{schema}"'))
        engine = create_async_engine(
            url,
            connect_args={"server_settings": {"search_path": schema}},
        )
        async with engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)
        factory = async_sessionmaker(engine, expire_on_commit=False)
        now = datetime(2026, 8, 23, 20, 0, tzinfo=timezone.utc)
        outbox = OutboxRepository(max_event_bytes=65_536)
        async with factory() as session, session.begin():
            user = User(telegram_user_id=9901, chat_id=9902)
            session.add(user)
            await session.flush()
            subscription = Subscription(user_id=user.id, name="Postgres stage 5")
            session.add(subscription)
            await session.flush()
            run = DigestRun(
                id=uuid4(),
                subscription_id=subscription.id,
                user_id=user.id,
                logical_schedule_slot=now - timedelta(hours=1),
                correlation_id=uuid4(),
                generation=1,
                state="failed",
                render_attempt_count=5,
                next_attempt_at=now,
                last_error="RenderProviderError",
                created_at=now,
                updated_at=now,
            )
            session.add(run)
            await session.flush()
            record, _ = await DeadLetterRepository(outbox).record_terminal(
                session,
                source_topic="tpb.digest.run.requested.v1",
                source_event_id=uuid4(),
                source_partition=0,
                source_offset=1,
                work_type="digest_run",
                entity_ref=str(run.id),
                run_id=run.id,
                message_id=None,
                subscription_id=subscription.id,
                correlation_id=run.correlation_id,
                terminal_reason="attempts_exhausted",
                error_code="RenderProviderError",
                attempt_summary={"attempt_count": 5, "max_attempts": 5},
                generation=1,
                failed_at=now,
            )

        service = DeadLetterReplayService(factory, outbox, clock=lambda: now)
        first, second = await asyncio.gather(
            service.replay(record.id, idempotency_key="postgres-race-1", actor="admin-a"),
            service.replay(record.id, idempotency_key="postgres-race-1", actor="admin-a"),
        )
        assert first == second
        async with factory() as session:
            stored_run = await session.get(DigestRun, run.id)
            assert (stored_run.state, stored_run.generation) == ("pending", 2)
            assert await session.scalar(select(func.count()).select_from(DeadLetterReplay)) == 1
            assert await session.scalar(
                select(func.count()).select_from(OutboxEvent).where(
                    OutboxEvent.event_type == "tpb.digest.run.requested"
                )
            ) == 1
    finally:
        if engine is not None:
            await engine.dispose()
        async with admin_engine.begin() as connection:
            await connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        await admin_engine.dispose()
