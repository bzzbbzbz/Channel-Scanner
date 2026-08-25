"""Persistent lifecycle coverage for reliability role heartbeats."""

import asyncio
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy.dialects import postgresql, sqlite
from sqlalchemy.ext.asyncio import async_sessionmaker

from src.models.reliability_role_heartbeat import ReliabilityRoleHeartbeat
from src.reliability.heartbeat import RoleHeartbeatReporter, _heartbeat_upsert


@pytest.mark.asyncio
async def test_role_heartbeat_reporter_persists_lifecycle_and_periodic_heartbeat(engine) -> None:
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    clock = [datetime(2026, 8, 24, 12, 0, tzinfo=timezone.utc)]
    reporter = RoleHeartbeatReporter(
        "scheduler",
        session_factory,
        instance_id="4be9e933-66a7-4f70-958f-a52ad15a7890",
        interval_seconds=0.01,
        now=lambda: clock[0],
    )

    assert await reporter.start() is True
    async with session_factory() as session:
        row = await session.get(ReliabilityRoleHeartbeat, "scheduler")
        assert row is not None
        assert row.state == "starting"
        assert row.instance_id == reporter.instance_id

    await reporter.ready()
    clock[0] += timedelta(seconds=11)
    await reporter.heartbeat()
    await reporter.stopped()

    async with session_factory() as session:
        row = await session.get(ReliabilityRoleHeartbeat, "scheduler")
        assert row is not None
        assert row.state == "stopped"
        assert row.stopped_at is not None
        assert row.last_error_code is None


@pytest.mark.asyncio
async def test_role_heartbeat_reporter_persists_exception_type_and_is_best_effort(engine) -> None:
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    reporter = RoleHeartbeatReporter("digest-worker", session_factory)
    await reporter.start()
    await reporter.failed("RuntimeError")

    async with session_factory() as session:
        row = await session.get(ReliabilityRoleHeartbeat, "digest-worker")
        assert row is not None
        assert row.state == "failed"
        assert row.last_error_code == "RuntimeError"

    broken = RoleHeartbeatReporter("outbox-relay", session_factory, interval_seconds=60)

    async def fail_write(*, initial: bool) -> None:
        raise TimeoutError

    broken._persist = fail_write  # type: ignore[method-assign]
    assert await broken.start() is False
    assert await broken.ready() is False
    assert await broken.stopped() is False


@pytest.mark.asyncio
async def test_concurrent_initial_heartbeats_atomically_replace_one_role_generation(engine) -> None:
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    first = RoleHeartbeatReporter("scheduler", session_factory, instance_id="first-generation")
    second = RoleHeartbeatReporter("scheduler", session_factory, instance_id="second-generation")

    assert all(await asyncio.gather(first.start(), second.start()))
    await asyncio.gather(first.close(), second.close())

    async with session_factory() as session:
        row = await session.get(ReliabilityRoleHeartbeat, "scheduler")
        assert row is not None
        winner = row.instance_id
        assert winner in {first.instance_id, second.instance_id}
        assert row.state == "starting"

    loser = second if winner == first.instance_id else first
    await loser.ready()
    async with session_factory() as session:
        row = await session.get(ReliabilityRoleHeartbeat, "scheduler")
        assert row is not None
        assert row.instance_id == winner
        assert row.state == "starting"


@pytest.mark.asyncio
async def test_noninitial_heartbeat_inserts_after_initial_table_failure(engine) -> None:
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    reporter = RoleHeartbeatReporter("outbox-relay", session_factory, interval_seconds=60)
    table = ReliabilityRoleHeartbeat.__table__
    async with engine.begin() as connection:
        await connection.run_sync(table.drop)

    assert await reporter.start() is False
    async with engine.begin() as connection:
        await connection.run_sync(table.create)

    assert await reporter.ready() is True
    await reporter.close()
    async with session_factory() as session:
        row = await session.get(ReliabilityRoleHeartbeat, "outbox-relay")
        assert row is not None
        assert row.instance_id == reporter.instance_id
        assert row.state == "ready"


@pytest.mark.asyncio
async def test_delayed_older_initial_heartbeat_does_not_replace_new_generation(engine) -> None:
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    old_time = datetime(2026, 8, 24, 12, 0, tzinfo=timezone.utc)
    new_time = old_time + timedelta(seconds=1)
    old = RoleHeartbeatReporter(
        "digest-worker",
        session_factory,
        instance_id="old-generation",
        now=lambda: old_time,
    )
    new = RoleHeartbeatReporter(
        "digest-worker",
        session_factory,
        instance_id="new-generation",
        now=lambda: new_time,
    )

    assert await new._write(initial=True) is True
    assert await old._write(initial=True) is True

    async with session_factory() as session:
        row = await session.get(ReliabilityRoleHeartbeat, "digest-worker")
        assert row is not None
        assert row.instance_id == new.instance_id
        assert row.started_at.replace(tzinfo=timezone.utc) == new_time


@pytest.mark.asyncio
async def test_new_generation_recovers_after_its_initial_write_failed(engine) -> None:
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    old_time = datetime(2026, 8, 24, 12, 0, tzinfo=timezone.utc)
    old = RoleHeartbeatReporter(
        "scheduler", session_factory, instance_id="old-generation", now=lambda: old_time
    )
    new = RoleHeartbeatReporter(
        "scheduler",
        session_factory,
        instance_id="new-generation",
        now=lambda: old_time + timedelta(seconds=1),
    )

    assert await old._write(initial=True) is True
    assert await new._write(initial=False) is True

    async with session_factory() as session:
        row = await session.get(ReliabilityRoleHeartbeat, "scheduler")
        assert row is not None
        assert row.instance_id == new.instance_id


def test_initial_heartbeat_upsert_compiles_for_postgresql_and_sqlite() -> None:
    now = datetime(2026, 8, 24, 12, 0, tzinfo=timezone.utc)
    values = {
        "instance_id": "generation",
        "state": "starting",
        "started_at": now,
        "heartbeat_at": now,
        "stopped_at": None,
        "last_error_code": None,
        "updated_at": now,
    }

    postgres_sql = str(
        _heartbeat_upsert("postgresql", "scheduler", values, initial=True).compile(
            dialect=postgresql.dialect()
        )
    )
    sqlite_sql = str(
        _heartbeat_upsert("sqlite", "scheduler", values, initial=False).compile(dialect=sqlite.dialect())
    )

    assert "ON CONFLICT (role) DO UPDATE" in postgres_sql
    assert "started_at <=" in postgres_sql
    assert "ON CONFLICT (role) DO UPDATE" in sqlite_sql
    assert "instance_id =" in sqlite_sql
    assert "started_at <" in sqlite_sql
