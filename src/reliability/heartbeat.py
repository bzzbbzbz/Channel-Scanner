"""Best-effort persistent lifecycle reporting for reliability roles."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import logging
from typing import Callable
from uuid import uuid4

from sqlalchemy import or_
from sqlalchemy.dialects.postgresql import insert as postgresql_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.ext.asyncio import async_sessionmaker

from src.models.reliability_role_heartbeat import ReliabilityRoleHeartbeat

logger = logging.getLogger(__name__)


class RoleHeartbeatReporter:
    """Persist role health without allowing telemetry failures to stop work."""

    def __init__(
        self,
        role: str,
        session_factory: async_sessionmaker,
        *,
        instance_id: str | None = None,
        interval_seconds: float = 10.0,
        write_timeout_seconds: float = 2.0,
        now: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    ) -> None:
        self.role = role
        self.instance_id = instance_id or str(uuid4())
        self._session_factory = session_factory
        self._interval_seconds = interval_seconds
        self._write_timeout_seconds = write_timeout_seconds
        self._now = now
        self._state = "starting"
        self._started_at = self._utc_now()
        self._last_error_code: str | None = None
        self._stop_event = asyncio.Event()
        self._task: asyncio.Task | None = None
        self._write_lock = asyncio.Lock()

    async def start(self) -> bool:
        """Attempt the initial row and begin periodic best-effort updates."""
        persisted = await self._write(initial=True)
        self._task = asyncio.create_task(self._run(), name=f"{self.role}-db-heartbeat")
        return persisted

    async def ready(self) -> bool:
        self._state = "ready"
        self._last_error_code = None
        return await self._write()

    async def heartbeat(self) -> bool:
        return await self._write()

    async def stopped(self) -> bool:
        return await self._terminal("stopped", None)

    async def failed(self, error_code: str) -> bool:
        return await self._terminal("failed", error_code[:128] or "Exception")

    async def close(self) -> None:
        """Stop periodic writes without changing the persisted lifecycle state."""
        self._stop_event.set()
        if self._task is not None:
            await self._task
            self._task = None

    async def _terminal(self, state: str, error_code: str | None) -> bool:
        await self.close()
        self._state = state
        self._last_error_code = error_code
        return await self._write()

    async def _run(self) -> None:
        while not self._stop_event.is_set():
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=self._interval_seconds)
            except TimeoutError:
                await self._write()

    async def _write(self, *, initial: bool = False) -> bool:
        try:
            async with self._write_lock:
                await asyncio.wait_for(self._persist(initial=initial), timeout=self._write_timeout_seconds)
            return True
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning(
                "Reliability role heartbeat write failed: role=%s error=%s",
                self.role,
                type(exc).__name__,
            )
            return False

    async def _persist(self, *, initial: bool) -> None:
        now = self._utc_now()
        values = {
            "instance_id": self.instance_id,
            "state": self._state,
            "started_at": self._started_at,
            "heartbeat_at": now,
            "stopped_at": now if self._state in {"stopped", "failed"} else None,
            "last_error_code": self._last_error_code,
            "updated_at": now,
        }
        async with self._session_factory() as session:
            await session.execute(
                _heartbeat_upsert(session.bind.dialect.name, self.role, values, initial=initial)
            )
            await session.commit()

    def _utc_now(self) -> datetime:
        value = self._now()
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)


def _heartbeat_upsert(dialect: str, role: str, values: dict, *, initial: bool):
    if dialect == "postgresql":
        statement = postgresql_insert(ReliabilityRoleHeartbeat).values(role=role, **values)
    elif dialect == "sqlite":
        statement = sqlite_insert(ReliabilityRoleHeartbeat).values(role=role, **values)
    else:
        raise RuntimeError(f"Unsupported heartbeat database dialect: {dialect}")
    conflict_condition = (
        ReliabilityRoleHeartbeat.started_at <= values["started_at"]
        if initial
        else or_(
            ReliabilityRoleHeartbeat.instance_id == values["instance_id"],
            ReliabilityRoleHeartbeat.started_at < values["started_at"],
        )
    )
    return statement.on_conflict_do_update(
        index_elements=[ReliabilityRoleHeartbeat.role],
        set_=values,
        where=conflict_condition,
    )
