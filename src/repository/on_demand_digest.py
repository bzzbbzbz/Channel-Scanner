"""Persistence for idempotent on-demand digest results."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import delete, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.ext.asyncio import AsyncSession

from src.models.on_demand_digest import OnDemandDigest


class OnDemandDigestRepository:
    """Claim, complete, and retrieve cached manual digest requests."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get(
        self,
        user_id: int,
        subscription_id: int,
        period_start: datetime,
        period_end: datetime,
        prompt_fingerprint: str,
    ) -> OnDemandDigest | None:
        stmt = select(OnDemandDigest).where(
            OnDemandDigest.user_id == user_id,
            OnDemandDigest.subscription_id == subscription_id,
            OnDemandDigest.period_start == period_start,
            OnDemandDigest.period_end == period_end,
            OnDemandDigest.prompt_fingerprint == prompt_fingerprint,
        )
        return (await self._session.execute(stmt)).scalar_one_or_none()

    async def claim(
        self,
        user_id: int,
        subscription_id: int,
        period_start: datetime,
        period_end: datetime,
        prompt_fingerprint: str,
    ) -> bool:
        values = {
            "user_id": user_id,
            "subscription_id": subscription_id,
            "period_start": period_start,
            "period_end": period_end,
            "prompt_fingerprint": prompt_fingerprint,
            "status": "generating",
        }
        dialect = self._session.bind.dialect.name if self._session.bind else "unknown"
        if dialect == "postgresql":
            stmt = pg_insert(OnDemandDigest).values(values).on_conflict_do_nothing(
                constraint="uq_on_demand_digests_request",
            )
        elif dialect == "sqlite":
            stmt = sqlite_insert(OnDemandDigest).values(values).on_conflict_do_nothing(
                index_elements=["subscription_id", "period_start", "period_end", "prompt_fingerprint"],
            )
        else:
            self._session.add(OnDemandDigest(**values))
            await self._session.flush()
            return True
        result = await self._session.execute(stmt)
        await self._session.flush()
        return bool(result.rowcount)

    async def complete(self, digest_id: int, messages: list[str], completed_at: datetime) -> None:
        await self._session.execute(
            update(OnDemandDigest)
            .where(OnDemandDigest.id == digest_id)
            .values(status="ready", rendered_messages=messages, completed_at=completed_at)
        )
        await self._session.flush()

    async def remove(self, digest_id: int) -> None:
        await self._session.execute(delete(OnDemandDigest).where(OnDemandDigest.id == digest_id))
        await self._session.flush()
