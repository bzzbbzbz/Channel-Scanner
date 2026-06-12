"""Channel repository — manages channel lifecycle (active/error/scraped)."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from src.models.channel import Channel, ChannelStatus


class ChannelRepository:
    """Manage Channel records in the database."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get_active_channels(self) -> list[Channel]:
        """Return all active channels, ordered by last_scraped ASC NULLS FIRST.

        Channels that have never been scraped come first, ensuring new
        additions are processed before already-scraped ones.
        """
        stmt = (
            select(Channel)
            .where(Channel.status == ChannelStatus.ACTIVE)
            .order_by(Channel.last_scraped.asc().nulls_first())
        )
        result = await self._session.execute(stmt)
        return list(result.scalars().all())

    async def get_by_telegram_id(self, telegram_id: int) -> Optional[Channel]:
        """Look up a channel by its Telegram numeric ID."""
        stmt = select(Channel).where(Channel.telegram_id == telegram_id)
        result = await self._session.execute(stmt)
        return result.scalar_one_or_none()

    async def get_by_username(self, username: str) -> Optional[Channel]:
        """Look up a channel by Telegram username."""
        stmt = select(Channel).where(Channel.username == username)
        result = await self._session.execute(stmt)
        return result.scalar_one_or_none()

    async def upsert_channel(
        self,
        telegram_id: Optional[int],
        username: str,
        name: str,
    ) -> Channel:
        """Insert a new channel or update an existing one on conflict.

        Uses ``telegram_id`` as the conflict target. On conflict, updates
        ``username`` and ``name`` to the latest values.
        """
        existing = None
        if telegram_id is not None:
            existing = await self.get_by_telegram_id(telegram_id)
        if existing is None:
            existing = await self.get_by_username(username)

        if existing is not None:
            existing.telegram_id = telegram_id
            existing.username = username
            existing.name = name
            existing.updated_at = datetime.now(timezone.utc)
            await self._session.flush()
            return existing

        channel = Channel(
            telegram_id=telegram_id,
            username=username,
            name=name,
            status=ChannelStatus.ACTIVE,
        )
        self._session.add(channel)
        await self._session.flush()
        return channel

    async def upsert_by_username(self, username: str, name: Optional[str] = None) -> Channel:
        """Insert or update a channel using its public username."""
        normalized_name = name or username
        return await self.upsert_channel(None, username, normalized_name)

    async def mark_scraped(self, channel_id: int) -> None:
        """Update channel's last_scraped timestamp to now."""
        stmt = (
            update(Channel)
            .where(Channel.id == channel_id)
            .values(last_scraped=datetime.now(timezone.utc), updated_at=datetime.now(timezone.utc))
        )
        await self._session.execute(stmt)
        await self._session.flush()

    async def mark_error(self, channel_id: int, error: str) -> None:
        """Mark a channel as errored with an error message."""
        stmt = (
            update(Channel)
            .where(Channel.id == channel_id)
            .values(
                status=ChannelStatus.ERROR,
                last_error=error,
                updated_at=datetime.now(timezone.utc),
            )
        )
        await self._session.execute(stmt)
        await self._session.flush()

    async def mark_active(self, channel_id: int) -> None:
        """Reset a channel back to active status, clearing any error."""
        stmt = (
            update(Channel)
            .where(Channel.id == channel_id)
            .values(
                status=ChannelStatus.ACTIVE,
                last_error=None,
                updated_at=datetime.now(timezone.utc),
            )
        )
        await self._session.execute(stmt)
        await self._session.flush()
