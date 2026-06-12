"""Post repository — deduplicating storage of scraped posts."""

from __future__ import annotations

from typing import Optional

from sqlalchemy import func, insert, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from src.models.post import Post
from src.scraper.parser import ParsedPost


class PostRepository:
    """Manage Post records with deduplication via ON CONFLICT DO NOTHING."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def upsert_posts(
        self,
        channel_id: int,
        posts: list[ParsedPost],
    ) -> int:
        """Bulk-insert posts, skipping duplicates via ON CONFLICT DO NOTHING.

        Uses SQLAlchemy Core ``insert()`` with ``on_conflict_do_nothing``
        on the ``uq_posts_channel_post`` unique constraint. Returns the
        count of actually inserted (new) rows.

        For SQLite compatibility in tests, uses a manual dedup approach
        when the dialect does not support ``on_conflict_do_nothing``.
        """
        if not posts:
            return 0

        dialect = self._session.bind.dialect.name if self._session.bind else "unknown"

        if dialect == "sqlite":
            return await self._upsert_posts_sqlite(channel_id, posts)

        return await self._upsert_posts_postgresql(channel_id, posts)

    async def _upsert_posts_postgresql(
        self,
        channel_id: int,
        posts: list[ParsedPost],
    ) -> int:
        """PostgreSQL path: INSERT … ON CONFLICT DO NOTHING."""
        rows = [self._post_to_dict(channel_id, p) for p in posts]

        stmt = pg_insert(Post).values(rows).on_conflict_do_nothing(
            index_elements=["channel_id", "post_id"],
        )
        result = await self._session.execute(stmt)
        await self._session.flush()
        return result.rowcount

    async def _upsert_posts_sqlite(
        self,
        channel_id: int,
        posts: list[ParsedPost],
    ) -> int:
        """SQLite fallback: check existing and insert only new posts."""
        inserted = 0
        for parsed in posts:
            # Check if this post already exists
            exists_stmt = select(func.count()).select_from(Post).where(
                Post.channel_id == channel_id,
                Post.post_id == parsed.post_id,
            )
            result = await self._session.execute(exists_stmt)
            if result.scalar_one() > 0:
                continue

            row = self._post_to_dict(channel_id, parsed)
            stmt = insert(Post).values(row)
            await self._session.execute(stmt)
            inserted += 1

        await self._session.flush()
        return inserted

    @staticmethod
    def _post_to_dict(channel_id: int, parsed: ParsedPost) -> dict:
        """Convert a ParsedPost to a dict suitable for INSERT."""
        from datetime import datetime

        # Parse ISO datetime string to datetime object
        dt = parsed.datetime
        if isinstance(dt, str):
            # Handle various ISO formats
            dt = datetime.fromisoformat(dt.replace("Z", "+00:00"))

        return {
            "post_id": parsed.post_id,
            "channel_id": channel_id,
            "content": parsed.content,
            "datetime": dt,
            "views": parsed.views,
            "reactions": parsed.reactions,
            "author": parsed.author,
            "link_preview": parsed.link_preview,
        }

    async def get_posts_by_channel(
        self,
        channel_id: int,
        limit: int = 20,
        offset: int = 0,
    ) -> list[Post]:
        """Retrieve posts for a channel, newest first."""
        stmt = (
            select(Post)
            .where(Post.channel_id == channel_id)
            .order_by(Post.datetime.desc())
            .limit(limit)
            .offset(offset)
        )
        result = await self._session.execute(stmt)
        return list(result.scalars().all())

    async def count_posts(self, channel_id: int) -> int:
        """Count total posts for a given channel."""
        stmt = select(func.count()).select_from(Post).where(
            Post.channel_id == channel_id,
        )
        result = await self._session.execute(stmt)
        return result.scalar_one()
