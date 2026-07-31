"""Post repository — deduplicating storage of scraped posts."""

from __future__ import annotations

from typing import Optional

from sqlalchemy import func, insert, select
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

        return await self._upsert_posts_idempotent(channel_id, posts)

    async def _upsert_posts_idempotent(
        self,
        channel_id: int,
        posts: list[ParsedPost],
    ) -> int:
        """Insert new posts and update changed canonical content without duplication.

        Telegram occasionally edits public posts.  A changed body keeps the
        parent row and marks only its knowledge enrichment stale, so the
        scheduled indexer can rebuild representations without a new export.
        """
        changed = 0
        for parsed in posts:
            existing_stmt = select(Post).where(
                Post.channel_id == channel_id,
                Post.post_id == parsed.post_id,
            )
            existing = (await self._session.execute(existing_stmt)).scalar_one_or_none()
            row = self._post_to_dict(channel_id, parsed)
            if existing is not None:
                if existing.content != row["content"]:
                    existing.content = row["content"]
                    existing.datetime = row["datetime"]
                    existing.views = row["views"]
                    existing.reactions = row["reactions"]
                    existing.author = row["author"]
                    existing.link_preview = row["link_preview"]
                    from src.models.knowledge import EnrichmentStatus, KnowledgeDocument

                    document = (await self._session.execute(select(KnowledgeDocument).where(KnowledgeDocument.post_id == existing.id))).scalar_one_or_none()
                    if document is not None:
                        document.enrichment_status = EnrichmentStatus.STALE
                    changed += 1
                continue
            stmt = insert(Post).values(row)
            await self._session.execute(stmt)
            changed += 1

        await self._session.flush()
        return changed

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
