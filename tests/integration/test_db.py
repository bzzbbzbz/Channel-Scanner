"""Integration tests for the repository / DB layer.

Uses in-memory SQLite via async fixtures from conftest.py.
Tests verify idempotent operations (ON CONFLICT DO NOTHING) and
channel lifecycle management.
"""

from datetime import datetime, timezone

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from src.models.channel import Channel, ChannelStatus
from src.repository.channel import ChannelRepository
from src.repository.post import PostRepository
from src.scraper.parser import ParsedPost


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_post(
    post_id: int = 1,
    channel_username: str = "testchannel",
    content: str = "Hello world",
    dt: str = "2026-01-15T12:00:00+00:00",
    views: int | None = None,
) -> ParsedPost:
    """Create a ParsedPost for testing."""
    return ParsedPost(
        post_id=post_id,
        channel_username=channel_username,
        content=content,
        datetime=dt,
        views=views,
    )


async def _seed_channel(session: AsyncSession, telegram_id: int = 12345) -> Channel:
    """Insert a channel directly and return it."""
    channel = Channel(
        telegram_id=telegram_id,
        username="testchannel",
        name="Test Channel",
        status=ChannelStatus.ACTIVE,
    )
    session.add(channel)
    await session.flush()
    return channel


# ---------------------------------------------------------------------------
# ChannelRepository tests
# ---------------------------------------------------------------------------


class TestChannelRepository:
    """Tests for ChannelRepository."""

    async def test_upsert_channel_creates_new(self, session: AsyncSession) -> None:
        """upsert_channel inserts a new channel when telegram_id is new."""
        repo = ChannelRepository(session)
        channel = await repo.upsert_channel(
            telegram_id=11111,
            username="mychannel",
            name="My Channel",
        )
        await session.commit()

        assert channel.id is not None
        assert channel.telegram_id == 11111
        assert channel.username == "mychannel"
        assert channel.name == "My Channel"
        assert channel.status == ChannelStatus.ACTIVE

    async def test_upsert_channel_updates_existing_on_conflict(
        self, session: AsyncSession
    ) -> None:
        """upsert_channel updates username/name when telegram_id already exists."""
        repo = ChannelRepository(session)

        # First insert
        ch1 = await repo.upsert_channel(22222, "old_name", "Old Display")
        await session.commit()

        # Upsert with same telegram_id
        ch2 = await repo.upsert_channel(22222, "new_name", "New Display")
        await session.commit()

        assert ch1.id == ch2.id
        assert ch2.username == "new_name"
        assert ch2.name == "New Display"

    async def test_get_active_channels_returns_only_active(
        self, session: AsyncSession
    ) -> None:
        """get_active_channels returns only channels with status='active'."""
        repo = ChannelRepository(session)

        # Seed: one active, one error
        active_ch = Channel(
            telegram_id=100, username="active_ch", name="Active",
            status=ChannelStatus.ACTIVE,
        )
        error_ch = Channel(
            telegram_id=200, username="error_ch", name="Error",
            status=ChannelStatus.ERROR,
        )
        session.add_all([active_ch, error_ch])
        await session.flush()
        await session.commit()

        channels = await repo.get_active_channels()
        assert len(channels) == 1
        assert channels[0].telegram_id == 100

    async def test_mark_error_sets_status_to_error(
        self, session: AsyncSession
    ) -> None:
        """mark_error changes channel status to 'error' and sets last_error."""
        repo = ChannelRepository(session)
        channel = await _seed_channel(session)
        await session.commit()

        await repo.mark_error(channel.id, "Channel not found or private")
        await session.commit()

        # Re-fetch
        refreshed = await repo.get_by_telegram_id(12345)
        assert refreshed is not None
        assert refreshed.status == ChannelStatus.ERROR
        assert refreshed.last_error == "Channel not found or private"

    async def test_mark_scraped_updates_last_scraped(
        self, session: AsyncSession
    ) -> None:
        """mark_scraped updates the last_scraped timestamp."""
        repo = ChannelRepository(session)
        channel = await _seed_channel(session)
        await session.commit()

        assert channel.last_scraped is None

        await repo.mark_scraped(channel.id)
        await session.commit()

        refreshed = await repo.get_by_telegram_id(12345)
        assert refreshed is not None
        assert refreshed.last_scraped is not None

    async def test_mark_active_resets_error_state(
        self, session: AsyncSession
    ) -> None:
        """mark_active clears error status and last_error."""
        repo = ChannelRepository(session)
        channel = await _seed_channel(session)
        await session.commit()

        await repo.mark_error(channel.id, "Some error")
        await session.commit()

        await repo.mark_active(channel.id)
        await session.commit()

        refreshed = await repo.get_by_telegram_id(12345)
        assert refreshed is not None
        assert refreshed.status == ChannelStatus.ACTIVE
        assert refreshed.last_error is None


# ---------------------------------------------------------------------------
# PostRepository tests
# ---------------------------------------------------------------------------


class TestPostRepository:
    """Tests for PostRepository — deduplication is the key invariant."""

    async def test_upsert_posts_inserts_new_posts_and_returns_count(
        self, session: AsyncSession
    ) -> None:
        """upsert_posts inserts new posts and returns the insertion count."""
        channel = await _seed_channel(session)
        await session.commit()

        repo = PostRepository(session)
        posts = [
            _make_post(post_id=1, content="First post"),
            _make_post(post_id=2, content="Second post"),
        ]

        count = await repo.upsert_posts(channel.id, posts)
        await session.commit()

        assert count == 2

        total = await repo.count_posts(channel.id)
        assert total == 2

    async def test_upsert_posts_zero_duplicates_on_second_call(
        self, session: AsyncSession
    ) -> None:
        """Re-scraping the same channel produces zero duplicate posts.

        This is the core invariant: ON CONFLICT DO NOTHING ensures
        idempotent scraping.
        """
        channel = await _seed_channel(session)
        await session.commit()

        repo = PostRepository(session)
        posts = [
            _make_post(post_id=10, content="Post A"),
            _make_post(post_id=20, content="Post B"),
            _make_post(post_id=30, content="Post C"),
        ]

        # First insert
        first_count = await repo.upsert_posts(channel.id, posts)
        await session.commit()
        assert first_count == 3

        # Second insert — same posts
        second_count = await repo.upsert_posts(channel.id, posts)
        await session.commit()
        assert second_count == 0

        # Total must still be 3
        total = await repo.count_posts(channel.id)
        assert total == 3

    async def test_upsert_posts_mixed_new_and_duplicate(
        self, session: AsyncSession
    ) -> None:
        """Inserting a mix of new and duplicate posts only inserts the new ones."""
        channel = await _seed_channel(session)
        await session.commit()

        repo = PostRepository(session)

        # First batch
        batch1 = [_make_post(post_id=1), _make_post(post_id=2)]
        count1 = await repo.upsert_posts(channel.id, batch1)
        await session.commit()
        assert count1 == 2

        # Second batch — post_id=2 is duplicate, post_id=3 is new
        batch2 = [_make_post(post_id=2, content="Updated"), _make_post(post_id=3)]
        count2 = await repo.upsert_posts(channel.id, batch2)
        await session.commit()
        assert count2 == 1

        total = await repo.count_posts(channel.id)
        assert total == 3

    async def test_get_posts_by_channel_returns_ordered(
        self, session: AsyncSession
    ) -> None:
        """get_posts_by_channel returns posts newest-first."""
        channel = await _seed_channel(session)
        await session.commit()

        repo = PostRepository(session)
        posts = [
            _make_post(post_id=1, dt="2026-01-10T10:00:00+00:00"),
            _make_post(post_id=2, dt="2026-01-15T10:00:00+00:00"),
            _make_post(post_id=3, dt="2026-01-12T10:00:00+00:00"),
        ]
        await repo.upsert_posts(channel.id, posts)
        await session.commit()

        result = await repo.get_posts_by_channel(channel.id, limit=10)
        assert len(result) == 3
        # Newest first
        assert result[0].post_id == 2
        assert result[1].post_id == 3
        assert result[2].post_id == 1

    async def test_count_posts_returns_zero_for_empty_channel(
        self, session: AsyncSession
    ) -> None:
        """count_posts returns 0 for a channel with no posts."""
        channel = await _seed_channel(session)
        await session.commit()

        repo = PostRepository(session)
        assert await repo.count_posts(channel.id) == 0

    async def test_upsert_posts_empty_list_returns_zero(
        self, session: AsyncSession
    ) -> None:
        """upsert_posts handles an empty list gracefully."""
        channel = await _seed_channel(session)
        await session.commit()

        repo = PostRepository(session)
        count = await repo.upsert_posts(channel.id, [])
        assert count == 0
