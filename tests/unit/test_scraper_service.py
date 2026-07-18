"""Unit coverage for bounded period scraping."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

import pytest

from src.scraper.parser import ParsedPost
from src.scraper.service import ScraperService


@pytest.mark.asyncio
async def test_scrape_channel_period_paginates_until_period_start() -> None:
    client = AsyncMock()
    client.fetch_page.side_effect = [("first", 200), ("second", 200)]
    newer = ParsedPost(3, "ai", "newer", "2026-04-27T10:00:00+00:00")
    selected = ParsedPost(2, "ai", "selected", "2026-04-26T10:30:00+00:00")
    older = ParsedPost(1, "ai", "older", "2026-04-26T09:30:00+00:00")

    with patch(
        "src.scraper.service.parse_page",
        side_effect=[([newer, selected], "?before=2"), ([older], None)],
    ):
        posts = await ScraperService(client).scrape_channel_period(
            "ai",
            datetime(2026, 4, 26, 10, 0, tzinfo=timezone.utc),
            datetime(2026, 4, 26, 11, 0, tzinfo=timezone.utc),
        )

    assert [post.post_id for post in posts] == [2]
    assert client.fetch_page.await_args_list[1].kwargs == {"before": 2}
