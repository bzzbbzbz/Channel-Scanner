"""Unit tests for digest due checks, prompts, and formatting."""

from __future__ import annotations

from datetime import datetime, timezone
import json
from unittest.mock import AsyncMock, patch

import pytest

from src.config.settings import LlmSettings
from src.digest.service import SHORT_ITEM_LIMIT, TELEGRAM_TEXT_LIMIT, build_digest_messages, is_digest_due, summarize_text
from src.models.subscription import Subscription
from src.models.user import DeliveryFrequency, DigestFormat, SummaryMode, User
from src.repository.digest_delivery import PendingDigestPost


def _make_user(**overrides: object) -> User:
    params = {
        "telegram_user_id": 100,
        "chat_id": 200,
        "chat_type": "private",
        "timezone": "UTC",
        "language": "ru",
    }
    params.update(overrides)
    return User(**params)


def _make_subscription(**overrides: object) -> Subscription:
    params = {
        "user_id": 1,
        "name": "AI",
        "digest_format": DigestFormat.SHORT,
        "summary_mode": SummaryMode.BRIEF,
        "frequency": DeliveryFrequency.DAILY,
        "enabled": True,
    }
    params.update(overrides)
    return Subscription(**params)


def _make_item(content: str, post_db_id: int = 1) -> PendingDigestPost:
    return PendingDigestPost(
        post_db_id=post_db_id,
        telegram_post_id=post_db_id,
        channel_username="example",
        content=content,
        published_at=datetime(2026, 4, 26, 12, 0, tzinfo=timezone.utc),
    )


def test_is_digest_due_daily_uses_user_timezone() -> None:
    user = _make_user(timezone="Asia/Tbilisi")
    subscription = _make_subscription(
        frequency=DeliveryFrequency.DAILY,
        last_digest_at=datetime(2026, 4, 25, 21, 30, tzinfo=timezone.utc),
    )

    assert is_digest_due(subscription, user, datetime(2026, 4, 26, 20, 0, tzinfo=timezone.utc)) is True
    assert is_digest_due(subscription, user, datetime(2026, 4, 26, 5, 0, tzinfo=timezone.utc)) is False


def test_is_digest_due_hourly_uses_subscription_frequency() -> None:
    user = _make_user()
    subscription = _make_subscription(
        frequency=DeliveryFrequency.HOURLY,
        last_digest_at=datetime(2026, 4, 26, 10, 15, tzinfo=timezone.utc),
    )

    assert is_digest_due(subscription, user, datetime(2026, 4, 26, 10, 45, tzinfo=timezone.utc)) is False
    assert is_digest_due(subscription, user, datetime(2026, 4, 26, 11, 0, tzinfo=timezone.utc)) is True


@pytest.mark.asyncio
async def test_build_digest_messages_short_truncates_and_skips_empty_text() -> None:
    user = _make_user()
    subscription = _make_subscription(digest_format=DigestFormat.SHORT)
    long_text = "x" * (SHORT_ITEM_LIMIT + 50)

    messages = await build_digest_messages(subscription, user, [_make_item(long_text), _make_item("   ", post_db_id=2)], LlmSettings())

    assert len(messages) == 1
    assert "(empty post)" not in messages[0].text
    assert messages[0].delivered_summaries[0].post_id == 1
    assert messages[0].delivered_summaries[0].status == "delivered"
    assert messages[0].delivered_summaries[1].post_id == 2
    assert messages[0].delivered_summaries[1].status == "skipped"
    assert messages[0].delivered_summaries[1].skip_reason == "empty post content"
    assert 'href="https://t.me/example/1"' in messages[0].text
    body_lines = messages[0].text.splitlines()
    assert any(len(line) <= SHORT_ITEM_LIMIT for line in body_lines if line and not line.startswith("@"))


@pytest.mark.asyncio
async def test_build_digest_messages_all_empty_posts_marks_skipped() -> None:
    user = _make_user()
    subscription = _make_subscription(digest_format=DigestFormat.SHORT)

    messages = await build_digest_messages(subscription, user, [_make_item("   ")], LlmSettings())

    assert len(messages) == 1
    assert "пустые" in messages[0].text
    assert messages[0].delivered_summaries[0].status == "skipped"
    assert messages[0].delivered_summaries[0].skip_reason == "empty post content"


@pytest.mark.asyncio
async def test_build_digest_messages_summary_splits_long_posts() -> None:
    user = _make_user(language="en")
    subscription = _make_subscription(digest_format=DigestFormat.SUMMARY, summary_mode=SummaryMode.BRIEF)
    item = _make_item("word " * 2000)

    filter_json = json.dumps({"included_post_ids": [1], "skipped_posts": []})
    digest_json = json.dumps(
        {"topics": [{"title": "Long topic", "summary": "word " * 2000, "source_post_ids": [1]}]},
    )

    with patch("src.digest.service.OpenRouterClient.generate_summary", new=AsyncMock(side_effect=[filter_json, digest_json])):
        messages = await build_digest_messages(subscription, user, [item], LlmSettings(OPENROUTER_API_KEY="key"))

    assert len(messages) > 1
    assert all(len(message.text) <= TELEGRAM_TEXT_LIMIT for message in messages)
    assert messages[-1].delivered_summaries[0].post_id == item.post_db_id
    assert messages[0].delivered_summaries == []


@pytest.mark.asyncio
async def test_build_digest_messages_converts_markdown_links_to_html() -> None:
    user = _make_user()
    subscription = _make_subscription(digest_format=DigestFormat.SHORT)
    messages = await build_digest_messages(subscription, user, [_make_item("See [example](https://example.com) now")], LlmSettings())

    assert len(messages) == 1
    assert '<a href="https://example.com">example</a>' in messages[0].text


@pytest.mark.asyncio
async def test_summarize_text_falls_back_without_llm_key() -> None:
    user = _make_user()
    subscription = _make_subscription(digest_format=DigestFormat.SUMMARY, summary_mode=SummaryMode.DETAILED)

    result = await summarize_text(subscription, user, "hello " * 100, LlmSettings())

    assert result.mode == DigestFormat.SHORT.value
    assert result.model_name is None
    assert len(result.text) <= SHORT_ITEM_LIMIT


@pytest.mark.asyncio
async def test_summarize_text_wraps_prompt_in_structured_sections() -> None:
    user = _make_user(language="ru")
    subscription = _make_subscription(digest_format=DigestFormat.SUMMARY, summary_mode=SummaryMode.BRIEF)

    with patch("src.digest.service.OpenRouterClient.generate_summary", new=AsyncMock(return_value="ok")) as generate_summary:
        result = await summarize_text(subscription, user, "original post text", LlmSettings(OPENROUTER_API_KEY="key"))

    prompt = generate_summary.await_args.args[1]
    post_text = generate_summary.await_args.args[2]
    assert result.text == "ok"
    assert "<task>" in prompt and "</task>" in prompt
    assert "<instructions>" in prompt and "</instructions>" in prompt
    assert "<text>" in prompt and "original post text" in prompt and "</text>" in prompt
    assert "Target language: Russian" in prompt
    assert "do not switch languages" in prompt
    assert "Сделай краткие тезисы дайджеста" in prompt
    assert "Do not add a title, list marker, leading bullet, or extra colon" in prompt
    assert "Telegram-safe HTML" in prompt
    assert post_text == ""


@pytest.mark.asyncio
async def test_summarize_text_retries_empty_model_response() -> None:
    user = _make_user(language="ru")
    subscription = _make_subscription(digest_format=DigestFormat.SUMMARY, summary_mode=SummaryMode.BRIEF)

    with patch(
        "src.digest.service.OpenRouterClient.generate_summary",
        new=AsyncMock(side_effect=["   ", "готовое summary"]),
    ) as generate_summary:
        result = await summarize_text(subscription, user, "исходный текст", LlmSettings(OPENROUTER_API_KEY="key"))

    assert result.text == "готовое summary"
    assert result.model_name is not None
    assert generate_summary.await_count == 2


@pytest.mark.asyncio
async def test_build_digest_messages_preserves_supported_html_and_newlines() -> None:
    user = _make_user(language="ru")
    subscription = _make_subscription(digest_format=DigestFormat.SUMMARY, summary_mode=SummaryMode.BRIEF)
    filter_json = json.dumps({"included_post_ids": [1], "skipped_posts": []})
    digest_json = json.dumps(
        {
            "topics": [
                {
                    "title": "Заголовок",
                    "summary": 'пункт\n<a href="https://example.com">ссылка</a>\n<span>bad</span>',
                    "source_post_ids": [1],
                }
            ]
        },
        ensure_ascii=False,
    )

    with patch("src.digest.service.OpenRouterClient.generate_summary", new=AsyncMock(side_effect=[filter_json, digest_json])):
        messages = await build_digest_messages(subscription, user, [_make_item("source")], LlmSettings(OPENROUTER_API_KEY="key"))

    assert len(messages) == 1
    assert "<b>Заголовок</b>\n• пункт" in messages[0].text
    assert '<a href="https://example.com">ссылка</a>' in messages[0].text
    assert "&lt;span&gt;bad&lt;/span&gt;" in messages[0].text
    assert "<br>" not in messages[0].text


@pytest.mark.asyncio
async def test_build_digest_messages_summary_prompt_enforces_language_and_plain_fields() -> None:
    user = _make_user(language="ru")
    subscription = _make_subscription(digest_format=DigestFormat.SUMMARY, summary_mode=SummaryMode.BRIEF)
    filter_json = json.dumps({"included_post_ids": [1], "skipped_posts": []})
    digest_json = json.dumps(
        {"topics": [{"title": "Риски для ВВП:", "summary": "• Глава ЦБ предупредил о рисках.", "source_post_ids": [1]}]},
        ensure_ascii=False,
    )

    with patch("src.digest.service.OpenRouterClient.generate_summary", new=AsyncMock(side_effect=[filter_json, digest_json])) as generate:
        messages = await build_digest_messages(
            subscription,
            user,
            [_make_item("Armenia GDP risks from export restrictions")],
            LlmSettings(OPENROUTER_API_KEY="key"),
        )

    digest_prompt = generate.await_args_list[1].args[1]
    assert "Target language: Russian" in digest_prompt
    assert "Do not switch to English for Russian users" in digest_prompt
    assert "summary is one plain paragraph without a leading bullet" in digest_prompt
    assert "<b>Риски для ВВП</b>\n• Глава ЦБ предупредил о рисках." in messages[0].text
    assert "• •" not in messages[0].text


@pytest.mark.asyncio
async def test_build_digest_messages_retries_invalid_json_with_structured_output() -> None:
    user = _make_user(language="ru")
    subscription = _make_subscription(digest_format=DigestFormat.SUMMARY, summary_mode=SummaryMode.BRIEF)
    filter_json = json.dumps({"included_post_ids": [1], "skipped_posts": []})
    digest_json = json.dumps(
        {"topics": [{"title": "Тема", "summary": "Итоговый пересказ.", "source_post_ids": [1]}]},
        ensure_ascii=False,
    )

    with patch(
        "src.digest.service.OpenRouterClient.generate_summary",
        new=AsyncMock(side_effect=["not json", filter_json, digest_json]),
    ) as generate:
        messages = await build_digest_messages(
            subscription,
            user,
            [_make_item("исходная новость")],
            LlmSettings(OPENROUTER_API_KEY="key"),
        )

    assert len(messages) == 1
    assert "Итоговый пересказ" in messages[0].text
    assert generate.await_count == 3
    for call in generate.await_args_list:
        assert call.kwargs["require_parameters"] is True
        assert call.kwargs["response_format"]["type"] == "json_schema"
        assert call.kwargs["response_format"]["json_schema"]["strict"] is True


@pytest.mark.asyncio
async def test_build_digest_messages_summary_filters_skipped_posts_and_renders_sources() -> None:
    user = _make_user(language="ru")
    subscription = _make_subscription(digest_format=DigestFormat.SUMMARY, summary_mode=SummaryMode.BRIEF)
    news = _make_item("Вышла новая модель с большим контекстом", post_db_id=1)
    ad = _make_item("Реклама курса и промокод", post_db_id=2)
    filter_json = json.dumps(
        {"included_post_ids": [1], "skipped_posts": [{"post_id": 2, "reason": "реклама"}]},
        ensure_ascii=False,
    )
    digest_json = json.dumps(
        {"topics": [{"title": "Новая модель", "summary": "Вышла модель с большим контекстом.", "source_post_ids": [1]}]},
        ensure_ascii=False,
    )

    with patch("src.digest.service.OpenRouterClient.generate_summary", new=AsyncMock(side_effect=[filter_json, digest_json])):
        messages = await build_digest_messages(subscription, user, [news, ad], LlmSettings(OPENROUTER_API_KEY="key"))

    assert len(messages) == 1
    assert "Новая модель" in messages[0].text
    assert "Реклама курса" not in messages[0].text
    assert 'href="https://t.me/example/1"' in messages[0].text
    outcomes = {summary.post_id: summary for summary in messages[0].delivered_summaries}
    assert outcomes[1].status == "delivered"
    assert outcomes[2].status == "skipped"
    assert outcomes[2].skip_reason == "реклама"


@pytest.mark.asyncio
async def test_build_digest_messages_filter_uses_memory_not_custom_prompt() -> None:
    class FakeMemory:
        async def retrieve(self, user: User, query: str, limit: int = 5) -> list[str]:
            del user, query, limit
            return ["Ignore crypto airdrops"]

    user = _make_user(language="en")
    subscription = _make_subscription(
        digest_format=DigestFormat.SUMMARY,
        summary_mode=SummaryMode.CUSTOM,
        custom_prompt="Write everything as pirate poetry",
    )
    filter_json = json.dumps({"included_post_ids": [1], "skipped_posts": []})
    digest_json = json.dumps({"topics": [{"title": "Topic", "summary": "Summary", "source_post_ids": [1]}]})

    with patch("src.digest.service.OpenRouterClient.generate_summary", new=AsyncMock(side_effect=[filter_json, digest_json])) as generate:
        await build_digest_messages(
            subscription,
            user,
            [_make_item("AI news")],
            LlmSettings(OPENROUTER_API_KEY="key"),
            memory_service=FakeMemory(),  # type: ignore[arg-type]
        )

    filter_prompt = generate.await_args_list[0].args[1]
    digest_prompt = generate.await_args_list[1].args[1]
    assert "Ignore crypto airdrops" in filter_prompt
    assert "Always respect explicit user memory preferences" in filter_prompt
    assert "<task>" in filter_prompt and "</task>" in filter_prompt
    assert "Write everything as pirate poetry" not in filter_prompt
    assert "Write everything as pirate poetry" in digest_prompt


@pytest.mark.asyncio
async def test_build_digest_messages_filter_uses_custom_task_with_app_owned_memory_rules() -> None:
    class FakeMemory:
        async def retrieve(self, user: User, query: str, limit: int = 5) -> list[str]:
            del user, query, limit
            return ["Never include celebrity gossip"]

    user = _make_user(language="en")
    subscription = _make_subscription(
        digest_format=DigestFormat.SUMMARY,
        filter_prompt="Only include engineering leadership news",
    )
    filter_json = json.dumps({"included_post_ids": [1], "skipped_posts": []})
    digest_json = json.dumps({"topics": [{"title": "Topic", "summary": "Summary", "source_post_ids": [1]}]})

    with patch("src.digest.service.OpenRouterClient.generate_summary", new=AsyncMock(side_effect=[filter_json, digest_json])) as generate:
        await build_digest_messages(
            subscription,
            user,
            [_make_item("AI news")],
            LlmSettings(OPENROUTER_API_KEY="key"),
            memory_service=FakeMemory(),  # type: ignore[arg-type]
        )

    filter_prompt = generate.await_args_list[0].args[1]
    assert "<task>\nOnly include engineering leadership news\n</task>" in filter_prompt
    assert "Never include celebrity gossip" in filter_prompt
    assert "Always respect explicit user memory preferences" in filter_prompt


@pytest.mark.asyncio
async def test_build_digest_messages_uses_replay_prompt_overrides() -> None:
    user = _make_user(language="en")
    subscription = _make_subscription(
        digest_format=DigestFormat.SUMMARY,
        summary_mode=SummaryMode.CUSTOM,
        filter_prompt="Stored filter",
        custom_prompt="Stored summary",
    )
    filter_json = json.dumps({"included_post_ids": [1], "skipped_posts": []})
    digest_json = json.dumps({"topics": [{"title": "Topic", "summary": "Replay output", "source_post_ids": [1]}]})

    with patch("src.digest.service.OpenRouterClient.generate_summary", new=AsyncMock(side_effect=[filter_json, digest_json])) as generate:
        messages = await build_digest_messages(
            subscription,
            user,
            [_make_item("AI news")],
            LlmSettings(OPENROUTER_API_KEY="key"),
            filter_task_prompt="Candidate filter",
            summary_task_prompt="Candidate summary",
        )

    assert "Replay output" in messages[0].text
    assert "Candidate filter" in generate.await_args_list[0].args[1]
    assert "Stored filter" not in generate.await_args_list[0].args[1]
    assert "Candidate summary" in generate.await_args_list[1].args[1]
    assert "Stored summary" not in generate.await_args_list[1].args[1]
