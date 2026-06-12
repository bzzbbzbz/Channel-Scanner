"""Unit tests for Telegram-safe formatting helpers."""

from __future__ import annotations

from src.telegram_formatting import telegram_html_format_instructions, telegram_safe_html


def test_telegram_safe_html_converts_markdown_bold_and_escapes_unsupported_tags() -> None:
    text = '**Важно** <span>bad</span> [док](https://example.com?a=1&b=2)'

    rendered = telegram_safe_html(text)

    assert '<b>Важно</b>' in rendered
    assert '&lt;span&gt;bad&lt;/span&gt;' in rendered
    assert '<a href="https://example.com?a=1&amp;b=2">док</a>' in rendered
    assert '**' not in rendered


def test_telegram_html_format_instructions_forbid_markdown_bold() -> None:
    instructions = telegram_html_format_instructions("ru")

    assert "Telegram-safe HTML" in instructions
    assert "**текст**" in instructions
    assert "<b>текст</b>" in instructions
