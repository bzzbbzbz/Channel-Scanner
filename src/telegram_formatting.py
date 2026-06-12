"""Telegram-safe text formatting helpers."""

from __future__ import annotations

import re
from html import escape
from typing import Callable


def telegram_html_format_instructions(language: str) -> str:
    """Return LLM-facing instructions for Telegram HTML parse mode."""
    if language == "ru":
        return (
            "Верни только текст для Telegram c parse mode HTML. "
            "Основная цель: стабильно рендерящийся Telegram-safe HTML, а не MarkdownV2. "
            "Используй обычные переводы строк, не используй <br>. "
            'Ссылки оформляй только как <a href="https://...">текст</a>. '
            "Если нужен список, делай plain-text bullets: каждая строка начинается с символа •. "
            "Используй только простые поддерживаемые теги Telegram: <b>, <i>, <code>, <pre>, <a>. "
            "Не используй Markdown, тройные кавычки, HTML-документ, неподдерживаемые теги или атрибуты. "
            "Не используй Markdown-жирный текст вида **текст**; вместо этого используй <b>текст</b>."
        )
    return (
        "Return only Telegram text for HTML parse mode. "
        "Primary goal: stable Telegram-safe HTML, not MarkdownV2. "
        "Use normal newlines and do not use <br>. "
        'Render links only as <a href="https://...">text</a>. '
        "If you need a list, use plain-text bullets with one line per item starting with •. "
        "Use only simple Telegram-supported tags: <b>, <i>, <code>, <pre>, <a>. "
        "Do not use Markdown, triple backticks, full HTML documents, unsupported tags, or extra attributes. "
        "Do not use Markdown bold like **text**; use <b>text</b> instead."
    )


def telegram_safe_html(text: str) -> str:
    """Convert common Markdown leftovers and escape everything except Telegram-safe HTML tags."""
    text = re.sub(
        r"\[([^\]]+)\]\((https?://[^\s\)]+)\)",
        lambda match: f'<a href="{match.group(2)}">{match.group(1)}</a>',
        text or "",
    )
    text = re.sub(r"(?i)<\s*br\s*/?\s*>", "\n", text)
    text = re.sub(r"(?i)<\s*/?strong\s*>", lambda match: "</b>" if "/" in match.group(0) else "<b>", text)
    text = re.sub(r"(?i)<\s*/?em\s*>", lambda match: "</i>" if "/" in match.group(0) else "<i>", text)
    text = re.sub(r"\*\*(.+?)\*\*", r"<b>\1</b>", text)
    text = re.sub(r"__(.+?)__", r"<b>\1</b>", text)

    placeholders: dict[str, str] = {}

    def protect(pattern: str, value_builder: Callable[[re.Match[str]], str] | str) -> None:
        compiled = re.compile(pattern, re.IGNORECASE)

        def replacer(match: re.Match[str]) -> str:
            token = f"__TG_HTML_{len(placeholders)}__"
            placeholders[token] = value_builder(match) if callable(value_builder) else value_builder
            return token

        nonlocal text
        text = compiled.sub(replacer, text)

    protect(r"</?(?:b|i|code|pre)\s*>", lambda match: match.group(0).lower())
    protect(r"<a\s+href=(['\"])(https?://[^'\"]+)\1\s*>", lambda match: f'<a href="{escape(match.group(2), quote=True)}">')
    protect(r"</a\s*>", "</a>")

    escaped = escape(text)
    for token, value in placeholders.items():
        escaped = escaped.replace(token, value)
    return escaped
