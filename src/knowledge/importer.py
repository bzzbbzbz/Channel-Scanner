"""Strict parser for official Telegram JSON exports, without storing raw exports in SQL."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any


@dataclass(frozen=True, slots=True)
class ImportedPost:
    telegram_post_id: int
    content: str
    published_at: datetime
    author: str | None


class TelegramExportError(ValueError):
    pass


def parse_official_export(raw: bytes, max_bytes: int) -> list[ImportedPost]:
    if len(raw) > max_bytes:
        raise TelegramExportError("export exceeds configured size limit")
    try:
        payload = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise TelegramExportError("export is not valid JSON") from exc
    if not isinstance(payload, dict) or not isinstance(payload.get("messages"), list):
        raise TelegramExportError("export must contain a messages array")
    posts: list[ImportedPost] = []
    for message in payload["messages"]:
        if not isinstance(message, dict) or message.get("type") != "message":
            continue
        message_id = message.get("id")
        date = message.get("date")
        if not isinstance(message_id, int) or not isinstance(date, str):
            continue
        try:
            published_at = datetime.fromisoformat(date.replace("Z", "+00:00"))
        except ValueError:
            continue
        if published_at.tzinfo is None:
            unix_time = message.get("date_unixtime")
            try:
                published_at = datetime.fromtimestamp(int(unix_time), tz=timezone.utc)
            except (TypeError, ValueError, OSError):
                continue
        content = _text_value(message.get("text"))
        if not content:
            continue
        author = message.get("from") if isinstance(message.get("from"), str) else None
        posts.append(ImportedPost(message_id, content, published_at, author))
    if not posts:
        raise TelegramExportError("export contains no public text messages")
    return posts


def _text_value(value: Any) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        return "".join(_text_value(item) for item in value).strip()
    if isinstance(value, dict):
        inner = value.get("text")
        return _text_value(inner) if inner is not None else ""
    return ""
