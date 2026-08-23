"""Search real user questions and post links in a Telegram chat export.

Operator-only tool for BL-26 benchmark building. It reads the large chat
export once, extracts candidate messages (questions, t.me/turboproject post
links, GRACE threads) with their reply context, and writes a local JSONL
candidates file. It never writes back into the export and keeps output free
of any secret or telemetry content.
"""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

_POST_LINK = re.compile(r"(?:https?://)?t\.me/turboproject/(\d+)")
_GRACE = re.compile(r"\bGRACE\b")
_WEAK_QUESTION_MARKERS = (
    "?",
    "как вы",
    "что вы",
    "есть ли",
    "можно ли",
    "кто-нибудь",
    "почему",
    "зачем",
    "какой",
    "какая",
    "какие",
    "какое",
    "когда",
    "сколько",
    "что это",
)
_STRONG_QUESTION_MARKERS = (
    "что писал",
    "не могу найти",
    "не могу найти пост",
    "где пост",
    "подскажите",
    "расскажите",
    "помогите",
    "в чем разница",
    "в чём разница",
    "нашёл у вас",
    "нашел у вас",
    "что автор",
    "а что по",
    "это какой пост",
    "какой пост",
    "про какой пост",
    "ссылка на пост",
    "дай ссылку",
    "скинь ссылку",
    "не нашёл",
    "не нашел",
    "искал",
    "напомните",
    "кто знает",
    "кто-нибудь знает",
    "интересно, а",
    "вопрос по",
)
_STRONG_WORD_QUESTION = re.compile(
    r"(что писал|не могу найти|не на(й|ш)ёл|где (пост|найти)|подскажите|расскажите|помогите|как найти|в чём разница|в чем разница|какой пост|это какой пост|кто знает|кто-нибудь знает|напомните|дай ссылку|скинь ссылку|про какой пост|ссылка на пост)"
)


def _is_question(text: str) -> bool:
    lowered = text.lower()
    if any(marker in lowered for marker in _STRONG_QUESTION_MARKERS):
        return True
    if "?" in lowered and any(marker in lowered for marker in _WEAK_QUESTION_MARKERS):
        return True
    return False


def _flat_text(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        parts: list[str] = []
        for item in value:
            if isinstance(item, str):
                parts.append(item)
            elif isinstance(item, dict):
                parts.append(item.get("text") or "")
        return "".join(parts)
    return str(value)


def _extract_links(value: object) -> tuple[list[str], list[str]]:
    """Return (raw_urls, post_ids) found in plain text and entities."""
    raw: list[str] = []
    text = _flat_text(value)
    for match in re.finditer(r"https?://\S+|t\.me/[\w/]+", text):
        url = match.group(0).rstrip(".,;:)»»")
        if url not in raw:
            raw.append(url)
    if isinstance(value, list):
        for item in value:
            if isinstance(item, dict):
                url = item.get("href") or item.get("text")
                if isinstance(url, str) and (url.startswith("http") or url.startswith("t.me/")):
                    url = url.rstrip(".,;:)»»")
                    if url not in raw:
                        raw.append(url)
    post_ids: list[str] = []
    for url in raw:
        match = _POST_LINK.search(url)
        if match and match.group(1) not in post_ids:
            post_ids.append(match.group(1))
    return raw, post_ids


def _is_grace(text: str) -> bool:
    return bool(_GRACE.search(text))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("export", type=Path, help="Telegram JSON chat export")
    parser.add_argument("--output", type=Path, default=Path(".planning/evaluations/v2-candidates.jsonl"))
    parser.add_argument("--max-text", type=int, default=1200, help="Maximum candidate text characters")
    parser.add_argument("--max-replies", type=int, default=6, help="Maximum reply context entries per candidate")
    args = parser.parse_args()

    messages = json.loads(args.export.read_text(encoding="utf-8"))["messages"]
    children: dict[int, list[dict]] = {}
    for message in messages:
        if not isinstance(message, dict):
            continue
        message_id = message.get("id")
        if isinstance(message_id, int):
            parent = message.get("reply_to_message_id")
            if isinstance(parent, int):
                children.setdefault(parent, []).append(message)

    def subtree_posts(message_id: int) -> list[str]:
        """Collect turboproject post ids reachable from direct replies."""
        result: list[str] = []
        for reply in children.get(message_id, []):
            _, reply_post_ids = _extract_links(reply.get("text_entities") or reply.get("text"))
            for post_id in reply_post_ids:
                if post_id not in result:
                    result.append(post_id)
        return result

    def reply_rows(message_id: int) -> list[dict]:
        rows: list[dict] = []
        for reply in children.get(message_id, [])[: args.max_replies]:
            reply_text = _flat_text(reply.get("text"))
            reply_raw, reply_post_ids = _extract_links(reply.get("text_entities") or reply.get("text"))
            rows.append(
                {
                    "id": reply.get("id"),
                    "date": reply.get("date"),
                    "from": reply.get("from"),
                    "text": reply_text[: args.max_text],
                    "post_links": [f"https://t.me/turboproject/{pid}" for pid in reply_post_ids],
                }
            )
        return rows

    candidates: list[dict] = []
    seen: set[int] = set()
    for message in messages:
        if not isinstance(message, dict):
            continue
        message_id = message.get("id")
        if not isinstance(message_id, int) or message_id in seen:
            continue
        text = _flat_text(message.get("text"))
        raw_links, post_ids = _extract_links(message.get("text_entities") or message.get("text"))
        question = _is_question(text)
        grace = _is_grace(text)
        repo_links = [url for url in raw_links if "github.com" in url or "gitlab" in url or "huggingface" in url]
        answer_post_ids = subtree_posts(message_id)

        keep = False
        reason: str | None = None
        if post_ids:
            keep = True
            reason = "posts" + ("+grace" if grace else "+question" if question else "")
        elif grace and (repo_links or answer_post_ids):
            keep = True
            reason = "grace_links"
        elif question and answer_post_ids:
            keep = True
            reason = "question_with_post_reply"

        if not keep:
            continue
        seen.add(message_id)

        candidates.append(
            {
                "id": message_id,
                "date": message.get("date"),
                "from": message.get("from"),
                "from_id": message.get("from_id"),
                "text": text[: args.max_text],
                "post_links": [f"https://t.me/turboproject/{pid}" for pid in post_ids],
                "answer_post_links": [f"https://t.me/turboproject/{pid}" for pid in answer_post_ids],
                "repo_links": repo_links,
                "other_links": [url for url in raw_links if "turboproject" not in url and "github.com" not in url and "gitlab" not in url and "huggingface" not in url],
                "is_question": question,
                "grace": grace,
                "reason": reason,
                "replies": reply_rows(message_id),
            }
        )

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", encoding="utf-8") as handle:
        for candidate in candidates:
            handle.write(json.dumps(candidate, ensure_ascii=False) + "\n")
    print(f"wrote {len(candidates)} candidates to {args.output}")


if __name__ == "__main__":
    main()
