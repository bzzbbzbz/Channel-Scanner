"""Parent-level hybrid ranking and Telegram-safe source rendering."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from html import escape
from typing import Iterable

from src.knowledge.chunking import estimate_tokens
from src.knowledge.indexer import VectorHit
from src.models.channel import Channel
from src.models.post import Post


@dataclass(frozen=True, slots=True)
class RankedPost:
    post_id: int
    score: float
    matched_type: str | None = None
    matched_ordinal: int | None = None


@dataclass(frozen=True, slots=True)
class SourceContext:
    post: Post
    channel: Channel
    text: str
    matched_type: str | None


def collapse_vector_hits(hits: Iterable[VectorHit]) -> list[RankedPost]:
    """One parent gets one rank; extra representations provide only a capped boost."""
    grouped: dict[int, list[VectorHit]] = defaultdict(list)
    for hit in hits:
        grouped[hit.post_id].append(hit)
    collapsed: list[RankedPost] = []
    for post_id, values in grouped.items():
        values.sort(key=lambda item: item.score, reverse=True)
        primary = values[0]
        score = primary.score + min(0.05, 0.01 * (len(values) - 1))
        collapsed.append(RankedPost(post_id, score, primary.representation_type, primary.ordinal))
    return sorted(collapsed, key=lambda item: item.score, reverse=True)


def reciprocal_rank_fusion(lexical_posts: Iterable[Post], vector_posts: Iterable[RankedPost], *, k: int = 60) -> list[RankedPost]:
    scores: dict[int, float] = defaultdict(float)
    vector_metadata: dict[int, RankedPost] = {}
    for rank, post in enumerate(lexical_posts, start=1):
        scores[post.id] += 1 / (k + rank)
    for rank, post in enumerate(vector_posts, start=1):
        scores[post.post_id] += 1 / (k + rank)
        vector_metadata[post.post_id] = post
    return sorted(
        (RankedPost(post_id, score, vector_metadata.get(post_id).matched_type if post_id in vector_metadata else None, vector_metadata.get(post_id).matched_ordinal if post_id in vector_metadata else None) for post_id, score in scores.items()),
        key=lambda item: item.score,
        reverse=True,
    )


def build_context(post: Post, channel: Channel, *, matched_type: str | None, matched_ordinal: int | None, chunks, parent_context_limit: int, neighbor_expansion: int) -> SourceContext:
    if matched_type != "chunk" or estimate_tokens(post.content) <= parent_context_limit:
        return SourceContext(post, channel, post.content, matched_type)
    matching = next((chunk for chunk in chunks if chunk.ordinal == matched_ordinal), None)
    if matching is None:
        return SourceContext(post, channel, post.content[: max(1, parent_context_limit * 5)], matched_type)
    siblings = [chunk for chunk in chunks if chunk.ordinal is not None and abs(chunk.ordinal - matching.ordinal) <= neighbor_expansion]
    text = "\n\n".join(chunk.text for chunk in siblings)
    return SourceContext(post, channel, text, matched_type)


def permalink(channel: Channel, post: Post) -> str | None:
    return f"https://t.me/{channel.username}/{post.post_id}" if channel.username else None


def render_grounded_answer(language: str, answer: str, sources: list[SourceContext], *, mode: str, synced_at: str | None = None, conflict: bool = False) -> str:
    mode_label = "глубокий поиск" if mode == "deep" else ("смешанный поиск" if mode == "mixed" else "обычный поиск")
    if language != "ru":
        mode_label = "deep search" if mode == "deep" else ("mixed search" if mode == "mixed" else "normal search")
    lines = [f"<b>{escape(mode_label)}</b>", escape(answer)]
    if conflict:
        lines.append("В выбранных источниках есть датированные противоречивые утверждения." if language == "ru" else "The selected sources contain dated conflicting claims.")
    if synced_at:
        lines.append(("Последняя синхронизация: " if language == "ru" else "Last synchronization: ") + escape(synced_at))
    for source in sources[:5]:
        link = permalink(source.channel, source.post)
        if link:
            date = source.post.datetime.date().isoformat()
            lines.append(f"<a href=\"{escape(link, quote=True)}\">{escape('@' + (source.channel.username or 'channel'))}, {date}</a>")
    return "\n\n".join(lines)
