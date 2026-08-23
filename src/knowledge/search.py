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


def merge_vector_query_results(hit_sets: list[list[VectorHit]], *, facet_boost: float = 0.15) -> list[RankedPost]:
    """Fuse a primary vector query with explicit fixed retrieval facets."""
    merged: dict[int, RankedPost] = {}
    for index, hits in enumerate(hit_sets):
        for item in collapse_vector_hits(hits):
            candidate = RankedPost(item.post_id, item.score + (facet_boost if index else 0), item.matched_type, item.matched_ordinal)
            if item.post_id not in merged or candidate.score > merged[item.post_id].score:
                merged[item.post_id] = candidate
    return sorted(merged.values(), key=lambda item: item.score, reverse=True)


def reciprocal_rank_fusion(
    lexical_posts: Iterable[Post],
    vector_posts: Iterable[RankedPost],
    *,
    additional_vector_lists: Iterable[Iterable[RankedPost]] = (),
    k: int = 60,
) -> list[RankedPost]:
    scores: dict[int, float] = defaultdict(float)
    vector_metadata: dict[int, RankedPost] = {}
    for rank, post in enumerate(lexical_posts, start=1):
        scores[post.id] += 1 / (k + rank)
    for ranked_posts in (vector_posts, *additional_vector_lists):
        for rank, post in enumerate(ranked_posts, start=1):
            scores[post.post_id] += 1 / (k + rank)
            vector_metadata.setdefault(post.post_id, post)
    return sorted(
        (RankedPost(post_id, score, vector_metadata.get(post_id).matched_type if post_id in vector_metadata else None, vector_metadata.get(post_id).matched_ordinal if post_id in vector_metadata else None) for post_id, score in scores.items()),
        key=lambda item: item.score,
        reverse=True,
    )


def promote_ranked_posts(ranked: Iterable[RankedPost], promoted: Iterable[RankedPost]) -> list[RankedPost]:
    """Keep explicit fixed-facet evidence in the rerank candidate window."""
    promoted_by_id: dict[int, RankedPost] = {}
    for item in promoted:
        promoted_by_id.setdefault(item.post_id, item)
    return list(promoted_by_id.values()) + [item for item in ranked if item.post_id not in promoted_by_id]


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


def render_grounded_answer(language: str, claims, sources: list[SourceContext], *, mode: str, synced_at: str | None = None, conflict: bool = False, evidence_sufficient: bool = True) -> str:
    # Candidate modes preserve the retrieval family in their prefix (for example,
    # ``deep_rerank``).  The user-facing label must not downgrade that to normal.
    mode_label = "глубокий поиск" if mode.startswith("deep") else ("смешанный поиск" if mode.startswith("mixed") else "обычный поиск")
    if language != "ru":
        mode_label = "deep search" if mode.startswith("deep") else ("mixed search" if mode.startswith("mixed") else "normal search")
    sources_by_id = {source.post.id: source for source in sources}
    link_numbers: dict[int, int] = {}
    lines = [f"<b>{escape(mode_label)}</b>"]
    # Kept for older non-product callers that only exercise the mode label.
    if isinstance(claims, str):
        lines.append(escape(claims))
        return "\n\n".join(lines)
    for claim in claims:
        rendered_links = []
        for post_id in claim.cited_post_ids:
            source = sources_by_id.get(post_id)
            if source is None:
                continue
            if post_id not in link_numbers:
                if len(link_numbers) >= 5:
                    continue
                link_numbers[post_id] = len(link_numbers) + 1
            link = permalink(source.channel, source.post)
            if link:
                rendered_links.append(f'<a href="{escape(link, quote=True)}">[{link_numbers[post_id]}]</a>')
        if rendered_links:
            lines.append(f"{escape(claim.text.strip())} {' '.join(rendered_links)}")
    if not claims or len(lines) == 1:
        lines.append("Недостаточно подтверждённых материалов в выбранном канале." if language == "ru" else "There is not enough supporting material in the selected channel.")
    elif not evidence_sufficient:
        lines.append("Данных недостаточно для полного ответа." if language == "ru" else "The available evidence is insufficient for a complete answer.")
    if conflict:
        lines.append("В выбранных источниках есть датированные противоречивые утверждения." if language == "ru" else "The selected sources contain dated conflicting claims.")
    if synced_at:
        lines.append(("Последняя синхронизация: " if language == "ru" else "Last synchronization: ") + escape(synced_at))
    return "\n\n".join(lines)
