"""Build versioned summary/full/chunk representations from one canonical post."""

from __future__ import annotations

import hashlib
import uuid
from dataclasses import dataclass

from src.config.settings import KnowledgeSettings
from src.knowledge.chunking import estimate_tokens, paragraph_chunks
from src.knowledge.enrichment import Enrichment
from src.models.knowledge import RepresentationType


@dataclass(frozen=True, slots=True)
class RepresentationDraft:
    post_id: int
    representation_type: RepresentationType
    text: str
    ordinal: int | None
    start_offset: int | None
    end_offset: int | None
    token_count: int
    text_hash: str
    point_id: str


def deterministic_point_id(post_id: int, representation_type: RepresentationType, ordinal: int | None, index_version: int) -> str:
    value = f"{post_id}:{representation_type.value}:{'' if ordinal is None else ordinal}:{index_version}"
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"telegram-parser-bot:{value}"))


def content_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def build_representations(post_id: int, content: str, enrichment: Enrichment | None, settings: KnowledgeSettings, *, index_version: int) -> list[RepresentationDraft]:
    """Generate retrieval records; only full/chunk drafts contain original text."""
    drafts: list[RepresentationDraft] = []
    is_short = estimate_tokens(content) <= settings.short_post_max_tokens
    if settings.summary_enabled and enrichment is not None:
        summary = enrichment.retrieval_text()
        if summary:
            drafts.append(_draft(post_id, RepresentationType.SUMMARY, summary, None, None, None, index_version))
    if is_short and settings.full_for_short_posts:
        drafts.append(_draft(post_id, RepresentationType.FULL, content, None, 0, len(content), index_version))
    elif not is_short and settings.chunks_for_long_posts:
        for chunk in paragraph_chunks(content, target_tokens=settings.target_chunk_tokens, max_tokens=settings.max_chunk_tokens):
            drafts.append(_draft(post_id, RepresentationType.CHUNK, chunk.text, chunk.ordinal, chunk.start_offset, chunk.end_offset, index_version))
    return drafts


def _draft(post_id: int, kind: RepresentationType, text: str, ordinal: int | None, start: int | None, end: int | None, index_version: int) -> RepresentationDraft:
    return RepresentationDraft(post_id, kind, text, ordinal, start, end, estimate_tokens(text), content_hash(text), deterministic_point_id(post_id, kind, ordinal, index_version))
