"""Paragraph-first original-text chunking with stable character offsets."""

from __future__ import annotations

import re
from dataclasses import dataclass


_PARAGRAPH_RE = re.compile(r"\S(?:.*?\S)?(?=\n\s*\n|\Z)", re.DOTALL)
_SENTENCE_RE = re.compile(r"(?<=[.!?…])\s+")


@dataclass(frozen=True, slots=True)
class TextChunk:
    ordinal: int
    text: str
    start_offset: int
    end_offset: int
    token_count: int


def estimate_tokens(text: str) -> int:
    """Cheap, deterministic estimate used for configurable chunk boundaries."""
    return len(re.findall(r"\S+", text))


def paragraph_chunks(text: str, *, target_tokens: int, max_tokens: int) -> list[TextChunk]:
    """Keep fitting paragraphs intact and split oversized ones only by sentences."""
    blocks = [(match.start(), match.end(), match.group(0)) for match in _PARAGRAPH_RE.finditer(text)]
    if not blocks:
        return []
    pieces: list[tuple[int, int, str]] = []
    for start, end, block in blocks:
        if estimate_tokens(block) <= max_tokens:
            pieces.append((start, end, block))
            continue
        pieces.extend(_split_oversized_block(start, block, max_tokens))

    combined: list[tuple[int, int, str]] = []
    current: tuple[int, int, str] | None = None
    for start, end, block in pieces:
        if current is None:
            current = (start, end, block)
            continue
        current_tokens = estimate_tokens(current[2])
        candidate = f"{current[2]}\n\n{block}"
        if current_tokens < target_tokens and estimate_tokens(candidate) <= max_tokens:
            current = (current[0], end, candidate)
        else:
            combined.append(current)
            current = (start, end, block)
    if current is not None:
        combined.append(current)
    return [TextChunk(index, block, start, end, estimate_tokens(block)) for index, (start, end, block) in enumerate(combined)]


def _split_oversized_block(start: int, block: str, max_tokens: int) -> list[tuple[int, int, str]]:
    sentences = _SENTENCE_RE.split(block)
    pieces: list[tuple[int, int, str]] = []
    cursor = 0
    current = ""
    current_start = 0
    for sentence in sentences:
        sentence_start = block.find(sentence, cursor)
        cursor = sentence_start + len(sentence)
        candidate = f"{current} {sentence}".strip()
        if current and estimate_tokens(candidate) > max_tokens:
            pieces.append((start + current_start, start + sentence_start - 1, current))
            current, current_start = sentence, sentence_start
        else:
            current = candidate
    if current:
        pieces.append((start + current_start, start + len(block), current))
    return pieces
