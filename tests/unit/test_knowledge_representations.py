"""Unit coverage for fixed-schema enrichment and canonical-post representations."""

import pytest
from pydantic import ValidationError

from src.config.settings import KnowledgeSettings
from src.knowledge.chunking import paragraph_chunks
from src.knowledge.enrichment import Enrichment
from src.knowledge.representations import build_representations, deterministic_point_id
from src.knowledge.search import collapse_vector_hits
from src.knowledge.indexer import VectorHit
from src.models.knowledge import RepresentationType


def _enrichment() -> Enrichment:
    return Enrichment.model_validate({
        "title": "Hybrid retrieval",
        "summary": "Combining lexical and vector search improves recall.",
        "topics": ["RAG"],
        "entities": [{"name": "Qdrant", "type": "technology"}],
        "content_type": "technical_explanation",
        "epistemic_status": "factual",
        "questions_answered": ["How does hybrid retrieval work?"],
        "claims": [{"text": "Hybrid retrieval improves recall.", "status": "author_claim"}],
    })


def test_short_post_has_summary_and_full_with_canonical_post_id() -> None:
    drafts = build_representations(42, "A short original post about hybrid search.", _enrichment(), KnowledgeSettings(), index_version=1)

    assert [(draft.representation_type, draft.post_id) for draft in drafts] == [
        (RepresentationType.SUMMARY, 42),
        (RepresentationType.FULL, 42),
    ]
    assert drafts[0].start_offset is None
    assert drafts[1].start_offset == 0
    assert deterministic_point_id(42, RepresentationType.FULL, None, 1) == deterministic_point_id(42, RepresentationType.FULL, None, 1)


def test_long_post_has_summary_and_paragraph_chunks_without_full_embedding() -> None:
    settings = KnowledgeSettings(short_post_max_tokens=4, target_chunk_tokens=4, max_chunk_tokens=5)
    text = "one two three\n\nfour five six\n\nseven eight nine"
    drafts = build_representations(7, text, _enrichment(), settings, index_version=1)

    assert drafts[0].representation_type == RepresentationType.SUMMARY
    assert all(draft.representation_type != RepresentationType.FULL for draft in drafts)
    chunks = [draft for draft in drafts if draft.representation_type == RepresentationType.CHUNK]
    assert [chunk.ordinal for chunk in chunks] == [0, 1, 2]
    assert [text[chunk.start_offset:chunk.end_offset] for chunk in chunks] == [chunk.text for chunk in chunks]


def test_chunker_keeps_fitting_paragraph_intact() -> None:
    text = "First paragraph has four words.\n\nSecond paragraph also has four words."
    chunks = paragraph_chunks(text, target_tokens=4, max_tokens=6)

    assert [chunk.text for chunk in chunks] == ["First paragraph has four words.", "Second paragraph also has four words."]


def test_enrichment_rejects_unknown_fields_and_enum_values() -> None:
    payload = _enrichment().model_dump()
    payload["unexpected"] = "no"
    with pytest.raises(ValidationError):
        Enrichment.model_validate(payload)
    payload = _enrichment().model_dump()
    payload["content_type"] = "made_up"
    with pytest.raises(ValidationError):
        Enrichment.model_validate(payload)


def test_vector_hits_collapse_to_one_parent_source() -> None:
    collapsed = collapse_vector_hits([
        VectorHit(post_id=1, representation_type="summary", ordinal=None, score=0.9),
        VectorHit(post_id=1, representation_type="chunk", ordinal=2, score=0.8),
        VectorHit(post_id=2, representation_type="full", ordinal=None, score=0.85),
    ])

    assert [item.post_id for item in collapsed] == [1, 2]
    assert collapsed[0].score == pytest.approx(0.91)
