from types import SimpleNamespace

from src.knowledge.indexer import VectorHit
from src.knowledge.search import RankedPost, SourceContext, merge_vector_query_results, promote_ranked_posts, reciprocal_rank_fusion, render_grounded_answer
from src.knowledge.service import _has_supported_claim
from src.models.channel import Channel
from src.models.post import Post


def test_structured_claims_render_escaped_inline_canonical_links_and_cap_sources() -> None:
    channel = Channel(id=1, username="catalog")
    sources = [
        SourceContext(Post(id=index, channel_id=1, post_id=100 + index, content="source"), channel, "source", None)
        for index in range(1, 7)
    ]
    claims = [
        SimpleNamespace(text="<unsafe>", cited_post_ids=[1, 2]),
        SimpleNamespace(text="supported", cited_post_ids=[3, 4, 5]),
        SimpleNamespace(text="capped", cited_post_ids=[6]),
    ]

    rendered = render_grounded_answer("ru", claims, sources, mode="deep_rerank")

    assert "&lt;unsafe&gt;" in rendered
    assert 'href="https://t.me/catalog/101">[1]</a>' in rendered
    assert 'href="https://t.me/catalog/105">[5]</a>' in rendered
    assert "https://t.me/catalog/106" not in rendered
    assert "@catalog," not in rendered


def test_structured_claims_keep_insufficient_evidence_notice() -> None:
    channel = Channel(id=1, username="catalog")
    source = SourceContext(Post(id=1, channel_id=1, post_id=12, content="source"), channel, "source", None)

    rendered = render_grounded_answer(
        "ru", [SimpleNamespace(text="Есть материал", cited_post_ids=[1])], [source], mode="normal", evidence_sufficient=False
    )

    assert "Данных недостаточно" in rendered


def test_explicit_facet_query_gets_a_deterministic_retrieval_boost() -> None:
    merged = merge_vector_query_results([
        [VectorHit(post_id=1, representation_type="full", ordinal=None, score=0.80)],
        [VectorHit(post_id=2, representation_type="full", ordinal=None, score=0.72)],
    ])

    assert [item.post_id for item in merged] == [2, 1]


def test_explicit_facet_adds_an_independent_rrf_signal() -> None:
    lexical = [
        Post(id=3, channel_id=1, post_id=103, content="lexical"),
        Post(id=4, channel_id=1, post_id=104, content="lexical"),
        Post(id=1, channel_id=1, post_id=101, content="lexical"),
    ]
    primary = [RankedPost(post_id=1, score=0.9), RankedPost(post_id=2, score=0.8)]
    facet = [RankedPost(post_id=2, score=0.7)]

    ranked = reciprocal_rank_fusion(lexical, primary, additional_vector_lists=[facet])

    assert [item.post_id for item in ranked[:2]] == [2, 1]


def test_explicit_facet_evidence_is_kept_in_the_rerank_candidate_window() -> None:
    ranked = [RankedPost(post_id=1, score=0.9), RankedPost(post_id=2, score=0.8)]
    facet = RankedPost(post_id=3, score=0.7)

    promoted = promote_ranked_posts(ranked, [facet])

    assert [item.post_id for item in promoted] == [3, 1, 2]


def test_grounded_answer_contract_requires_explicit_facet_citations() -> None:
    answer = '{"claims":[{"text":"Supported","cited_post_ids":[1]}],"evidence_sufficient":true,"conflict_detected":false}'

    assert _has_supported_claim(answer, {1, 2}, {1})
    assert not _has_supported_claim(answer, {1, 2}, {1, 2})


def test_grounded_answer_contract_accepts_honest_abstention_without_required_ids() -> None:
    abstention = '{"claims":[],"evidence_sufficient":false,"conflict_detected":false}'

    assert _has_supported_claim(abstention, {1, 2})


def test_grounded_answer_contract_rejects_abstention_when_facet_ids_are_required() -> None:
    abstention = '{"claims":[],"evidence_sufficient":false,"conflict_detected":false}'

    assert not _has_supported_claim(abstention, {1, 2}, {1})


def test_grounded_answer_contract_rejects_empty_claims_with_sufficient_evidence() -> None:
    inconsistent = '{"claims":[],"evidence_sufficient":true,"conflict_detected":false}'

    assert not _has_supported_claim(inconsistent, {1, 2})
