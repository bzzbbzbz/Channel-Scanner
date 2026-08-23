"""Operator-only tool: build a human-labeled judge calibration file.

Runs the offline retrieval+answer path for positive dataset cases and, for
each generated claim that shares a canonical post with a reviewed claim,
writes the pair and the DeepSeek semantic-judge verdict to a local JSONL.
The operator then marks each row as agree/disagree to calibrate the judge
against human judgment. The file holds only claim text and post ids; it
never contains raw questions, source posts, or prompts.
"""

from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path

from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from src.config.settings import get_settings
from src.knowledge.evaluation import load_dataset
from src.knowledge.judge import SemanticJudge
from src.knowledge.repository import KnowledgeRepository
from src.knowledge.search import build_context, collapse_vector_hits, merge_vector_query_results, promote_ranked_posts, reciprocal_rank_fusion
from src.knowledge.service import KnowledgeService
from src.llm import OpenRouterModelPool
from src.llm.deepseek import DeepSeekClient
from src.models.channel import Channel
from src.models.post import Post


async def run(dataset: Path, username: str, output: Path, limit: int) -> None:
    settings = get_settings()
    cases, _ = load_dataset(dataset)
    positive = [case for case in cases if case.answer_expected]
    if limit:
        positive = positive[:limit]

    engine = create_async_engine(settings.database.url)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    judge_client = DeepSeekClient(
        settings.knowledge.deepseek_api_key,
        settings.knowledge.deepseek_base_url,
        telemetry_recorder=None,
    )
    judge = SemanticJudge(settings.knowledge, judge_client)
    output.parent.mkdir(parents=True, exist_ok=True)
    try:
        service = KnowledgeService(session_factory, settings.knowledge, settings.llm, OpenRouterModelPool(settings.llm))
        with output.open("w", encoding="utf-8") as handle:
            async with session_factory() as session:
                repo = KnowledgeRepository(session)
                catalog = await repo.get_catalog_channel_by_username(username)
                if catalog is None:
                    raise LookupError("knowledge catalog channel not found")
                for case_index, case in enumerate(positive, start=1):
                    print(f"[{case_index}/{len(positive)}] {case.id}", flush=True)
                    lexical = await repo.lexical_search(channel_ids=[catalog.channel_id], subscription_baselines={}, query=case.question)
                    vector_queries = service._candidate_vector_queries(case.question)
                    vector_hit_sets = []
                    raw_vector_hits = []
                    for query in vector_queries:
                        hit_set = await service._vector_search(query, {catalog.channel_id})
                        vector_hit_sets.append(hit_set)
                        raw_vector_hits.extend(hit_set)
                    vector = merge_vector_query_results(vector_hit_sets) if vector_hit_sets else collapse_vector_hits(raw_vector_hits)
                    facet_rankings = [collapse_vector_hits(hit_set) for hit_set in vector_hit_sets[1:]] if len(vector_hit_sets) > 1 else []
                    ranked = reciprocal_rank_fusion(lexical, vector, additional_vector_lists=facet_rankings)
                    if facet_rankings:
                        ranked = promote_ranked_posts(ranked, [items[0] for items in facet_rankings if items])
                    candidate_limit = service._settings.rag_rerank_candidate_limit
                    candidate_posts = list(
                        (await session.execute(select(Post).where(Post.id.in_([item.post_id for item in ranked[:candidate_limit]])))).scalars()
                    )
                    outcome = await service.rerank_authorized_posts(case.question, ranked, {post.id: post for post in candidate_posts})
                    ranked = outcome.ranked
                    if facet_rankings:
                        ranked = promote_ranked_posts(ranked, [items[0] for items in facet_rankings if items])
                    ranked = ranked[:5]
                    ranked_ids = [item.post_id for item in ranked]
                    posts = list((await session.execute(select(Post).where(Post.id.in_(ranked_ids)))).scalars()) if ranked_ids else []
                    by_id = {post.id: post for post in posts}
                    sources = []
                    for item in ranked:
                        post = by_id.get(item.post_id)
                        if post is None:
                            continue
                        channel = (await session.execute(select(Channel).where(Channel.id == post.channel_id))).scalar_one()
                        sources.append(build_context(
                            post,
                            channel,
                            matched_type=item.matched_type,
                            matched_ordinal=item.matched_ordinal,
                            chunks=[],
                            parent_context_limit=service._settings.parent_context_limit,
                            neighbor_expansion=service._settings.neighbor_expansion,
                        ))
                    if not sources:
                        continue
                    telegram_ids_by_id = {source.post.id: source.post.post_id for source in sources}
                    generated_claims, _sufficient, _conflict = await service._answer(
                        "ru", case.question, sources, timeout=None,
                        required_source_ids={items[0].post_id for items in facet_rankings if items},
                    )
                    expected_claims = list(case.expected_claims)
                    used_expected: set[int] = set()
                    for generated in generated_claims:
                        cited_telegram_ids = tuple(
                            telegram_ids_by_id[post_id]
                            for post_id in generated.cited_post_ids
                            if post_id in telegram_ids_by_id
                        )
                        for expected_index, expected in enumerate(expected_claims):
                            if expected_index in used_expected:
                                continue
                            if not set(cited_telegram_ids) & set(expected["telegram_post_ids"]):
                                continue
                            verdict = await judge.equivalence(
                                generated.text, cited_telegram_ids,
                                str(expected["text"]), tuple(expected["telegram_post_ids"]),
                            )
                            row = {
                                "case_id": case.id,
                                "generated_claim": generated.text,
                                "generated_post_ids": list(cited_telegram_ids),
                                "expected_claim": expected["text"],
                                "expected_post_ids": list(expected["telegram_post_ids"]),
                                "judge_verdict": verdict,
                                "operator_agrees": None,
                            }
                            handle.write(json.dumps(row, ensure_ascii=False) + "\n")
                            handle.flush()
                            used_expected.add(expected_index)
                            break
                    print(f"[{case_index}/{len(positive)}] {case.id} done", flush=True)
        print(f"calibration rows written to {output}", flush=True)
    finally:
        await judge_client.close()
        await engine.dispose()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("dataset", type=Path)
    parser.add_argument("--username", required=True)
    parser.add_argument("--output", type=Path, default=Path(".data/judge-calibration.jsonl"))
    parser.add_argument("--limit", type=int, default=15, help="Maximum positive cases to run")
    args = parser.parse_args()
    asyncio.run(run(args.dataset, args.username, args.output, args.limit))


if __name__ == "__main__":
    main()
