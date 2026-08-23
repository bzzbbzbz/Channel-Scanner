"""Repeatable offline retrieval evaluation for an approved knowledge catalog."""

from __future__ import annotations

import asyncio
import hashlib
import json
import math
import os
import re
import tempfile
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path

from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker

from src.knowledge.chunking import estimate_tokens
from src.knowledge.search import build_context, collapse_vector_hits, merge_vector_query_results, promote_ranked_posts, reciprocal_rank_fusion
from src.knowledge.service import KnowledgeService
from src.models.channel import Channel
from src.models.knowledge import KnowledgeEvaluationRun
from src.models.post import Post


@dataclass(frozen=True, slots=True)
class EvaluationCase:
    id: str
    question: str
    expected_telegram_post_ids: frozenset[int]
    relevance_complete: bool = False
    answer_expected: bool = True
    reference_answer_html: str | None = None
    expected_claims: tuple[dict, ...] = ()
    split: str = "dev"


@dataclass(frozen=True, slots=True)
class AnswerAudit:
    """Content-free aggregate from a separately reviewed answer sample."""

    sample_size: int
    judge_version: str
    faithfulness: float
    citation_validity: float
    citation_completeness: float
    answer_relevance: float


_CHECKPOINT_METRICS = (
    "recalls", "reciprocal_ranks", "ndcgs", "duplicate_shares", "latencies",
    "retrieval_latencies", "answer_generation_latencies", "context_tokens", "precisions",
    "rerank_fallbacks", "rerank_costs", "correct_abstentions", "false_attributions",
    "source_sufficiencies", "claim_coverage_sufficiencies", "citation_precisions",
    "citation_recalls", "citation_f1s", "citation_placements",
    "claim_precisions", "claim_recalls", "claim_f1s",
    "judge_claim_precisions", "judge_claim_recalls", "judge_claim_f1s",
)


@dataclass(slots=True)
class EvaluationCheckpoint:
    """Resumable, content-free state for one exact offline evaluation run."""

    path: Path
    dataset_hash: str
    configuration_id: str
    candidate: bool
    question_count: int
    next_case_index: int
    metrics: dict[str, list[float | bool | int]]

    @classmethod
    def load_or_create(
        cls,
        path: Path,
        *,
        dataset_hash: str,
        configuration_id: str,
        candidate: bool,
        question_count: int,
    ) -> "EvaluationCheckpoint":
        if not path.exists():
            return cls(
                path, dataset_hash, configuration_id, candidate, question_count, 0,
                {name: [] for name in _CHECKPOINT_METRICS},
            )
        value = json.loads(path.read_text(encoding="utf-8"))
        allowed = {"dataset_hash", "configuration_id", "candidate", "question_count", "next_case_index", "metrics"}
        if not isinstance(value, dict) or set(value) != allowed:
            raise ValueError("evaluation checkpoint has an invalid schema")
        expected = {
            "dataset_hash": dataset_hash,
            "configuration_id": configuration_id,
            "candidate": candidate,
            "question_count": question_count,
        }
        if any(value.get(name) != expected_value for name, expected_value in expected.items()):
            raise ValueError("evaluation checkpoint belongs to a different run")
        next_case_index = value.get("next_case_index")
        metrics = value.get("metrics")
        if not isinstance(next_case_index, int) or not 0 <= next_case_index <= question_count or not isinstance(metrics, dict) or set(metrics) != set(_CHECKPOINT_METRICS):
            raise ValueError("evaluation checkpoint has invalid progress")
        if any(not isinstance(entries, list) or any(not isinstance(item, (int, float, bool)) for item in entries) for entries in metrics.values()):
            raise ValueError("evaluation checkpoint contains non-metric data")
        if any(len(entries) > next_case_index for entries in metrics.values()):
            raise ValueError("evaluation checkpoint has inconsistent metric progress")
        return cls(path, dataset_hash, configuration_id, candidate, question_count, next_case_index, metrics)

    def save(self) -> None:
        """Atomically persist only numeric aggregates and the next case position."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        value = {
            "dataset_hash": self.dataset_hash,
            "configuration_id": self.configuration_id,
            "candidate": self.candidate,
            "question_count": self.question_count,
            "next_case_index": self.next_case_index,
            "metrics": self.metrics,
        }
        with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=self.path.parent, prefix=f".{self.path.name}.", delete=False) as handle:
            json.dump(value, handle, ensure_ascii=False, separators=(",", ":"))
            handle.write("\n")
            temporary_path = Path(handle.name)
        os.chmod(temporary_path, 0o600)
        os.replace(temporary_path, self.path)


def load_answer_audit(path: Path, dataset_hash: str) -> AnswerAudit:
    """Load only aggregate manual/judge scores tied to one exact retrieval dataset."""
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict) or value.get("dataset_hash") != dataset_hash:
        raise ValueError("answer audit must name the exact evaluation dataset hash")
    allowed = {"dataset_hash", "sample_size", "judge_version", "faithfulness", "citation_validity", "citation_completeness", "answer_relevance"}
    if set(value) != allowed:
        raise ValueError("answer audit may contain only content-free aggregate fields")
    sample_size = value.get("sample_size")
    judge_version = value.get("judge_version")
    if not isinstance(sample_size, int) or sample_size < 1 or not isinstance(judge_version, str) or not judge_version.strip():
        raise ValueError("answer audit needs a positive sample_size and judge_version")
    scores = {}
    for name in ("faithfulness", "citation_validity", "citation_completeness", "answer_relevance"):
        score = value.get(name)
        if not isinstance(score, (int, float)) or not 0 <= float(score) <= 1:
            raise ValueError(f"answer audit has invalid {name}")
        scores[name] = float(score)
    return AnswerAudit(sample_size, judge_version.strip(), **scores)


def load_dataset(path: Path) -> tuple[list[EvaluationCase], str]:
    """Load manually labelled JSONL without admitting raw user-chat content."""
    raw = path.read_bytes()
    cases = []
    for line_number, line in enumerate(raw.decode("utf-8").splitlines(), start=1):
        if not line.strip():
            continue
        value = json.loads(line)
        allowed = {"id", "question", "expected_telegram_post_ids", "relevance_complete", "answer_expected", "reference_answer_html", "expected_claims", "review_note", "split"}
        if set(value) - allowed:
            raise ValueError(f"evaluation case at line {line_number} has unknown fields")
        expected = value.get("expected_telegram_post_ids")
        if not isinstance(value.get("id"), str) or not isinstance(value.get("question"), str) or not isinstance(expected, list):
            raise ValueError(f"invalid evaluation case at line {line_number}")
        expected_ids = frozenset(int(post_id) for post_id in expected)
        answer_expected = value.get("answer_expected", bool(expected_ids))
        if not value["question"].strip() or not isinstance(answer_expected, bool) or (answer_expected and not expected_ids):
            raise ValueError(f"evaluation case at line {line_number} needs a question and expected posts when an answer is expected")
        split = value.get("split", "dev")
        if split not in ("dev", "eval"):
            raise ValueError(f"evaluation case at line {line_number} has invalid split")
        # The reviewed BL-24 dataset labels every positive case with all and
        # only the posts needed for its reference answer.  Future partial sets
        # must opt out explicitly, so precision is not silently suppressed.
        complete = value.get("relevance_complete", answer_expected)
        if not isinstance(complete, bool):
            raise ValueError(f"evaluation case at line {line_number} has invalid relevance_complete")
        reference = value.get("reference_answer_html")
        claims = value.get("expected_claims")
        if answer_expected:
            if not isinstance(reference, str) or not isinstance(claims, list) or not claims:
                raise ValueError(f"positive evaluation case at line {line_number} needs reference answer and claims")
            if "[" in reference and "](" in reference:
                raise ValueError(f"reference answer at line {line_number} contains Markdown")
            links = re.findall(r'<a href="https://t\.me/([A-Za-z0-9_]+)/([0-9]+)">\[[0-9]+\]</a>', reference)
            if not links or any(int(post_id) not in expected_ids for _username, post_id in links):
                raise ValueError(f"reference answer at line {line_number} has invalid canonical links")
            claim_ids: set[str] = set()
            claim_post_ids: list[int] = []
            normalized_claims: list[dict] = []
            for claim in claims:
                if set(claim) != {"id", "text", "telegram_post_ids"} or not isinstance(claim["id"], str) or not claim["id"].strip() or not isinstance(claim["text"], str) or not claim["text"].strip() or not isinstance(claim["telegram_post_ids"], list):
                    raise ValueError(f"invalid expected claim at line {line_number}")
                if claim["id"] in claim_ids or not claim["telegram_post_ids"] or any(not isinstance(post_id, int) or post_id not in expected_ids for post_id in claim["telegram_post_ids"]):
                    raise ValueError(f"invalid expected claim citations at line {line_number}")
                claim_ids.add(claim["id"])
                claim_post_ids.extend(claim["telegram_post_ids"])
                normalized_claims.append({"id": claim["id"], "text": claim["text"].strip(), "telegram_post_ids": tuple(claim["telegram_post_ids"])})
            ordered_union = tuple(dict.fromkeys(claim_post_ids))
            if ordered_union != tuple(expected) or {int(post_id) for _username, post_id in links} != expected_ids:
                raise ValueError(f"expected claims and reference links must cover expected posts at line {line_number}")
        else:
            if reference is not None or claims is not None:
                raise ValueError(f"negative evaluation case at line {line_number} must omit answer fields")
            normalized_claims = []
        cases.append(EvaluationCase(value["id"], value["question"], expected_ids, complete, answer_expected, reference, tuple(normalized_claims), split))
    if not cases:
        raise ValueError("evaluation dataset is empty")
    return cases, hashlib.sha256(raw).hexdigest()


async def evaluate_catalog(
    session_factory: async_sessionmaker,
    service: KnowledgeService,
    *,
    channel_username: str,
    cases: list[EvaluationCase],
    dataset_hash: str,
    limit: int = 5,
    candidate: bool = False,
    answer_audit: AnswerAudit | None = None,
    evaluate_answers: bool = True,
    checkpoint_path: Path | None = None,
    judge: Callable[[str, tuple[int, ...], str, tuple[int, ...]], Awaitable[bool | None]] | None = None,
    concurrency: int = 1,
) -> dict[str, float | int | str]:
    """Evaluate retrieval plus generated claims against the reviewed answer set.

    Claim metrics are deterministic and intentionally conservative: a generated
    claim must share a cited canonical post and meaningful lexical content with
    a reviewed claim.  They complement, rather than replace, the optional
    human/judge aggregate quality audit.  External calls (rerank, answer,
    judge) run concurrently up to *concurrency*; retrieval reads are
    read-only and telemetry writes use isolated sessions.
    """
    configuration_id = service._settings.rag_configuration_id if candidate else "baseline"
    checkpoint = (
        EvaluationCheckpoint.load_or_create(
            checkpoint_path,
            dataset_hash=dataset_hash,
            configuration_id=configuration_id,
            candidate=candidate,
            question_count=len(cases),
        )
        if checkpoint_path else None
    )
    if checkpoint and checkpoint.next_case_index == len(cases):
        raise ValueError("evaluation checkpoint is already complete; use a new checkpoint path")
    metric_lists = checkpoint.metrics if checkpoint else {name: [] for name in _CHECKPOINT_METRICS}
    labels_complete = all(case.relevance_complete for case in cases)

    async with session_factory() as session:
        from src.knowledge.repository import KnowledgeRepository

        repo = KnowledgeRepository(session)
        catalog = await repo.get_catalog_channel_by_username(channel_username)
        if catalog is None:
            raise LookupError("knowledge catalog channel not found")
        if candidate:
            await repo.ensure_rag_configuration(service._settings)

        start_index = checkpoint.next_case_index if checkpoint else 0
        pending = list(enumerate(cases[start_index:], start=start_index))
        semaphore = asyncio.Semaphore(max(1, concurrency))

        async def guarded(index: int, case: EvaluationCase) -> tuple[int, dict[str, object]]:
            async with semaphore:
                return index, await _run_evaluation_case(
                    session_factory,
                    service,
                    channel_id=catalog.channel_id,
                    case=case,
                    limit=limit,
                    candidate=candidate,
                    labels_complete=labels_complete,
                    evaluate_answers=evaluate_answers,
                    judge=judge,
                )

        completed: list[tuple[int, dict[str, object]]] = []
        for batch_start in range(0, len(pending), concurrency):
            batch = pending[batch_start : batch_start + concurrency]
            results = await asyncio.gather(*(guarded(index, case) for index, case in batch))
            completed.extend(results)
            if checkpoint:
                checkpoint.next_case_index = max((index for index, _ in completed), default=start_index)
                checkpoint.save()
            for index, _ in completed:
                print(json.dumps({"progress": index, "questions": len(cases)}, ensure_ascii=False), flush=True)

        for index, metrics in completed:
            _absorb_case_metrics(metric_lists, metrics)

        record = _build_evaluation_record(
            service=service,
            catalog=catalog,
            dataset_hash=dataset_hash,
            limit=limit,
            candidate=candidate,
            answer_audit=answer_audit,
            labels_complete=labels_complete,
            judge_enabled=judge is not None,
            question_count=len(cases),
            metric_lists=metric_lists,
        )
        session.add(record)
        await session.commit()

    return _evaluation_result(record, metric_lists, labels_complete)


def _percentile(values: list[int], percentile: int) -> int | None:
    if not values:
        return None
    ordered = sorted(values)
    index = max(0, math.ceil(percentile / 100 * len(ordered)) - 1)
    return ordered[index]


async def _run_evaluation_case(
    session_factory: async_sessionmaker,
    service: KnowledgeService,
    *,
    channel_id: int,
    case: EvaluationCase,
    limit: int,
    candidate: bool,
    labels_complete: bool,
    evaluate_answers: bool,
    judge: Callable[[str, tuple[int, ...], str, tuple[int, ...]], Awaitable[bool | None]] | None,
) -> dict[str, object]:
    """Run one offline evaluation case with its own isolated session.

    Returns a content-free dict of metric values for the case.  All external
    provider calls (embedding, rerank, answer, judge) are isolated per call
    and telemetry writes use fresh sessions, so many cases may run
    concurrently without sharing mutable state.
    """
    async with session_factory() as session:
        from src.knowledge.repository import KnowledgeRepository

        repo = KnowledgeRepository(session)
        started = time.monotonic()
        lexical = await repo.lexical_search(channel_ids=[channel_id], subscription_baselines={}, query=case.question)
        try:
            vector_queries = (
                service._candidate_vector_queries(case.question)
                if candidate and hasattr(service, "_candidate_vector_queries")
                else ([service._instructed_query(case.question)] if candidate else [case.question])
            )
            raw_vector_hits = []
            vector_hit_sets = []
            for query in vector_queries:
                hit_set = await service._vector_search(query, {channel_id})
                vector_hit_sets.append(hit_set)
                raw_vector_hits.extend(hit_set)
        except Exception as exc:
            raise RuntimeError("evaluation vector retrieval is unavailable") from exc
        vector = merge_vector_query_results(vector_hit_sets) if candidate and vector_hit_sets else collapse_vector_hits(raw_vector_hits)
        facet_rankings = (
            [collapse_vector_hits(hit_set) for hit_set in vector_hit_sets[1:]]
            if candidate and len(vector_hit_sets) > 1
            else []
        )
        ranked = reciprocal_rank_fusion(lexical, vector, additional_vector_lists=facet_rankings)
        if facet_rankings:
            ranked = promote_ranked_posts(ranked, [items[0] for items in facet_rankings if items])
        rerank_fallback = None
        rerank_cost = None
        if candidate and hasattr(service, "rerank_authorized_posts"):
            candidate_limit = service._settings.rag_rerank_candidate_limit
            candidate_posts = list(
                (await session.execute(
                    select(Post).where(Post.id.in_([item.post_id for item in ranked[:candidate_limit]]))
                )).scalars()
            )
            outcome = await service.rerank_authorized_posts(case.question, ranked, {post.id: post for post in candidate_posts})
            ranked = outcome.ranked
            if facet_rankings:
                ranked = promote_ranked_posts(ranked, [items[0] for items in facet_rankings if items])
            rerank_fallback = outcome.fallback_reason is not None
            rerank_cost = outcome.cost
        ranked = ranked[:limit]
        ranked_ids = [item.post_id for item in ranked]
        posts = list((await session.execute(select(Post).where(Post.id.in_(ranked_ids)))).scalars()) if ranked_ids else []
        by_id = {post.id: post for post in posts}
        retrieved = [by_id[post_id].post_id for post_id in ranked_ids if post_id in by_id]
        relevant_ranks = [rank for rank, post_id in enumerate(retrieved, start=1) if post_id in case.expected_telegram_post_ids]

        recall = None
        precision = None
        reciprocal_rank = None
        ndcg = None
        source_sufficiency = None
        claim_coverage_sufficiency = None
        correct_abstention = None
        false_attribution = None
        if case.answer_expected:
            recall = len(set(retrieved) & case.expected_telegram_post_ids) / len(case.expected_telegram_post_ids)
            if labels_complete:
                precision = len(set(retrieved) & case.expected_telegram_post_ids) / limit
            reciprocal_rank = 1 / relevant_ranks[0] if relevant_ranks else 0
            dcg = sum(1 / math.log2(rank + 1) for rank in relevant_ranks)
            ideal = sum(1 / math.log2(rank + 1) for rank in range(1, min(limit, len(case.expected_telegram_post_ids)) + 1))
            ndcg = dcg / ideal if ideal else 0
            source_sufficiency = float(bool(relevant_ranks))
            claim_coverage_sufficiency = _claim_coverage_sufficiency(case, retrieved)
        else:
            correct_abstention = float(not retrieved)
            false_attribution = float(bool(retrieved))
        raw_top = raw_vector_hits[:limit]
        duplicate_share = (len(raw_top) - len({hit.post_id for hit in raw_top})) / len(raw_top) if raw_top else 0
        retrieval_latency = int((time.monotonic() - started) * 1000)
        context_token = sum(estimate_tokens(by_id[post_id].content) for post_id in ranked_ids if post_id in by_id)

        answer_generation_latency = None
        citation_precision = None
        citation_recall = None
        citation_f1 = None
        citation_placement = None
        claim_precision = None
        claim_recall = None
        claim_f1 = None
        judge_claim_precision = None
        judge_claim_recall = None
        judge_claim_f1 = None
        if evaluate_answers and case.answer_expected and hasattr(service, "_answer"):
            sources = []
            for item in ranked[:limit]:
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
            answer_started = time.monotonic()
            generated_claims, _sufficient, _conflict = await service._answer(
                "ru",
                case.question,
                sources,
                timeout=None,
                required_source_ids={items[0].post_id for items in facet_rankings if items},
            )
            answer_generation_latency = int((time.monotonic() - answer_started) * 1000)
            telegram_ids_by_id = {source.post.id: source.post.post_id for source in sources}
            metrics = _answer_metrics(generated_claims, case, telegram_post_ids_by_id=telegram_ids_by_id)
            citation_precision = metrics["citation_precision"]
            citation_recall = metrics["citation_recall"]
            citation_f1 = metrics["citation_f1"]
            citation_placement = metrics["citation_placement"]
            claim_precision = metrics["claim_precision"]
            claim_recall = metrics["claim_recall"]
            claim_f1 = metrics["claim_f1"]
            if judge is not None:
                judge_metrics = await _judge_claim_metrics(judge, generated_claims, case, telegram_post_ids_by_id=telegram_ids_by_id)
                judge_claim_precision = judge_metrics["claim_precision"]
                judge_claim_recall = judge_metrics["claim_recall"]
                judge_claim_f1 = judge_metrics["claim_f1"]

        return {
            "recall": recall,
            "precision": precision,
            "reciprocal_rank": reciprocal_rank,
            "ndcg": ndcg,
            "duplicate_share": duplicate_share,
            "latency": int((time.monotonic() - started) * 1000),
            "retrieval_latency": retrieval_latency,
            "answer_generation_latency": answer_generation_latency,
            "context_tokens": context_token,
            "rerank_fallback": rerank_fallback,
            "rerank_cost": rerank_cost,
            "correct_abstention": correct_abstention,
            "false_attribution": false_attribution,
            "source_sufficiency": source_sufficiency,
            "claim_coverage_sufficiency": claim_coverage_sufficiency,
            "citation_precision": citation_precision,
            "citation_recall": citation_recall,
            "citation_f1": citation_f1,
            "citation_placement": citation_placement,
            "claim_precision": claim_precision,
            "claim_recall": claim_recall,
            "claim_f1": claim_f1,
            "judge_claim_precision": judge_claim_precision,
            "judge_claim_recall": judge_claim_recall,
            "judge_claim_f1": judge_claim_f1,
        }


def _absorb_case_metrics(metric_lists: dict[str, list], metrics: dict[str, object]) -> None:
    mapping = {
        "recalls": "recall",
        "reciprocal_ranks": "reciprocal_rank",
        "ndcgs": "ndcg",
        "duplicate_shares": "duplicate_share",
        "latencies": "latency",
        "retrieval_latencies": "retrieval_latency",
        "answer_generation_latencies": "answer_generation_latency",
        "context_tokens": "context_tokens",
        "precisions": "precision",
        "rerank_fallbacks": "rerank_fallback",
        "rerank_costs": "rerank_cost",
        "correct_abstentions": "correct_abstention",
        "false_attributions": "false_attribution",
        "source_sufficiencies": "source_sufficiency",
        "claim_coverage_sufficiencies": "claim_coverage_sufficiency",
        "citation_precisions": "citation_precision",
        "citation_recalls": "citation_recall",
        "citation_f1s": "citation_f1",
        "citation_placements": "citation_placement",
        "claim_precisions": "claim_precision",
        "claim_recalls": "claim_recall",
        "claim_f1s": "claim_f1",
        "judge_claim_precisions": "judge_claim_precision",
        "judge_claim_recalls": "judge_claim_recall",
        "judge_claim_f1s": "judge_claim_f1",
    }
    for list_name, metric_name in mapping.items():
        value = metrics.get(metric_name)
        if value is not None:
            metric_lists[list_name].append(value)


def _build_evaluation_record(
    *,
    service: KnowledgeService,
    catalog,
    dataset_hash: str,
    limit: int,
    candidate: bool,
    answer_audit: AnswerAudit | None,
    labels_complete: bool,
    judge_enabled: bool,
    question_count: int,
    metric_lists: dict[str, list],
) -> KnowledgeEvaluationRun:
    recalls = metric_lists["recalls"]
    reciprocal_ranks = metric_lists["reciprocal_ranks"]
    ndcgs = metric_lists["ndcgs"]
    duplicate_shares = metric_lists["duplicate_shares"]
    latencies = metric_lists["latencies"]
    retrieval_latencies = metric_lists["retrieval_latencies"]
    answer_generation_latencies = metric_lists["answer_generation_latencies"]
    context_tokens = metric_lists["context_tokens"]
    precisions = metric_lists["precisions"]
    rerank_fallbacks = metric_lists["rerank_fallbacks"]
    rerank_costs = metric_lists["rerank_costs"]
    correct_abstentions = metric_lists["correct_abstentions"]
    false_attributions = metric_lists["false_attributions"]
    source_sufficiencies = metric_lists["source_sufficiencies"]
    claim_coverage_sufficiencies = metric_lists["claim_coverage_sufficiencies"]
    citation_precisions = metric_lists["citation_precisions"]
    citation_recalls = metric_lists["citation_recalls"]
    citation_f1s = metric_lists["citation_f1s"]
    citation_placements = metric_lists["citation_placements"]
    claim_precisions = metric_lists["claim_precisions"]
    claim_recalls = metric_lists["claim_recalls"]
    claim_f1s = metric_lists["claim_f1s"]
    judge_claim_precisions = metric_lists["judge_claim_precisions"]
    judge_claim_recalls = metric_lists["judge_claim_recalls"]
    judge_claim_f1s = metric_lists["judge_claim_f1s"]

    return KnowledgeEvaluationRun(
        knowledge_channel_id=catalog.id,
        index_version=service._settings.index_version,
        dataset_hash=dataset_hash,
        mode=f"hybrid_parent_rrf@{limit}",
        recall_at_k=(sum(recalls) / len(recalls)) if recalls else None,
        mrr=(sum(reciprocal_ranks) / len(reciprocal_ranks)) if reciprocal_ranks else None,
        ndcg=(sum(ndcgs) / len(ndcgs)) if ndcgs else None,
        duplicate_source_share=sum(duplicate_shares) / len(duplicate_shares) if duplicate_shares else 0,
        precision_at_k=(sum(precisions) / len(precisions)) if precisions and labels_complete else None,
        question_count=question_count,
        labels_complete=labels_complete,
        configuration_id=service._settings.rag_configuration_id if candidate else "baseline",
        reranker_model=service._settings.rag_reranker_model if candidate else None,
        rerank_fallback_share=(sum(rerank_fallbacks) / len(rerank_fallbacks)) if rerank_fallbacks else (0 if candidate else None),
        correct_abstention_share=(sum(correct_abstentions) / len(correct_abstentions)) if correct_abstentions else None,
        false_attribution_share=(sum(false_attributions) / len(false_attributions)) if false_attributions else None,
        source_sufficiency_share=(sum(source_sufficiencies) / len(source_sufficiencies)) if source_sufficiencies else None,
        claim_coverage_sufficiency_share=(sum(claim_coverage_sufficiencies) / len(claim_coverage_sufficiencies)) if claim_coverage_sufficiencies else None,
        faithfulness=answer_audit.faithfulness if answer_audit else None,
        citation_validity=answer_audit.citation_validity if answer_audit else None,
        citation_completeness=answer_audit.citation_completeness if answer_audit else None,
        answer_relevance=answer_audit.answer_relevance if answer_audit else None,
        answer_audit_sample_size=answer_audit.sample_size if answer_audit else None,
        judge_version=(service._settings.judge_version if (judge_enabled and judge_claim_f1s) else None) or (answer_audit.judge_version if answer_audit else None),
        p50_latency_ms=_percentile(latencies, 50),
        p95_latency_ms=_percentile(latencies, 95),
        p99_latency_ms=_percentile(latencies, 99),
        latency_ms=round(sum(latencies) / len(latencies)),
        p50_retrieval_latency_ms=_percentile(retrieval_latencies, 50),
        p95_retrieval_latency_ms=_percentile(retrieval_latencies, 95),
        p99_retrieval_latency_ms=_percentile(retrieval_latencies, 99),
        retrieval_latency_ms=round(sum(retrieval_latencies) / len(retrieval_latencies)),
        p50_answer_generation_ms=_percentile(answer_generation_latencies, 50),
        p95_answer_generation_ms=_percentile(answer_generation_latencies, 95),
        p99_answer_generation_ms=_percentile(answer_generation_latencies, 99),
        answer_generation_ms=round(sum(answer_generation_latencies) / len(answer_generation_latencies)) if answer_generation_latencies else None,
        context_tokens=round(sum(context_tokens) / len(context_tokens)),
        cost=(sum(rerank_costs) / len(rerank_costs)) if rerank_costs else None,
        citation_precision=_mean_or_none(citation_precisions),
        citation_recall=_mean_or_none(citation_recalls),
        citation_f1=_mean_or_none(citation_f1s),
        citation_placement=_mean_or_none(citation_placements),
        claim_precision=_mean_or_none(claim_precisions),
        claim_recall=_mean_or_none(claim_recalls),
        claim_f1=_mean_or_none(claim_f1s),
        judge_claim_precision=_mean_or_none(judge_claim_precisions),
        judge_claim_recall=_mean_or_none(judge_claim_recalls),
        judge_claim_f1=_mean_or_none(judge_claim_f1s),
    )


def _evaluation_result(record: KnowledgeEvaluationRun, metric_lists: dict[str, list], labels_complete: bool) -> dict[str, float | int | str]:
    recalls = metric_lists["recalls"]
    reciprocal_ranks = metric_lists["reciprocal_ranks"]
    ndcgs = metric_lists["ndcgs"]
    duplicate_shares = metric_lists["duplicate_shares"]
    latencies = metric_lists["latencies"]
    retrieval_latencies = metric_lists["retrieval_latencies"]
    answer_generation_latencies = metric_lists["answer_generation_latencies"]
    context_tokens = metric_lists["context_tokens"]
    precisions = metric_lists["precisions"]
    correct_abstentions = metric_lists["correct_abstentions"]
    false_attributions = metric_lists["false_attributions"]
    source_sufficiencies = metric_lists["source_sufficiencies"]
    claim_coverage_sufficiencies = metric_lists["claim_coverage_sufficiencies"]
    citation_precisions = metric_lists["citation_precisions"]
    citation_recalls = metric_lists["citation_recalls"]
    citation_f1s = metric_lists["citation_f1s"]
    citation_placements = metric_lists["citation_placements"]
    claim_precisions = metric_lists["claim_precisions"]
    claim_recalls = metric_lists["claim_recalls"]
    claim_f1s = metric_lists["claim_f1s"]
    judge_claim_precisions = metric_lists["judge_claim_precisions"]
    judge_claim_recalls = metric_lists["judge_claim_recalls"]
    judge_claim_f1s = metric_lists["judge_claim_f1s"]

    return {
        "evaluation_id": record.id,
        "questions": record.question_count,
        "recall_at_k": round(sum(recalls) / len(recalls), 6) if recalls else None,
        "mrr": round(sum(reciprocal_ranks) / len(reciprocal_ranks), 6) if reciprocal_ranks else None,
        "ndcg": round(sum(ndcgs) / len(ndcgs), 6) if ndcgs else None,
        "duplicate_source_share": round(sum(duplicate_shares) / len(duplicate_shares), 6) if duplicate_shares else 0,
        "precision_at_k": round(sum(precisions) / len(precisions), 6) if precisions and labels_complete else None,
        "labels_complete": labels_complete,
        "correct_abstention_share": round(sum(correct_abstentions) / len(correct_abstentions), 6) if correct_abstentions else None,
        "false_attribution_share": round(sum(false_attributions) / len(false_attributions), 6) if false_attributions else None,
        "source_sufficiency_share": round(sum(source_sufficiencies) / len(source_sufficiencies), 6) if source_sufficiencies else None,
        "claim_coverage_sufficiency_share": round(sum(claim_coverage_sufficiencies) / len(claim_coverage_sufficiencies), 6) if claim_coverage_sufficiencies else None,
        "latency_ms": round(sum(latencies) / len(latencies)),
        "p50_retrieval_latency_ms": _percentile(retrieval_latencies, 50),
        "p95_retrieval_latency_ms": _percentile(retrieval_latencies, 95),
        "p99_retrieval_latency_ms": _percentile(retrieval_latencies, 99),
        "retrieval_latency_ms": round(sum(retrieval_latencies) / len(retrieval_latencies)),
        "p50_answer_generation_ms": _percentile(answer_generation_latencies, 50),
        "p95_answer_generation_ms": _percentile(answer_generation_latencies, 95),
        "p99_answer_generation_ms": _percentile(answer_generation_latencies, 99),
        "answer_generation_ms": round(sum(answer_generation_latencies) / len(answer_generation_latencies)) if answer_generation_latencies else None,
        "context_tokens": round(sum(context_tokens) / len(context_tokens)),
        "citation_precision": _rounded_mean(citation_precisions),
        "citation_recall": _rounded_mean(citation_recalls),
        "citation_f1": _rounded_mean(citation_f1s),
        "citation_placement": _rounded_mean(citation_placements),
        "claim_precision": _rounded_mean(claim_precisions),
        "claim_recall": _rounded_mean(claim_recalls),
        "claim_f1": _rounded_mean(claim_f1s),
        "judge_claim_precision": _rounded_mean(judge_claim_precisions),
        "judge_claim_recall": _rounded_mean(judge_claim_recalls),
        "judge_claim_f1": _rounded_mean(judge_claim_f1s),
    }


def _answer_metrics(
    generated_claims,
    case: EvaluationCase,
    *,
    telegram_post_ids_by_id: dict[int, int],
) -> dict[str, float]:
    expected_ids = set(case.expected_telegram_post_ids)
    cited_ids = {
        telegram_post_ids_by_id[post_id]
        for claim in generated_claims
        for post_id in claim.cited_post_ids
        if post_id in telegram_post_ids_by_id
    }
    citation_precision = _ratio(len(cited_ids & expected_ids), len(cited_ids))
    citation_recall = _ratio(len(cited_ids & expected_ids), len(expected_ids))
    expected_claims = list(case.expected_claims)
    matched_expected: set[int] = set()
    matched_generated: set[int] = set()
    placed = 0
    matched_pairs = 0
    for generated_index, generated in enumerate(generated_claims):
        for expected_index, expected in enumerate(expected_claims):
            if expected_index in matched_expected:
                continue
            cited_telegram_ids = [
                telegram_post_ids_by_id[post_id]
                for post_id in generated.cited_post_ids
                if post_id in telegram_post_ids_by_id
            ]
            if _claims_match(generated.text, cited_telegram_ids, expected):
                matched_generated.add(generated_index)
                matched_expected.add(expected_index)
                matched_pairs += 1
                if set(cited_telegram_ids) <= set(expected["telegram_post_ids"]):
                    placed += 1
                break
    claim_precision = _ratio(len(matched_generated), len(generated_claims))
    claim_recall = _ratio(len(matched_expected), len(expected_claims))
    return {
        "citation_precision": citation_precision,
        "citation_recall": citation_recall,
        "citation_f1": _f1(citation_precision, citation_recall),
        "citation_placement": _ratio(placed, matched_pairs),
        "claim_precision": claim_precision,
        "claim_recall": claim_recall,
        "claim_f1": _f1(claim_precision, claim_recall),
    }


async def _judge_claim_metrics(
    judge: Callable[[str, tuple[int, ...], str, tuple[int, ...]], Awaitable[bool | None]],
    generated_claims,
    case: EvaluationCase,
    *,
    telegram_post_ids_by_id: dict[int, int],
) -> dict[str, float]:
    """Compare generated claims against the reviewed claims with a semantic judge.

    The judge receives only claim text and canonical telegram post ids (never
    raw source text) and returns True, False, or None when it cannot decide.
    The judge is content-free and its verdicts are aggregated only.
    """
    expected_claims = list(case.expected_claims)
    matched_expected: set[int] = set()
    matched_generated: set[int] = set()
    for generated_index, generated in enumerate(generated_claims):
        cited_telegram_ids = tuple(
            telegram_post_ids_by_id[post_id]
            for post_id in generated.cited_post_ids
            if post_id in telegram_post_ids_by_id
        )
        for expected_index, expected in enumerate(expected_claims):
            if expected_index in matched_expected:
                continue
            if not set(cited_telegram_ids) & set(expected["telegram_post_ids"]):
                continue
            verdict = await judge(generated.text, cited_telegram_ids, str(expected["text"]), tuple(expected["telegram_post_ids"]))
            if verdict is True:
                matched_generated.add(generated_index)
                matched_expected.add(expected_index)
                break
    claim_precision = _ratio(len(matched_generated), len(generated_claims))
    claim_recall = _ratio(len(matched_expected), len(expected_claims))
    return {
        "claim_precision": claim_precision,
        "claim_recall": claim_recall,
        "claim_f1": _f1(claim_precision, claim_recall),
    }


def _claim_coverage_sufficiency(case: EvaluationCase, retrieved: list[int]) -> float:
    """Share of expected claims whose posts appear among the retrieved posts."""
    retrieved_set = set(retrieved)
    if not case.expected_claims:
        return 0.0
    covered = sum(
        1
        for claim in case.expected_claims
        if set(claim["telegram_post_ids"]) & retrieved_set
    )
    return covered / len(case.expected_claims)


def _claims_match(text: str, cited_post_ids, expected: dict) -> bool:
    if not set(cited_post_ids) & set(expected["telegram_post_ids"]):
        return False
    actual_tokens = _claim_tokens(text)
    expected_tokens = _claim_tokens(str(expected["text"]))
    if not actual_tokens or not expected_tokens:
        return False
    return len(actual_tokens & expected_tokens) / min(len(actual_tokens), len(expected_tokens)) >= 0.35


def _claim_tokens(value: str) -> set[str]:
    return {token for token in re.findall(r"[\w-]+", value.casefold()) if len(token) >= 4}


def _ratio(numerator: int, denominator: int) -> float:
    return numerator / denominator if denominator else 0.0


def _f1(precision: float, recall: float) -> float:
    return 2 * precision * recall / (precision + recall) if precision + recall else 0.0


def _mean_or_none(values: list[float]) -> float | None:
    return sum(values) / len(values) if values else None


def _rounded_mean(values: list[float]) -> float | None:
    mean = _mean_or_none(values)
    return round(mean, 6) if mean is not None else None
