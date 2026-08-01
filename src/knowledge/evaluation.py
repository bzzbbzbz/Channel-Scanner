"""Repeatable offline retrieval evaluation for an approved knowledge catalog."""

from __future__ import annotations

import hashlib
import json
import math
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping, Protocol, Sequence

from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker

from src.knowledge.chunking import estimate_tokens
from src.knowledge.experiments import EvaluationMetrics, ExperimentError, evaluation_metrics_record, phase_timing_summary
from src.knowledge.search import collapse_vector_hits, reciprocal_rank_fusion
from src.knowledge.service import KnowledgeService
from src.models.knowledge import KnowledgeEvaluationRun
from src.models.post import Post


@dataclass(frozen=True, slots=True)
class EvaluationCase:
    id: str
    question: str
    expected_telegram_post_ids: frozenset[int]


@dataclass(frozen=True, slots=True)
class EvaluationRun:
    """Stable content-free aggregate record for a future injected evaluator."""

    phase: str
    metrics: EvaluationMetrics
    percentiles: dict[str, dict[str, float | int]]

    def record(self) -> dict[str, object]:
        if self.phase not in {"dev", "holdout"}:
            raise ExperimentError("evaluation phase must be dev or holdout")
        return {
            "phase": self.phase,
            "metrics": evaluation_metrics_record(self.metrics),
            "percentiles": self.percentiles,
        }


class CandidateEvaluator(Protocol):
    """Injection boundary for batch candidates; implementations stay process-local."""

    async def evaluate(self, *, phase: str, cases: Sequence[EvaluationCase]) -> EvaluationRun: ...


def evaluation_run(
    *,
    phase: str,
    metrics: EvaluationMetrics,
    phase_timings_ms: Mapping[str, Sequence[float]],
) -> EvaluationRun:
    """Build the common dev/holdout record without changing the legacy evaluator."""
    return EvaluationRun(phase, metrics, phase_timing_summary(phase_timings_ms))


def load_dataset(path: Path) -> tuple[list[EvaluationCase], str]:
    """Load manually labelled JSONL without admitting raw user-chat content."""
    return load_dataset_bytes(path.read_bytes())


def load_dataset_bytes(raw: bytes) -> tuple[list[EvaluationCase], str]:
    """Parse one already-read dataset snapshot without reopening its path."""
    cases = []
    for line_number, line in enumerate(raw.decode("utf-8").splitlines(), start=1):
        if not line.strip():
            continue
        value = json.loads(line)
        expected = value.get("expected_telegram_post_ids")
        if not isinstance(value.get("id"), str) or not isinstance(value.get("question"), str) or not isinstance(expected, list):
            raise ValueError(f"invalid evaluation case at line {line_number}")
        expected_ids = frozenset(int(post_id) for post_id in expected)
        if not value["question"].strip() or not expected_ids:
            raise ValueError(f"evaluation case at line {line_number} needs a question and expected posts")
        cases.append(EvaluationCase(value["id"], value["question"], expected_ids))
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
) -> dict[str, float | int | str]:
    """Evaluate hybrid parent retrieval and persist one aggregate audit row."""
    recalls: list[float] = []
    reciprocal_ranks: list[float] = []
    ndcgs: list[float] = []
    duplicate_shares: list[float] = []
    latencies: list[int] = []
    context_tokens: list[int] = []

    async with session_factory() as session:
        from src.knowledge.repository import KnowledgeRepository

        repo = KnowledgeRepository(session)
        catalog = await repo.get_catalog_channel_by_username(channel_username)
        if catalog is None:
            raise LookupError("knowledge catalog channel not found")
        for case in cases:
            started = time.monotonic()
            lexical = await repo.lexical_search(channel_ids=[catalog.channel_id], subscription_baselines={}, query=case.question)
            try:
                raw_vector_hits = await service._vector_search(case.question, {catalog.channel_id})
            except Exception:
                raw_vector_hits = []
            ranked = reciprocal_rank_fusion(lexical, collapse_vector_hits(raw_vector_hits))[:limit]
            ranked_ids = [item.post_id for item in ranked]
            posts = list((await session.execute(select(Post).where(Post.id.in_(ranked_ids)))).scalars()) if ranked_ids else []
            by_id = {post.id: post for post in posts}
            retrieved = [by_id[post_id].post_id for post_id in ranked_ids if post_id in by_id]
            relevant_ranks = [rank for rank, post_id in enumerate(retrieved, start=1) if post_id in case.expected_telegram_post_ids]
            recalls.append(len(set(retrieved) & case.expected_telegram_post_ids) / len(case.expected_telegram_post_ids))
            reciprocal_ranks.append(1 / relevant_ranks[0] if relevant_ranks else 0)
            dcg = sum(1 / math.log2(rank + 1) for rank in relevant_ranks)
            ideal = sum(1 / math.log2(rank + 1) for rank in range(1, min(limit, len(case.expected_telegram_post_ids)) + 1))
            ndcgs.append(dcg / ideal if ideal else 0)
            raw_top = raw_vector_hits[:limit]
            duplicate_shares.append((len(raw_top) - len({hit.post_id for hit in raw_top})) / len(raw_top) if raw_top else 0)
            latencies.append(int((time.monotonic() - started) * 1000))
            context_tokens.append(sum(estimate_tokens(by_id[post_id].content) for post_id in ranked_ids if post_id in by_id))

        record = KnowledgeEvaluationRun(
            knowledge_channel_id=catalog.id,
            index_version=service._settings.index_version,
            dataset_hash=dataset_hash,
            mode=f"hybrid_parent_rrf@{limit}",
            recall_at_k=sum(recalls) / len(recalls),
            mrr=sum(reciprocal_ranks) / len(reciprocal_ranks),
            ndcg=sum(ndcgs) / len(ndcgs),
            duplicate_source_share=sum(duplicate_shares) / len(duplicate_shares),
            latency_ms=round(sum(latencies) / len(latencies)),
            context_tokens=round(sum(context_tokens) / len(context_tokens)),
            cost=None,
        )
        session.add(record)
        await session.commit()

    return {
        "evaluation_id": record.id,
        "questions": len(cases),
        "recall_at_k": round(sum(recalls) / len(recalls), 6),
        "mrr": round(sum(reciprocal_ranks) / len(reciprocal_ranks), 6),
        "ndcg": round(sum(ndcgs) / len(ndcgs), 6),
        "duplicate_source_share": round(sum(duplicate_shares) / len(duplicate_shares), 6),
        "latency_ms": round(sum(latencies) / len(latencies)),
        "context_tokens": round(sum(context_tokens) / len(context_tokens)),
    }
