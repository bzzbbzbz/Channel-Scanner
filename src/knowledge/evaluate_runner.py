"""Operator-only runner for a manually labelled knowledge retrieval dataset."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
from pathlib import Path

from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from src.config.settings import get_settings
from src.knowledge.evaluation import evaluate_catalog, load_answer_audit, load_dataset
from src.knowledge.judge import build_judge
from src.knowledge.service import KnowledgeService
from src.llm import OpenRouterModelPool


def _selected_dataset_hash(cases, limit: int | None, full_hash: str) -> str:
    """Bind a smoke run to exactly its selected reviewed cases."""
    if limit is None:
        return full_hash
    payload = [
        {
            "id": case.id,
            "question": case.question,
            "expected_telegram_post_ids": sorted(case.expected_telegram_post_ids),
            "relevance_complete": case.relevance_complete,
            "answer_expected": case.answer_expected,
            "reference_answer_html": case.reference_answer_html,
            "expected_claims": case.expected_claims,
            "split": case.split,
        }
        for case in cases
    ]
    return hashlib.sha256(json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()


async def run(
    path: Path,
    username: str,
    candidate: bool,
    answer_audit_path: Path | None,
    limit: int | None,
    checkpoint_path: Path | None,
    split: str | None,
    use_judge: bool,
    print_samples_path: Path | None,
    rerank_limit: int | None,
    direct: bool,
    concurrency: int = 1,
) -> None:
    settings = get_settings()
    if rerank_limit is not None:
        if not candidate:
            raise SystemExit("--rerank-limit requires --candidate")
        if not 1 <= rerank_limit <= 20:
            raise SystemExit("--rerank-limit must be between 1 and 20")
        settings.knowledge.rag_rerank_candidate_limit = rerank_limit
        settings.knowledge.rag_configuration_id = f"bl24-rerank{rerank_limit}-partial"
    if direct:
        settings.knowledge.answer_direct_enabled = True
        if not settings.knowledge.deepseek_api_key:
            raise SystemExit("--direct requires KNOWLEDGE_DEEPSEEK_API_KEY")
    cases, full_dataset_hash = load_dataset(path)
    if split:
        cases = [case for case in cases if case.split == split]
        if not cases:
            raise ValueError(f"--split {split} selected no cases")
    if limit is not None:
        if limit < 1 or limit > len(cases):
            raise ValueError(f"--limit must be between 1 and {len(cases)}")
        cases = cases[:limit]
    dataset_hash = _selected_dataset_hash(cases, limit, full_dataset_hash)
    answer_audit = load_answer_audit(answer_audit_path, dataset_hash) if answer_audit_path else None
    engine = create_async_engine(settings.database.url)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        service = KnowledgeService(session_factory, settings.knowledge, settings.llm, OpenRouterModelPool(settings.llm))
        judge = None
        if use_judge:
            judge = build_judge(settings.knowledge)
            if judge is None:
                raise SystemExit("--judge requires KNOWLEDGE_DEEPSEEK_API_KEY")
        result = await evaluate_catalog(
            session_factory,
            service,
            channel_username=username,
            cases=cases,
            dataset_hash=dataset_hash,
            candidate=candidate,
            answer_audit=answer_audit,
            evaluate_answers=True,
            checkpoint_path=checkpoint_path,
            judge=judge,
            concurrency=concurrency,
        )
        print(json.dumps(result, ensure_ascii=False), flush=True)
        if print_samples_path:
            await _print_samples(print_samples_path, cases)
    finally:
        await engine.dispose()


async def _print_samples(path: Path, cases) -> None:
    """Write a local operator-review file with the exact questions evaluated.

    The file intentionally holds only the reviewed dataset cases (questions,
    expected posts, split), never generated answers, prompts, or raw sources;
    it documents which cases a run measured.  If the requested path is not
    writable (read-only dataset mount), fall back to the writable runtime
    directory and report the actual path.
    """
    rows = [
        {
            "id": case.id,
            "question": case.question,
            "expected_telegram_post_ids": sorted(case.expected_telegram_post_ids),
            "split": case.split,
            "answer_expected": case.answer_expected,
        }
        for case in cases
    ]
    payload = json.dumps(rows, ensure_ascii=False, indent=1)
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(payload, encoding="utf-8")
        print(f"samples written to {path}", flush=True)
        return
    except OSError:
        fallback = Path("/app/.data") / path.name
        fallback.parent.mkdir(parents=True, exist_ok=True)
        fallback.write_text(payload, encoding="utf-8")
        print(f"samples path read-only; written to {fallback}", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("dataset", type=Path)
    parser.add_argument("--username", required=True)
    parser.add_argument("--candidate", action="store_true", help="Evaluate the disabled-by-default BL-21 candidate without serving user traffic")
    parser.add_argument("--answer-audit", type=Path, help="Optional content-free JSON aggregate for a separately reviewed answer sample")
    parser.add_argument("--limit", type=int, help="Run only the first N reviewed cases as a reproducible smoke check")
    parser.add_argument("--checkpoint", type=Path, help="Content-free, atomic progress checkpoint; rerunning resumes the exact same evaluation")
    parser.add_argument("--split", choices=("dev", "eval"), help="Run only one dataset split")
    parser.add_argument("--judge", action="store_true", help="Run the content-free DeepSeek semantic claim judge")
    parser.add_argument("--print-samples", type=Path, help="Write the reviewed cases measured by this run to a local file")
    parser.add_argument("--rerank-limit", type=int, help="Override the candidate rerank candidate limit (1..20, requires --candidate)")
    parser.add_argument("--direct", action="store_true", help="Use the direct DeepSeek API for grounded answers instead of OpenRouter")
    parser.add_argument("--concurrency", type=int, default=1, help="Maximum concurrent evaluation cases")
    args = parser.parse_args()
    asyncio.run(
        run(
            args.dataset,
            args.username,
            args.candidate,
            args.answer_audit,
            args.limit,
            args.checkpoint,
            args.split,
            args.judge,
            args.print_samples,
            args.rerank_limit,
            args.direct,
            args.concurrency,
        )
    )


if __name__ == "__main__":
    main()
