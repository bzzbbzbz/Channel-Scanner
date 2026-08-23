"""Score a judge prompt against a fixed, operator-labelled calibration file.

Reads pairs from a calibration JSONL (generated_claim, expected_claim,
operator-derived truth), runs the DeepSeek semantic judge on those exact
texts, and reports agreement. This isolates prompt quality from answer
generation randomness because the pairs never change between prompt runs.
"""

from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path

from src.config.settings import get_settings
from src.knowledge.judge import SemanticJudge
from src.llm.deepseek import DeepSeekClient


def _derive_truth(row: dict) -> bool | None:
    """Recover the true equivalence from the operator's v1 label.

    operator_agrees=True means the recorded judge verdict was correct, so the
    verdict is the truth; operator_agrees=False means it was wrong, so the
    truth is the opposite (or None when the verdict itself was None).
    """
    op = row.get("operator_agrees")
    verdict = row.get("judge_verdict")
    if op is True:
        return verdict
    if op is False:
        return not verdict if verdict is not None else None
    return None


async def run(calibration: Path, output: Path) -> None:
    settings = get_settings()
    rows = [json.loads(line) for line in calibration.read_text(encoding="utf-8").splitlines() if line.strip()]
    client = DeepSeekClient(
        settings.knowledge.deepseek_api_key,
        settings.knowledge.deepseek_base_url,
        telemetry_recorder=None,
    )
    judge = SemanticJudge(settings.knowledge, client)
    results: list[dict] = []
    try:
        for index, row in enumerate(rows, start=1):
            truth = _derive_truth(row)
            verdict = await judge.equivalence(
                row["generated_claim"], tuple(row["generated_post_ids"]),
                row["expected_claim"], tuple(row["expected_post_ids"]),
            )
            results.append({
                "case_id": row["case_id"],
                "generated_claim": row["generated_claim"],
                "expected_claim": row["expected_claim"],
                "judge_verdict": verdict,
                "truth": truth,
                "correct": (truth == verdict) if (truth is not None and verdict is not None) else None,
            })
            print(f"[{index}/{len(rows)}] {row['case_id']} verdict={verdict} truth={truth}", flush=True)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text("\n".join(json.dumps(r, ensure_ascii=False) for r in results), encoding="utf-8")
        decided = [r for r in results if r["correct"] is not None]
        correct = sum(1 for r in decided if r["correct"])
        print(f"scored: {correct}/{len(decided)} = {correct/len(decided):.3f} (on decided pairs)", flush=True)
        print(f"verdict distribution: {json.dumps({str(v): sum(1 for r in results if r['judge_verdict'] == v) for v in (True, False, None)})}", flush=True)
    finally:
        await client.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("calibration", type=Path, help="Operator-labelled calibration JSONL")
    parser.add_argument("--output", type=Path, default=Path(".data/judge-prompt-scored.jsonl"))
    args = parser.parse_args()
    asyncio.run(run(args.calibration, args.output))


if __name__ == "__main__":
    main()
