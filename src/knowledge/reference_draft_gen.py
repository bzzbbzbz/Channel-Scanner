"""Generate reference answer drafts for BL-26 new benchmark cases.

Operator-only tool. It reads the approved case skeleton
(.planning/research/v2-new-cases.json), pulls the full canonical post
content from the live database, and asks the direct DeepSeek API
(deepseek-v4-flash) for a Telegram-safe HTML reference answer plus
expected_claims in the strict evaluation schema. Output goes to a local
draft file for operator review; nothing is persisted to product telemetry.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import time
from pathlib import Path

import httpx

_REFERENCE_SCHEMA = """Return JSON only (no Markdown fences) with this exact shape:
{
  "reference_answer_html": "One professional concise Russian answer, 2-4 sentences, directly answering the user's question. After each supporting statement put inline canonical links as <a href=\"https://t.me/turboproject/POST_ID\">[N]</a> in first-use order starting from [1]. Use only the supplied post ids.",
  "expected_claims": [
    {"id": "CASE_ID-1", "text": "One atomic statement directly supported by the cited posts.", "telegram_post_ids": [POST_ID]}
  ]
}
Rules:
- The answer must be a direct, self-contained answer "as asked from scratch"; never repeat the question wording.
- Every expected_telegram_post_id must appear in the answer links and be cited by at least one claim.
- Each claim cites one to three post ids and its text must be directly supported by those posts.
- Do not invent facts, do not mention ids that were not supplied, no Markdown."""


def _load_env(path: Path) -> None:
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, value = line.split("=", 1)
            os.environ.setdefault(key, value)


def _fetch_posts(post_ids: list[int], channel_id: int) -> dict[int, str]:
    ids = ", ".join(str(pid) for pid in post_ids)
    sql = (
        f"select post_id, content from posts "
        f"where channel_id={channel_id} and post_id in ({ids})"
    )
    result = subprocess.run(
        ["docker", "compose", "exec", "-T", "db", "psql", "-U", "bot", "-d", "telegram_bot", "-t", "-A", "-F", "\t", "-c", sql],
        capture_output=True,
        text=True,
        timeout=120,
    )
    if result.returncode != 0:
        raise RuntimeError(f"psql failed: {result.stderr}")
    out: dict[int, str] = {}
    for line in result.stdout.splitlines():
        if not line or "\t" not in line:
            continue
        pid, content = line.split("\t", 1)
        out[int(pid)] = content
    return out


def _call_deepseek(api_key: str, system: str, user: str, *, timeout: float) -> dict:
    with httpx.Client(timeout=timeout) as client:
        response = client.post(
            "https://api.deepseek.com/chat/completions",
            headers={"Authorization": f"Bearer {api_key}"},
            json={
                "model": "deepseek-v4-flash",
                "messages": [{"role": "system", "content": system}, {"role": "user", "content": user}],
                "temperature": 0.2,
                "response_format": {"type": "json_object"},
            },
        )
        response.raise_for_status()
        payload = response.json()
    content = payload["choices"][0]["message"]["content"]
    usage = payload.get("usage", {})
    return {
        "content": content,
        "input_tokens": usage.get("prompt_tokens", 0),
        "output_tokens": usage.get("completion_tokens", 0),
    }


def _extract_json(content: str) -> dict:
    match = re.search(r"\{.*\}", content, re.DOTALL)
    if not match:
        raise ValueError(f"no JSON in model output: {content[:300]}")
    return json.loads(match.group(0))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--skeleton", type=Path, default=Path(".planning/research/v2-new-cases.json"))
    parser.add_argument("--output", type=Path, default=Path(".planning/research/v2-reference-drafts.jsonl"))
    parser.add_argument("--channel-id", type=int, default=4)
    parser.add_argument("--limit", type=int, help="Generate drafts only for the first N cases (smoke)")
    args = parser.parse_args()

    _load_env(Path(".env"))
    api_key = os.environ.get("DEEPSEEK_API_KEY", "")
    if not api_key:
        raise SystemExit("DEEPSEEK_API_KEY is not set")

    cases = json.loads(args.skeleton.read_text(encoding="utf-8"))
    if args.limit:
        cases = cases[: args.limit]

    with args.output.open("w", encoding="utf-8") as handle:
        for index, case in enumerate(cases, start=1):
            print(f"[{index}/{len(cases)}] {case['id']}", flush=True)
            posts = _fetch_posts(case["posts"], args.channel_id)
            missing = [pid for pid in case["posts"] if pid not in posts]
            if missing:
                raise RuntimeError(f"case {case['id']}: posts missing from DB: {missing}")
            source_block = "\n\n---\n\n".join(
                f"POST {pid}:\n{posts[pid]}" for pid in case["posts"]
            )
            user = (
                f"Question: {case['question']}\n"
                f"Expected post ids: {case['posts']}\n\n"
                f"Source posts:\n{source_block}"
            )
            result = _call_deepseek(api_key, _REFERENCE_SCHEMA, user, timeout=120.0)
            parsed = _extract_json(result["content"])
            claims = _normalize_claims(case["id"], case["posts"], parsed.get("expected_claims", []))
            reference = parsed.get("reference_answer_html", "")
            draft = {
                "id": case["id"],
                "question": case["question"],
                "expected_telegram_post_ids": case["posts"],
                "split": "eval" if case.get("eval") else "dev",
                "src": case.get("src"),
                "reference_answer_html": reference,
                "expected_claims": claims,
                "draft_input_tokens": result["input_tokens"],
                "draft_output_tokens": result["output_tokens"],
            }
            handle.write(json.dumps(draft, ensure_ascii=False) + "\n")
            handle.flush()
            time.sleep(0.5)
    print(f"drafts written to {args.output}")


def _normalize_claims(case_id: str, expected_posts: list[int], raw_claims: list) -> list[dict]:
    """Rename claim ids to <case_id>-N and reorder claims so their post union
    reproduces the expected post order (required by the evaluation loader)."""
    normalized: list[dict] = []
    for index, claim in enumerate(raw_claims, start=1):
        post_ids = [int(pid) for pid in claim.get("telegram_post_ids", []) if isinstance(pid, int)]
        normalized.append(
            {
                "id": f"{case_id}-{index}",
                "text": str(claim.get("text", "")).strip(),
                "telegram_post_ids": post_ids,
            }
        )
    # Reorder claims so their union reproduces exactly the expected post order.
    # Try permutations (datasets are small: at most a handful of claims) and
    # fall back to the model's original order if no permutation matches.
    import itertools

    def union_of(claims: list[dict]) -> list[int]:
        result: list[int] = []
        for claim in claims:
            for post_id in claim["telegram_post_ids"]:
                if post_id not in result:
                    result.append(post_id)
        return result

    best: list[dict] | None = None
    if len(normalized) <= 8:
        for permutation in itertools.permutations(normalized):
            if union_of(list(permutation)) == expected_posts:
                best = list(permutation)
                break
    if best is None and union_of(normalized) == expected_posts:
        best = normalized
    if best is None and len(normalized) == 1:
        best = normalized
    return best if best is not None else normalized


if __name__ == "__main__":
    main()
