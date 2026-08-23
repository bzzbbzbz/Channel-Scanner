"""Content-free semantic claim judge for offline RAG evaluation.

The judge compares a generated claim against a reviewed reference claim and
returns True (equivalent), False (not equivalent), or None when it cannot
decide (model failure, invalid output, or a transport error).  It receives
only claim text and canonical telegram post ids; it never sees raw source
posts, questions, prompts, or user data.  Verdicts are aggregated only.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Awaitable, Callable
from typing import Any

from src.config.settings import KnowledgeSettings
from src.llm.deepseek import DeepSeekClient

logger = logging.getLogger(__name__)

_CLAIM_JUDGE_SYSTEM = (
    "You compare two short statements about the same Telegram channel content. "
    "Return JSON without Markdown fences: {\"equivalent\": true|false}. "
    "Decide by coverage of essential aspects, not by wording. The essential aspects of a statement "
    "are its central assertion plus any negation, limitation, scope, mapping, or second thesis it "
    "carries. "
    "equivalent=true when both statements carry the same essential aspects: the second statement "
    "covers all essential aspects of the first and only adds or rephrases details that do not change "
    "or narrow them. A paraphrase that keeps the same facts, numbers, and qualifiers is equivalent; "
    "adding explanatory detail on top of the same central assertion is still equivalent. The cited "
    "post ids must overlap (order irrelevant). "
    "equivalent=false when the essential aspects differ: (1) one statement omits a negation, "
    "limitation, scope, mapping, or second thesis that the other carries (for example 'X is useful' "
    "vs 'X is useful but has limits', 'tool replaces review' vs 'tool complements review', 'describes "
    "a shared space' vs 'describes a shared space and maps it to hub-and-spoke', 'Claude writes v1 "
    "and DeepSeek debugs' vs also 'the code must be handed over with its embedded documentation'); "
    "(2) one statement asserts a specific fact, number, or mechanism the other does not support; "
    "(3) the claims contradict each other. "
    "Be accurate rather than generous: reward paraphrases that keep the same essential aspects, but a "
    "claim that omits or narrows an essential aspect of the other is not the same point. When "
    "genuinely unable to decide, return null."
)


class SemanticJudge:
    """Bounded DeepSeek-backed judge for offline claim evaluation."""

    def __init__(self, settings: KnowledgeSettings, client: DeepSeekClient) -> None:
        self._settings = settings
        self._client = client

    async def equivalence(
        self,
        generated_text: str,
        generated_telegram_ids: tuple[int, ...],
        expected_text: str,
        expected_telegram_ids: tuple[int, ...],
    ) -> bool | None:
        payload = {
            "generated_statement": generated_text,
            "generated_post_ids": list(generated_telegram_ids),
            "reference_statement": expected_text,
            "reference_post_ids": list(expected_telegram_ids),
        }
        try:
            text = await self._client.chat_completion(
                self._settings.judge_model,
                _CLAIM_JUDGE_SYSTEM,
                json.dumps(payload, ensure_ascii=False),
                response_format={"type": "json_object"},
                use_case="knowledge_answer_judge",
            )
        except Exception as exc:
            logger.info("Semantic judge unavailable for a claim pair", exc_info=exc)
            return None
        return _parse_verdict(text)


def _parse_verdict(text: str) -> bool | None:
    try:
        value = json.loads(text)
    except Exception:
        return None
    if not isinstance(value, dict) or "equivalent" not in value:
        return None
    verdict = value.get("equivalent")
    if not isinstance(verdict, bool):
        return None
    return verdict


def build_judge(settings: KnowledgeSettings, client: DeepSeekClient | None = None) -> Callable[[str, tuple[int, ...], str, tuple[int, ...]], Awaitable[bool | None]] | None:
    """Return a judge callable, or None when no direct DeepSeek key is configured."""
    if not settings.deepseek_api_key:
        return None
    return SemanticJudge(settings, client or DeepSeekClient(settings.deepseek_api_key, settings.deepseek_base_url)).equivalence
