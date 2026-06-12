"""Independent mem0 memory layer around assistant turns."""

from __future__ import annotations

import asyncio
import logging
import os
from pathlib import Path
from typing import Any

from src.config.settings import LlmSettings, MemorySettings
from src.llm import MODEL_FALLBACK_CHAIN
from src.models.user import User

logger = logging.getLogger(__name__)

MEMORY_TRIGGERS = (
    "запомни",
    "не показывай",
    "неинтерес",
    "не интерес",
    "пропускай",
    "исключай",
    "предпочитаю",
    "remember",
    "don't show",
    "do not show",
    "not interested",
    "prefer",
    "skip",
)


class AssistantMemoryService:
    """Retrieve and update long-term memories without exposing memory tools to the assistant."""

    def __init__(self, settings: MemorySettings, llm_settings: LlmSettings) -> None:
        self._settings = settings
        self._llm_settings = llm_settings
        self._memory: Any | None = None
        self._available = False
        if settings.enabled:
            self._initialize()

    @property
    def available(self) -> bool:
        return self._available

    async def retrieve(self, user: User, query: str, limit: int = 5) -> list[str]:
        if not self._available or self._memory is None or not query.strip():
            return []
        try:
            result = await asyncio.to_thread(
                self._memory.search,
                query=query,
                filters={"user_id": str(user.id)},
                top_k=limit,
            )
        except Exception:
            logger.warning("mem0 memory search failed", exc_info=True)
            return []
        memories = result.get("results", result) if isinstance(result, dict) else result
        return [str(item.get("memory", "")).strip() for item in memories or [] if isinstance(item, dict) and item.get("memory")]

    async def extract_after_turn(
        self,
        *,
        user: User,
        user_message: str,
        assistant_message: str,
        system_messages: list[str],
    ) -> list[str]:
        if not self._available or self._memory is None:
            return []
        if not self._should_extract(user_message, assistant_message, system_messages):
            return []

        messages = [{"role": "user", "content": user_message}]
        for message in system_messages:
            messages.append({"role": "assistant", "content": f"System confirmation: {message}"})
        if assistant_message:
            messages.append({"role": "assistant", "content": assistant_message})

        try:
            result = await asyncio.to_thread(
                self._memory.add,
                messages,
                user_id=str(user.id),
                metadata={"telegram_user_id": user.telegram_user_id, "source": "telegram_bot_turn"},
            )
        except Exception:
            logger.warning("mem0 memory add failed", exc_info=True)
            return []

        results = result.get("results", result) if isinstance(result, dict) else result
        added = [item for item in results or [] if isinstance(item, dict) and item.get("memory") and item.get("event") in {None, "ADD"}]
        if not added:
            return []
        confirmations: list[str] = []
        for item in added[:3]:
            memory = str(item["memory"]).strip()
            if user.language == "ru":
                confirmations.append(f"Запомнил: {memory}")
            else:
                confirmations.append(f"Remembered: {memory}")
        return confirmations

    def _initialize(self) -> None:
        if not self._llm_settings.openrouter_api_key:
            logger.warning("mem0 memory disabled: OPENROUTER_API_KEY is empty")
            return

        try:
            from mem0 import Memory
        except Exception:
            logger.warning("mem0 memory disabled: mem0ai package is unavailable", exc_info=True)
            return

        os.environ.setdefault("MEM0_TELEMETRY", "false")
        qdrant_path = Path(self._settings.qdrant_path)
        history_db_path = Path(self._settings.history_db_path)
        qdrant_path.mkdir(parents=True, exist_ok=True)
        history_db_path.parent.mkdir(parents=True, exist_ok=True)

        config = {
            "vector_store": {
                "provider": "qdrant",
                "config": {
                    "collection_name": self._settings.collection_name,
                    "path": str(qdrant_path),
                    "embedding_model_dims": self._settings.embedding_dims,
                    "on_disk": True,
                },
            },
            "llm": {
                "provider": "openai",
                "config": {
                    "api_key": self._llm_settings.openrouter_api_key,
                    "model": MODEL_FALLBACK_CHAIN[0],
                    "models": MODEL_FALLBACK_CHAIN,
                    "route": "fallback",
                    "openai_base_url": self._llm_settings.openrouter_base_url,
                    "openrouter_base_url": self._llm_settings.openrouter_base_url,
                    "temperature": 0.1,
                },
            },
            "embedder": {
                "provider": "openai",
                "config": {
                    "api_key": self._llm_settings.openrouter_api_key,
                    "model": self._settings.embedder_model,
                    "openai_base_url": self._llm_settings.openrouter_base_url,
                },
            },
            "history_db_path": str(history_db_path),
        }

        try:
            self._memory = Memory.from_config(config) if hasattr(Memory, "from_config") else Memory(config)
            self._available = True
            logger.info("mem0 memory initialized with embedder=%s", self._settings.embedder_model)
        except Exception:
            logger.warning("mem0 memory disabled: initialization failed", exc_info=True)

    def _should_extract(self, user_message: str, assistant_message: str, system_messages: list[str]) -> bool:
        del assistant_message, system_messages
        normalized = user_message.lower()
        return any(trigger in normalized for trigger in MEMORY_TRIGGERS)
