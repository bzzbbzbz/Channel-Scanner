"""Scheduled legacy digest delivery entrypoint."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from sqlalchemy.ext.asyncio import async_sessionmaker

from src.digest.service import DigestService

if TYPE_CHECKING:
    from src.assistant.memory import AssistantMemoryService
    from src.llm import OpenRouterModelPool

logger = logging.getLogger(__name__)


async def digest_delivery_job(
    session_factory: async_sessionmaker,
    bot_token: str,
    llm_settings: Any,
    model_pool: OpenRouterModelPool | None = None,
    memory_service: AssistantMemoryService | None = None,
    reliable_delivery=None,
    sender=None,
    now: datetime | None = None,
) -> int:
    """Deliver scheduled digests to users through the Bot API."""
    delivered_users = await DigestService(
        session_factory,
        bot_token,
        llm_settings=llm_settings,
        model_pool=model_pool,
        memory_service=memory_service,
        reliable_delivery=reliable_delivery,
        sender=sender,
    ).run_once(
        now=now or datetime.now(timezone.utc),
    )
    logger.info("Digest delivery cycle complete: %d users served", delivered_users)
    return delivered_users
