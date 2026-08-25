"""Lifecycle wrapper for the optional dashboard server in the main process."""

from __future__ import annotations

import asyncio
import logging

import uvicorn

from src.admin.app import create_admin_app
from src.config.settings import (
    AdminSettings,
    KafkaSettings,
    KnowledgeSettings,
    MemorySettings,
    ReliableDeliverySettings,
)

logger = logging.getLogger(__name__)


class AdminRuntime:
    """Run the optional read-only dashboard alongside bot polling and scheduling."""

    def __init__(
        self,
        settings: AdminSettings,
        session_factory,
        knowledge_settings: KnowledgeSettings | None = None,
        *,
        max_event_bytes: int = 65_536,
        kafka_settings: KafkaSettings | None = None,
        reliable_settings: ReliableDeliverySettings | None = None,
        memory_settings: MemorySettings | None = None,
        kafka_probe=None,
    ) -> None:
        self._settings = settings
        self._session_factory = session_factory
        self._knowledge_settings = knowledge_settings
        self._max_event_bytes = max_event_bytes
        self._kafka_settings = kafka_settings
        self._reliable_settings = reliable_settings
        self._memory_settings = memory_settings
        self._kafka_probe = kafka_probe
        self._server: uvicorn.Server | None = None
        self._task: asyncio.Task | None = None

    async def start(self) -> None:
        app = create_admin_app(
            self._settings,
            self._session_factory,
            self._knowledge_settings,
            max_event_bytes=self._max_event_bytes,
            kafka_settings=self._kafka_settings,
            reliable_settings=self._reliable_settings,
            memory_settings=self._memory_settings,
            kafka_probe=self._kafka_probe,
        )
        self._server = uvicorn.Server(
            uvicorn.Config(app, host=self._settings.host, port=self._settings.port, log_level="warning")
        )
        self._task = asyncio.create_task(self._server.serve(), name="admin-dashboard")
        logger.info("Admin dashboard started on %s:%s", self._settings.host, self._settings.port)

    async def shutdown(self) -> None:
        if self._server is not None:
            self._server.should_exit = True
        if self._task is not None:
            await self._task
