"""Lifecycle wrapper for the optional dashboard server in the main process."""

from __future__ import annotations

import asyncio
import logging

import uvicorn

from src.admin.app import create_admin_app
from src.config.settings import AdminSettings, KnowledgeSettings

logger = logging.getLogger(__name__)


class AdminRuntime:
    """Run the optional read-only dashboard alongside bot polling and scheduling."""

    def __init__(self, settings: AdminSettings, session_factory, knowledge_settings: KnowledgeSettings | None = None) -> None:
        self._settings = settings
        self._session_factory = session_factory
        self._knowledge_settings = knowledge_settings
        self._server: uvicorn.Server | None = None
        self._task: asyncio.Task | None = None

    async def start(self) -> None:
        app = create_admin_app(self._settings, self._session_factory, self._knowledge_settings)
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
