"""Ephemeral dashboard server used only by Playwright browser regression tests."""

from __future__ import annotations

import asyncio

import uvicorn
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

import src.models  # noqa: F401
from src.admin.app import create_admin_app
from src.admin.passwords import hash_password
from src.config.settings import AdminSettings
from src.models.base import Base


async def create_test_app():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    return create_admin_app(
        AdminSettings(
            enabled=True,
            username="admin",
            password_hash=hash_password("playwright-password"),
            session_secret="playwright-session-secret",
            secure_cookies=False,
        ),
        session_factory,
    )


if __name__ == "__main__":
    uvicorn.run(asyncio.run(create_test_app()), host="127.0.0.1", port=4173, log_level="warning")
