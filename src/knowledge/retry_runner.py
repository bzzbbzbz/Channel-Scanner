"""Operator-only runner for bounded knowledge enrichment and indexing retries."""

from __future__ import annotations

import argparse
import asyncio

from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from src.config.settings import get_settings
from src.knowledge.service import KnowledgeService
from src.llm import OpenRouterModelPool


async def run(username: str) -> None:
    settings = get_settings()
    engine = create_async_engine(settings.database.url)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        service = KnowledgeService(session_factory, settings.knowledge, settings.llm, OpenRouterModelPool(settings.llm))
        attempted, completed = await service.retry_failed_indexing(username)
        print(f"attempted={attempted} completed={completed}", flush=True)
    finally:
        await engine.dispose()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--username", required=True)
    args = parser.parse_args()
    asyncio.run(run(args.username))


if __name__ == "__main__":
    main()
