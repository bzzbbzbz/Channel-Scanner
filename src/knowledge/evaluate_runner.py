"""Operator-only runner for a manually labelled knowledge retrieval dataset."""

from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path

from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from src.config.settings import get_settings
from src.knowledge.evaluation import evaluate_catalog, load_dataset
from src.knowledge.service import KnowledgeService
from src.llm import OpenRouterModelPool


async def run(path: Path, username: str) -> None:
    settings = get_settings()
    cases, dataset_hash = load_dataset(path)
    engine = create_async_engine(settings.database.url)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        service = KnowledgeService(session_factory, settings.knowledge, settings.llm, OpenRouterModelPool(settings.llm))
        result = await evaluate_catalog(session_factory, service, channel_username=username, cases=cases, dataset_hash=dataset_hash)
        print(json.dumps(result, ensure_ascii=False), flush=True)
    finally:
        await engine.dispose()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("dataset", type=Path)
    parser.add_argument("--username", required=True)
    args = parser.parse_args()
    asyncio.run(run(args.dataset, args.username))


if __name__ == "__main__":
    main()
