"""Operator-only runner for an approved Telegram JSON knowledge import."""

from __future__ import annotations

import argparse
import asyncio
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from src.config.settings import get_settings
from src.knowledge.importer import parse_official_export
from src.knowledge.service import KnowledgeService
from src.llm import OpenRouterModelPool
from src.models.user import User


async def run(path: Path, username: str, administrator_telegram_id: int, *, preflight: bool, start_at: datetime | None, workers: int, destructive_reset: bool) -> None:
    settings = get_settings()
    raw = path.read_bytes()
    posts = parse_official_export(raw, settings.knowledge.import_max_bytes)
    if start_at is not None:
        posts = [post for post in posts if post.published_at >= start_at]
    token_estimate = sum(len(post.content.split()) for post in posts)
    print(f"validated_posts={len(posts)} token_estimate={token_estimate}", flush=True)
    if preflight:
        return

    engine = create_async_engine(settings.database.url)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with session_factory() as session:
            user = (await session.execute(select(User).where(User.telegram_user_id == administrator_telegram_id))).scalar_one_or_none()
        if user is None:
            raise LookupError("configured knowledge administrator has no registered bot user")
        service = KnowledgeService(session_factory, settings.knowledge, settings.llm, OpenRouterModelPool(settings.llm))
        request_id, _ = await service.request_channel(user, username)
        await service.approve_request(administrator_telegram_id, request_id, approved=True)
        import_id = await service.queue_import(administrator_telegram_id, username, path.name, raw)
        print(f"import_id={import_id} status=running", flush=True)
        await service.process_import(import_id, raw, start_at=start_at, concurrency=workers, destructive_reset=destructive_reset)
        print(f"import_id={import_id} status=completed", flush=True)
    finally:
        await engine.dispose()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("path", type=Path)
    parser.add_argument("--username", required=True)
    parser.add_argument("--administrator", type=int, required=True)
    parser.add_argument("--preflight", action="store_true")
    parser.add_argument("--start-date", help="Inclusive UTC date in YYYY-MM-DD format")
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--destructive-reset", action="store_true")
    args = parser.parse_args()
    start_at = datetime.fromisoformat(args.start_date).replace(tzinfo=timezone.utc) if args.start_date else None
    asyncio.run(run(args.path, args.username, args.administrator, preflight=args.preflight, start_at=start_at, workers=args.workers, destructive_reset=args.destructive_reset))


if __name__ == "__main__":
    main()
