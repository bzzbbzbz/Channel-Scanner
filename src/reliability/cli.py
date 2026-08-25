"""Operator CLI for BL-22 stage-1 provisioning and readiness checks."""

from __future__ import annotations

import argparse
import asyncio
import json
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from src.config.settings import Settings, get_settings
from src.reliability.kafka_admin import check_cluster, check_topics, ensure_topics


async def check_database(settings: Settings) -> None:
    engine = create_async_engine(settings.database.url, pool_size=1, pool_recycle=settings.database.pool_recycle)
    try:
        async with engine.connect() as connection:
            await connection.execute(text("SELECT 1"))
    finally:
        await engine.dispose()


async def run_checks(
    settings: Settings,
    *,
    database: bool,
    kafka: bool,
    topics: bool,
) -> dict[str, Any]:
    """Run independent checks and return content-free status details."""
    result: dict[str, Any] = {"ok": True}
    if database:
        try:
            await check_database(settings)
            result["postgres"] = "ok"
        except Exception as exc:
            result.update(ok=False, postgres="failed", postgres_error=type(exc).__name__)

    if kafka or topics:
        try:
            report = await check_topics(settings.kafka) if topics else None
            if report is None:
                await check_cluster(settings.kafka)
            result["kafka"] = "ok"
            if topics and report is not None:
                result["topics"] = "ok" if report.ok else "failed"
                result["missing_topics"] = list(report.missing_topics)
                result["topic_mismatches"] = list(report.mismatches)
                result["ok"] = bool(result["ok"] and report.ok)
        except Exception as exc:
            result.update(ok=False, kafka="failed", kafka_error=type(exc).__name__)
            if topics:
                result["topics"] = "unknown"
    return result


async def _run(args: argparse.Namespace) -> int:
    settings = get_settings()
    if args.command == "ensure-topics":
        if not settings.kafka.enabled:
            print(json.dumps({"ok": False, "error": "kafka_disabled"}, sort_keys=True))
            return 1
        try:
            async with asyncio.timeout(settings.kafka.startup_timeout_seconds):
                report = await ensure_topics(settings.kafka)
            print(json.dumps({"ok": report.ok, "topics": "ok"}, sort_keys=True))
            return 0
        except Exception as exc:
            print(json.dumps({"ok": False, "error": type(exc).__name__}, sort_keys=True))
            return 1

    if args.command == "check":
        if (args.kafka or args.topics) and not settings.kafka.enabled:
            print(json.dumps({"ok": False, "error": "kafka_disabled"}, sort_keys=True))
            return 1
        result = await run_checks(
            settings,
            database=args.database,
            kafka=args.kafka,
            topics=args.topics,
        )
        print(json.dumps(result, sort_keys=True))
        return 0 if result["ok"] else 1
    return 2


def main() -> None:
    parser = argparse.ArgumentParser(description="BL-22 stage-1 Kafka operator commands")
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("ensure-topics", help="Create missing topics and verify exact configuration")
    check_parser = subparsers.add_parser("check", help="Run content-free dependency readiness checks")
    check_parser.add_argument("--database", action="store_true")
    check_parser.add_argument("--kafka", action="store_true")
    check_parser.add_argument("--topics", action="store_true")
    args = parser.parse_args()
    raise SystemExit(asyncio.run(_run(args)))


if __name__ == "__main__":
    main()
