"""Read-only aggregate queries backing the administration dashboard."""

from __future__ import annotations

import asyncio
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import re
from typing import Any
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import async_sessionmaker

from src.models.channel import Channel, ChannelStatus
from src.models.chat_message import ChatMessage
from src.models.dead_letter import DeadLetterRecord, DeadLetterReplay
from src.models.digest_delivery import DigestDelivery
from src.models.digest_processing_log import DigestProcessingLog
from src.models.llm_usage import LlmUsage
from src.models.knowledge import KnowledgeChannel, KnowledgeChannelRequest, KnowledgeEvaluationRun, KnowledgeImport, KnowledgeQuery, KnowledgeRequestStatus
from src.models.outbox_event import OutboxEvent
from src.config.settings import KafkaSettings, KnowledgeSettings, MemorySettings, ReliableDeliverySettings
from src.models.post import Post
from src.models.reliable_digest import DigestOutboxMessage, DigestRun, InboxEvent
from src.models.reliability_role_heartbeat import ReliabilityRoleHeartbeat
from src.models.subscription import Subscription
from src.models.user import User
from src.reliability.kafka_operations import KafkaOperationsProbe, unavailable_kafka_report

_RELIABILITY_ROLES = ("scheduler", "outbox-relay", "digest-worker", "telegram-delivery-worker")
_SAFE_ERROR_CODE = re.compile(r"^[A-Za-z0-9_.:-]{1,128}$")


class AdminDashboardService:
    """Aggregate product data without exposing or modifying raw user content."""

    def __init__(
        self,
        session_factory: async_sessionmaker,
        knowledge_settings: KnowledgeSettings | None = None,
        kafka_settings: KafkaSettings | None = None,
        reliable_settings: ReliableDeliverySettings | None = None,
        memory_settings: MemorySettings | None = None,
        *,
        kafka_probe: Any | None = None,
    ) -> None:
        self._session_factory = session_factory
        self._knowledge_settings = knowledge_settings
        self._kafka_settings = kafka_settings or KafkaSettings()
        self._reliable_settings = reliable_settings or ReliableDeliverySettings()
        self._memory_settings = memory_settings or MemorySettings()
        self._kafka_probe = kafka_probe if kafka_probe is not None else KafkaOperationsProbe(self._kafka_settings)

    async def first_event_at(self) -> datetime | None:
        """Return the earliest timestamp that can appear in dashboard charts."""
        async with self._session_factory() as session:
            values = [
                (await session.execute(select(func.min(User.created_at)))).scalar_one(),
                (await session.execute(select(func.min(Subscription.created_at)))).scalar_one(),
                (await session.execute(select(func.min(Channel.created_at)))).scalar_one(),
                (await session.execute(select(func.min(Post.created_at)))).scalar_one(),
                (await session.execute(select(func.min(ChatMessage.created_at)))).scalar_one(),
                (await session.execute(select(func.min(DigestDelivery.delivered_at)))).scalar_one(),
                (await session.execute(select(func.min(DigestProcessingLog.completed_at)))).scalar_one(),
                (await session.execute(select(func.min(LlmUsage.created_at)))).scalar_one(),
                (await session.execute(select(func.min(KnowledgeChannel.created_at)))).scalar_one(),
                (await session.execute(select(func.min(KnowledgeQuery.created_at)))).scalar_one(),
                (await session.execute(select(func.min(KnowledgeEvaluationRun.created_at)))).scalar_one(),
                (await session.execute(select(func.min(DeadLetterRecord.first_failed_at)))).scalar_one(),
            ]
        first = min((value for value in values if value is not None), default=None)
        if first is not None and first.tzinfo is None:
            first = first.replace(tzinfo=timezone.utc)
        return first.astimezone(timezone.utc) if first is not None else None

    async def metrics(self, start: datetime, end: datetime) -> dict[str, Any]:
        """Return the dashboard snapshot for a UTC half-open time range."""
        duration = end - start
        bucket = "hour" if duration <= timedelta(hours=24) else ("day" if duration.days <= 90 else "month")
        async with self._session_factory() as session:
            users_total = await _count(session, select(func.count(User.id)))
            subscriptions_total = await _count(session, select(func.count(Subscription.id)))
            channels_total = await _count(session, select(func.count(Channel.id)))
            current_channel_errors = await _count(
                session,
                select(func.count(Channel.id)).where(Channel.status == ChannelStatus.ERROR),
            )
            knowledge_catalog_channels = await _count(session, select(func.count(KnowledgeChannel.id)))
            knowledge_pending_requests = await _count(session, select(func.count(KnowledgeChannelRequest.id)).where(KnowledgeChannelRequest.status == KnowledgeRequestStatus.PENDING))
            knowledge_imports = await _count(session, select(func.count(KnowledgeImport.id)).where(KnowledgeImport.created_at >= start, KnowledgeImport.created_at < end))
            knowledge_queries = await _count(session, select(func.count(KnowledgeQuery.id)).where(KnowledgeQuery.created_at >= start, KnowledgeQuery.created_at < end))
            rag_evaluations = (await session.execute(
                select(
                    Channel.username,
                    KnowledgeEvaluationRun.mode,
                    KnowledgeEvaluationRun.recall_at_k,
                    KnowledgeEvaluationRun.mrr,
                    KnowledgeEvaluationRun.ndcg,
                    KnowledgeEvaluationRun.duplicate_source_share,
                    KnowledgeEvaluationRun.precision_at_k,
                    KnowledgeEvaluationRun.question_count,
                    KnowledgeEvaluationRun.labels_complete,
                    KnowledgeEvaluationRun.configuration_id,
                    KnowledgeEvaluationRun.reranker_model,
                    KnowledgeEvaluationRun.rerank_fallback_share,
                    KnowledgeEvaluationRun.correct_abstention_share,
                    KnowledgeEvaluationRun.false_attribution_share,
                    KnowledgeEvaluationRun.source_sufficiency_share,
                    KnowledgeEvaluationRun.faithfulness,
                    KnowledgeEvaluationRun.citation_validity,
                    KnowledgeEvaluationRun.citation_completeness,
                    KnowledgeEvaluationRun.answer_relevance,
                    KnowledgeEvaluationRun.answer_audit_sample_size,
                    KnowledgeEvaluationRun.judge_version,
                    KnowledgeEvaluationRun.p50_latency_ms,
                    KnowledgeEvaluationRun.p95_latency_ms,
                    KnowledgeEvaluationRun.p99_latency_ms,
                    KnowledgeEvaluationRun.latency_ms,
                    KnowledgeEvaluationRun.p50_retrieval_latency_ms,
                    KnowledgeEvaluationRun.p95_retrieval_latency_ms,
                    KnowledgeEvaluationRun.p99_retrieval_latency_ms,
                    KnowledgeEvaluationRun.retrieval_latency_ms,
                    KnowledgeEvaluationRun.p50_answer_generation_ms,
                    KnowledgeEvaluationRun.p95_answer_generation_ms,
                    KnowledgeEvaluationRun.p99_answer_generation_ms,
                    KnowledgeEvaluationRun.answer_generation_ms,
                    KnowledgeEvaluationRun.context_tokens,
                    KnowledgeEvaluationRun.cost,
                    KnowledgeEvaluationRun.created_at,
                )
                .join(KnowledgeChannel, KnowledgeChannel.channel_id == Channel.id)
                .join(KnowledgeEvaluationRun, KnowledgeEvaluationRun.knowledge_channel_id == KnowledgeChannel.id)
                .order_by(KnowledgeEvaluationRun.created_at.desc())
                .limit(20)
            )).all()

            users = await _timed_rows(session, User.id, User.created_at, start, end)
            subscriptions = await _timed_rows(session, Subscription.user_id, Subscription.created_at, start, end)
            channels = await _timed_rows(session, Channel.id, Channel.created_at, start, end)
            posts = await _timed_rows(session, Post.id, Post.created_at, start, end)
            messages = await _timed_rows(
                session,
                ChatMessage.user_id,
                ChatMessage.created_at,
                start,
                end,
                ChatMessage.role == "user",
            )
            deliveries = await _timed_rows(
                session,
                DigestDelivery.user_id,
                DigestDelivery.delivered_at,
                start,
                end,
                DigestDelivery.status == "delivered",
            )
            skipped = await _timed_rows(
                session,
                DigestDelivery.user_id,
                DigestDelivery.delivered_at,
                start,
                end,
                DigestDelivery.status == "skipped",
            )
            processing_rows = (await session.execute(
                select(
                    DigestProcessingLog.completed_at,
                    DigestProcessingLog.found_count,
                    DigestProcessingLog.filtered_count,
                    DigestProcessingLog.included_count,
                ).where(
                    DigestProcessingLog.completed_at >= start,
                    DigestProcessingLog.completed_at < end,
                )
            )).all()
            usage_rows = (await session.execute(
                select(
                    LlmUsage.model,
                    LlmUsage.use_case,
                    LlmUsage.status,
                    LlmUsage.total_tokens,
                    LlmUsage.cost,
                    LlmUsage.created_at,
                ).where(LlmUsage.created_at >= start, LlmUsage.created_at < end)
            )).all()
            telemetry_started_at = (await session.execute(select(func.min(LlmUsage.created_at)))).scalar_one()
            return_users = (await session.execute(
                select(ChatMessage.user_id).join(User, User.id == ChatMessage.user_id).where(
                    ChatMessage.role == "user",
                    ChatMessage.created_at >= start,
                    ChatMessage.created_at < end,
                    User.created_at < start,
                ).distinct()
            )).scalars().all()
            channel_errors = (await session.execute(
                select(Channel.username, Channel.name, Channel.last_error, Channel.last_scraped)
                .where(Channel.status == ChannelStatus.ERROR)
                .order_by(Channel.updated_at.desc())
                .limit(10)
            )).all()

        daily = _new_buckets(start, end, bucket)
        _add_rows(daily, users, bucket, "new_users")
        _add_rows(daily, subscriptions, bucket, "subscriptions")
        _add_rows(daily, channels, bucket, "channels")
        _add_rows(daily, posts, bucket, "posts")
        _add_rows(daily, deliveries, bucket, "delivered")
        _add_rows(daily, skipped, bucket, "skipped")
        _add_rows(daily, messages, bucket, "assistant_messages")
        _add_active_rows(daily, bucket, messages, deliveries, subscriptions)
        processing_totals = {"found": 0, "filtered": 0, "included": 0}
        for completed_at, found, filtered, included in processing_rows:
            row = daily[_bucket_key(completed_at, bucket)]
            row["found"] += int(found)
            row["filtered"] += int(filtered)
            row["included"] += int(included)
            processing_totals["found"] += int(found)
            processing_totals["filtered"] += int(filtered)
            processing_totals["included"] += int(included)

        models: dict[str, dict[str, Any]] = {}
        llm_errors: dict[str, int] = defaultdict(int)
        for model, use_case, status, total_tokens, cost, created_at in usage_rows:
            entry = models.setdefault(model, {"model": model, "calls": 0, "tokens": 0, "cost": 0.0, "cost_available": False, "use_cases": set()})
            entry["calls"] += 1
            entry["tokens"] += int(total_tokens or 0)
            entry["use_cases"].add(use_case)
            if cost is not None:
                entry["cost"] += float(cost)
                entry["cost_available"] = True
            if status != "success":
                llm_errors[model] += 1
            row = daily[_bucket_key(created_at, bucket)]
            row["llm_calls"] += 1
            row["llm_tokens"] += int(total_tokens or 0)
            if cost is not None:
                row["llm_cost"] += float(cost)

        llm_cost_available = any(item["cost_available"] for item in models.values())
        model_rows = [
            {**item, "use_cases": sorted(item["use_cases"]), "cost": round(item["cost"], 6)}
            for item in models.values()
        ]
        model_rows.sort(key=lambda item: item["calls"], reverse=True)
        active_users = {item_id for item_id, _ in [*messages, *deliveries, *subscriptions]}
        errors = [
            {
                "component": f"@{username}" if username else (name or "Unknown channel"),
                "message": error or "Channel is marked as error",
                "at": _iso_or_none(last_scraped),
            }
            for username, name, error, last_scraped in channel_errors
        ]
        errors.extend({"component": f"LLM: {model}", "message": f"{count} failed request(s) in selected period", "at": None} for model, count in llm_errors.items())

        return {
            "range": {"start": start.isoformat(), "end": end.isoformat(), "bucket": bucket},
            "overview": {
                "users_total": users_total,
                "new_users": len(users),
                "active_users": len(active_users),
                "subscriptions_total": subscriptions_total,
                "subscriptions_created": len(subscriptions),
                "channels_total": channels_total,
                "channels_created": len(channels),
                "posts_created": len(posts),
                "delivered": len(deliveries),
                "skipped": len(skipped),
                "processing": processing_totals,
                "assistant_messages": len(messages),
                "returning_users": len(return_users),
                "llm_calls": len(usage_rows),
                "llm_tokens": sum(int(row[3] or 0) for row in usage_rows),
                "llm_cost": round(sum(float(row[4] or Decimal(0)) for row in usage_rows), 6),
                "llm_cost_available": llm_cost_available,
                "llm_tracking_since": _iso_or_none(telemetry_started_at),
                "llm_errors": sum(llm_errors.values()),
                "current_channel_errors": current_channel_errors,
            },
            "daily": list(daily.values()),
            "models": model_rows,
            "knowledge": {
                "catalog_channels": knowledge_catalog_channels,
                "pending_requests": knowledge_pending_requests,
                "imports": knowledge_imports,
                "queries": knowledge_queries,
                "active_configuration": self._active_configuration(),
                "evaluations": [
                    {
                        "channel": username,
                        "mode": mode,
                        "recall_at_k": float(recall_at_k) if recall_at_k is not None else None,
                        "mrr": float(mrr) if mrr is not None else None,
                        "ndcg": float(ndcg) if ndcg is not None else None,
                        "duplicate_source_share": float(duplicate_source_share) if duplicate_source_share is not None else None,
                        "precision_at_k": float(precision_at_k) if precision_at_k is not None else None,
                        "question_count": question_count,
                        "labels_complete": bool(labels_complete),
                        "configuration_id": configuration_id,
                        "reranker_model": reranker_model,
                        "rerank_fallback_share": float(rerank_fallback_share) if rerank_fallback_share is not None else None,
                        "correct_abstention_share": float(correct_abstention_share) if correct_abstention_share is not None else None,
                        "false_attribution_share": float(false_attribution_share) if false_attribution_share is not None else None,
                        "source_sufficiency_share": float(source_sufficiency_share) if source_sufficiency_share is not None else None,
                        "faithfulness": float(faithfulness) if faithfulness is not None else None,
                        "citation_validity": float(citation_validity) if citation_validity is not None else None,
                        "citation_completeness": float(citation_completeness) if citation_completeness is not None else None,
                        "answer_relevance": float(answer_relevance) if answer_relevance is not None else None,
                        "answer_audit_sample_size": answer_audit_sample_size,
                        "judge_version": judge_version,
                        "p50_latency_ms": p50_latency_ms,
                        "p95_latency_ms": p95_latency_ms,
                        "p99_latency_ms": p99_latency_ms,
                        "latency_ms": latency_ms,
                        "p50_retrieval_latency_ms": p50_retrieval_latency_ms,
                        "p95_retrieval_latency_ms": p95_retrieval_latency_ms,
                        "p99_retrieval_latency_ms": p99_retrieval_latency_ms,
                        "retrieval_latency_ms": retrieval_latency_ms,
                        "p50_answer_generation_ms": p50_answer_generation_ms,
                        "p95_answer_generation_ms": p95_answer_generation_ms,
                        "p99_answer_generation_ms": p99_answer_generation_ms,
                        "answer_generation_ms": answer_generation_ms,
                        "context_tokens": context_tokens,
                        "cost": float(cost) if cost is not None else None,
                        "created_at": _iso_or_none(created_at),
                    }
                    for username, mode, recall_at_k, mrr, ndcg, duplicate_source_share, precision_at_k, question_count, labels_complete, configuration_id, reranker_model, rerank_fallback_share, correct_abstention_share, false_attribution_share, source_sufficiency_share, faithfulness, citation_validity, citation_completeness, answer_relevance, answer_audit_sample_size, judge_version, p50_latency_ms, p95_latency_ms, p99_latency_ms, latency_ms, p50_retrieval_latency_ms, p95_retrieval_latency_ms, p99_retrieval_latency_ms, retrieval_latency_ms, p50_answer_generation_ms, p95_answer_generation_ms, p99_answer_generation_ms, answer_generation_ms, context_tokens, cost, created_at in rag_evaluations
                ],
            },
            "errors": errors,
        }

    async def reliability_metrics(self, now: datetime | None = None) -> dict[str, Any]:
        """Return content-free current queue, lease, retry, and DLQ gauges."""
        now = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
        async with self._session_factory() as session:
            unpublished_count, unpublished_oldest = await _count_oldest(
                session,
                OutboxEvent,
                OutboxEvent.created_at,
                OutboxEvent.state != "published",
            )
            outbox_retry_count, outbox_retry_oldest = await _count_oldest(
                session,
                OutboxEvent,
                OutboxEvent.updated_at,
                OutboxEvent.state == "pending",
                OutboxEvent.publication_attempt_count > 0,
            )
            run_retry_count, run_retry_oldest = await _count_oldest(
                session,
                DigestRun,
                DigestRun.updated_at,
                DigestRun.state == "render_retry_wait",
            )
            message_retry_count, message_retry_oldest = await _count_oldest(
                session,
                DigestOutboxMessage,
                DigestOutboxMessage.updated_at,
                DigestOutboxMessage.state == "retry_wait",
            )
            expired_outbox, expired_outbox_oldest = await _count_oldest(
                session,
                OutboxEvent,
                OutboxEvent.lease_until,
                OutboxEvent.state == "publishing",
                OutboxEvent.lease_until < now,
            )
            expired_inbox, expired_inbox_oldest = await _count_oldest(
                session,
                InboxEvent,
                InboxEvent.lease_until,
                InboxEvent.state == "processing",
                InboxEvent.lease_until < now,
            )
            expired_runs, expired_runs_oldest = await _count_oldest(
                session,
                DigestRun,
                DigestRun.lease_until,
                DigestRun.state == "rendering",
                DigestRun.lease_until < now,
            )
            expired_messages, expired_messages_oldest = await _count_oldest(
                session,
                DigestOutboxMessage,
                DigestOutboxMessage.lease_until,
                DigestOutboxMessage.state == "sending",
                DigestOutboxMessage.lease_until < now,
            )
            dead_letter_count, dead_letter_oldest = await _count_oldest(
                session,
                DeadLetterRecord,
                DeadLetterRecord.first_failed_at,
                DeadLetterRecord.status == "open",
            )

        retry_oldest = min(
            (value for value in (outbox_retry_oldest, run_retry_oldest, message_retry_oldest) if value is not None),
            default=None,
        )
        retry_count = outbox_retry_count + run_retry_count + message_retry_count
        expired_total = expired_outbox + expired_inbox + expired_runs + expired_messages
        expired_oldest = min(
            (
                value
                for value in (
                    expired_outbox_oldest,
                    expired_inbox_oldest,
                    expired_runs_oldest,
                    expired_messages_oldest,
                )
                if value is not None
            ),
            default=None,
        )
        return {
            "as_of": now.isoformat(),
            "unpublished_outbox": {
                "count": unpublished_count,
                "oldest_age_seconds": _age_seconds(now, unpublished_oldest),
            },
            "pending_retries": {
                "count": retry_count,
                "oldest_age_seconds": _age_seconds(now, retry_oldest),
                "outbox": outbox_retry_count,
                "runs": run_retry_count,
                "messages": message_retry_count,
            },
            "expired_leases": {
                "count": expired_total,
                "oldest_age_seconds": _age_seconds(now, expired_oldest),
                "outbox": expired_outbox,
                "inbox": expired_inbox,
                "runs": expired_runs,
                "messages": expired_messages,
            },
            "open_dead_letters": {
                "count": dead_letter_count,
                "oldest_age_seconds": _age_seconds(now, dead_letter_oldest),
            },
        }

    async def kafka_operations(self, now: datetime | None = None) -> dict[str, Any]:
        """Return one content-free Kafka and PostgreSQL operations snapshot."""
        now = _as_utc(now or datetime.now(timezone.utc))
        async with self._session_factory() as session:
            heartbeat_rows = list((await session.scalars(select(ReliabilityRoleHeartbeat))).all())
            recent_errors = await _recent_reliability_errors(session)
        queues = await self.reliability_metrics(now)
        try:
            kafka = await self._kafka_probe.probe()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            kafka = unavailable_kafka_report("unavailable", type(exc).__name__[:128], self._kafka_settings)

        heartbeats = {row.role: row for row in heartbeat_rows}
        roles = [_role_health(role, heartbeats.get(role), now) for role in _RELIABILITY_ROLES]
        reliable = self._reliable_settings
        return {
            "as_of": now.isoformat(),
            "mode": {
                "kafka_enabled": self._kafka_settings.enabled,
                "reliable_enabled": reliable.enabled,
                "reliable_all_subscriptions": reliable.all_subscriptions,
                "reliable_subscription_count": len(set(reliable.subscription_ids)),
                "memory_enabled": self._memory_settings.enabled,
                "delivery_path": "reliable" if reliable.enabled else "legacy",
            },
            "broker": kafka["broker"],
            "topics": kafka["topics"],
            "consumer_groups": kafka["consumer_groups"],
            "roles": roles,
            "queues": queues,
            "recent_errors": recent_errors,
        }

    def _active_configuration(self) -> dict[str, Any]:
        """Expose rollout state, never the allowlist, prompt, or any user content."""
        settings = self._knowledge_settings
        if settings is None:
            return {"id": "baseline", "status": "baseline", "index_version": None, "reranker_model": None, "candidate_limit": None}
        active = bool(settings.rag_rollout_enabled and (settings.rag_enabled_for_all_users or settings.rag_canary_telegram_ids))
        status = (
            "all_users" if active and settings.rag_enabled_for_all_users
            else ("canary" if active else "baseline")
        )
        return {
            "id": settings.rag_configuration_id if active else "baseline",
            "status": status,
            "index_version": settings.index_version,
            "reranker_model": settings.rag_reranker_model if active else None,
            "candidate_limit": settings.rag_rerank_candidate_limit if active else None,
        }

    async def dead_letters(
        self,
        *,
        page: int = 1,
        page_size: int = 25,
        status: str | None = None,
        work_type: str | None = None,
        reason: str | None = None,
        entity_ref: str | None = None,
        correlation_id: UUID | None = None,
    ) -> dict[str, Any]:
        """List content-free reliability records with bounded basic filters."""
        conditions = []
        if status:
            conditions.append(DeadLetterRecord.status == status)
        if work_type:
            conditions.append(DeadLetterRecord.work_type == work_type)
        if reason:
            conditions.append(DeadLetterRecord.terminal_reason == reason)
        if entity_ref:
            conditions.append(DeadLetterRecord.entity_ref == entity_ref)
        if correlation_id:
            conditions.append(DeadLetterRecord.correlation_id == correlation_id)
        async with self._session_factory() as session:
            total = int(
                (await session.scalar(select(func.count()).select_from(DeadLetterRecord).where(*conditions))) or 0
            )
            records = list(
                (
                    await session.scalars(
                        select(DeadLetterRecord)
                        .where(*conditions)
                        .order_by(DeadLetterRecord.last_failed_at.desc(), DeadLetterRecord.id.desc())
                        .offset((page - 1) * page_size)
                        .limit(page_size)
                    )
                ).all()
            )
        return {
            "items": [_dead_letter_json(record, detail=False) for record in records],
            "page": page,
            "page_size": page_size,
            "total": total,
        }

    async def dead_letter(self, record_id: UUID) -> dict[str, Any] | None:
        """Return one content-free record and append-only replay audit."""
        async with self._session_factory() as session:
            record = await session.get(DeadLetterRecord, record_id)
            if record is None:
                return None
            replays = list(
                (
                    await session.scalars(
                        select(DeadLetterReplay)
                        .where(DeadLetterReplay.dead_letter_id == record_id)
                        .order_by(DeadLetterReplay.requested_at, DeadLetterReplay.id)
                    )
                ).all()
            )
        payload = _dead_letter_json(record, detail=True)
        payload["replays"] = [
            {
                "id": str(replay.id),
                "actor": replay.actor,
                "requested_at": _iso_or_none(replay.requested_at),
                "result": replay.result,
                "generation": replay.generation,
                "error_code": replay.error_code,
                "outbox_event_id": str(replay.outbox_event_id) if replay.outbox_event_id else None,
            }
            for replay in replays
        ]
        return payload


def _role_health(role: str, row: ReliabilityRoleHeartbeat | None, now: datetime) -> dict[str, Any]:
    if row is None:
        return {
            "role": role,
            "health": "missing",
            "state": None,
            "instance_id": None,
            "started_at": None,
            "heartbeat_at": None,
            "age_seconds": None,
            "stopped_at": None,
            "last_error_code": None,
        }
    heartbeat_at = _as_utc(row.heartbeat_at)
    if row.state == "failed":
        health = "failed"
    elif row.state == "stopped":
        health = "stopped"
    elif (now - heartbeat_at).total_seconds() > 30:
        health = "stale"
    elif row.state == "starting":
        health = "starting"
    else:
        health = "healthy"
    return {
        "role": role,
        "health": health,
        "state": row.state,
        "instance_id": row.instance_id,
        "started_at": _iso_or_none(row.started_at),
        "heartbeat_at": heartbeat_at.isoformat(),
        "age_seconds": max(0, int((now - heartbeat_at).total_seconds())),
        "stopped_at": _iso_or_none(row.stopped_at),
        "last_error_code": _safe_error_code(row.last_error_code),
    }


async def _recent_reliability_errors(session) -> list[dict[str, Any]]:
    rows: list[tuple[datetime, dict[str, Any]]] = []

    outbox = (await session.execute(
        select(
            OutboxEvent.event_id,
            OutboxEvent.last_error,
            OutboxEvent.publication_attempt_count,
            OutboxEvent.state,
            OutboxEvent.updated_at,
        )
        .where(OutboxEvent.last_error.is_not(None))
        .order_by(OutboxEvent.updated_at.desc())
        .limit(20)
    )).all()
    rows.extend(
        (_as_utc(at), _operation_error("outbox-relay", entity_id, error, attempt, state, at))
        for entity_id, error, attempt, state, at in outbox
    )

    inbox = (await session.execute(
        select(InboxEvent.event_id, InboxEvent.last_error, InboxEvent.processing_attempt_count, InboxEvent.state, InboxEvent.updated_at)
        .where(InboxEvent.last_error.is_not(None))
        .order_by(InboxEvent.updated_at.desc())
        .limit(20)
    )).all()
    rows.extend(
        (_as_utc(at), _operation_error("inbox", entity_id, error, attempt, state, at))
        for entity_id, error, attempt, state, at in inbox
    )

    runs = (await session.execute(
        select(DigestRun.id, DigestRun.last_error, DigestRun.render_attempt_count, DigestRun.state, DigestRun.updated_at)
        .where(DigestRun.last_error.is_not(None))
        .order_by(DigestRun.updated_at.desc())
        .limit(20)
    )).all()
    rows.extend(
        (_as_utc(at), _operation_error("digest-worker", entity_id, error, attempt, state, at))
        for entity_id, error, attempt, state, at in runs
    )

    messages = (await session.execute(
        select(
            DigestOutboxMessage.id,
            DigestOutboxMessage.last_error,
            DigestOutboxMessage.attempt_count,
            DigestOutboxMessage.state,
            DigestOutboxMessage.updated_at,
        )
        .where(DigestOutboxMessage.last_error.is_not(None))
        .order_by(DigestOutboxMessage.updated_at.desc())
        .limit(20)
    )).all()
    rows.extend(
        (_as_utc(at), _operation_error("telegram-delivery-worker", entity_id, error, attempt, state, at))
        for entity_id, error, attempt, state, at in messages
    )

    dead_letters = (await session.scalars(
        select(DeadLetterRecord).order_by(DeadLetterRecord.last_failed_at.desc()).limit(20)
    )).all()
    for record in dead_letters:
        attempt = next(
            (
                int(record.attempt_summary[key])
                for key in ("attempt_count", "processing_attempt_count", "max_attempts")
                if isinstance(record.attempt_summary.get(key), int)
            ),
            None,
        )
        rows.append(
            (
                _as_utc(record.last_failed_at),
                _operation_error(
                    "dead-letter",
                    record.id,
                    record.error_code,
                    attempt,
                    record.status,
                    record.last_failed_at,
                ),
            )
        )

    failed_roles = (await session.scalars(
        select(ReliabilityRoleHeartbeat)
        .where(ReliabilityRoleHeartbeat.last_error_code.is_not(None))
        .order_by(ReliabilityRoleHeartbeat.updated_at.desc())
        .limit(20)
    )).all()
    rows.extend(
        (
            _as_utc(row.updated_at),
            _operation_error("role-heartbeat", row.role, row.last_error_code, None, row.state, row.updated_at),
        )
        for row in failed_roles
    )

    rows.sort(key=lambda item: item[0], reverse=True)
    return [item for _, item in rows[:20]]


def _operation_error(
    component: str,
    entity_id: Any,
    error_code: str | None,
    attempt: int | None,
    state: str,
    at: datetime,
) -> dict[str, Any]:
    return {
        "component": component,
        "entity_id": str(entity_id),
        "error_code": _safe_error_code(error_code) or "UnknownError",
        "attempt": int(attempt) if attempt is not None else None,
        "state": state,
        "at": _as_utc(at).isoformat(),
    }


def _safe_error_code(value: str | None) -> str | None:
    return value if value is not None and _SAFE_ERROR_CODE.fullmatch(value) else None


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


async def _count(session, statement) -> int:
    return int((await session.execute(statement)).scalar_one() or 0)


async def _count_oldest(session, model, timestamp, *conditions) -> tuple[int, datetime | None]:
    count, oldest = (
        await session.execute(select(func.count(), func.min(timestamp)).select_from(model).where(*conditions))
    ).one()
    if oldest is not None and oldest.tzinfo is None:
        oldest = oldest.replace(tzinfo=timezone.utc)
    return int(count or 0), oldest.astimezone(timezone.utc) if oldest is not None else None


def _age_seconds(now: datetime, oldest: datetime | None) -> int | None:
    return max(0, int((now - oldest).total_seconds())) if oldest is not None else None


async def _timed_rows(session, identifier, timestamp, start: datetime, end: datetime, *conditions):
    result = await session.execute(
        select(identifier, timestamp).where(timestamp >= start, timestamp < end, *conditions)
    )
    return result.all()


def _new_buckets(start: datetime, end: datetime, bucket: str) -> dict[str, dict[str, Any]]:
    rows: dict[str, dict[str, Any]] = {}
    current = datetime(start.year, start.month, start.day, start.hour if bucket == "hour" else 0, tzinfo=timezone.utc)
    while current < end:
        key = _bucket_key(current, bucket)
        rows.setdefault(key, {"date": key, "new_users": 0, "active_users": 0, "subscriptions": 0, "channels": 0, "posts": 0, "delivered": 0, "skipped": 0, "found": 0, "filtered": 0, "included": 0, "assistant_messages": 0, "llm_calls": 0, "llm_tokens": 0, "llm_cost": 0.0, "_active": set()})
        if bucket == "month":
            current = datetime(current.year + (current.month == 12), (current.month % 12) + 1, 1, tzinfo=timezone.utc)
        elif bucket == "hour":
            current += timedelta(hours=1)
        else:
            current += timedelta(days=1)
    return rows


def _add_rows(rows, values, bucket: str, field: str) -> None:
    for _, timestamp in values:
        rows[_bucket_key(timestamp, bucket)][field] += 1


def _add_active_rows(rows, bucket: str, *value_sets) -> None:
    for values in value_sets:
        for identifier, timestamp in values:
            rows[_bucket_key(timestamp, bucket)]["_active"].add(identifier)
    for row in rows.values():
        row["active_users"] = len(row.pop("_active"))


def _bucket_key(value: datetime, bucket: str) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    value = value.astimezone(timezone.utc)
    if bucket == "month":
        return value.strftime("%Y-%m")
    if bucket == "hour":
        return value.strftime("%Y-%m-%d %H:00")
    return value.strftime("%Y-%m-%d")


def _iso_or_none(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.isoformat()


def _dead_letter_json(record: DeadLetterRecord, *, detail: bool) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "id": str(record.id),
        "source_topic": record.source_topic,
        "source_event_id": str(record.source_event_id) if record.source_event_id else None,
        "source_partition": record.source_partition,
        "source_offset": record.source_offset,
        "work_type": record.work_type,
        "entity_ref": record.entity_ref,
        "run_id": str(record.run_id) if record.run_id else None,
        "message_id": str(record.message_id) if record.message_id else None,
        "subscription_id": record.subscription_id,
        "correlation_id": str(record.correlation_id) if record.correlation_id else None,
        "terminal_reason": record.terminal_reason,
        "error_code": record.error_code,
        "first_failed_at": _iso_or_none(record.first_failed_at),
        "last_failed_at": _iso_or_none(record.last_failed_at),
        "status": record.status,
        "generation": record.generation,
        "dlq_outbox_event_id": str(record.dlq_outbox_event_id) if record.dlq_outbox_event_id else None,
    }
    if detail:
        payload["attempt_summary"] = {
            key: record.attempt_summary[key]
            for key in ("attempt_count", "max_attempts", "ambiguous", "processing_attempt_count")
            if key in record.attempt_summary and isinstance(record.attempt_summary[key], (int, bool))
        }
    return payload
