"""Lease and publish transactional outbox events."""

from __future__ import annotations

import asyncio
import logging
import random
from datetime import datetime, timedelta, timezone
from typing import Callable

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.config.settings import KafkaSettings
from src.repository.outbox import OutboxRepository
from src.reliability.kafka_producer import KafkaEventProducer

logger = logging.getLogger(__name__)


def _consume_task_outcome(task: asyncio.Task) -> None:
    try:
        task.result()
    except BaseException:
        pass


def backoff_ceiling(*, attempt: int, base_seconds: float, cap_seconds: float) -> float:
    """Return bounded exponential delay without constructing an unbounded integer."""
    delay = min(base_seconds, cap_seconds)
    for _ in range(min(max(attempt - 1, 0), 63)):
        delay = min(delay * 2, cap_seconds)
        if delay >= cap_seconds:
            break
    return delay


class OutboxRelay:
    """Continuously relay leased PostgreSQL rows to Kafka."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        repository: OutboxRepository,
        producer: KafkaEventProducer,
        settings: KafkaSettings,
        *,
        owner: str,
        clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
        random_uniform: Callable[[float, float], float] = random.uniform,
        after_broker_ack: Callable[[], None] | None = None,
    ) -> None:
        self._session_factory = session_factory
        self._repository = repository
        self._producer = producer
        self._settings = settings
        self._owner = owner
        self._clock = clock
        self._random_uniform = random_uniform
        self._after_broker_ack = after_broker_ack

    async def run_once(self) -> int:
        """Claim one batch and independently persist each broker outcome."""
        async with self._session_factory() as session, session.begin():
            events = await self._repository.claim_batch(
                session,
                owner=self._owner,
                now=self._clock(),
                lease_seconds=self._settings.outbox_lease_seconds,
                batch_size=self._settings.outbox_batch_size,
            )

        for event in events:
            logger.info(
                "Outbox event transition: event_id=%s correlation_id=%s attempt=%d state=publishing",
                event.event_id,
                event.correlation_id,
                event.publication_attempt_count,
            )
            publish_task = asyncio.create_task(
                self._producer.publish(
                    topic=event.topic,
                    event_key=event.event_key,
                    value=self._repository.serialize(event),
                )
            )
            try:
                done, _ = await asyncio.wait(
                    {publish_task},
                    timeout=self._settings.outbox_publish_timeout_seconds,
                )
                if not done:
                    publish_task.cancel()
                    publish_task.add_done_callback(_consume_task_outcome)
                    await asyncio.sleep(0)
                    raise TimeoutError
                result = publish_task.result()
            except asyncio.CancelledError:
                publish_task.cancel()
                publish_task.add_done_callback(_consume_task_outcome)
                raise
            except Exception as exc:
                delay = backoff_ceiling(
                    attempt=event.publication_attempt_count,
                    base_seconds=self._settings.outbox_backoff_base_seconds,
                    cap_seconds=self._settings.outbox_backoff_cap_seconds,
                )
                retry_at = self._clock() + timedelta(seconds=self._random_uniform(0, delay))
                async with self._session_factory() as session, session.begin():
                    marked = await self._repository.mark_failure(
                        session,
                        event_id=event.event_id,
                        owner=self._owner,
                        error_code=type(exc).__name__[:128],
                        next_attempt_at=retry_at,
                    )
                logger.warning(
                    "Outbox event transition: event_id=%s correlation_id=%s attempt=%d state=pending error=%s lease_owned=%s",
                    event.event_id,
                    event.correlation_id,
                    event.publication_attempt_count,
                    type(exc).__name__,
                    marked,
                )
                continue

            if self._after_broker_ack is not None:
                self._after_broker_ack()

            async with self._session_factory() as session, session.begin():
                marked = await self._repository.mark_published(
                    session,
                    event_id=event.event_id,
                    owner=self._owner,
                    partition=result.partition,
                    offset=result.offset,
                    published_at=result.published_at,
                )
            logger.info(
                "Outbox event transition: event_id=%s correlation_id=%s attempt=%d state=published partition=%d offset=%d lease_owned=%s",
                event.event_id,
                event.correlation_id,
                event.publication_attempt_count,
                result.partition,
                result.offset,
                marked,
            )
        return len(events)

    async def run(self, stop_event: asyncio.Event) -> None:
        while not stop_event.is_set():
            count = await self.run_once()
            if count:
                continue
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=self._settings.outbox_poll_interval_seconds)
            except TimeoutError:
                pass
