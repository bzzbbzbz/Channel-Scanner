"""Database operations for the knowledge catalog; all retrieval starts at parent posts."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Iterable

from sqlalchemy import and_, delete, func, insert, or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from src.knowledge.importer import ImportedPost
from src.knowledge.representations import RepresentationDraft, content_hash
from src.models.channel import Channel
from src.models.knowledge import (
    EnrichmentStatus,
    IndexStatus,
    KnowledgeChannel,
    KnowledgeChannelRequest,
    KnowledgeChannelState,
    KnowledgeDocument,
    KnowledgeImport,
    KnowledgeImportStatus,
    KnowledgeQuery,
    KnowledgeRepresentation,
    KnowledgeRequestStatus,
    RagSearchConfiguration,
)
from src.models.post import Post
from src.models.digest_delivery import DigestDelivery
from src.models.subscription import Subscription, SubscriptionChannel


def normalize_username(value: str) -> str:
    return value.strip().removeprefix("https://t.me/").removeprefix("@").strip("/").lower()


class KnowledgeRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def request_channel(self, requester_user_id: int, username: str) -> tuple[KnowledgeChannelRequest, bool]:
        username = normalize_username(username)
        existing = (await self._session.execute(select(KnowledgeChannelRequest).where(
            KnowledgeChannelRequest.requester_user_id == requester_user_id,
            KnowledgeChannelRequest.username == username,
        ))).scalar_one_or_none()
        if existing is not None:
            return existing, False
        request = KnowledgeChannelRequest(username=username, requester_user_id=requester_user_id)
        self._session.add(request)
        await self._session.flush()
        return request, True

    async def list_pending_requests(self) -> list[KnowledgeChannelRequest]:
        result = await self._session.execute(select(KnowledgeChannelRequest).where(
            KnowledgeChannelRequest.status == KnowledgeRequestStatus.PENDING,
        ).order_by(KnowledgeChannelRequest.created_at))
        return list(result.scalars())

    async def get_request(self, request_id: int) -> KnowledgeChannelRequest | None:
        return (await self._session.execute(select(KnowledgeChannelRequest).where(KnowledgeChannelRequest.id == request_id))).scalar_one_or_none()

    async def decide_request(self, request: KnowledgeChannelRequest, *, approved: bool, administrator_telegram_id: int, reason: str | None = None) -> KnowledgeChannel | None:
        request.status = KnowledgeRequestStatus.APPROVED if approved else KnowledgeRequestStatus.REJECTED
        request.administrator_telegram_id = administrator_telegram_id
        request.decision_reason = reason
        request.decided_at = datetime.now(timezone.utc)
        if not approved:
            await self._session.flush()
            return None
        channel = (await self._session.execute(select(Channel).where(Channel.username == request.username))).scalar_one_or_none()
        if channel is None:
            channel = Channel(username=request.username, name=request.username)
            self._session.add(channel)
            await self._session.flush()
        knowledge_channel = (await self._session.execute(select(KnowledgeChannel).where(KnowledgeChannel.channel_id == channel.id))).scalar_one_or_none()
        if knowledge_channel is None:
            knowledge_channel = KnowledgeChannel(channel_id=channel.id, state=KnowledgeChannelState.PENDING_IMPORT)
            self._session.add(knowledge_channel)
        await self._session.flush()
        return knowledge_channel

    async def get_catalog_channel(self, channel_id: int) -> KnowledgeChannel | None:
        stmt = select(KnowledgeChannel).options(selectinload(KnowledgeChannel.channel)).where(KnowledgeChannel.channel_id == channel_id)
        return (await self._session.execute(stmt)).scalar_one_or_none()

    async def get_catalog_entry(self, knowledge_channel_id: int) -> KnowledgeChannel | None:
        stmt = select(KnowledgeChannel).options(selectinload(KnowledgeChannel.channel)).where(KnowledgeChannel.id == knowledge_channel_id)
        return (await self._session.execute(stmt)).scalar_one_or_none()

    async def get_catalog_channel_by_username(self, username: str) -> KnowledgeChannel | None:
        stmt = select(KnowledgeChannel).join(Channel).options(selectinload(KnowledgeChannel.channel)).where(Channel.username == normalize_username(username))
        return (await self._session.execute(stmt)).scalar_one_or_none()

    async def list_catalog(self) -> list[KnowledgeChannel]:
        stmt = select(KnowledgeChannel).join(Channel).options(selectinload(KnowledgeChannel.channel)).where(
            KnowledgeChannel.state == KnowledgeChannelState.READY,
        ).order_by(Channel.username)
        return list((await self._session.execute(stmt)).scalars())

    async def description_input(self, channel_id: int, *, limit: int = 30) -> list[dict[str, Any]]:
        """Return deterministic, metadata-only catalog material for a description.

        Canonical post bodies intentionally never cross this boundary.
        """
        rows = (await self._session.execute(
            select(KnowledgeDocument, Post)
            .join(Post, Post.id == KnowledgeDocument.post_id)
            .where(
                Post.channel_id == channel_id,
                KnowledgeDocument.enrichment_status == EnrichmentStatus.READY,
            )
            .order_by(Post.datetime.desc(), Post.id.desc())
            .limit(limit)
        )).all()
        return [
            {
                "title": document.title or "",
                "topics": document.topics or [],
                "entities": document.entities or [],
                "summary": document.summary or "",
            }
            for document, _post in rows
        ]

    async def update_description(self, channel_id: int, *, text: str, source_hash: str) -> None:
        entry = await self.get_catalog_channel(channel_id)
        if entry is None:
            return
        entry.description = text
        entry.description_source_hash = source_hash
        entry.description_updated_at = datetime.now(timezone.utc)
        await self._session.flush()

    async def create_import(self, knowledge_channel: KnowledgeChannel, *, request_id: int | None, administrator_telegram_id: int, filename: str, checksum: str, import_version: str) -> KnowledgeImport:
        record = KnowledgeImport(
            knowledge_channel_id=knowledge_channel.id,
            request_id=request_id,
            administrator_telegram_id=administrator_telegram_id,
            filename=filename,
            checksum=checksum,
            import_version=import_version,
            status=KnowledgeImportStatus.QUEUED,
        )
        self._session.add(record)
        await self._session.flush()
        return record

    async def import_posts(self, channel_id: int, posts: Iterable[ImportedPost]) -> tuple[list[Post], int]:
        """Upsert canonical parent posts and invalidate metadata only if text changed."""
        changed: list[Post] = []
        skipped = 0
        for item in posts:
            post = (await self._session.execute(select(Post).where(Post.channel_id == channel_id, Post.post_id == item.telegram_post_id))).scalar_one_or_none()
            if post is None:
                post = Post(channel_id=channel_id, post_id=item.telegram_post_id, content=item.content, datetime=item.published_at, author=item.author)
                self._session.add(post)
                changed.append(post)
                continue
            if post.content != item.content:
                post.content = item.content
                post.datetime = item.published_at
                post.author = item.author
                if post.knowledge_document is not None:
                    post.knowledge_document.enrichment_status = EnrichmentStatus.STALE
                changed.append(post)
            else:
                skipped += 1
        await self._session.flush()
        return changed, skipped

    async def clear_channel_knowledge(self, channel_id: int) -> list[str]:
        """Remove stale representation rows after their Qdrant points are deleted."""
        point_ids = list((await self._session.execute(
            select(KnowledgeRepresentation.qdrant_point_id)
            .join(Post, Post.id == KnowledgeRepresentation.post_id)
            .where(Post.channel_id == channel_id)
        )).scalars())
        await self._session.execute(
            delete(KnowledgeRepresentation).where(
                KnowledgeRepresentation.post_id.in_(select(Post.id).where(Post.channel_id == channel_id))
            )
        )
        await self._session.execute(
            delete(KnowledgeDocument).where(
                KnowledgeDocument.post_id.in_(select(Post.id).where(Post.channel_id == channel_id))
            )
        )
        await self._session.flush()
        return point_ids

    async def purge_channel_posts_before(self, channel_id: int, boundary: datetime) -> int:
        """Delete canonical history and dependent delivery/knowledge state for an approved destructive reset."""
        post_ids = select(Post.id).where(Post.channel_id == channel_id, Post.datetime < boundary)
        await self._session.execute(delete(DigestDelivery).where(DigestDelivery.post_id.in_(post_ids)))
        await self._session.execute(delete(KnowledgeRepresentation).where(KnowledgeRepresentation.post_id.in_(post_ids)))
        await self._session.execute(delete(KnowledgeDocument).where(KnowledgeDocument.post_id.in_(post_ids)))
        result = await self._session.execute(delete(Post).where(Post.id.in_(post_ids)))
        await self._session.flush()
        return int(result.rowcount or 0)

    async def post_ids_for_channel(self, channel_id: int, start_at: datetime | None = None) -> list[int]:
        stmt = select(Post.id).where(Post.channel_id == channel_id)
        if start_at is not None:
            stmt = stmt.where(Post.datetime >= start_at)
        return list((await self._session.execute(stmt.order_by(Post.id))).scalars())

    async def post_ids_for_telegram_posts(self, channel_id: int, telegram_post_ids: Iterable[int]) -> list[int]:
        ids = list(telegram_post_ids)
        if not ids:
            return []
        return list((await self._session.execute(
            select(Post.id).where(Post.channel_id == channel_id, Post.post_id.in_(ids)).order_by(Post.id)
        )).scalars())

    async def mark_import_backfill_skipped(self, channel_id: int, post_ids: list[int]) -> int:
        """Prevent a catalog JSON backfill from becoming a subscription digest backlog."""
        if not post_ids:
            return 0
        rows = (await self._session.execute(
            select(SubscriptionChannel.subscription_id, Subscription.user_id, Post.id)
            .join(Subscription, Subscription.id == SubscriptionChannel.subscription_id)
            .join(Post, Post.channel_id == SubscriptionChannel.channel_id)
            .where(SubscriptionChannel.channel_id == channel_id, Post.id.in_(post_ids))
        )).all()
        if not rows:
            return 0
        values = [
            {
                "subscription_id": subscription_id,
                "user_id": user_id,
                "post_id": post_id,
                "status": "skipped",
                "skip_reason": "knowledge_import_backfill",
                "delivered_at": datetime.now(timezone.utc),
            }
            for subscription_id, user_id, post_id in rows
        ]
        dialect = self._session.bind.dialect.name if self._session.bind else "unknown"
        if dialect == "postgresql":
            from sqlalchemy.dialects.postgresql import insert as dialect_insert

            result = await self._session.execute(dialect_insert(DigestDelivery).values(values).on_conflict_do_nothing(index_elements=["subscription_id", "post_id"]))
        elif dialect == "sqlite":
            from sqlalchemy.dialects.sqlite import insert as dialect_insert

            result = await self._session.execute(dialect_insert(DigestDelivery).values(values).on_conflict_do_nothing(index_elements=["subscription_id", "post_id"]))
        else:
            result = await self._session.execute(insert(DigestDelivery).values(values))
        await self._session.flush()
        return int(result.rowcount or 0)

    async def upsert_document(self, post: Post, *, source_hash: str, enrichment, model: str, prompt_version: str) -> KnowledgeDocument:
        document = post.knowledge_document
        if document is None:
            document = (await self._session.execute(select(KnowledgeDocument).where(KnowledgeDocument.post_id == post.id))).scalar_one_or_none()
        if document is None:
            document = KnowledgeDocument(post_id=post.id, source_content_hash=source_hash)
            self._session.add(document)
        document.title = enrichment.title
        document.summary = enrichment.summary
        document.topics = enrichment.topics
        document.entities = [entity.model_dump() for entity in enrichment.entities]
        document.content_type = enrichment.content_type
        document.epistemic_status = enrichment.epistemic_status
        document.questions_answered = enrichment.questions_answered
        document.claims = [claim.model_dump() for claim in enrichment.claims]
        document.source_content_hash = source_hash
        document.enrichment_model = model
        document.enrichment_prompt_version = prompt_version
        document.enrichment_status = EnrichmentStatus.READY
        document.enrichment_error = None
        document.enriched_at = datetime.now(timezone.utc)
        await self._session.flush()
        return document

    async def mark_enrichment_failed(self, post: Post, source_hash: str, prompt_version: str, error: Exception) -> None:
        document = post.knowledge_document or (await self._session.execute(select(KnowledgeDocument).where(KnowledgeDocument.post_id == post.id))).scalar_one_or_none()
        if document is None:
            document = KnowledgeDocument(post_id=post.id, source_content_hash=source_hash)
            self._session.add(document)
        document.source_content_hash = source_hash
        document.enrichment_prompt_version = prompt_version
        document.enrichment_status = EnrichmentStatus.FAILED
        document.enrichment_attempts = (document.enrichment_attempts or 0) + 1
        document.enrichment_error = f"{type(error).__name__}: {error}"[:2000]
        await self._session.flush()

    async def replace_representations(self, document: KnowledgeDocument, drafts: list[RepresentationDraft], *, embedding_model: str, embedding_version: str, index_version: int) -> list[KnowledgeRepresentation]:
        """Store a replacement version; stale records remain until its points are indexed."""
        if any(draft.post_id != document.post_id for draft in drafts):
            raise ValueError("representation draft parent does not match knowledge document")
        existing = list((await self._session.execute(select(KnowledgeRepresentation).where(
            KnowledgeRepresentation.knowledge_document_id == document.id,
            KnowledgeRepresentation.index_version == index_version,
        ))).scalars())
        by_key = {(record.representation_type, record.ordinal): record for record in existing}
        records: list[KnowledgeRepresentation] = []
        for draft in drafts:
            record = by_key.get((draft.representation_type, draft.ordinal))
            if record is None:
                record = KnowledgeRepresentation(
                    knowledge_document_id=document.id,
                    post_id=document.post_id,
                    representation_type=draft.representation_type,
                    ordinal=draft.ordinal,
                    text=draft.text,
                    text_hash=draft.text_hash,
                    token_count=draft.token_count,
                    start_offset=draft.start_offset,
                    end_offset=draft.end_offset,
                    qdrant_point_id=draft.point_id,
                    embedding_model=embedding_model,
                    embedding_version=embedding_version,
                    index_version=index_version,
                    index_status=IndexStatus.PENDING,
                )
                self._session.add(record)
            else:
                record.text = draft.text
                record.text_hash = draft.text_hash
                record.token_count = draft.token_count
                record.start_offset = draft.start_offset
                record.end_offset = draft.end_offset
                record.qdrant_point_id = draft.point_id
                record.embedding_model = embedding_model
                record.embedding_version = embedding_version
                record.index_status = IndexStatus.PENDING
                record.index_error = None
            records.append(record)
        await self._session.flush()
        return records

    async def mark_representations_indexed(self, records: list[KnowledgeRepresentation]) -> None:
        now = datetime.now(timezone.utc)
        for record in records:
            record.index_status = IndexStatus.INDEXED
            record.indexed_at = now
        await self._session.flush()

    async def mark_representations_failed(self, records: list[KnowledgeRepresentation], error: Exception) -> None:
        for record in records:
            record.index_status = IndexStatus.FAILED
            record.index_attempts = (record.index_attempts or 0) + 1
            record.index_error = f"{type(error).__name__}: {error}"[:2000]
        await self._session.flush()

    async def retryable_post_ids(self, *, index_version: int, max_attempts: int, channel_id: int | None = None) -> list[int]:
        """Return catalog posts whose failed work still has bounded retries left."""
        conditions = [KnowledgeChannel.state == KnowledgeChannelState.READY]
        if channel_id is not None:
            conditions.append(Post.channel_id == channel_id)
        result = await self._session.execute(
            select(Post.id)
            .join(KnowledgeChannel, KnowledgeChannel.channel_id == Post.channel_id)
            .outerjoin(KnowledgeDocument, KnowledgeDocument.post_id == Post.id)
            .outerjoin(
                KnowledgeRepresentation,
                and_(
                    KnowledgeRepresentation.post_id == Post.id,
                    KnowledgeRepresentation.index_version == index_version,
                ),
            )
            .where(
                *conditions,
                or_(
                    KnowledgeDocument.id.is_(None),
                    KnowledgeDocument.enrichment_status.in_([EnrichmentStatus.PENDING, EnrichmentStatus.STALE]),
                    and_(KnowledgeDocument.enrichment_status == EnrichmentStatus.FAILED, KnowledgeDocument.enrichment_attempts < max_attempts),
                    and_(
                        KnowledgeDocument.enrichment_status == EnrichmentStatus.READY,
                        or_(
                            KnowledgeRepresentation.id.is_(None),
                            KnowledgeRepresentation.index_status.in_([IndexStatus.PENDING, IndexStatus.STALE]),
                            and_(KnowledgeRepresentation.index_status == IndexStatus.FAILED, KnowledgeRepresentation.index_attempts < max_attempts),
                        ),
                    ),
                ),
            )
            .distinct()
        )
        return list(result.scalars())

    async def refresh_catalog_counts(self, channel_id: int) -> None:
        """Synchronize catalog counters from canonical rows and successful representations."""
        catalog = await self.get_catalog_channel(channel_id)
        if catalog is None:
            return
        catalog.post_count = int((await self._session.execute(select(func.count(Post.id)).where(Post.channel_id == channel_id))).scalar_one())
        catalog.representation_count = int((await self._session.execute(
            select(func.count(KnowledgeRepresentation.id))
            .join(Post, Post.id == KnowledgeRepresentation.post_id)
            .where(Post.channel_id == channel_id, KnowledgeRepresentation.index_status == IndexStatus.INDEXED)
        )).scalar_one())
        await self._session.flush()

    async def lexical_search(self, *, channel_ids: list[int], subscription_baselines: dict[int, datetime], query: str, limit: int = 30) -> list[Post]:
        if not channel_ids:
            return []
        terms = [term for term in query.split() if len(term) > 1][:8]
        stmt = select(Post).where(Post.channel_id.in_(channel_ids))
        if subscription_baselines:
            stmt = stmt.where(or_(*[
                and_(Post.channel_id == channel_id, Post.datetime >= baseline)
                for channel_id, baseline in subscription_baselines.items()
            ]))
        if terms:
            stmt = stmt.where(or_(*[Post.content.ilike(f"%{term}%") for term in terms]))
        stmt = stmt.order_by(Post.datetime.desc()).limit(limit)
        posts = list((await self._session.execute(stmt)).scalars())
        normalized_terms = [term.casefold() for term in terms]
        return sorted(
            posts,
            key=lambda post: (
                sum(post.content.casefold().count(term) for term in normalized_terms),
                post.datetime,
            ),
            reverse=True,
        )

    async def subscription_scope(self, user_id: int, subscription_id: int) -> dict[int, datetime] | None:
        subscription = (await self._session.execute(select(Subscription).where(Subscription.id == subscription_id, Subscription.user_id == user_id))).scalar_one_or_none()
        if subscription is None:
            return None
        links = (await self._session.execute(select(SubscriptionChannel).where(SubscriptionChannel.subscription_id == subscription_id))).scalars()
        return {link.channel_id: link.subscribed_at for link in links}

    async def add_query(self, **values) -> KnowledgeQuery:
        record = KnowledgeQuery(**values)
        self._session.add(record)
        await self._session.flush()
        return record

    async def ensure_rag_configuration(self, settings) -> RagSearchConfiguration:
        """Record a candidate snapshot once; an ID must never silently change meaning."""
        import hashlib

        instruction_hash = hashlib.sha256(settings.rag_query_instruction.encode("utf-8")).hexdigest()
        current = (await self._session.execute(select(RagSearchConfiguration).where(
            RagSearchConfiguration.id == settings.rag_configuration_id,
        ))).scalar_one_or_none()
        values = {
            "code_version": settings.rag_code_version,
            "index_version": settings.index_version,
            "query_instruction_hash": instruction_hash,
            "reranker_model": settings.rag_reranker_model,
            "candidate_limit": settings.rag_rerank_candidate_limit,
            "status": "canary_candidate",
            "operator": settings.rag_configuration_operator,
        }
        if current is None:
            current = RagSearchConfiguration(id=settings.rag_configuration_id, **values)
            self._session.add(current)
            await self._session.flush()
            return current
        if any(getattr(current, key) != value for key, value in values.items()):
            raise ValueError("RAG configuration ID already records different immutable settings")
        return current

    async def representation_for_post(self, post_id: int, index_version: int) -> list[KnowledgeRepresentation]:
        return list((await self._session.execute(select(KnowledgeRepresentation).where(
            KnowledgeRepresentation.post_id == post_id,
            KnowledgeRepresentation.index_version == index_version,
            KnowledgeRepresentation.index_status == IndexStatus.INDEXED,
        ).order_by(KnowledgeRepresentation.ordinal))).scalars())

    async def channel_has_active_index(self, channel_id: int, index_version: int) -> bool:
        result = await self._session.execute(
            select(KnowledgeRepresentation.id)
            .join(Post, Post.id == KnowledgeRepresentation.post_id)
            .where(
                Post.channel_id == channel_id,
                KnowledgeRepresentation.index_version == index_version,
                KnowledgeRepresentation.index_status == IndexStatus.INDEXED,
            )
            .limit(1)
        )
        return result.scalar_one_or_none() is not None

    @staticmethod
    def source_hash(post: Post) -> str:
        return content_hash(post.content)
