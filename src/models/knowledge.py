"""Persistent catalog, indexing, and audit records for channel knowledge search."""

from __future__ import annotations

from datetime import datetime
from enum import Enum as PyEnum
from typing import Any

from sqlalchemy import Boolean, DateTime, Enum, ForeignKey, Index, Integer, Numeric, String, Text, UniqueConstraint, func
from sqlalchemy import JSON
from sqlalchemy.orm import Mapped, mapped_column, relationship

from src.knowledge.experiments import CampaignState, CandidateState, PromotionDecision
from src.models.base import Base


class KnowledgeRequestStatus(str, PyEnum):
    PENDING = "pending"
    APPROVED = "approved"
    REJECTED = "rejected"


class KnowledgeChannelState(str, PyEnum):
    PENDING_IMPORT = "pending_import"
    IMPORTING = "importing"
    READY = "ready"
    ERROR = "error"


class KnowledgeImportStatus(str, PyEnum):
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class EnrichmentStatus(str, PyEnum):
    PENDING = "pending"
    READY = "ready"
    FAILED = "failed"
    STALE = "stale"


class RepresentationType(str, PyEnum):
    SUMMARY = "summary"
    FULL = "full"
    CHUNK = "chunk"


class IndexStatus(str, PyEnum):
    PENDING = "pending"
    INDEXED = "indexed"
    FAILED = "failed"
    STALE = "stale"


class KnowledgeChannelRequest(Base):
    __tablename__ = "knowledge_channel_requests"
    __table_args__ = (UniqueConstraint("username", "requester_user_id", name="uq_knowledge_request_username_requester"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    username: Mapped[str] = mapped_column(String(255), nullable=False)
    requester_user_id: Mapped[int] = mapped_column(Integer, ForeignKey("users.id"), nullable=False)
    status: Mapped[KnowledgeRequestStatus] = mapped_column(Enum(KnowledgeRequestStatus, name="knowledge_request_status", create_constraint=True), nullable=False, default=KnowledgeRequestStatus.PENDING)
    decision_reason: Mapped[str | None] = mapped_column(Text)
    administrator_telegram_id: Mapped[int | None] = mapped_column(nullable=True)
    decided_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now())


class KnowledgeChannel(Base):
    __tablename__ = "knowledge_channels"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    channel_id: Mapped[int] = mapped_column(Integer, ForeignKey("channels.id"), nullable=False, unique=True)
    state: Mapped[KnowledgeChannelState] = mapped_column(Enum(KnowledgeChannelState, name="knowledge_channel_state", create_constraint=True), nullable=False, default=KnowledgeChannelState.PENDING_IMPORT)
    active_index_version: Mapped[int | None] = mapped_column(Integer)
    last_imported_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_synced_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    post_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    representation_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    error_summary: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now())

    channel = relationship("Channel", back_populates="knowledge_channel", lazy="selectin")


class KnowledgeImport(Base):
    __tablename__ = "knowledge_imports"
    __table_args__ = (Index("ix_knowledge_import_channel_status", "knowledge_channel_id", "status"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    knowledge_channel_id: Mapped[int] = mapped_column(Integer, ForeignKey("knowledge_channels.id"), nullable=False)
    request_id: Mapped[int | None] = mapped_column(Integer, ForeignKey("knowledge_channel_requests.id"))
    administrator_telegram_id: Mapped[int] = mapped_column(nullable=False)
    filename: Mapped[str] = mapped_column(String(512), nullable=False)
    checksum: Mapped[str] = mapped_column(String(64), nullable=False)
    import_version: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[KnowledgeImportStatus] = mapped_column(Enum(KnowledgeImportStatus, name="knowledge_import_status", create_constraint=True), nullable=False, default=KnowledgeImportStatus.QUEUED)
    validated_posts: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    imported_posts: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    skipped_posts: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    error: Mapped[str | None] = mapped_column(Text)
    enrichment_cost: Mapped[float | None] = mapped_column(Numeric(14, 6))
    embedding_cost: Mapped[float | None] = mapped_column(Numeric(14, 6))
    total_cost: Mapped[float | None] = mapped_column(Numeric(14, 6))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class KnowledgeDocument(Base):
    __tablename__ = "knowledge_documents"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    post_id: Mapped[int] = mapped_column(Integer, ForeignKey("posts.id"), nullable=False, unique=True)
    title: Mapped[str | None] = mapped_column(String(500))
    summary: Mapped[str | None] = mapped_column(Text)
    topics: Mapped[list[str] | None] = mapped_column(JSON)
    entities: Mapped[list[dict[str, Any]] | None] = mapped_column(JSON)
    content_type: Mapped[str | None] = mapped_column(String(64))
    epistemic_status: Mapped[str | None] = mapped_column(String(64))
    questions_answered: Mapped[list[str] | None] = mapped_column(JSON)
    claims: Mapped[list[dict[str, Any]] | None] = mapped_column(JSON)
    source_content_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    enrichment_model: Mapped[str | None] = mapped_column(String(255))
    enrichment_prompt_version: Mapped[str | None] = mapped_column(String(64))
    enrichment_status: Mapped[EnrichmentStatus] = mapped_column(Enum(EnrichmentStatus, name="knowledge_enrichment_status", create_constraint=True), nullable=False, default=EnrichmentStatus.PENDING)
    enrichment_attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    enrichment_error: Mapped[str | None] = mapped_column(Text)
    enriched_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now())

    post = relationship("Post", back_populates="knowledge_document", lazy="selectin")
    representations: Mapped[list["KnowledgeRepresentation"]] = relationship(back_populates="document", cascade="all, delete-orphan", lazy="selectin")


class KnowledgeRepresentation(Base):
    __tablename__ = "knowledge_representations"
    __table_args__ = (
        UniqueConstraint("post_id", "representation_type", "ordinal", "index_version", name="uq_knowledge_representation_version"),
        Index("ix_knowledge_representation_post_status", "post_id", "index_status"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    knowledge_document_id: Mapped[int] = mapped_column(Integer, ForeignKey("knowledge_documents.id"), nullable=False)
    post_id: Mapped[int] = mapped_column(Integer, ForeignKey("posts.id"), nullable=False)
    representation_type: Mapped[RepresentationType] = mapped_column(Enum(RepresentationType, name="knowledge_representation_type", create_constraint=True), nullable=False)
    ordinal: Mapped[int | None] = mapped_column(Integer)
    text: Mapped[str] = mapped_column(Text, nullable=False)
    text_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    token_count: Mapped[int] = mapped_column(Integer, nullable=False)
    start_offset: Mapped[int | None] = mapped_column(Integer)
    end_offset: Mapped[int | None] = mapped_column(Integer)
    qdrant_point_id: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
    embedding_model: Mapped[str | None] = mapped_column(String(255))
    embedding_version: Mapped[str | None] = mapped_column(String(64))
    index_version: Mapped[int] = mapped_column(Integer, nullable=False, default=1, server_default="1")
    index_status: Mapped[IndexStatus] = mapped_column(Enum(IndexStatus, name="knowledge_index_status", create_constraint=True), nullable=False, default=IndexStatus.PENDING)
    index_attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    index_error: Mapped[str | None] = mapped_column(Text)
    indexed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now())

    document: Mapped[KnowledgeDocument] = relationship(back_populates="representations", lazy="selectin")


class KnowledgeQuery(Base):
    __tablename__ = "knowledge_queries"
    __table_args__ = (Index("ix_knowledge_queries_user_created", "user_id", "created_at"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(Integer, ForeignKey("users.id"), nullable=False)
    scope_type: Mapped[str] = mapped_column(String(32), nullable=False)
    scope_id: Mapped[int] = mapped_column(Integer, nullable=False)
    mode: Mapped[str] = mapped_column(String(16), nullable=False)
    candidate_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    unique_parent_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    source_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    evidence_sufficient: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    conflict_detected: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    duration_ms: Mapped[int | None] = mapped_column(Integer)
    failure: Mapped[str | None] = mapped_column(String(500))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())


class KnowledgeFeedback(Base):
    __tablename__ = "knowledge_feedback"
    __table_args__ = (UniqueConstraint("query_id", "user_id", name="uq_knowledge_feedback_query_user"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    query_id: Mapped[int] = mapped_column(Integer, ForeignKey("knowledge_queries.id"), nullable=False)
    user_id: Mapped[int] = mapped_column(Integer, ForeignKey("users.id"), nullable=False)
    useful: Mapped[bool] = mapped_column(Boolean, nullable=False)
    reason_code: Mapped[str | None] = mapped_column(String(64))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())


class KnowledgeEvaluationRun(Base):
    __tablename__ = "knowledge_evaluation_runs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    knowledge_channel_id: Mapped[int] = mapped_column(Integer, ForeignKey("knowledge_channels.id"), nullable=False)
    index_version: Mapped[int] = mapped_column(Integer, nullable=False)
    dataset_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    mode: Mapped[str] = mapped_column(String(64), nullable=False)
    recall_at_k: Mapped[float | None] = mapped_column(nullable=True)
    mrr: Mapped[float | None] = mapped_column(nullable=True)
    ndcg: Mapped[float | None] = mapped_column(nullable=True)
    duplicate_source_share: Mapped[float | None] = mapped_column(nullable=True)
    latency_ms: Mapped[int | None] = mapped_column(Integer)
    context_tokens: Mapped[int | None] = mapped_column(Integer)
    cost: Mapped[float | None] = mapped_column(nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())


class ExperimentCampaign(Base):
    """Content-free durable control record for one isolated BL-21 campaign."""

    __tablename__ = "experiment_campaigns"
    __table_args__ = (
        UniqueConstraint("campaign_key", name="uq_experiment_campaign_key"),
        Index("ix_experiment_campaign_channel_status", "channel_sha256", "status"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    campaign_key: Mapped[str] = mapped_column(String(128), nullable=False)
    channel_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    dataset_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    source_snapshot_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    source_snapshot_table_count: Mapped[int] = mapped_column(Integer, nullable=False)
    config_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    policy_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    resume_key: Mapped[str] = mapped_column(String(64), nullable=False)
    budget_usd: Mapped[float] = mapped_column(Numeric(14, 6), nullable=False)
    status: Mapped[CampaignState] = mapped_column(Enum(CampaignState, name="experiment_campaign_status", create_constraint=True), nullable=False, default=CampaignState.DRAFT)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now())

    candidates: Mapped[list["ExperimentCandidate"]] = relationship(back_populates="campaign", cascade="all, delete-orphan")


class ExperimentCampaignLock(Base):
    """One durable, hashed-channel owner prevents concurrent campaign runners."""

    __tablename__ = "experiment_campaign_locks"

    channel_sha256: Mapped[str] = mapped_column(String(64), primary_key=True)
    campaign_id: Mapped[int] = mapped_column(Integer, ForeignKey("experiment_campaigns.id", ondelete="CASCADE"), nullable=False, unique=True)
    acquired_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())


class ExperimentCandidate(Base):
    """Content-free candidate lifecycle and aggregate evaluation result."""

    __tablename__ = "experiment_candidates"
    __table_args__ = (
        UniqueConstraint("campaign_id", "config_sha256", name="uq_experiment_candidate_campaign_config"),
        Index("ix_experiment_candidate_campaign_status", "campaign_id", "status"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    campaign_id: Mapped[int] = mapped_column(Integer, ForeignKey("experiment_campaigns.id", ondelete="CASCADE"), nullable=False)
    hypothesis_id: Mapped[str] = mapped_column(String(128), nullable=False)
    config_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    index_label: Mapped[str] = mapped_column(String(128), nullable=False)
    status: Mapped[CandidateState] = mapped_column(Enum(CandidateState, name="experiment_candidate_status", create_constraint=True), nullable=False, default=CandidateState.PLANNED)
    failure_reason: Mapped[str | None] = mapped_column(String(128))
    dev_metrics: Mapped[dict[str, object] | None] = mapped_column(JSON)
    holdout_metrics: Mapped[dict[str, object] | None] = mapped_column(JSON)
    phase_percentiles: Mapped[dict[str, object] | None] = mapped_column(JSON)
    projected_cost_usd: Mapped[float | None] = mapped_column(Numeric(14, 6))
    actual_cost_usd: Mapped[float | None] = mapped_column(Numeric(14, 6))
    promotion_decision: Mapped[PromotionDecision | None] = mapped_column(Enum(PromotionDecision, name="experiment_promotion_decision", create_constraint=True))
    claimed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now())

    campaign: Mapped[ExperimentCampaign] = relationship(back_populates="candidates")
