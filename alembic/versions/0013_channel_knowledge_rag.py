"""channel knowledge RAG

Revision ID: 0013_channel_knowledge_rag
Revises: 0012_llm_usage_telemetry
"""

from alembic import op
import sqlalchemy as sa


revision = "0013_channel_knowledge_rag"
down_revision = "0012_llm_usage_telemetry"
branch_labels = None
depends_on = None


def _enum(name: str, *values: str) -> sa.Enum:
    return sa.Enum(*values, name=name)


def upgrade() -> None:
    op.create_table(
        "knowledge_channel_requests",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("username", sa.String(length=255), nullable=False),
        sa.Column("requester_user_id", sa.Integer(), sa.ForeignKey("users.id"), nullable=False),
        sa.Column("status", _enum("knowledge_request_status", "PENDING", "APPROVED", "REJECTED"), nullable=False),
        sa.Column("decision_reason", sa.Text()),
        sa.Column("administrator_telegram_id", sa.BigInteger()),
        sa.Column("decided_at", sa.DateTime(timezone=True)),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("CURRENT_TIMESTAMP"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("CURRENT_TIMESTAMP"), nullable=False),
        sa.UniqueConstraint("username", "requester_user_id", name="uq_knowledge_request_username_requester"),
    )
    op.create_table(
        "knowledge_channels",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("channel_id", sa.Integer(), sa.ForeignKey("channels.id"), nullable=False, unique=True),
        sa.Column("state", _enum("knowledge_channel_state", "PENDING_IMPORT", "IMPORTING", "READY", "ERROR"), nullable=False),
        sa.Column("active_index_version", sa.Integer()),
        sa.Column("last_imported_at", sa.DateTime(timezone=True)),
        sa.Column("last_synced_at", sa.DateTime(timezone=True)),
        sa.Column("post_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("representation_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("error_summary", sa.Text()),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("CURRENT_TIMESTAMP"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("CURRENT_TIMESTAMP"), nullable=False),
    )
    op.create_table(
        "knowledge_imports",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("knowledge_channel_id", sa.Integer(), sa.ForeignKey("knowledge_channels.id"), nullable=False),
        sa.Column("request_id", sa.Integer(), sa.ForeignKey("knowledge_channel_requests.id")),
        sa.Column("administrator_telegram_id", sa.BigInteger(), nullable=False),
        sa.Column("filename", sa.String(length=512), nullable=False),
        sa.Column("checksum", sa.String(length=64), nullable=False),
        sa.Column("import_version", sa.String(length=64), nullable=False),
        sa.Column("status", _enum("knowledge_import_status", "QUEUED", "RUNNING", "COMPLETED", "FAILED"), nullable=False),
        sa.Column("validated_posts", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("imported_posts", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("skipped_posts", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("error", sa.Text()),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("CURRENT_TIMESTAMP"), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True)),
    )
    op.create_index("ix_knowledge_import_channel_status", "knowledge_imports", ["knowledge_channel_id", "status"])
    op.create_table(
        "knowledge_documents",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("post_id", sa.Integer(), sa.ForeignKey("posts.id"), nullable=False, unique=True),
        sa.Column("title", sa.String(length=500)), sa.Column("summary", sa.Text()),
        sa.Column("topics", sa.JSON()), sa.Column("entities", sa.JSON()),
        sa.Column("content_type", sa.String(length=64)), sa.Column("epistemic_status", sa.String(length=64)),
        sa.Column("questions_answered", sa.JSON()), sa.Column("claims", sa.JSON()),
        sa.Column("source_content_hash", sa.String(length=64), nullable=False),
        sa.Column("enrichment_model", sa.String(length=255)), sa.Column("enrichment_prompt_version", sa.String(length=64)),
        sa.Column("enrichment_status", _enum("knowledge_enrichment_status", "PENDING", "READY", "FAILED", "STALE"), nullable=False),
        sa.Column("enriched_at", sa.DateTime(timezone=True)),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("CURRENT_TIMESTAMP"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("CURRENT_TIMESTAMP"), nullable=False),
    )
    op.create_table(
        "knowledge_representations",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("knowledge_document_id", sa.Integer(), sa.ForeignKey("knowledge_documents.id"), nullable=False),
        sa.Column("post_id", sa.Integer(), sa.ForeignKey("posts.id"), nullable=False),
        sa.Column("representation_type", _enum("knowledge_representation_type", "SUMMARY", "FULL", "CHUNK"), nullable=False),
        sa.Column("ordinal", sa.Integer()), sa.Column("text", sa.Text(), nullable=False), sa.Column("text_hash", sa.String(length=64), nullable=False),
        sa.Column("token_count", sa.Integer(), nullable=False), sa.Column("start_offset", sa.Integer()), sa.Column("end_offset", sa.Integer()),
        sa.Column("qdrant_point_id", sa.String(length=64), nullable=False, unique=True), sa.Column("embedding_model", sa.String(length=255)),
        sa.Column("embedding_version", sa.String(length=64)), sa.Column("index_version", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("index_status", _enum("knowledge_index_status", "PENDING", "INDEXED", "FAILED", "STALE"), nullable=False),
        sa.Column("indexed_at", sa.DateTime(timezone=True)),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("CURRENT_TIMESTAMP"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("CURRENT_TIMESTAMP"), nullable=False),
        sa.UniqueConstraint("post_id", "representation_type", "ordinal", "index_version", name="uq_knowledge_representation_version"),
    )
    op.create_index("ix_knowledge_representation_post_status", "knowledge_representations", ["post_id", "index_status"])
    op.create_table(
        "knowledge_queries",
        sa.Column("id", sa.Integer(), primary_key=True), sa.Column("user_id", sa.Integer(), sa.ForeignKey("users.id"), nullable=False),
        sa.Column("scope_type", sa.String(length=32), nullable=False), sa.Column("scope_id", sa.Integer(), nullable=False), sa.Column("mode", sa.String(length=16), nullable=False),
        sa.Column("candidate_count", sa.Integer(), nullable=False, server_default="0"), sa.Column("unique_parent_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("source_count", sa.Integer(), nullable=False, server_default="0"), sa.Column("evidence_sufficient", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("conflict_detected", sa.Boolean(), nullable=False, server_default=sa.false()), sa.Column("duration_ms", sa.Integer()), sa.Column("failure", sa.String(length=500)),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("CURRENT_TIMESTAMP"), nullable=False),
    )
    op.create_index("ix_knowledge_queries_user_created", "knowledge_queries", ["user_id", "created_at"])
    op.create_table(
        "knowledge_feedback",
        sa.Column("id", sa.Integer(), primary_key=True), sa.Column("query_id", sa.Integer(), sa.ForeignKey("knowledge_queries.id"), nullable=False),
        sa.Column("user_id", sa.Integer(), sa.ForeignKey("users.id"), nullable=False), sa.Column("useful", sa.Boolean(), nullable=False), sa.Column("reason_code", sa.String(length=64)),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("CURRENT_TIMESTAMP"), nullable=False),
        sa.UniqueConstraint("query_id", "user_id", name="uq_knowledge_feedback_query_user"),
    )
    op.create_table(
        "knowledge_evaluation_runs",
        sa.Column("id", sa.Integer(), primary_key=True), sa.Column("knowledge_channel_id", sa.Integer(), sa.ForeignKey("knowledge_channels.id"), nullable=False),
        sa.Column("index_version", sa.Integer(), nullable=False), sa.Column("dataset_hash", sa.String(length=64), nullable=False), sa.Column("mode", sa.String(length=64), nullable=False),
        sa.Column("recall_at_k", sa.Float()), sa.Column("mrr", sa.Float()), sa.Column("ndcg", sa.Float()), sa.Column("duplicate_source_share", sa.Float()),
        sa.Column("latency_ms", sa.Integer()), sa.Column("context_tokens", sa.Integer()), sa.Column("cost", sa.Float()),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("CURRENT_TIMESTAMP"), nullable=False),
    )


def downgrade() -> None:
    for table in ["knowledge_evaluation_runs", "knowledge_feedback", "knowledge_queries", "knowledge_representations", "knowledge_documents", "knowledge_imports", "knowledge_channels", "knowledge_channel_requests"]:
        op.drop_table(table)
    for name in ["knowledge_index_status", "knowledge_representation_type", "knowledge_enrichment_status", "knowledge_import_status", "knowledge_channel_state", "knowledge_request_status"]:
        _enum(name).drop(op.get_bind(), checkfirst=True)
