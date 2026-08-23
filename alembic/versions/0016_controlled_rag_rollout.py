"""controlled RAG rollout audit fields

Revision ID: 0016_controlled_rag_rollout
Revises: 0015_knowledge_retry_state
"""

from alembic import op
import sqlalchemy as sa


revision = "0016_controlled_rag_rollout"
down_revision = "0015_knowledge_retry_state"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "rag_search_configurations",
        sa.Column("id", sa.String(length=64), primary_key=True),
        sa.Column("code_version", sa.String(length=64), nullable=False),
        sa.Column("index_version", sa.Integer(), nullable=False),
        sa.Column("query_instruction_hash", sa.String(length=64), nullable=False),
        sa.Column("reranker_model", sa.String(length=255), nullable=True),
        sa.Column("candidate_limit", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("operator", sa.String(length=128), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("CURRENT_TIMESTAMP"), nullable=False),
    )
    op.add_column("knowledge_queries", sa.Column("rag_variant", sa.String(length=64), nullable=False, server_default="baseline"))
    op.add_column("knowledge_queries", sa.Column("rerank_fallback_reason", sa.String(length=64), nullable=True))
    op.add_column("knowledge_queries", sa.Column("rerank_cost", sa.Numeric(14, 6), nullable=True))
    op.add_column("knowledge_evaluation_runs", sa.Column("precision_at_k", sa.Float(), nullable=True))
    op.add_column("knowledge_evaluation_runs", sa.Column("question_count", sa.Integer(), nullable=False, server_default="0"))
    op.add_column("knowledge_evaluation_runs", sa.Column("labels_complete", sa.Boolean(), nullable=False, server_default=sa.false()))
    op.add_column("knowledge_evaluation_runs", sa.Column("configuration_id", sa.String(length=64), nullable=False, server_default="baseline"))
    op.add_column("knowledge_evaluation_runs", sa.Column("reranker_model", sa.String(length=255), nullable=True))
    op.add_column("knowledge_evaluation_runs", sa.Column("rerank_fallback_share", sa.Float(), nullable=True))
    op.add_column("knowledge_evaluation_runs", sa.Column("correct_abstention_share", sa.Float(), nullable=True))
    op.add_column("knowledge_evaluation_runs", sa.Column("false_attribution_share", sa.Float(), nullable=True))
    op.add_column("knowledge_evaluation_runs", sa.Column("source_sufficiency_share", sa.Float(), nullable=True))
    op.add_column("knowledge_evaluation_runs", sa.Column("faithfulness", sa.Float(), nullable=True))
    op.add_column("knowledge_evaluation_runs", sa.Column("citation_validity", sa.Float(), nullable=True))
    op.add_column("knowledge_evaluation_runs", sa.Column("citation_completeness", sa.Float(), nullable=True))
    op.add_column("knowledge_evaluation_runs", sa.Column("answer_relevance", sa.Float(), nullable=True))
    op.add_column("knowledge_evaluation_runs", sa.Column("answer_audit_sample_size", sa.Integer(), nullable=True))
    op.add_column("knowledge_evaluation_runs", sa.Column("judge_version", sa.String(length=128), nullable=True))
    op.add_column("knowledge_evaluation_runs", sa.Column("p50_latency_ms", sa.Integer(), nullable=True))
    op.add_column("knowledge_evaluation_runs", sa.Column("p95_latency_ms", sa.Integer(), nullable=True))
    op.add_column("knowledge_evaluation_runs", sa.Column("p99_latency_ms", sa.Integer(), nullable=True))


def downgrade() -> None:
    for column in ("judge_version", "answer_audit_sample_size", "answer_relevance", "citation_completeness", "citation_validity", "faithfulness", "source_sufficiency_share", "false_attribution_share", "correct_abstention_share", "p99_latency_ms", "p95_latency_ms", "p50_latency_ms", "rerank_fallback_share", "reranker_model", "configuration_id", "labels_complete", "question_count", "precision_at_k"):
        op.drop_column("knowledge_evaluation_runs", column)
    for column in ("rerank_cost", "rerank_fallback_reason", "rag_variant"):
        op.drop_column("knowledge_queries", column)
    op.drop_table("rag_search_configurations")
