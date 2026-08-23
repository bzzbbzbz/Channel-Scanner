"""conversational RAG catalog descriptions and stage timings

Revision ID: 0017_conversational_rag_catalog
Revises: 0016_controlled_rag_rollout
"""

from alembic import op
import sqlalchemy as sa


revision = "0017_conversational_rag_catalog"
down_revision = "0016_controlled_rag_rollout"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("knowledge_channels", sa.Column("description", sa.Text(), nullable=True))
    op.add_column("knowledge_channels", sa.Column("description_source_hash", sa.String(length=64), nullable=True))
    op.add_column("knowledge_channels", sa.Column("description_updated_at", sa.DateTime(timezone=True), nullable=True))
    for name in (
        "catalog_selection_ms", "vector_retrieval_ms", "lexical_retrieval_ms",
        "rerank_ms", "answer_generation_ms", "rendering_ms",
    ):
        op.add_column("knowledge_queries", sa.Column(name, sa.Integer(), nullable=True))
    for name in ("citation_precision", "citation_recall", "citation_f1", "claim_precision", "claim_recall", "claim_f1"):
        op.add_column("knowledge_evaluation_runs", sa.Column(name, sa.Float(), nullable=True))


def downgrade() -> None:
    for name in ("claim_f1", "claim_recall", "claim_precision", "citation_f1", "citation_recall", "citation_precision"):
        op.drop_column("knowledge_evaluation_runs", name)
    for name in (
        "rendering_ms", "answer_generation_ms", "rerank_ms", "lexical_retrieval_ms",
        "vector_retrieval_ms", "catalog_selection_ms",
    ):
        op.drop_column("knowledge_queries", name)
    op.drop_column("knowledge_channels", "description_updated_at")
    op.drop_column("knowledge_channels", "description_source_hash")
    op.drop_column("knowledge_channels", "description")
