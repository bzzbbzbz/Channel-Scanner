"""persist content-free evaluation stage-latency aggregates

Revision ID: 0018_evaluation_stage_latency
Revises: 0017_conversational_rag_catalog
"""

from alembic import op
import sqlalchemy as sa


revision = "0018_evaluation_stage_latency"
down_revision = "0017_conversational_rag_catalog"
branch_labels = None
depends_on = None


def upgrade() -> None:
    for name in (
        "p50_retrieval_latency_ms", "p95_retrieval_latency_ms", "p99_retrieval_latency_ms", "retrieval_latency_ms",
        "p50_answer_generation_ms", "p95_answer_generation_ms", "p99_answer_generation_ms", "answer_generation_ms",
    ):
        op.add_column("knowledge_evaluation_runs", sa.Column(name, sa.Integer(), nullable=True))


def downgrade() -> None:
    for name in (
        "answer_generation_ms", "p99_answer_generation_ms", "p95_answer_generation_ms", "p50_answer_generation_ms",
        "retrieval_latency_ms", "p99_retrieval_latency_ms", "p95_retrieval_latency_ms", "p50_retrieval_latency_ms",
    ):
        op.drop_column("knowledge_evaluation_runs", name)
