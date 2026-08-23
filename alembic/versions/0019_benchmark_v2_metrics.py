"""persist content-free benchmark v2 metric aggregates

Revision ID: 0019_benchmark_v2_metrics
Revises: 0018_evaluation_stage_latency
"""

from alembic import op
import sqlalchemy as sa


revision = "0019_benchmark_v2_metrics"
down_revision = "0018_evaluation_stage_latency"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("knowledge_evaluation_runs", sa.Column("claim_coverage_sufficiency_share", sa.Float(), nullable=True))
    op.add_column("knowledge_evaluation_runs", sa.Column("citation_placement", sa.Float(), nullable=True))
    op.add_column("knowledge_evaluation_runs", sa.Column("judge_claim_precision", sa.Float(), nullable=True))
    op.add_column("knowledge_evaluation_runs", sa.Column("judge_claim_recall", sa.Float(), nullable=True))
    op.add_column("knowledge_evaluation_runs", sa.Column("judge_claim_f1", sa.Float(), nullable=True))


def downgrade() -> None:
    for name in (
        "judge_claim_f1", "judge_claim_recall", "judge_claim_precision",
        "citation_placement", "claim_coverage_sufficiency_share",
    ):
        op.drop_column("knowledge_evaluation_runs", name)
