"""Bind BL-21 campaigns to immutable evaluation baselines.

Revision ID: 0018_experiment_baseline_snapshot
Revises: 0017_experiment_candidate_decision_reason
"""

from alembic import op
import sqlalchemy as sa

from src.knowledge.experiment_storage import ContentFreeExperimentJSON


revision = "0018_experiment_baseline_snapshot"
down_revision = "0017_experiment_candidate_decision_reason"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Existing campaigns are intentionally left without a baseline and cannot resume.
    op.add_column("experiment_campaigns", sa.Column("baseline_run_id", sa.Integer()))
    op.add_column("experiment_campaigns", sa.Column("baseline_snapshot_sha256", sa.String(length=64)))
    op.add_column("experiment_campaigns", sa.Column("baseline_snapshot", ContentFreeExperimentJSON("baseline_snapshot")))


def downgrade() -> None:
    op.drop_column("experiment_campaigns", "baseline_snapshot")
    op.drop_column("experiment_campaigns", "baseline_snapshot_sha256")
    op.drop_column("experiment_campaigns", "baseline_run_id")
