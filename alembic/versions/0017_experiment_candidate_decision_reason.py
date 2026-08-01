"""Store content-free BL-21 candidate decision codes.

Revision ID: 0017_experiment_candidate_decision_reason
Revises: 0016_experiment_control_plane
"""

from alembic import op
import sqlalchemy as sa


revision = "0017_experiment_candidate_decision_reason"
down_revision = "0016_experiment_control_plane"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("experiment_candidates", sa.Column("decision_reason", sa.String(length=128)))


def downgrade() -> None:
    op.drop_column("experiment_candidates", "decision_reason")
