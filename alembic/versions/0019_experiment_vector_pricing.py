"""Record content-free BL-21 vector pricing provenance.

Revision ID: 0019_experiment_vector_pricing
Revises: 0018_experiment_baseline_snapshot
"""

from alembic import op
import sqlalchemy as sa


revision = "0019_experiment_vector_pricing"
down_revision = "0018_experiment_baseline_snapshot"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("experiment_candidates") as batch:
        batch.add_column(sa.Column("embedding_model_id", sa.String(length=255), nullable=True))
        batch.add_column(sa.Column("embedding_pricing_version", sa.String(length=128), nullable=True))
        batch.add_column(sa.Column("embedding_pricing_source", sa.String(length=64), nullable=True))
        batch.add_column(sa.Column("embedding_input_tokens", sa.Integer(), nullable=True))


def downgrade() -> None:
    with op.batch_alter_table("experiment_candidates") as batch:
        batch.drop_column("embedding_input_tokens")
        batch.drop_column("embedding_pricing_source")
        batch.drop_column("embedding_pricing_version")
        batch.drop_column("embedding_model_id")
