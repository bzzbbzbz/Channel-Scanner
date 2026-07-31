"""knowledge retry state

Revision ID: 0015_knowledge_retry_state
Revises: 0014_knowledge_import_costs
"""

from alembic import op
import sqlalchemy as sa


revision = "0015_knowledge_retry_state"
down_revision = "0014_knowledge_import_costs"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("knowledge_documents", sa.Column("enrichment_attempts", sa.Integer(), nullable=False, server_default="0"))
    op.add_column("knowledge_documents", sa.Column("enrichment_error", sa.Text(), nullable=True))
    op.add_column("knowledge_representations", sa.Column("index_attempts", sa.Integer(), nullable=False, server_default="0"))
    op.add_column("knowledge_representations", sa.Column("index_error", sa.Text(), nullable=True))


def downgrade() -> None:
    op.drop_column("knowledge_representations", "index_error")
    op.drop_column("knowledge_representations", "index_attempts")
    op.drop_column("knowledge_documents", "enrichment_error")
    op.drop_column("knowledge_documents", "enrichment_attempts")
