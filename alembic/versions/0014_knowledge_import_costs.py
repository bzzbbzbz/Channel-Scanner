"""knowledge import cost totals

Revision ID: 0014_knowledge_import_costs
Revises: 0013_channel_knowledge_rag
"""

from alembic import op
import sqlalchemy as sa


revision = "0014_knowledge_import_costs"
down_revision = "0013_channel_knowledge_rag"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("knowledge_imports", sa.Column("enrichment_cost", sa.Numeric(precision=14, scale=6), nullable=True))
    op.add_column("knowledge_imports", sa.Column("embedding_cost", sa.Numeric(precision=14, scale=6), nullable=True))
    op.add_column("knowledge_imports", sa.Column("total_cost", sa.Numeric(precision=14, scale=6), nullable=True))


def downgrade() -> None:
    op.drop_column("knowledge_imports", "total_cost")
    op.drop_column("knowledge_imports", "embedding_cost")
    op.drop_column("knowledge_imports", "enrichment_cost")
