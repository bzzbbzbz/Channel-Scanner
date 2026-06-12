"""digest delivery v1

Revision ID: 0003_digest_delivery_v1
Revises: 0002_bot_v1_runtime
Create Date: 2026-04-26 01:30:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0003_digest_delivery_v1"
down_revision: Union[str, Sequence[str], None] = "0002_bot_v1_runtime"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("users", sa.Column("last_digest_at", sa.DateTime(timezone=True), nullable=True))

    op.create_table(
        "digest_deliveries",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("user_id", sa.Integer(), nullable=False),
        sa.Column("post_id", sa.Integer(), nullable=False),
        sa.Column("delivered_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.ForeignKeyConstraint(["post_id"], ["posts.id"], name="fk_digest_deliveries_posts_post_id"),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], name="fk_digest_deliveries_users_user_id"),
        sa.PrimaryKeyConstraint("id", name="pk_digest_deliveries"),
        sa.UniqueConstraint("user_id", "post_id", name="uq_digest_deliveries_user_post"),
    )
    op.create_index("ix_digest_deliveries_user_id", "digest_deliveries", ["user_id"], unique=False)
    op.create_index("ix_digest_deliveries_post_id", "digest_deliveries", ["post_id"], unique=False)


def downgrade() -> None:
    op.drop_index("ix_digest_deliveries_post_id", table_name="digest_deliveries")
    op.drop_index("ix_digest_deliveries_user_id", table_name="digest_deliveries")
    op.drop_table("digest_deliveries")
    op.drop_column("users", "last_digest_at")
