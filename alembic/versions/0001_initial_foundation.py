"""initial foundation

Revision ID: 0001_initial_foundation
Revises:
Create Date: 2026-04-26 00:00:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "0001_initial_foundation"
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


channel_status = postgresql.ENUM(
    "active",
    "error",
    "paused",
    name="channel_status",
)


def upgrade() -> None:
    op.create_table(
        "channels",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("telegram_id", sa.BigInteger(), nullable=False, comment="Numeric channel ID (survives username changes)"),
        sa.Column("username", sa.String(length=255), nullable=True, comment="Channel username (can change)"),
        sa.Column("name", sa.String(length=500), nullable=True, comment="Display name"),
        sa.Column("status", channel_status, server_default="active", nullable=False),
        sa.Column("last_scraped", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_error", sa.Text(), nullable=True, comment="Last error message if status=error"),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("id", name="pk_channels"),
        sa.UniqueConstraint("telegram_id", name="uq_channels_telegram_id"),
    )
    op.create_index("ix_channels_status", "channels", ["status"], unique=False)
    op.create_index("ix_channels_telegram_id", "channels", ["telegram_id"], unique=False)

    op.create_table(
        "posts",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("post_id", sa.BigInteger(), nullable=False, comment="Numeric ID from data-post attribute"),
        sa.Column("channel_id", sa.Integer(), nullable=False),
        sa.Column("content", sa.Text(), nullable=False, comment="Post content as Markdown"),
        sa.Column("datetime", sa.DateTime(timezone=True), nullable=False, comment="Post publication time"),
        sa.Column("views", sa.Integer(), nullable=True, comment="View count (parsed from '1.5K' format)"),
        sa.Column("reactions", sa.JSON(), nullable=True, comment='{"emoji": count}'),
        sa.Column("link_preview", sa.JSON(), nullable=True, comment='{"title", "site_name", "description", "url"}'),
        sa.Column("author", sa.String(length=500), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.ForeignKeyConstraint(["channel_id"], ["channels.id"], name="fk_posts_channels_channel_id"),
        sa.PrimaryKeyConstraint("id", name="pk_posts"),
        sa.UniqueConstraint("channel_id", "post_id", name="uq_posts_channel_post"),
    )
    op.create_index("ix_posts_channel_id", "posts", ["channel_id"], unique=False)
    op.create_index("ix_posts_channel_post_id", "posts", ["channel_id", "post_id"], unique=False)
    op.create_index("ix_posts_datetime", "posts", ["datetime"], unique=False)


def downgrade() -> None:
    op.drop_index("ix_posts_datetime", table_name="posts")
    op.drop_index("ix_posts_channel_post_id", table_name="posts")
    op.drop_index("ix_posts_channel_id", table_name="posts")
    op.drop_table("posts")

    op.drop_index("ix_channels_telegram_id", table_name="channels")
    op.drop_index("ix_channels_status", table_name="channels")
    op.drop_table("channels")

    bind = op.get_bind()
    channel_status.drop(bind, checkfirst=True)
