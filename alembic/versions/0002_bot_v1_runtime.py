"""bot v1 runtime

Revision ID: 0002_bot_v1_runtime
Revises: 0001_initial_foundation
Create Date: 2026-04-26 00:30:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "0002_bot_v1_runtime"
down_revision: Union[str, Sequence[str], None] = "0001_initial_foundation"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


digest_format = postgresql.ENUM("short", "full", name="digest_format")
delivery_frequency = postgresql.ENUM("hourly", "daily", name="delivery_frequency")


def upgrade() -> None:
    op.alter_column("channels", "telegram_id", existing_type=sa.BigInteger(), nullable=True)
    op.create_unique_constraint("uq_channels_username", "channels", ["username"])

    op.create_table(
        "users",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("telegram_user_id", sa.BigInteger(), nullable=False),
        sa.Column("chat_id", sa.BigInteger(), nullable=False),
        sa.Column("chat_type", sa.String(length=32), nullable=False),
        sa.Column("username", sa.String(length=255), nullable=True),
        sa.Column("first_name", sa.String(length=255), nullable=True),
        sa.Column("last_name", sa.String(length=255), nullable=True),
        sa.Column("digest_format", digest_format, server_default="short", nullable=False),
        sa.Column("frequency", delivery_frequency, server_default="daily", nullable=False),
        sa.Column("timezone", sa.String(length=64), server_default="UTC", nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("id", name="pk_users"),
        sa.UniqueConstraint("telegram_user_id", name="uq_users_telegram_user_id"),
    )
    op.create_index("ix_users_chat_id", "users", ["chat_id"], unique=False)
    op.create_index("ix_users_telegram_user_id", "users", ["telegram_user_id"], unique=False)

    op.create_table(
        "subscriptions",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("user_id", sa.Integer(), nullable=False),
        sa.Column("channel_id", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.ForeignKeyConstraint(["channel_id"], ["channels.id"], name="fk_subscriptions_channels_channel_id"),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], name="fk_subscriptions_users_user_id"),
        sa.PrimaryKeyConstraint("id", name="pk_subscriptions"),
        sa.UniqueConstraint("user_id", "channel_id", name="uq_subscriptions_user_channel"),
    )


def downgrade() -> None:
    op.drop_table("subscriptions")

    op.drop_index("ix_users_telegram_user_id", table_name="users")
    op.drop_index("ix_users_chat_id", table_name="users")
    op.drop_table("users")

    op.drop_constraint("uq_channels_username", "channels", type_="unique")
    op.alter_column("channels", "telegram_id", existing_type=sa.BigInteger(), nullable=False)

    bind = op.get_bind()
    delivery_frequency.drop(bind, checkfirst=True)
    digest_format.drop(bind, checkfirst=True)
