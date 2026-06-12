"""named subscriptions

Revision ID: 0006_named_subscriptions
Revises: 0005_summary_modes_and_llm_delivery
Create Date: 2026-04-27 12:00:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "0006_named_subscriptions"
down_revision: Union[str, Sequence[str], None] = "0005_summary_modes_and_llm_delivery"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


DIGEST_FORMAT_ENUM = postgresql.ENUM("short", "summary", name="digest_format", create_type=False)
SUMMARY_MODE_ENUM = postgresql.ENUM("brief", "detailed", "custom", name="summary_mode", create_type=False)
FREQUENCY_ENUM = postgresql.ENUM("hourly", "daily", name="delivery_frequency", create_type=False)


def upgrade() -> None:
    op.rename_table("subscriptions", "subscription_channels_legacy")

    op.create_table(
        "subscriptions",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("user_id", sa.Integer(), nullable=False),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("digest_format", DIGEST_FORMAT_ENUM, server_default="short", nullable=False),
        sa.Column("summary_mode", SUMMARY_MODE_ENUM, server_default="brief", nullable=False),
        sa.Column("custom_prompt", sa.Text(), nullable=True),
        sa.Column("frequency", FREQUENCY_ENUM, server_default="daily", nullable=False),
        sa.Column("enabled", sa.Boolean(), server_default=sa.text("true"), nullable=False),
        sa.Column("last_digest_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], name="fk_named_subscriptions_user_id_users"),
        sa.PrimaryKeyConstraint("id", name="pk_named_subscriptions"),
    )

    op.create_table(
        "subscription_channels",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("subscription_id", sa.Integer(), nullable=False),
        sa.Column("channel_id", sa.Integer(), nullable=False),
        sa.Column("subscribed_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.ForeignKeyConstraint(["subscription_id"], ["subscriptions.id"], name="fk_subscription_channels_subscription_id_subscriptions"),
        sa.ForeignKeyConstraint(["channel_id"], ["channels.id"], name="fk_subscription_channels_channel_id_channels"),
        sa.PrimaryKeyConstraint("id", name="pk_subscription_channels"),
        sa.UniqueConstraint("subscription_id", "channel_id", name="uq_subscription_channels_subscription_channel"),
    )

    op.execute(
        """
        INSERT INTO subscriptions (user_id, name, digest_format, summary_mode, custom_prompt, frequency, enabled, last_digest_at)
        SELECT DISTINCT
            users.id,
            CASE WHEN users.language = 'en' THEN 'Subscription 1' ELSE 'Подписка 1' END,
            users.digest_format,
            COALESCE(users.summary_mode, 'brief'),
            users.custom_prompt,
            users.frequency,
            true,
            users.last_digest_at
        FROM users
        JOIN subscription_channels_legacy legacy ON legacy.user_id = users.id
        """
    )

    op.execute(
        """
        INSERT INTO subscription_channels (subscription_id, channel_id, subscribed_at, created_at)
        SELECT subscriptions.id, legacy.channel_id, legacy.subscribed_at, legacy.created_at
        FROM subscription_channels_legacy legacy
        JOIN subscriptions ON subscriptions.user_id = legacy.user_id
        """
    )

    op.drop_table("subscription_channels_legacy")

    op.add_column("digest_deliveries", sa.Column("subscription_id", sa.Integer(), nullable=True))
    op.create_foreign_key(
        "fk_digest_deliveries_subscription_id_subscriptions",
        "digest_deliveries",
        "subscriptions",
        ["subscription_id"],
        ["id"],
    )

    op.execute(
        """
        UPDATE digest_deliveries
        SET subscription_id = subscriptions.id
        FROM posts, subscription_channels, subscriptions
        WHERE digest_deliveries.post_id = posts.id
          AND subscription_channels.channel_id = posts.channel_id
          AND subscriptions.id = subscription_channels.subscription_id
          AND subscriptions.user_id = digest_deliveries.user_id
        """
    )

    op.alter_column("digest_deliveries", "subscription_id", nullable=False)
    op.drop_constraint("uq_digest_deliveries_user_post", "digest_deliveries", type_="unique")
    op.create_unique_constraint(
        "uq_digest_deliveries_subscription_post",
        "digest_deliveries",
        ["subscription_id", "post_id"],
    )

    op.drop_column("users", "last_digest_at")
    op.drop_column("users", "frequency")
    op.drop_column("users", "custom_prompt")
    op.drop_column("users", "summary_mode")
    op.drop_column("users", "digest_format")


def downgrade() -> None:
    op.add_column("users", sa.Column("digest_format", DIGEST_FORMAT_ENUM, server_default="short", nullable=False))
    op.add_column("users", sa.Column("summary_mode", SUMMARY_MODE_ENUM, server_default="brief", nullable=False))
    op.add_column("users", sa.Column("custom_prompt", sa.Text(), nullable=True))
    op.add_column("users", sa.Column("frequency", FREQUENCY_ENUM, server_default="daily", nullable=False))
    op.add_column("users", sa.Column("last_digest_at", sa.DateTime(timezone=True), nullable=True))

    op.execute(
        """
        UPDATE users
        SET digest_format = subscriptions.digest_format,
            summary_mode = subscriptions.summary_mode,
            custom_prompt = subscriptions.custom_prompt,
            frequency = subscriptions.frequency,
            last_digest_at = subscriptions.last_digest_at
        FROM subscriptions
        WHERE subscriptions.user_id = users.id
        """
    )

    op.rename_table("subscription_channels", "subscription_channels_new")
    op.create_table(
        "subscriptions_legacy",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("user_id", sa.Integer(), nullable=False),
        sa.Column("channel_id", sa.Integer(), nullable=False),
        sa.Column("subscribed_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], name="fk_subscriptions_legacy_user_id_users"),
        sa.ForeignKeyConstraint(["channel_id"], ["channels.id"], name="fk_subscriptions_legacy_channel_id_channels"),
        sa.PrimaryKeyConstraint("id", name="pk_subscriptions_legacy"),
        sa.UniqueConstraint("user_id", "channel_id", name="uq_subscriptions_user_channel"),
    )

    op.execute(
        """
        INSERT INTO subscriptions_legacy (user_id, channel_id, subscribed_at, created_at)
        SELECT subscriptions.user_id, subscription_channels_new.channel_id, subscription_channels_new.subscribed_at, subscription_channels_new.created_at
        FROM subscription_channels_new
        JOIN subscriptions ON subscriptions.id = subscription_channels_new.subscription_id
        """
    )

    op.drop_constraint("uq_digest_deliveries_subscription_post", "digest_deliveries", type_="unique")
    op.drop_constraint("fk_digest_deliveries_subscription_id_subscriptions", "digest_deliveries", type_="foreignkey")
    op.create_unique_constraint("uq_digest_deliveries_user_post", "digest_deliveries", ["user_id", "post_id"])
    op.drop_column("digest_deliveries", "subscription_id")

    op.drop_table("subscription_channels_new")
    op.drop_table("subscriptions")
    op.rename_table("subscriptions_legacy", "subscriptions")
