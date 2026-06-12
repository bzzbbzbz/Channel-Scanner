"""summary modes and llm delivery

Revision ID: 0005_summary_modes_and_llm_delivery
Revises: 0004_user_language_and_subscription_baseline
Create Date: 2026-04-27 01:30:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0005_summary_modes_and_llm_delivery"
down_revision: Union[str, Sequence[str], None] = "0004_user_language_and_subscription_baseline"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    bind = op.get_bind()
    dialect = bind.dialect.name
    summary_mode_enum = sa.Enum("brief", "detailed", "custom", name="summary_mode")

    if dialect == "postgresql":
        op.execute("ALTER TYPE digest_format RENAME TO digest_format_old")
        op.execute("CREATE TYPE digest_format AS ENUM ('short', 'summary')")
        op.execute("ALTER TABLE users ALTER COLUMN digest_format DROP DEFAULT")
        op.execute(
            "ALTER TABLE users ALTER COLUMN digest_format TYPE digest_format USING "
            "CASE WHEN digest_format::text = 'full' THEN 'summary'::digest_format ELSE digest_format::text::digest_format END"
        )
        op.execute("ALTER TABLE users ALTER COLUMN digest_format SET DEFAULT 'short'")
        op.execute("DROP TYPE digest_format_old")

    summary_mode_enum.create(bind, checkfirst=True)
    op.add_column("users", sa.Column("summary_mode", summary_mode_enum, server_default="brief", nullable=False))
    op.add_column("users", sa.Column("custom_prompt", sa.Text(), nullable=True))
    op.add_column("digest_deliveries", sa.Column("summary_text", sa.Text(), nullable=True))
    op.add_column("digest_deliveries", sa.Column("summary_mode", sa.String(length=32), nullable=True))
    op.add_column("digest_deliveries", sa.Column("summary_model", sa.String(length=255), nullable=True))
    op.add_column("digest_deliveries", sa.Column("prompt_snapshot", sa.Text(), nullable=True))

    if dialect != "postgresql":
        op.execute("UPDATE users SET digest_format = 'summary' WHERE digest_format = 'full'")


def downgrade() -> None:
    bind = op.get_bind()
    dialect = bind.dialect.name
    summary_mode_enum = sa.Enum("brief", "detailed", "custom", name="summary_mode")

    op.drop_column("digest_deliveries", "prompt_snapshot")
    op.drop_column("digest_deliveries", "summary_model")
    op.drop_column("digest_deliveries", "summary_mode")
    op.drop_column("digest_deliveries", "summary_text")
    op.drop_column("users", "custom_prompt")
    op.drop_column("users", "summary_mode")
    summary_mode_enum.drop(bind, checkfirst=True)

    if dialect == "postgresql":
        op.execute("ALTER TYPE digest_format RENAME TO digest_format_new")
        op.execute("CREATE TYPE digest_format AS ENUM ('short', 'full')")
        op.execute("ALTER TABLE users ALTER COLUMN digest_format DROP DEFAULT")
        op.execute(
            "ALTER TABLE users ALTER COLUMN digest_format TYPE digest_format USING "
            "CASE WHEN digest_format::text = 'summary' THEN 'full'::digest_format ELSE digest_format::text::digest_format END"
        )
        op.execute("ALTER TABLE users ALTER COLUMN digest_format SET DEFAULT 'short'")
        op.execute("DROP TYPE digest_format_new")
    else:
        op.execute("UPDATE users SET digest_format = 'full' WHERE digest_format = 'summary'")
