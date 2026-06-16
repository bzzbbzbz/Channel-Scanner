"""custom filter prompt

Revision ID: 0009_custom_filter_prompt
Revises: 0008_digest_delivery_status
Create Date: 2026-06-16 00:00:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0009_custom_filter_prompt"
down_revision: Union[str, Sequence[str], None] = "0008_digest_delivery_status"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("subscriptions", sa.Column("filter_prompt", sa.Text(), nullable=True))
    op.execute("UPDATE subscriptions SET digest_format = 'summary', summary_mode = 'brief' WHERE digest_format = 'short'")
    op.alter_column("subscriptions", "digest_format", server_default="summary")


def downgrade() -> None:
    op.alter_column("subscriptions", "digest_format", server_default="short")
    op.drop_column("subscriptions", "filter_prompt")
