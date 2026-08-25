"""Alembic async migration environment."""

import asyncio
from logging.config import fileConfig

from sqlalchemy import Column, MetaData, PrimaryKeyConstraint, String, Table, pool, text
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import async_engine_from_config

from alembic import context

from src.config.settings import get_settings
from src.models.base import Base

# Import models so they register with Base.metadata
import src.models.channel  # noqa: F401
import src.models.dead_letter  # noqa: F401
import src.models.digest_delivery  # noqa: F401
import src.models.digest_processing_log  # noqa: F401
import src.models.llm_usage  # noqa: F401
import src.models.knowledge  # noqa: F401
import src.models.on_demand_digest  # noqa: F401
import src.models.outbox_event  # noqa: F401
import src.models.post  # noqa: F401
import src.models.reliable_digest  # noqa: F401
import src.models.reliability_role_heartbeat  # noqa: F401
import src.models.subscription  # noqa: F401
import src.models.user  # noqa: F401

# Alembic Config object
config = context.config

# Set up logging from ini file
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Target metadata for autogenerate
target_metadata = Base.metadata


def ensure_version_table_capacity(connection: Connection) -> None:
    """Allow the repository's descriptive revision IDs on new and existing databases."""
    version_table = Table(
        "alembic_version",
        MetaData(),
        Column("version_num", String(64), nullable=False),
        PrimaryKeyConstraint("version_num", name="alembic_version_pkc"),
    )
    with connection.begin():
        version_table.create(connection, checkfirst=True)
        if connection.dialect.name == "postgresql":
            connection.execute(
                text("ALTER TABLE alembic_version ALTER COLUMN version_num TYPE VARCHAR(64)")
            )


def get_url() -> str:
    """Get database URL from Settings."""
    settings = get_settings()
    return settings.database.url


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode (SQL script generation)."""
    url = get_url()
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def do_run_migrations(connection: Connection) -> None:
    """Run migrations using a provided connection."""
    ensure_version_table_capacity(connection)
    context.configure(connection=connection, target_metadata=target_metadata)

    with context.begin_transaction():
        context.run_migrations()


async def run_async_migrations() -> None:
    """Run migrations in 'online' mode using async engine."""
    configuration = config.get_section(config.config_ini_section, {})
    configuration["sqlalchemy.url"] = get_url()

    connectable = async_engine_from_config(
        configuration,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    async with connectable.connect() as connection:
        await connection.run_sync(do_run_migrations)

    await connectable.dispose()


def run_migrations_online() -> None:
    """Entry point for online migrations — delegates to async."""
    asyncio.run(run_async_migrations())


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
