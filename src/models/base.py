"""SQLAlchemy async declarative base with naming conventions."""

from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    """Async-friendly declarative base with naming conventions."""

    pass


# Naming convention for constraints
convention = {
    "ix": "ix_%(column_0_label)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "fk": "fk_%(table_name)s_%(referred_table_name)s_%(column_0_name)s",
    "pk": "pk_%(table_name)s",
}

Base.metadata.naming_convention = convention
