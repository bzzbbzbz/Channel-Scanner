"""Typed persistence boundary for content-free BL-21 experiment JSON."""

from __future__ import annotations

from typing import Any

from sqlalchemy import JSON
from sqlalchemy.types import TypeDecorator

from src.knowledge.experiments import validate_experiment_json


class ContentFreeExperimentJSON(TypeDecorator[Any]):
    """Validate the closed JSON shape on every SQLAlchemy typed bind.

    This protects ORM attributes and Core statements built from mapped columns.
    It does not make raw untyped SQL safe.
    """

    impl = JSON
    cache_ok = True

    def __init__(self, field_name: str) -> None:
        self.field_name = field_name
        super().__init__()

    def process_bind_param(self, value: object, dialect: object) -> object:
        if value is not None:
            validate_experiment_json(self.field_name, value)
        return value
