#!/usr/bin/env python3
"""Fail closed unless inspected BL-21 db metadata is the exact expected shape."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any


PGDATA_DESTINATION = "/var/lib/postgresql/data"
SNAPSHOT_DESTINATION = "/bl21-snapshot"
EXPECTED_IMAGE = "postgres:16"


class DatabaseIdentityError(ValueError):
    """The inspected container is not the isolated BL-21 database."""


def _object(value: object, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise DatabaseIdentityError(f"{label} is invalid")
    return value


def _require(value: object, expected: object, label: str) -> None:
    if value != expected:
        raise DatabaseIdentityError(f"{label} is invalid")


def validate_identity(
    inspected: object,
    *,
    container_id: str,
    project_name: str,
    pgdata_volume: str,
    snapshot_directory: str,
) -> None:
    """Validate the exact fixed db container and its only two allowed mounts."""
    container = _object(inspected, "container metadata")
    _require(container.get("Id"), container_id, "container ID")
    _require(container.get("Name"), f"/{project_name}-db-1", "container name")

    config = _object(container.get("Config"), "container config")
    _require(config.get("Image"), EXPECTED_IMAGE, "container image")
    labels = _object(config.get("Labels"), "container labels")
    _require(labels.get("com.docker.compose.project"), project_name, "container project label")
    _require(labels.get("com.docker.compose.service"), "db", "container service label")

    mounts = container.get("Mounts")
    if not isinstance(mounts, list) or len(mounts) != 2:
        raise DatabaseIdentityError("container mounts are invalid")

    expected_mounts = {
        ("volume", PGDATA_DESTINATION): {
            "Name": pgdata_volume,
            "Source": None,
            "RW": True,
        },
        ("bind", SNAPSHOT_DESTINATION): {
            "Name": None,
            "Source": snapshot_directory,
            "RW": False,
        },
    }
    seen: set[tuple[str, str]] = set()
    for raw_mount in mounts:
        mount = _object(raw_mount, "container mount")
        mount_type = mount.get("Type")
        destination = mount.get("Destination")
        key = (mount_type, destination)
        expected = expected_mounts.get(key)
        if expected is None or key in seen:
            raise DatabaseIdentityError("container mounts are invalid")
        seen.add(key)
        _require(mount.get("RW"), expected["RW"], "container mount mode")
        if mount_type == "volume":
            _require(mount.get("Name"), expected["Name"], "container volume")
            if not isinstance(mount.get("Source"), str) or not mount["Source"]:
                raise DatabaseIdentityError("container volume source is invalid")
        else:
            if mount.get("Name") not in (None, ""):
                raise DatabaseIdentityError("container bind name is invalid")
            _require(mount.get("Source"), expected["Source"], "container bind source")
    if seen != set(expected_mounts):
        raise DatabaseIdentityError("container mounts are invalid")


def main() -> int:
    if len(sys.argv) != 5:
        print("error: database identity validator accepts fixed launcher metadata only", file=sys.stderr)
        return 2
    try:
        validate_identity(
            json.load(sys.stdin),
            container_id=sys.argv[1],
            project_name=sys.argv[2],
            pgdata_volume=sys.argv[3],
            snapshot_directory=str(Path(sys.argv[4])),
        )
    except (DatabaseIdentityError, json.JSONDecodeError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
