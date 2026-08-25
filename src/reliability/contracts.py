"""Versioned, content-free Kafka event contracts for BL-22."""

from __future__ import annotations

import json
from importlib import resources
from typing import Any, Mapping

from jsonschema import Draft202012Validator, FormatChecker

DIGEST_RUN_REQUESTED_TOPIC = "tpb.digest.run.requested.v1"
TELEGRAM_DELIVERY_REQUESTED_TOPIC = "tpb.telegram.delivery.requested.v1"
DIGEST_RUN_REQUESTED_DLQ_TOPIC = "tpb.digest.run.requested.dlq.v1"
TELEGRAM_DELIVERY_REQUESTED_DLQ_TOPIC = "tpb.telegram.delivery.requested.dlq.v1"

DIGEST_RUN_REQUESTED_EVENT = "tpb.digest.run.requested"
TELEGRAM_DELIVERY_REQUESTED_EVENT = "tpb.telegram.delivery.requested"
DIGEST_RUN_REQUESTED_DLQ_EVENT = "tpb.digest.run.requested.dlq"
TELEGRAM_DELIVERY_REQUESTED_DLQ_EVENT = "tpb.telegram.delivery.requested.dlq"

EVENT_SCHEMA_FILES = {
    DIGEST_RUN_REQUESTED_EVENT: "digest-run-requested-v1.schema.json",
    TELEGRAM_DELIVERY_REQUESTED_EVENT: "telegram-delivery-requested-v1.schema.json",
    DIGEST_RUN_REQUESTED_DLQ_EVENT: "digest-run-requested-dlq-v1.schema.json",
    TELEGRAM_DELIVERY_REQUESTED_DLQ_EVENT: "telegram-delivery-requested-dlq-v1.schema.json",
}

EVENT_EXAMPLE_FILES = {
    DIGEST_RUN_REQUESTED_EVENT: "digest-run-requested-v1.json",
    TELEGRAM_DELIVERY_REQUESTED_EVENT: "telegram-delivery-requested-v1.json",
    DIGEST_RUN_REQUESTED_DLQ_EVENT: "digest-run-requested-dlq-v1.json",
    TELEGRAM_DELIVERY_REQUESTED_DLQ_EVENT: "telegram-delivery-requested-dlq-v1.json",
}

EVENT_TOPICS = {
    DIGEST_RUN_REQUESTED_EVENT: DIGEST_RUN_REQUESTED_TOPIC,
    TELEGRAM_DELIVERY_REQUESTED_EVENT: TELEGRAM_DELIVERY_REQUESTED_TOPIC,
    DIGEST_RUN_REQUESTED_DLQ_EVENT: DIGEST_RUN_REQUESTED_DLQ_TOPIC,
    TELEGRAM_DELIVERY_REQUESTED_DLQ_EVENT: TELEGRAM_DELIVERY_REQUESTED_DLQ_TOPIC,
}

DEFAULT_MAX_EVENT_BYTES = 65_536
_SENSITIVE_KEY_FRAGMENTS = (
    "api_key",
    "content",
    "html",
    "password",
    "prompt",
    "provider_response",
    "secret",
    "text",
    "token",
)


class EventContractError(ValueError):
    """An event does not satisfy the supported content-free contract."""


def load_event_schema(event_type: str) -> dict[str, Any]:
    """Load the checked-in schema for a supported event type."""
    try:
        filename = EVENT_SCHEMA_FILES[event_type]
    except KeyError as exc:
        raise EventContractError(f"Unsupported event_type: {event_type}") from exc
    schema_path = resources.files("src.reliability").joinpath("schemas", filename)
    return json.loads(schema_path.read_text(encoding="utf-8"))


def load_event_example(event_type: str) -> dict[str, Any]:
    """Load the checked-in serialization example for a supported event type."""
    try:
        filename = EVENT_EXAMPLE_FILES[event_type]
    except KeyError as exc:
        raise EventContractError(f"Unsupported event_type: {event_type}") from exc
    example_path = resources.files("src.reliability").joinpath("examples", filename)
    return json.loads(example_path.read_text(encoding="utf-8"))


def _reject_sensitive_keys(value: Any, path: str = "$") -> None:
    if isinstance(value, Mapping):
        for key, nested in value.items():
            normalized = str(key).lower().replace("-", "_")
            if any(fragment in normalized for fragment in _SENSITIVE_KEY_FRAGMENTS):
                raise EventContractError(f"Sensitive field is forbidden at {path}.{key}")
            _reject_sensitive_keys(nested, f"{path}.{key}")
    elif isinstance(value, list):
        for index, nested in enumerate(value):
            _reject_sensitive_keys(nested, f"{path}[{index}]")


def validate_event(event: Mapping[str, Any]) -> None:
    """Validate schema/version and reject fields that can carry sensitive content."""
    if not isinstance(event, Mapping):
        raise EventContractError("Event must be a mapping")
    _reject_sensitive_keys(event)
    event_type = event.get("event_type")
    if not isinstance(event_type, str):
        raise EventContractError("event_type must be a string")
    schema = load_event_schema(event_type)
    errors = sorted(
        Draft202012Validator(schema, format_checker=FormatChecker()).iter_errors(event),
        key=lambda error: tuple(str(part) for part in error.absolute_path),
    )
    if errors:
        details = "; ".join(error.message for error in errors[:3])
        raise EventContractError(f"Invalid {event_type} v1 event: {details}")


def serialize_event(event: Mapping[str, Any], *, max_bytes: int = DEFAULT_MAX_EVENT_BYTES) -> bytes:
    """Return deterministic UTF-8 JSON after contract and size validation."""
    validate_event(event)
    encoded = json.dumps(event, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode("utf-8")
    if len(encoded) > max_bytes:
        raise EventContractError(f"Serialized event is {len(encoded)} bytes; limit is {max_bytes}")
    return encoded


def schema_compatibility_errors(old: Mapping[str, Any], new: Mapping[str, Any], path: str = "$") -> list[str]:
    """Report breaking changes; optional property additions remain compatible."""
    errors: list[str] = []
    old_required = set(old.get("required", []))
    new_required = set(new.get("required", []))
    for name in sorted(new_required - old_required):
        errors.append(f"{path}: new required property {name}")

    old_properties = old.get("properties", {})
    new_properties = new.get("properties", {})
    for name, old_property in old_properties.items():
        property_path = f"{path}.{name}"
        if name not in new_properties:
            errors.append(f"{property_path}: property removed")
            continue
        new_property = new_properties[name]
        for keyword in ("type", "const", "format"):
            if keyword in old_property and new_property.get(keyword) != old_property[keyword]:
                errors.append(f"{property_path}: {keyword} changed")
        if "enum" in old_property and not set(old_property["enum"]).issubset(set(new_property.get("enum", []))):
            errors.append(f"{property_path}: enum value removed")
        errors.extend(schema_compatibility_errors(old_property, new_property, property_path))
    return errors
