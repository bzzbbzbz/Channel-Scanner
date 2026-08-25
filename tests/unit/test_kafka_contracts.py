from __future__ import annotations

from copy import deepcopy

import pytest
from jsonschema import Draft202012Validator

from src.reliability.contracts import (
    EVENT_SCHEMA_FILES,
    DIGEST_RUN_REQUESTED_EVENT,
    EventContractError,
    load_event_example,
    load_event_schema,
    schema_compatibility_errors,
    serialize_event,
    validate_event,
)


@pytest.mark.parametrize("event_type", EVENT_SCHEMA_FILES)
def test_checked_in_examples_validate_and_serialize(event_type: str) -> None:
    Draft202012Validator.check_schema(load_event_schema(event_type))
    example = load_event_example(event_type)

    validate_event(example)
    encoded = serialize_event(example)

    assert encoded.startswith(b"{")
    assert len(encoded) < 65_536


def test_contract_rejects_unsupported_version() -> None:
    event = load_event_example(DIGEST_RUN_REQUESTED_EVENT)
    event["event_version"] = 2

    with pytest.raises(EventContractError, match="Invalid"):
        validate_event(event)


@pytest.mark.parametrize("field", ["post_text", "rendered_html", "bot_token", "filter_prompt", "api_key"])
def test_contract_rejects_sensitive_fields_recursively(field: str) -> None:
    event = load_event_example(DIGEST_RUN_REQUESTED_EVENT)
    event["payload"][field] = "must not enter Kafka"

    with pytest.raises(EventContractError, match="Sensitive field"):
        serialize_event(event)


def test_contract_enforces_encoded_size_after_validation() -> None:
    event = load_event_example(DIGEST_RUN_REQUESTED_EVENT)

    with pytest.raises(EventContractError, match="limit is 32"):
        serialize_event(event, max_bytes=32)


def test_dlq_reason_is_a_bounded_machine_code_not_free_text() -> None:
    event = load_event_example("tpb.digest.run.requested.dlq")
    event["payload"]["reason"] = "Full rendered digest must not be copied here"

    with pytest.raises(EventContractError, match="Invalid"):
        validate_event(event)


def test_optional_schema_addition_is_compatible() -> None:
    old = load_event_schema(DIGEST_RUN_REQUESTED_EVENT)
    new = deepcopy(old)
    new["properties"]["trace_id"] = {"type": "string"}

    assert schema_compatibility_errors(old, new) == []


def test_new_required_schema_property_is_breaking() -> None:
    old = load_event_schema(DIGEST_RUN_REQUESTED_EVENT)
    new = deepcopy(old)
    new["properties"]["trace_id"] = {"type": "string"}
    new["required"].append("trace_id")

    assert schema_compatibility_errors(old, new) == ["$: new required property trace_id"]


def test_existing_schema_type_change_is_breaking() -> None:
    old = load_event_schema(DIGEST_RUN_REQUESTED_EVENT)
    new = deepcopy(old)
    new["properties"]["attempt"]["type"] = "string"

    assert "$.attempt: type changed" in schema_compatibility_errors(old, new)
