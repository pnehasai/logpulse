"""
Tests for TelemetryPayload -- the Pydantic model that validates incoming
logs at the /v1/telemetry/ingest endpoint.

Like test_migration.py, these are fully isolated: creating a Pydantic
model in memory touches no database, no Redis, no Kafka. That's exactly
why we can test it fast and without any of your Docker containers running.
"""
import pytest
from pydantic import ValidationError

from app import TelemetryPayload


def test_valid_payload_is_accepted():
    """The happy path: all three fields present and non-empty."""
    payload = TelemetryPayload(
        service="checkout-api",
        level="ERROR",
        message="Payment gateway timeout after 3000ms",
    )
    assert payload.service == "checkout-api"
    assert payload.level == "ERROR"
    assert payload.message == "Payment gateway timeout after 3000ms"


def test_missing_message_field_is_rejected():
    """
    This is the EXACT bug we fixed earlier: a payload missing 'message'
    used to be silently accepted by ingest_log(payload: dict) and crash
    later, deep inside the background worker, where the client could never
    see it. Now it should be rejected immediately, right here.
    """
    with pytest.raises(ValidationError):
        TelemetryPayload(service="checkout-api", level="ERROR")
        # no message= given at all


def test_missing_service_field_is_rejected():
    with pytest.raises(ValidationError):
        TelemetryPayload(level="ERROR", message="Something broke")


def test_missing_level_field_is_rejected():
    with pytest.raises(ValidationError):
        TelemetryPayload(service="checkout-api", message="Something broke")


def test_empty_string_message_is_rejected():
    """
    min_length=1 in the model should reject an EMPTY string, not just a
    MISSING field. These are two different failure modes worth testing
    separately -- a client could send message="" (present, but useless)
    just as easily as omitting it entirely.
    """
    with pytest.raises(ValidationError):
        TelemetryPayload(service="checkout-api", level="ERROR", message="")


def test_wrong_type_is_rejected():
    """
    Pydantic also enforces TYPES, not just presence. Sending a number
    where a string is expected should be rejected too.
    """
    with pytest.raises(ValidationError):
        TelemetryPayload(service="checkout-api", level="ERROR", message=12345)


def test_validation_error_message_is_informative():
    """
    Beyond just 'it raised' -- confirm the error actually TELLS you which
    field was the problem. This is what makes the resulting 422 response
    to the client useful instead of a vague failure.
    """
    with pytest.raises(ValidationError) as exc_info:
        TelemetryPayload(service="checkout-api", level="ERROR")

    errors = exc_info.value.errors()
    # errors() returns a list of dicts; each has a 'loc' (which field) and 'msg'
    missing_fields = [err["loc"][0] for err in errors]
    assert "message" in missing_fields
