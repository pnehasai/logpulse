"""
Unit tests for parse_otlp_logs(): OTLP JSON in, LogPulse payloads out.
No network, no Kafka. The input shape is the standard OTLP/HTTP JSON
encoding of ExportLogsServiceRequest, as an OpenTelemetry Collector sends it.
"""
from app import parse_otlp_logs


def _request(records, service="payment-service"):
    return {
        "resourceLogs": [{
            "resource": {"attributes": [{"key": "service.name", "value": {"stringValue": service}}]},
            "scopeLogs": [{"logRecords": records}],
        }]
    }


def test_error_record_becomes_a_payload():
    body = _request([{"severityNumber": 17, "severityText": "ERROR",
                      "body": {"stringValue": "Payment provider returned HTTP 503"}}])
    assert parse_otlp_logs(body, min_severity=13) == [{
        "service": "payment-service", "level": "ERROR",
        "message": "Payment provider returned HTTP 503", "source": "otlp",
    }]


def test_records_below_min_severity_are_dropped():
    body = _request([
        {"severityNumber": 9, "body": {"stringValue": "Order placed"}},          # INFO
        {"severityNumber": 13, "body": {"stringValue": "Slow query took 900ms"}},  # WARN
    ])
    messages = [p["message"] for p in parse_otlp_logs(body, min_severity=13)]
    assert messages == ["Slow query took 900ms"]


def test_severity_text_is_used_when_number_is_missing():
    body = _request([{"severityText": "error", "body": {"stringValue": "boom"}}])
    payloads = parse_otlp_logs(body, min_severity=13)
    assert len(payloads) == 1 and payloads[0]["level"] == "ERROR"


def test_missing_service_name_and_empty_bodies():
    body = {"resourceLogs": [{"scopeLogs": [{"logRecords": [
        {"severityNumber": 17, "body": {"stringValue": "   "}},
        {"severityNumber": 17},
        {"severityNumber": 17, "body": {"stringValue": "disk full"}},
    ]}]}]}
    payloads = parse_otlp_logs(body, min_severity=13)
    assert payloads == [{"service": "unknown-service", "level": "ERROR", "message": "disk full", "source": "otlp"}]


def test_empty_or_garbage_request_gives_nothing():
    assert parse_otlp_logs({}, min_severity=0) == []
    assert parse_otlp_logs({"resourceLogs": None}, min_severity=0) == []
