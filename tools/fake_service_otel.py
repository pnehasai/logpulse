"""
A small stand-in "microservice" instrumented with the real OpenTelemetry SDK.

It writes ordinary Python log lines (logging.warning / logging.error). The
OpenTelemetry LoggingHandler turns them into OTLP log records and the OTLP
HTTP exporter ships them to LogPulse's /v1/logs endpoint, the same way any
OTel-instrumented service in a real company would ship logs to a collector.
LogPulse never sees this script's code; it only receives standard OTLP.

Install once:
    pip install opentelemetry-sdk opentelemetry-exporter-otlp-proto-http

Run (app must be running):
    python tools/fake_service_otel.py --service checkout-service --count 200 --rate 5

Run several at once with different --service names to simulate a fleet.
"""
import argparse
import logging
import os
import random
import time

from opentelemetry._logs import set_logger_provider
from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.sdk.resources import Resource

# Realistic failure templates; the numbers change on every line, the shape doesn't.
INCIDENTS = [
    (logging.ERROR, "Payment provider returned HTTP {code} for order {order}"),
    (logging.ERROR, "Database connection pool exhausted: {used}/{used} connections in use"),
    (logging.WARNING, "Slow query on orders table took {ms}ms"),
    (logging.ERROR, "Upstream inventory-service timed out after {ms}ms"),
    (logging.WARNING, "Retrying message {order} after transient Kafka error (attempt {attempt})"),
    (logging.ERROR, "JWT validation failed for user {user}: token expired"),
    (logging.INFO, "Order {order} placed successfully"),  # below WARN, filtered out by LogPulse
]


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--service", default="checkout-service")
    parser.add_argument("--count", type=int, default=200)
    parser.add_argument("--rate", type=float, default=5.0, help="log lines per second")
    parser.add_argument("--endpoint", default="http://localhost:8000/v1/logs")
    parser.add_argument("--api-key", default=os.environ.get("INGEST_API_KEY", ""))
    args = parser.parse_args()

    provider = LoggerProvider(resource=Resource.create({"service.name": args.service}))
    exporter = OTLPLogExporter(
        endpoint=args.endpoint,
        headers={"X-API-Key": args.api_key} if args.api_key else None,
    )
    provider.add_log_record_processor(BatchLogRecordProcessor(exporter))
    set_logger_provider(provider)

    log = logging.getLogger(args.service)
    log.setLevel(logging.INFO)
    log.addHandler(LoggingHandler(level=logging.INFO, logger_provider=provider))

    print(f"{args.service}: emitting {args.count} log lines over OTLP to {args.endpoint}")
    for i in range(args.count):
        level, template = random.choice(INCIDENTS)
        log.log(level, template.format(
            code=random.choice([500, 502, 503]),
            order=f"ord-{random.randint(10000, 99999)}",
            used=random.choice([50, 100]),
            ms=random.randint(1200, 9000),
            attempt=random.randint(1, 5),
            user=f"u{random.randint(1000, 9999)}",
        ))
        time.sleep(1 / args.rate if args.rate > 0 else 0)

    provider.shutdown()  # flushes anything still batched
    print("done")


if __name__ == "__main__":
    main()
