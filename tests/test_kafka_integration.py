"""
INTEGRATION tests for Kafka -- like test_redis_integration.py, these need
a REAL Kafka broker actually running (via `docker compose up -d`) to pass.

IMPORTANT DESIGN CHOICE: these tests use a SEPARATE topic
("telemetry-logs-test") rather than your real "telemetry-logs" topic.
If tests wrote to the same topic your actual running app consumes from,
your app's consumer and the test's consumer would be racing for the same
messages -- flaky, unpredictable, and could even "steal" a message meant
for one or the other. Keeping test traffic fully separate from real
traffic avoids that entirely.

Run these separately from your fast unit tests:
    pytest tests/test_kafka_integration.py -v
"""
import asyncio
import json
import uuid

import pytest
import pytest_asyncio
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TEST_TOPIC = "telemetry-logs-test"


@pytest_asyncio.fixture
async def kafka_producer():
    """
    Connects a real producer to your real Kafka broker. If the broker isn't
    reachable, `producer.start()` will raise here, and the test fails with
    a clear connection error -- same honest failure behavior we saw with
    the Redis fixture when Redis was down.
    """
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    await producer.start()
    yield producer
    await producer.stop()


@pytest.mark.asyncio
async def test_producer_can_connect_to_kafka(kafka_producer):
    """
    The most basic integration check: did producer.start() succeed at all.
    If this fails, nothing else in this file will work either -- it's the
    first thing to check when debugging a failure here.
    """
    # If we got this far, the fixture's producer.start() already succeeded
    # without raising -- that IS the check. This test exists mainly so a
    # connection failure shows up as its own clearly-named failing test,
    # rather than being buried inside a bigger test.
    assert kafka_producer is not None


@pytest.mark.asyncio
async def test_produce_then_consume_a_real_message(kafka_producer):
    """
    The real end-to-end proof: send a message through the ACTUAL Kafka
    broker, then read it back with a separate consumer -- exactly what
    your app's ingest_log() and message_broker_consumer() do for real,
    just without any of your app's other logic (SQLite, Redis, Groq)
    mixed in, so a failure here specifically means "the Kafka round trip
    itself is broken," not "something downstream of Kafka is broken."
    """
    # A unique ID lets us pick OUR message out, even if old test messages
    # are still sitting in the test topic from a previous run.
    unique_marker = str(uuid.uuid4())
    test_payload = {
        "service": "integration-test",
        "level": "INFO",
        "message": f"test message {unique_marker}",
    }

    await kafka_producer.send_and_wait(
        TEST_TOPIC, json.dumps(test_payload).encode("utf-8")
    )

    # A fresh, unique consumer group -- so this test always reads from the
    # beginning of the topic and never shares offset state with a previous
    # test run or with your actual app's "logpulse-consumer-group".
    consumer = AIOKafkaConsumer(
        TEST_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=f"test-group-{unique_marker}",
        auto_offset_reset="earliest",
    )
    await consumer.start()

    try:
        found_it = False
        # asyncio.wait_for puts a hard time limit on this. Without it, if
        # something's genuinely broken, `async for` would just hang
        # forever waiting for a message that's never coming -- a test
        # that can hang forever is a bad test. 10 seconds is generous
        # for a message that was already sent.
        async def read_until_found():
            nonlocal found_it
            async for msg in consumer:
                payload = json.loads(msg.value.decode("utf-8"))
                if payload.get("message") == test_payload["message"]:
                    found_it = True
                    return

        await asyncio.wait_for(read_until_found(), timeout=10.0)
        assert found_it is True
    finally:
        await consumer.stop()
