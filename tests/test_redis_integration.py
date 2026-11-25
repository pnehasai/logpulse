"""
INTEGRATION tests -- unlike test_migration.py and test_validation.py, these
tests need a REAL Redis server actually running (e.g. via `docker compose up -d`)
to pass. They're testing that your code and the real Redis service genuinely
work together, not just that your Python logic is internally consistent.

Run these separately from your fast unit tests, since they're slower and
depend on external infrastructure being up:
    pytest tests/test_redis_integration.py -v
"""
import asyncio
import pytest
import pytest_asyncio
import redis.asyncio as aioredis


@pytest_asyncio.fixture
async def redis_client():
    """
    A pytest FIXTURE: reusable setup/teardown code shared across tests.
    This connects to the real Redis (same host/port your app.py uses),
    hands the connection to each test that asks for it, and cleans up
    afterward -- so every test starts from a known, empty state instead
    of leftover data from a previous test run leaking in.
    """
    client = aioredis.Redis(host="localhost", port=6379, decode_responses=True)
    await client.flushdb()  # start with a clean slate
    yield client
    await client.flushdb()  # clean up after the test too
    await client.aclose()


@pytest.mark.asyncio
async def test_redis_is_actually_reachable(redis_client):
    """The most basic integration check: can we even talk to it."""
    pong = await redis_client.ping()
    assert pong is True


@pytest.mark.asyncio
async def test_write_then_read_a_compiled_rule(redis_client):
    """
    Mirrors exactly what compile_rule_with_ai() does in the real app:
    write a pattern->fix pair into the Redis Hash, then read it back --
    proving the real round trip works, not just that Python's dict logic
    is correct (that part was already covered by unit tests).
    """
    await redis_client.hset("logpulse:compiled_rules", "test_pattern_\\d+", "test fix")

    result = await redis_client.hgetall("logpulse:compiled_rules")
    assert result == {"test_pattern_\\d+": "test fix"}


@pytest.mark.asyncio
async def test_empty_cache_returns_empty_dict(redis_client):
    """
    Mirrors warm_rule_cache_from_redis()'s empty-Redis case -- confirms
    HGETALL on a key that was never set returns an empty dict, not an
    error, which is exactly what that function's fallback logic relies on.
    """
    result = await redis_client.hgetall("logpulse:compiled_rules")
    assert result == {}
