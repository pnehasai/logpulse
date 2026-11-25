"""
This is NOT a test of find_matching_fix() in isolation -- that's already
fully covered by tests/test_cache_matching.py, using fake dicts, no Redis
needed.

This tests the SEAM between two pieces: warm_rule_cache_from_redis() (which
genuinely talks to real Redis) and find_matching_fix() (pure logic). Unit
tests proved each piece works on its own; this proves they correctly work
TOGETHER -- i.e. that data which really came out of Redis is in a shape
find_matching_fix() can actually use correctly. That combination is exactly
what "integration test" means: not testing a pure function against a real
service it never talks to (impossible here), but testing where two real
pieces of your system actually meet.

Needs a real Redis running (`docker compose up -d`) to pass.
"""
import aiosqlite
import pytest
import pytest_asyncio
import redis.asyncio as aioredis

import app as app_module
from app import find_matching_fix, warm_rule_cache_from_redis, REDIS_RULES_KEY

DB_PATH = "enterprise_telemetry.db"


@pytest_asyncio.fixture
async def clean_redis_and_local_cache():
    """
    THREE things need resetting here, not two:
    1. Real Redis data (REDIS_RULES_KEY) -- same idea as the redis_client
       fixture in test_redis_integration.py.
    2. app_module.LOCAL_RULE_CACHE -- a plain Python dict living at module
       level in app.py. Every test in this file shares the SAME dict object
       unless we clear it ourselves.
    3. SQLite's compiled_rules_cache table -- warm_rule_cache_from_redis()
       falls back to THIS when Redis is empty. Forgetting to isolate this
       is exactly what caused test_empty_redis_means_no_matches_possible to
       fail: real rules you'd genuinely compiled earlier (via actual Groq
       calls) were still sitting in SQLite, so the "empty" test wasn't
       actually starting from empty.

    Since compiled_rules_cache holds REAL data from your actual running
    app, we don't just wipe it -- we snapshot it, clear it for the test,
    then restore the exact original rows afterward. Tests should never
    permanently destroy real data as a side effect of running.
    """
    client = aioredis.Redis(host="localhost", port=6379, decode_responses=True)
    await client.delete(REDIS_RULES_KEY)
    app_module.LOCAL_RULE_CACHE.clear()

    # Snapshot + clear the real SQLite fallback data
    async with aiosqlite.connect(DB_PATH) as conn:
        async with conn.execute("SELECT regex_pattern, cached_fix FROM compiled_rules_cache") as cursor:
            original_rows = await cursor.fetchall()
        await conn.execute("DELETE FROM compiled_rules_cache")
        await conn.commit()

    yield client

    # Restore SQLite to exactly how it was before this test ran
    async with aiosqlite.connect(DB_PATH) as conn:
        await conn.execute("DELETE FROM compiled_rules_cache")
        if original_rows:
            await conn.executemany(
                "INSERT INTO compiled_rules_cache (regex_pattern, cached_fix) VALUES (?, ?)",
                original_rows,
            )
        await conn.commit()

    await client.delete(REDIS_RULES_KEY)
    app_module.LOCAL_RULE_CACHE.clear()
    await client.aclose()


@pytest.mark.asyncio
async def test_rule_written_to_redis_is_findable_after_warming(clean_redis_and_local_cache):
    """
    ARRANGE: write a rule directly into REAL Redis (simulating what
             compile_rule_with_ai() would have done previously).
    ACT:     call the REAL warm_rule_cache_from_redis() -- this is the
             exact function your app calls once at startup.
    ASSERT:  find_matching_fix() -- your pure, already-unit-tested logic --
             can now correctly find that rule, proving the data Redis
             handed back is in the right shape and content for your
             matching logic to actually use.
    """
    redis_client = clean_redis_and_local_cache

    await redis_client.hset(
        REDIS_RULES_KEY,
        r"Payment gateway timeout after \d+ms",
        "Increase timeout threshold and add retry logic",
    )

    await warm_rule_cache_from_redis()

    # This is the real, shared LOCAL_RULE_CACHE dict inside app.py --
    # we're reading it back to confirm warm_rule_cache_from_redis()
    # actually populated it from Redis, not from nothing.
    assert r"Payment gateway timeout after \d+ms" in app_module.LOCAL_RULE_CACHE

    # And now the real proof: the SAME cache dict, run through the SAME
    # pure matching function your consumer uses, correctly matches a
    # brand new incoming message.
    result = find_matching_fix(
        "Payment gateway timeout after 9124ms", app_module.LOCAL_RULE_CACHE
    )
    assert result == "Increase timeout threshold and add retry logic"


@pytest.mark.asyncio
async def test_empty_redis_means_no_matches_possible(clean_redis_and_local_cache):
    """
    If Redis has nothing (fresh install, or SQLite backfill also empty --
    covered separately in test_migration.py's spirit, not repeated here),
    warm_rule_cache_from_redis() should leave LOCAL_RULE_CACHE empty, and
    find_matching_fix() should correctly report "no match" rather than
    erroring on an empty cache.
    """
    await warm_rule_cache_from_redis()

    assert app_module.LOCAL_RULE_CACHE == {}

    result = find_matching_fix("any incoming message", app_module.LOCAL_RULE_CACHE)
    assert result is None
