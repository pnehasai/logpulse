"""
Runs the consumer-side pipeline end to end, without Kafka, Redis or Groq:

  process_payload()  -> cache miss -> compile queue -> handle_cache_miss()
                     -> compile_rule_with_ai() -> Groq (faked) -> rule saved
  next payload       -> cache hit, no LLM call

Redis is replaced by a tiny in-memory fake, and Groq by an httpx
MockTransport that returns a real-shaped chat completion, including the
`usage` block, so the token accounting is exercised too.
"""
import json
import sqlite3

import httpx
import pytest

import app as app_module

_REAL_ASYNC_CLIENT = httpx.AsyncClient


class FakeRedis:
    def __init__(self):
        self.hashes, self.keys = {}, {}

    async def hgetall(self, key):
        return dict(self.hashes.get(key, {}))

    async def hset(self, key, field=None, value=None, mapping=None):
        h = self.hashes.setdefault(key, {})
        if mapping:
            h.update(mapping)
        if field is not None:
            h[field] = value

    async def set(self, key, value, nx=False, ex=None):
        if nx and key in self.keys:
            return None
        self.keys[key] = value
        return True

    async def delete(self, key):
        self.keys.pop(key, None)

    async def exists(self, key):
        return int(key in self.keys)


def groq_reply(regex: str, fix: str, total_tokens: int = 160):
    """A responder that answers every request like Groq's chat completions API."""
    requests = []

    def handler(request: httpx.Request):
        requests.append(json.loads(request.content))
        content = json.dumps({"regex_pattern": regex, "engineering_fix": fix})
        return httpx.Response(200, json={
            "choices": [{"message": {"content": content}}],
            "usage": {"prompt_tokens": total_tokens - 40, "completion_tokens": 40, "total_tokens": total_tokens},
        })

    handler.requests = requests
    return handler


@pytest.fixture
def pipeline(tmp_path, monkeypatch):
    db = str(tmp_path / "flow.db")
    app_module.init_db(db)
    monkeypatch.setattr(app_module, "DB_PATH", db)
    monkeypatch.setattr(app_module, "REDIS_CLIENT", FakeRedis())
    monkeypatch.setattr(app_module, "LOCAL_RULE_CACHE", {})
    monkeypatch.setattr(app_module, "GROQ_API_KEY", "test-key")
    monkeypatch.setattr(app_module, "_PII_AVAILABLE", False)
    # Skip the subprocess ReDoS probe here; it is not what these tests are about.
    monkeypatch.setattr(app_module, "check_regex", lambda pattern, timeout_seconds=3.0: (True, "ok"))
    while not app_module.COMPILE_QUEUE.empty():
        app_module.COMPILE_QUEUE.get_nowait()

    def use_groq(handler):
        monkeypatch.setattr(app_module.httpx, "AsyncClient",
                            lambda *a, **kw: _REAL_ASYNC_CLIENT(transport=httpx.MockTransport(handler)))
        return handler

    return db, use_groq


async def _drain_queue():
    while not app_module.COMPILE_QUEUE.empty():
        message, log_id = app_module.COMPILE_QUEUE.get_nowait()
        await app_module.handle_cache_miss(message, log_id)


def _rows(db, sql):
    conn = sqlite3.connect(db)
    try:
        return conn.execute(sql).fetchall()
    finally:
        conn.close()


@pytest.mark.asyncio
async def test_miss_compiles_once_then_hits(pipeline):
    db, use_groq = pipeline
    # Wrapped like Python source on purpose: the app must strip r'...' first.
    groq = use_groq(groq_reply(r"r'Disk usage at \d+% on node \d+'", "Free disk space or expand the volume."))
    base = {"service": "storage", "level": "ERROR", "source": "test", "ingested_at": 1.0}

    await app_module.process_payload({**base, "message": "Disk usage at 95% on node 4", "event_id": "T:E1"})
    await _drain_queue()
    await app_module.process_payload({**base, "message": "Disk usage at 99% on node 12", "event_id": "T:E1"})

    assert len(groq.requests) == 1
    rows = _rows(db, "SELECT routed_by, ai_analysis, matched_rule FROM system_logs ORDER BY id")
    assert rows[0] == ("LLM_COMPILED", "Free disk space or expand the volume.", r"Disk usage at \d+% on node \d+")
    assert rows[1][0] == "L1_LOCAL_CACHE"

    assert _rows(db, "SELECT mode, total_tokens, ok FROM llm_calls") == [("pipeline", 160, 1)]

    summary = app_module.compute_summary(db)
    assert summary["naive_comparison"]["actual_tokens"] == 160
    assert summary["naive_comparison"]["naive_tokens_estimate"] == 320
    assert summary["dataset_eval"]["rule_precision_pct"] == 100.0


@pytest.mark.asyncio
async def test_queued_duplicates_do_not_call_the_llm_again(pipeline):
    """Three logs of one kind arrive before any rule exists; only the first costs tokens."""
    db, use_groq = pipeline
    groq = use_groq(groq_reply(r"Disk usage at \d+% on node \d+", "Free disk space."))
    for node in (1, 2, 3):
        await app_module.process_payload({"service": "storage", "level": "ERROR",
                                          "message": f"Disk usage at 9{node}% on node {node}"})
    assert app_module.COMPILE_QUEUE.qsize() == 3
    await _drain_queue()

    assert len(groq.requests) == 1
    routed = [r[0] for r in _rows(db, "SELECT routed_by FROM system_logs ORDER BY id")]
    assert routed == ["LLM_COMPILED", "L1_LOCAL_CACHE_AFTER_QUEUE", "L1_LOCAL_CACHE_AFTER_QUEUE"]


@pytest.mark.asyncio
async def test_learning_off_freezes_rules_and_never_calls_the_llm(pipeline, monkeypatch):
    db, use_groq = pipeline
    groq = use_groq(groq_reply(r"Disk usage at \d+%", "Free disk space."))
    monkeypatch.setattr(app_module, "LEARNING_ENABLED", False)
    app_module.LOCAL_RULE_CACHE[r"Payment timeout after \d+ms"] = "Retry with backoff."

    await app_module.process_payload({"service": "s", "level": "ERROR", "message": "Payment timeout after 900ms"})
    await app_module.process_payload({"service": "s", "level": "ERROR", "message": "Disk usage at 95%"})

    assert app_module.COMPILE_QUEUE.empty() and groq.requests == []
    routed = [r[0] for r in _rows(db, "SELECT routed_by FROM system_logs ORDER BY id")]
    assert routed == ["L1_LOCAL_CACHE", "MISS_LEARNING_OFF"]
    logs = app_module.compute_summary(db)["logs"]
    assert logs["cache_hit"] == 1 and logs["unresolved"] == 1 and logs["handled"] == 2


@pytest.mark.asyncio
async def test_lock_loser_waits_for_a_slow_winner(pipeline):
    """Another instance holds the lock and takes 3 s; the loser must still pick up its rule."""
    import asyncio
    db, use_groq = pipeline
    groq = use_groq(groq_reply(r"Disk usage at \d+%", "Free disk space."))
    message = "Disk usage at 95%"
    await app_module.process_payload({"service": "s", "level": "ERROR", "message": message})
    _, log_id = app_module.COMPILE_QUEUE.get_nowait()
    assert await app_module.try_acquire_compile_lock(message)  # the "other instance" wins

    async def slow_winner():
        await asyncio.sleep(3)
        await app_module.REDIS_CLIENT.hset(app_module.REDIS_RULES_KEY, r"Disk usage at \d+%", "Free disk space.")
        await app_module.release_compile_lock(message)

    await asyncio.gather(slow_winner(), app_module.handle_cache_miss(message, log_id))
    assert groq.requests == []  # the loser never paid the LLM
    assert _rows(db, "SELECT routed_by FROM system_logs")[0][0] == "L2_REDIS_FALLBACK_AFTER_LOCK_WAIT"


@pytest.mark.asyncio
async def test_lock_loser_compiles_itself_if_the_winner_dies(pipeline):
    db, use_groq = pipeline
    groq = use_groq(groq_reply(r"Disk usage at \d+%", "Free disk space."))
    message = "Disk usage at 95%"
    await app_module.process_payload({"service": "s", "level": "ERROR", "message": message})
    _, log_id = app_module.COMPILE_QUEUE.get_nowait()
    assert await app_module.try_acquire_compile_lock(message)

    async def dying_winner():
        import asyncio
        await asyncio.sleep(1.5)
        await app_module.release_compile_lock(message)  # gone, no rule written

    import asyncio
    await asyncio.gather(dying_winner(), app_module.handle_cache_miss(message, log_id))
    assert len(groq.requests) == 1
    assert _rows(db, "SELECT routed_by FROM system_logs")[0][0] == "LLM_COMPILED"


def test_sync_baseline_endpoint_records_its_tokens_separately(pipeline):
    from fastapi.testclient import TestClient  # no `with`, so the Kafka lifespan doesn't run

    db, use_groq = pipeline
    use_groq(groq_reply(r"Disk usage at \d+%", "Free disk space.", total_tokens=210))
    response = TestClient(app_module.app).post(
        "/v1/telemetry/ingest-sync", json={"service": "s", "level": "ERROR", "message": "Disk usage at 95%"}
    )
    assert response.status_code == 200
    assert response.json()["engineering_fix"] == "Free disk space."
    assert _rows(db, "SELECT mode, total_tokens FROM llm_calls") == [("sync_baseline", 210)]
    # The baseline never writes rules or log rows: it only exists to be measured.
    assert _rows(db, "SELECT COUNT(*) FROM system_logs")[0][0] == 0


def test_otlp_json_endpoint_hands_records_to_kafka(pipeline, monkeypatch):
    from fastapi.testclient import TestClient

    sent = []

    class FakeProducer:
        async def send(self, topic, value):
            sent.append(json.loads(value))
            import asyncio
            future = asyncio.get_running_loop().create_future()
            future.set_result(None)
            return future

    monkeypatch.setattr(app_module, "kafka_producer", FakeProducer())
    body = {"resourceLogs": [{
        "resource": {"attributes": [{"key": "service.name", "value": {"stringValue": "cart"}}]},
        "scopeLogs": [{"logRecords": [
            {"severityNumber": 17, "body": {"stringValue": "Redis timeout after 2000ms"}},
            {"severityNumber": 9, "body": {"stringValue": "cart viewed"}},
        ]}],
    }]}
    response = TestClient(app_module.app).post("/v1/logs", json=body)
    assert response.status_code == 200
    assert [(r["service"], r["message"], r["source"]) for r in sent] == [("cart", "Redis timeout after 2000ms", "otlp")]
    assert isinstance(sent[0]["ingested_at"], float)


@pytest.mark.asyncio
async def test_too_generic_regex_is_rejected(pipeline):
    db, use_groq = pipeline
    groq = use_groq(groq_reply(".*", "Check it.", total_tokens=50))

    await app_module.process_payload({"service": "s", "level": "ERROR", "message": "anything at all"})
    await _drain_queue()

    (fix,) = _rows(db, "SELECT ai_analysis FROM system_logs")[0]
    assert fix.startswith("COMPILE FAILED: regex is too generic")
    assert len(groq.requests) == app_module.MAX_COMPILE_ATTEMPTS  # it retried, then gave up
    assert _rows(db, "SELECT COUNT(*) FROM compiled_rules_cache")[0][0] == 0
    assert app_module.LOCAL_RULE_CACHE == {}
