import asyncio
import gzip
import hashlib
import json
import math
import os
import re
import sqlite3
import subprocess
import sys
import time
from contextlib import asynccontextmanager
from datetime import datetime

import aiosqlite
import httpx
import redis.asyncio as aioredis
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from fastapi import Depends, FastAPI, Header, HTTPException, Request, Response, status
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

DB_PATH = os.environ.get("LOGPULSE_DB", "enterprise_telemetry.db")

# Several copies of this app can run at once (same Kafka consumer group, one
# partition each). INSTANCE_ID labels every row so results can be split per copy.
INSTANCE_ID = os.environ.get("INSTANCE_ID", "app-1")

# REDIS_SHARING=0 turns Redis off as a shared layer: no L2 lookup, no rule
# written to Redis, no cross-instance compile lock. Each instance then only
# knows the rules it compiled itself (L1). It exists to measure what Redis
# sharing saves: tools/multi_instance_experiment.py runs both settings.
REDIS_SHARING = os.environ.get("REDIS_SHARING", "1") != "0"

# =====================================================================
# REAL REDIS CLIENT + LOCAL L1 CACHE
# =====================================================================
REDIS_CLIENT = aioredis.Redis(host="localhost", port=6379, decode_responses=True)
# A single Redis Hash: {pattern: fix}. Overridable so an experiment can start
# from an empty rule set without touching the normal one.
REDIS_RULES_KEY = os.environ.get("LOGPULSE_REDIS_KEY", "logpulse:compiled_rules")

# When False, rules are frozen: a cache miss is recorded as unresolved and
# the LLM is never called. Used for the validation and test phases of an
# experiment, so they measure how well rules learned on training data
# generalize to logs the system has never seen. Toggled via /admin/learning.
LEARNING_ENABLED = True

LOCAL_RULE_CACHE = {}

# =====================================================================
# REAL KAFKA CONNECTION SETTINGS
# =====================================================================
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "telemetry-logs")
# One partition per app instance you want consuming in parallel; Kafka gives
# each partition to exactly one consumer in the group.
KAFKA_TOPIC_PARTITIONS = int(os.environ.get("KAFKA_TOPIC_PARTITIONS", "3"))

kafka_producer: AIOKafkaProducer | None = None
kafka_consumer: AIOKafkaConsumer | None = None

# In-memory counters for the live dashboard. They reset on restart; the
# numbers that survive restarts come from the database (/metrics/summary).
TELEMETRY_COUNTERS = {
    "cache_hits": 0,
    "cache_misses": 0,
    "messages_produced": 0,
    "llm_calls": 0,
    "llm_tokens": 0,
}

# Cache misses wait here for the compile worker(s). See compile_worker().
COMPILE_QUEUE: asyncio.Queue = asyncio.Queue()
COMPILE_WORKERS = int(os.environ.get("COMPILE_WORKERS", "1"))


# =====================================================================
# DATABASE SCHEMA + MIGRATIONS
# =====================================================================
def _get_existing_columns(cursor, table_name):
    """Ask SQLite what columns a table ACTUALLY has right now, not what we assume it has."""
    cursor.execute(f"PRAGMA table_info({table_name})")
    return [row[1] for row in cursor.fetchall()]


def _migrate_system_logs_schema(cursor):
    existing_columns = _get_existing_columns(cursor, "system_logs")
    if not existing_columns:
        return
    if "routed_by" in existing_columns:
        return
    if "processed_by" in existing_columns:
        print("🔧 [MIGRATION] Detected old column 'processed_by' -> renaming to 'routed_by'")
        cursor.execute("ALTER TABLE system_logs RENAME COLUMN processed_by TO routed_by")
        return
    raise RuntimeError(
        f"system_logs has neither 'processed_by' nor 'routed_by'. "
        f"Found columns: {existing_columns}. Manual migration needed."
    )


# Columns added for measurement. Older databases get them via ALTER TABLE.
SYSTEM_LOGS_EXTRA_COLUMNS = {
    "source": "TEXT",         # where the log came from: dashboard / loghub / otlp / benchmark
    "event_id": "TEXT",       # ground-truth template id when replaying a labelled dataset
    "matched_rule": "TEXT",   # the regex that resolved this log
    "ingested_at": "REAL",    # epoch seconds when the API accepted it
    "consumed_at": "REAL",    # epoch seconds when the Kafka consumer picked it up
    "resolved_at": "REAL",    # epoch seconds when a fix was attached
    "instance_id": "TEXT",    # which app instance handled it
    "lookup_ms": "REAL",      # time spent in the L1/L2 lookup itself
}
LLM_CALLS_EXTRA_COLUMNS = {"instance_id": "TEXT"}
RULES_EXTRA_COLUMNS = {
    "source_log_id": "INTEGER",  # the log the LLM compiled this rule from
    "created_at": "REAL",
}


def _add_missing_columns(cursor, table_name, columns: dict):
    existing = _get_existing_columns(cursor, table_name)
    for name, sql_type in columns.items():
        if name not in existing:
            print(f"🔧 [MIGRATION] Adding column {table_name}.{name}")
            cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {name} {sql_type}")


def init_db(db_path: str = DB_PATH):
    # Several instances can start at the same moment against one file; schema
    # setup (switching to WAL needs exclusive access) is retried if it's locked.
    for attempt in range(10):
        try:
            return _init_db_once(db_path)
        except sqlite3.OperationalError as e:
            if "locked" not in str(e) or attempt == 9:
                raise
            time.sleep(0.5 * (attempt + 1))


def _init_db_once(db_path: str):
    conn = sqlite3.connect(db_path, timeout=30)
    cursor = conn.cursor()
    # WAL lets readers work while one writer writes, which several app
    # instances sharing one database file need. Still one writer at a time:
    # Postgres is the step up past a few instances.
    cursor.execute("PRAGMA journal_mode=WAL")
    cursor.execute('''CREATE TABLE IF NOT EXISTS system_logs
                      (id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp TEXT, service TEXT, level TEXT, message TEXT, ai_analysis TEXT, routed_by TEXT)''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS compiled_rules_cache
                      (id INTEGER PRIMARY KEY AUTOINCREMENT, regex_pattern TEXT, cached_fix TEXT)''')
    # One row per real HTTP call to the LLM, with the token usage Groq reports.
    cursor.execute('''CREATE TABLE IF NOT EXISTS llm_calls
                      (id INTEGER PRIMARY KEY AUTOINCREMENT, log_id INTEGER, mode TEXT, attempt INTEGER,
                       prompt_tokens INTEGER, completion_tokens INTEGER, total_tokens INTEGER,
                       latency_ms REAL, ok INTEGER, error TEXT, created_at REAL)''')
    _migrate_system_logs_schema(cursor)
    _add_missing_columns(cursor, "system_logs", SYSTEM_LOGS_EXTRA_COLUMNS)
    _add_missing_columns(cursor, "compiled_rules_cache", RULES_EXTRA_COLUMNS)
    _add_missing_columns(cursor, "llm_calls", LLM_CALLS_EXTRA_COLUMNS)
    conn.commit()
    conn.close()


init_db()


# =====================================================================
# CACHE LOOKUP (L1 local dict -> L2 Redis)
# =====================================================================
async def warm_rule_cache_from_redis():
    """
    Startup: load every known rule into L1, and reconcile Redis with SQLite.
    SQLite is the system of record. A rule is written to SQLite first and
    Redis second, so a crash between the two leaves it only in SQLite; any
    such rule is copied back into Redis here, not only when Redis is empty.
    """
    redis_rules = {}
    if REDIS_SHARING:
        try:
            redis_rules = await REDIS_CLIENT.hgetall(REDIS_RULES_KEY)
        except Exception as e:
            print(f"⚠️ [CACHE WARM] Redis unavailable ({type(e).__name__}); loading from SQLite only")
    LOCAL_RULE_CACHE.update(redis_rules)

    async with aiosqlite.connect(DB_PATH, timeout=30) as conn:
        async with conn.execute("SELECT regex_pattern, cached_fix FROM compiled_rules_cache") as cursor:
            rows = await cursor.fetchall()
    missing_from_redis = {pattern: fix for pattern, fix in rows if pattern not in redis_rules}
    LOCAL_RULE_CACHE.update(missing_from_redis)

    if REDIS_SHARING and missing_from_redis:
        try:
            await REDIS_CLIENT.hset(REDIS_RULES_KEY, mapping=missing_from_redis)
        except Exception as e:
            print(f"⚠️ [CACHE WARM] Could not backfill Redis ({type(e).__name__})")
    print(f"🔥 [CACHE WARM] {len(LOCAL_RULE_CACHE)} rules in L1 ({len(redis_rules)} from Redis, "
          f"{len(missing_from_redis)} only in SQLite{', copied to Redis' if REDIS_SHARING and missing_from_redis else ''})")


def find_matching_rule(message: str, rule_cache: dict) -> tuple[str, str] | None:
    """Pure function: returns (pattern, fix) for the first rule that matches, else None."""
    for pattern, fix in list(rule_cache.items()):
        try:
            if re.search(pattern, message):
                return pattern, fix
        except re.error:
            continue
    return None


def find_matching_fix(message: str, rule_cache: dict) -> str | None:
    """Pure function: string + dict in, answer out. Fully unit-testable alone."""
    match = find_matching_rule(message, rule_cache)
    return match[1] if match else None


async def lookup_rule(message: str, local_cache: dict) -> tuple[str | None, str | None, str]:
    """
    Cascading cache lookup: L1 (local dict, no network) first, then a LIVE
    Redis (L2) check on an L1 miss -- catching rules another instance
    compiled that this process's L1 doesn't know about yet. Self-heals L1
    on an L2 hit so future lookups for the same pattern avoid Redis entirely.
    Returns (pattern, fix, source).
    """
    match = find_matching_rule(message, local_cache)
    if match:
        return match[0], match[1], "L1_LOCAL_CACHE"

    if not REDIS_SHARING:
        return None, None, "MISS"
    try:
        redis_rules = await REDIS_CLIENT.hgetall(REDIS_RULES_KEY)
    except Exception as e:
        # Redis down must not stop the pipeline: treat it as an L2 miss.
        print(f"⚠️ [L2] Redis unavailable ({type(e).__name__}); skipping L2")
        return None, None, "MISS"
    match = find_matching_rule(message, redis_rules)
    if match:
        local_cache.update(redis_rules)
        return match[0], match[1], "L2_REDIS_FALLBACK"

    return None, None, "MISS"


async def find_matching_fix_with_redis_fallback(message: str, local_cache: dict) -> tuple[str | None, str]:
    _, fix, source = await lookup_rule(message, local_cache)
    return fix, source


COMPILE_LOCK_TTL_SECONDS = 30


async def try_acquire_compile_lock(message: str) -> bool:
    """
    RACE CONDITION FIX: Redis atomic SET-if-not-exists as a distributed
    lock, keyed on the raw message text's hash, so two app instances seeing
    the same brand-new message near-simultaneously don't both call Groq.
    TTL auto-releases the lock if the winning process crashes mid-compile.
    """
    if not REDIS_SHARING:
        return True  # no shared store, so no cross-instance lock
    lock_key = f"logpulse:compiling:{hashlib.sha256(message.encode()).hexdigest()}"
    try:
        acquired = await REDIS_CLIENT.set(lock_key, "1", nx=True, ex=COMPILE_LOCK_TTL_SECONDS)
    except Exception as e:
        print(f"⚠️ [LOCK] Redis unavailable ({type(e).__name__}); compiling without the lock")
        return True
    return bool(acquired)


async def release_compile_lock(message: str):
    if not REDIS_SHARING:
        return
    lock_key = f"logpulse:compiling:{hashlib.sha256(message.encode()).hexdigest()}"
    try:
        await REDIS_CLIENT.delete(lock_key)
    except Exception:
        pass  # the TTL releases it anyway


# =====================================================================
# KAFKA CONSUMER + COMPILE WORKER
# =====================================================================
async def process_payload(payload: dict):
    """
    Handles one log taken off Kafka. A cache hit is resolved immediately.
    A miss is stored as COMPILING... and handed to the compile queue, so the
    consumer never waits on the LLM.
    """
    consumed_at = time.time()
    message = payload["message"]
    service = payload["service"]
    level = payload["level"]

    lookup_started = time.perf_counter()
    pattern, fix, cache_source = await lookup_rule(message, LOCAL_RULE_CACHE)
    lookup_ms = (time.perf_counter() - lookup_started) * 1000
    timestamp = datetime.utcnow().isoformat()
    base_values = (
        timestamp, service, level, message,
        payload.get("source"), payload.get("event_id"),
        payload.get("ingested_at"), consumed_at, INSTANCE_ID, lookup_ms,
    )
    base_columns = "timestamp, service, level, message, source, event_id, ingested_at, consumed_at, instance_id, lookup_ms"

    async with aiosqlite.connect(DB_PATH, timeout=30) as conn:
        if fix is not None:
            TELEMETRY_COUNTERS["cache_hits"] += 1
            await conn.execute(
                f"INSERT INTO system_logs ({base_columns}, ai_analysis, routed_by, matched_rule, resolved_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                base_values + (fix, cache_source, pattern, time.time()),
            )
            await conn.commit()
            print(f"⚡ [{cache_source}] Resolved for {service}")
            return

        TELEMETRY_COUNTERS["cache_misses"] += 1
        if not LEARNING_ENABLED:
            await conn.execute(
                f"INSERT INTO system_logs ({base_columns}, ai_analysis, routed_by) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'NO RULE (learning off)', 'MISS_LEARNING_OFF')",
                base_values,
            )
            await conn.commit()
            return

        cursor = await conn.execute(
            f"INSERT INTO system_logs ({base_columns}, ai_analysis, routed_by) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'COMPILING...', 'KAFKA_ASYNC_PROCESSOR')",
            base_values,
        )
        log_id = cursor.lastrowid
        await conn.commit()

    await COMPILE_QUEUE.put((message, log_id))


async def ensure_topic_partitions() -> int:
    """
    Make sure the topic has at least KAFKA_TOPIC_PARTITIONS partitions.
    aiokafka's admin calls report errors in the response instead of raising,
    so the real partition count is read back from Kafka after each step.
    """
    from aiokafka.admin import AIOKafkaAdminClient, NewPartitions, NewTopic

    admin = AIOKafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    await admin.start()

    async def partition_count(at_least: int = 0) -> int:
        # Metadata can lag a create/grow by a moment, so re-read briefly.
        for _ in range(10):
            info = (await admin.describe_topics([KAFKA_TOPIC]) or [{}])[0]
            count = 0 if info.get("error_code") else len(info.get("partitions") or [])
            if count >= at_least:
                return count
            await asyncio.sleep(0.5)
        return count

    try:
        count = await partition_count()
        if count == 0:
            await admin.create_topics([NewTopic(KAFKA_TOPIC, KAFKA_TOPIC_PARTITIONS, 1)])
            count = await partition_count(at_least=1)
        if 0 < count < KAFKA_TOPIC_PARTITIONS:
            await admin.create_partitions({KAFKA_TOPIC: NewPartitions(KAFKA_TOPIC_PARTITIONS)})
            count = await partition_count(at_least=KAFKA_TOPIC_PARTITIONS)
        print(f"🧱 [KAFKA] {KAFKA_TOPIC} has {count} partition(s) (wanted {KAFKA_TOPIC_PARTITIONS})")
        return count
    except Exception as e:
        print(f"⚠️ [KAFKA] Could not check partitions ({type(e).__name__}: {e}); continuing")
        return 0
    finally:
        await admin.close()


async def message_broker_consumer():
    global kafka_consumer
    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="logpulse-consumer-group",
        auto_offset_reset="earliest",
        # Re-read topic metadata every 10 s (default 5 min) so added partitions
        # get spread over the group's instances quickly.
        metadata_max_age_ms=10_000,
    )
    await consumer.start()
    kafka_consumer = consumer
    print(f"📢 [KAFKA CONSUMER] {INSTANCE_ID} connected and subscribed to telemetry-logs...")

    try:
        async for kafka_message in consumer:
            try:
                payload = json.loads(kafka_message.value.decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError) as parse_error:
                print(f"⚠️ [KAFKA] Skipping malformed message at offset {kafka_message.offset}: {parse_error}")
                continue

            if not isinstance(payload, dict) or not all(k in payload for k in ("service", "level", "message")):
                print(f"⚠️ [KAFKA] Skipping message missing required fields at offset {kafka_message.offset}: {payload}")
                continue

            try:
                await process_payload(payload)
            except Exception as e:
                # One bad record must never kill the consumer.
                print(f"⚠️ [KAFKA] Failed to process offset {kafka_message.offset}: {type(e).__name__}: {e}")
    finally:
        await consumer.stop()


async def _resolve_log(log_id: int, fix: str, routed_by: str, pattern: str | None):
    async with aiosqlite.connect(DB_PATH, timeout=30) as conn:
        await conn.execute(
            "UPDATE system_logs SET ai_analysis = ?, routed_by = ?, matched_rule = ?, resolved_at = ? WHERE id = ?",
            (fix, routed_by, pattern, time.time(), log_id),
        )
        await conn.commit()


async def handle_cache_miss(message: str, log_id: int):
    """
    Runs in the compile worker. Before spending tokens it checks the cache
    again: while this miss sat in the queue, an earlier log of the same kind
    may already have produced a rule that covers it (single-flight).
    """
    pattern, fix, source = await lookup_rule(message, LOCAL_RULE_CACHE)
    if fix is not None:
        await _resolve_log(log_id, fix, f"{source}_AFTER_QUEUE", pattern)
        print(f"♻️ [QUEUE] Log {log_id} resolved by a rule learned while it waited -- no LLM call")
        return

    if await try_acquire_compile_lock(message):
        try:
            await compile_rule_with_ai(message, log_id)
        finally:
            await release_compile_lock(message)
    else:
        print(f"🔒 [LOCK] Another instance is already compiling this message -- skipping duplicate Groq call: {message[:60]}")
        await _recheck_cache_after_delay(message, log_id)


async def compile_worker(worker_id: int):
    while True:
        message, log_id = await COMPILE_QUEUE.get()
        try:
            await handle_cache_miss(message, log_id)
        except Exception as e:
            print(f"⚠️ [COMPILE WORKER {worker_id}] Failed on log {log_id}: {type(e).__name__}: {e}")
            await _mark_compile_failed(log_id, f"{type(e).__name__}: {e}"[:200])
        finally:
            COMPILE_QUEUE.task_done()


async def _lock_is_held(message: str) -> bool:
    lock_key = f"logpulse:compiling:{hashlib.sha256(message.encode()).hexdigest()}"
    try:
        return bool(await REDIS_CLIENT.exists(lock_key))
    except Exception:
        return False


async def _recheck_cache_after_delay(message: str, log_id: int, poll_seconds: float = 1.0):
    """
    Another instance holds the compile lock for this exact message. Wait for
    its rule instead of paying the LLM again -- but keep polling while the
    lock is held (an LLM call can take several seconds), and if the lock goes
    away with no rule (the winner failed or crashed), compile it here.
    Never leaves the log stuck at COMPILING... .
    """
    deadline = time.time() + COMPILE_LOCK_TTL_SECONDS + 5
    while time.time() < deadline:
        await asyncio.sleep(poll_seconds)
        pattern, fix, source = await lookup_rule(message, LOCAL_RULE_CACHE)
        if fix:
            await _resolve_log(log_id, fix, f"{source}_AFTER_LOCK_WAIT", pattern)
            print(f"🔓 [LOCK] Found the winner's rule, applied to log {log_id}")
            return
        if not await _lock_is_held(message):
            break
    print(f"⚠️ [LOCK] No rule appeared for log {log_id} after the lock was released; compiling it here")
    if await try_acquire_compile_lock(message):
        try:
            await compile_rule_with_ai(message, log_id)
        finally:
            await release_compile_lock(message)
    else:
        await _mark_compile_failed(log_id, "another instance is still compiling this message")


# =====================================================================
# LIFESPAN
# =====================================================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    global kafka_producer
    print(f"🏷️ [INSTANCE] {INSTANCE_ID}, Redis sharing {'ON' if REDIS_SHARING else 'OFF'}")
    await warm_rule_cache_from_redis()
    await ensure_topic_partitions()
    kafka_producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    await kafka_producer.start()
    print("📤 [KAFKA PRODUCER] Connected and ready to send to telemetry-logs")
    background = [asyncio.create_task(message_broker_consumer())]
    background += [asyncio.create_task(compile_worker(i)) for i in range(COMPILE_WORKERS)]
    print(f"🧠 [COMPILE] {COMPILE_WORKERS} compile worker(s) started")
    yield
    for task in background:
        task.cancel()
    if kafka_producer:
        await kafka_producer.stop()


app = FastAPI(title="LogPulse: Distributed Telemetry Architecture Engine", lifespan=lifespan)

# =====================================================================
# AI SYSTEM COMPILER
# =====================================================================
GROQ_API_KEY = os.environ.get("GROQ_API_KEY")
GROQ_MODEL = "openai/gpt-oss-120b"
GROQ_URL = "https://api.groq.com/openai/v1/chat/completions"
GROQ_RATE_LIMIT_RETRIES = 3
MAX_LOG_MESSAGE_LENGTH_FOR_LLM = 500

_REGEX_PROBE_CODE = "import re, sys; re.search(sys.argv[1], sys.argv[2])"


def _clean_llm_regex(raw: str) -> str:
    """
    LLMs often return a regex written as source code instead of a bare pattern,
    e.g. r'Disk usage at [0-9]+%', "...", /.../ or wrapped in backticks. Strip that
    wrapper so what we validate and store is the pattern itself.
    """
    s = raw.strip()
    if len(s) >= 2 and s[0] == "`" and s[-1] == "`":
        s = s[1:-1].strip()
    m = re.fullmatch(r"""[rR]?(['"])(.*)\1""", s, flags=re.DOTALL)
    if m:
        s = m.group(2)
    elif len(s) >= 3 and s[0] == "/" and s[-1] == "/":
        s = s[1:-1]
    return s


def check_regex(pattern: str, timeout_seconds: float = 3.0) -> tuple[bool, str]:
    """
    SECURITY: the regex comes from an LLM, not a developer, so it is never
    trusted blindly. Returns (ok, reason):
      1. it must compile under Python's own `re` engine, and
      2. it must not cause catastrophic backtracking (ReDoS) on adversarial
         probe strings.
    The probe runs in a throwaway Python interpreter (a subprocess) with a hard
    timeout; subprocess.run kills the child when the timeout expires, so a
    catastrophic pattern cannot keep burning CPU.
    """
    if "\x00" in pattern:
        return False, "invalid regex (contains a null byte)"
    try:
        re.compile(pattern)
    except re.error as e:
        return False, f"invalid regex syntax ({e})"

    probes = ["a" * 35 + "!", "a" * 30 + "b" * 30 + "!"]
    for probe in probes:
        try:
            subprocess.run(
                [sys.executable, "-I", "-S", "-c", _REGEX_PROBE_CODE, pattern, probe],
                timeout=timeout_seconds, capture_output=True, check=False,
            )
        except subprocess.TimeoutExpired:
            return False, f"probe timed out after {timeout_seconds}s (possible ReDoS)"
        except OSError as e:
            return False, f"could not run safety probe ({e})"
    return True, "ok"


def is_regex_safe(pattern: str, timeout_seconds: float = 3.0) -> bool:
    return check_regex(pattern, timeout_seconds)[0]


# A rule that matches text like this would swallow unrelated logs.
_GENERIC_PROBES = ["", "x", "lorem ipsum zz-qq"]


def is_regex_too_generic(pattern: str) -> bool:
    try:
        return any(re.search(pattern, probe) for probe in _GENERIC_PROBES)
    except re.error:
        return False


# PII redaction via Presidio -- initialized once at module level.
try:
    from presidio_analyzer import AnalyzerEngine
    from presidio_analyzer.nlp_engine import NlpEngineProvider
    from presidio_anonymizer import AnonymizerEngine

    _nlp_configuration = {
        "nlp_engine_name": "spacy",
        "models": [{"lang_code": "en", "model_name": "en_core_web_sm"}],
    }
    _pii_nlp_engine = NlpEngineProvider(nlp_configuration=_nlp_configuration).create_engine()
    _pii_analyzer = AnalyzerEngine(nlp_engine=_pii_nlp_engine, supported_languages=["en"])
    _pii_anonymizer = AnonymizerEngine()
    _PII_AVAILABLE = True
except Exception as _pii_init_error:
    print(f"⚠️ [PII] Presidio not available ({_pii_init_error}); PII redaction disabled.")
    _PII_AVAILABLE = False


# Only entity types that are real PII in server logs. Presidio's defaults also
# include DATE_TIME, URL, ORGANIZATION, LOCATION and PERSON, which on log text
# are mostly false positives: on the OpenStack dataset it turned "2560MB" into
# "2560<ORGANIZATION>" and a process id into <DATE_TIME>, so the LLM wrote a
# regex for mangled text that could never match the real log.
PII_ENTITIES = [e.strip() for e in os.environ.get(
    "PII_ENTITIES", "EMAIL_ADDRESS,PHONE_NUMBER,CREDIT_CARD,IP_ADDRESS,IBAN_CODE,US_SSN"
).split(",") if e.strip()]


def redact_sensitive_data(text: str) -> str:
    """
    SECURITY: strips PII from a log message before it's ever sent to the
    external Groq API. CPU-bound (spaCy NER), so async callers run it with
    asyncio.to_thread to keep the event loop free.
    """
    if not _PII_AVAILABLE:
        return text
    results = _pii_analyzer.analyze(text=text, language="en", entities=PII_ENTITIES)
    anonymized = _pii_anonymizer.anonymize(text=text, analyzer_results=results)
    return anonymized.text


async def _mark_compile_failed(log_id: int, reason: str):
    """Replace the stuck 'COMPILING...' placeholder with the real reason, so the
    dashboard itself shows what went wrong instead of looking frozen forever."""
    try:
        async with aiosqlite.connect(DB_PATH, timeout=30) as conn:
            await conn.execute(
                "UPDATE system_logs SET ai_analysis = ? WHERE id = ?",
                (f"COMPILE FAILED: {reason}", log_id),
            )
            await conn.commit()
    except Exception as e:
        print(f"⚠️ [COMPILE] Could not record failure for log {log_id}: {e}")


async def _record_llm_call(log_id, mode, attempt, usage: dict, latency_ms, ok: bool, error=None):
    """Every real LLM call is written down with the token counts Groq reports."""
    prompt_tokens = int(usage.get("prompt_tokens") or 0)
    completion_tokens = int(usage.get("completion_tokens") or 0)
    total_tokens = int(usage.get("total_tokens") or (prompt_tokens + completion_tokens))
    TELEMETRY_COUNTERS["llm_calls"] += 1
    TELEMETRY_COUNTERS["llm_tokens"] += total_tokens
    try:
        async with aiosqlite.connect(DB_PATH, timeout=30) as conn:
            await conn.execute(
                "INSERT INTO llm_calls (log_id, mode, attempt, prompt_tokens, completion_tokens, total_tokens, "
                "latency_ms, ok, error, created_at, instance_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (log_id, mode, attempt, prompt_tokens, completion_tokens, total_tokens,
                 latency_ms, 1 if ok else 0, error, time.time(), INSTANCE_ID),
            )
            await conn.commit()
    except Exception as e:
        print(f"⚠️ [LLM] Could not record call: {e}")


MAX_COMPILE_ATTEMPTS = 2

SYSTEM_PROMPT = (
    "You are a log-analysis assistant. Respond with ONLY a JSON object "
    'matching exactly this schema: {"regex_pattern": string, "engineering_fix": string}. '
    "No other text."
)


def build_compile_prompt(safe_log_message: str) -> str:
    return (
        f"Analyze this server log text: '{safe_log_message}'. "
        f"Generate a valid python regular expression pattern replacing changing variables with .* or \\d+. "
        f"Numbers may contain decimals. Placeholders in angle brackets such as <IP_ADDRESS> replaced "
        f"sensitive values: match each one with \\S+, never with the placeholder text itself. "
        f"Provide a 1-sentence fix."
    )


async def _request_rule_from_groq(client, prompt: str, log_id=None, mode: str = "pipeline", attempt: int = 1):
    """
    One logical Groq call. Returns (regex_pattern, engineering_fix); raises on
    errors. A 429 (rate limit) is waited out using Retry-After, up to
    GROQ_RATE_LIMIT_RETRIES times; a 429 uses no tokens, so it isn't recorded
    as a call.
    """
    for rate_try in range(GROQ_RATE_LIMIT_RETRIES + 1):
        started = time.perf_counter()
        response = await client.post(
            GROQ_URL,
            headers={
                "Authorization": f"Bearer {GROQ_API_KEY}",
                "Content-Type": "application/json",
            },
            json={
                "model": GROQ_MODEL,
                "messages": [
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": prompt},
                ],
                "response_format": {"type": "json_object"},
            },
            timeout=45.0,
        )
        latency_ms = (time.perf_counter() - started) * 1000

        if response.status_code == 429 and rate_try < GROQ_RATE_LIMIT_RETRIES:
            try:
                wait = float(response.headers.get("retry-after", "5"))
            except ValueError:
                wait = 5.0
            wait = min(max(wait, 1.0), 30.0)
            print(f"⏳ [GROQ] Rate limited, waiting {wait:.0f}s before retrying")
            await asyncio.sleep(wait)
            continue

        if response.status_code != 200:
            await _record_llm_call(log_id, mode, attempt, {}, latency_ms, False, f"HTTP {response.status_code}")
            raise RuntimeError(f"Groq returned HTTP {response.status_code}: {response.text[:200]}")

        body = response.json()
        usage = body.get("usage") or {}
        try:
            data = json.loads(body["choices"][0]["message"]["content"])
        except (KeyError, IndexError, TypeError, json.JSONDecodeError) as e:
            await _record_llm_call(log_id, mode, attempt, usage, latency_ms, False, "unparseable reply")
            raise RuntimeError(f"Groq reply was not the expected JSON ({type(e).__name__})")

        await _record_llm_call(log_id, mode, attempt, usage, latency_ms, True)
        if not isinstance(data, dict):
            return None, None
        return data.get("regex_pattern"), data.get("engineering_fix")

    raise RuntimeError("Groq kept rate-limiting the request")


async def compile_rule_with_ai(log_message: str, log_id: int):
    print(f"🧠 [COMPILE] Starting AI compile for log {log_id}")

    if not GROQ_API_KEY:
        reason = "GROQ_API_KEY is not set in the uvicorn process"
        print(f"⚠️ [COMPILE] {reason}")
        await _mark_compile_failed(log_id, reason)
        return

    # One try/except around everything: an error anywhere (redaction, network,
    # parsing) is recorded on the log row instead of leaving it stuck.
    try:
        redacted = await asyncio.to_thread(redact_sensitive_data, log_message)
        safe_log_message = redacted[:MAX_LOG_MESSAGE_LENGTH_FOR_LLM]
        print(f"🧠 [COMPILE] Sending to Groq (after PII redaction): {safe_log_message!r}")

        base_prompt = build_compile_prompt(safe_log_message)
        prompt = base_prompt
        regex_rule = remediation = None
        reason = "no attempt made"

        async with httpx.AsyncClient() as client:
            for attempt in range(1, MAX_COMPILE_ATTEMPTS + 1):
                regex_rule, remediation = await _request_rule_from_groq(
                    client, prompt, log_id=log_id, mode="pipeline", attempt=attempt
                )
                print(f"🧠 [COMPILE] Attempt {attempt}: Groq proposed regex: {regex_rule!r}")
                if isinstance(regex_rule, str):
                    cleaned = _clean_llm_regex(regex_rule)
                    if cleaned != regex_rule:
                        print(f"🧹 [COMPILE] Stripped code-style wrapper from regex: {regex_rule!r} -> {cleaned!r}")
                        regex_rule = cleaned

                if not (isinstance(regex_rule, str) and isinstance(remediation, str) and regex_rule and remediation):
                    reason = "Groq reply was missing a valid regex_pattern or engineering_fix"
                else:
                    ok, why = await asyncio.to_thread(check_regex, regex_rule)
                    if not ok:
                        reason = why
                    elif is_regex_too_generic(regex_rule):
                        reason = "regex is too generic (it matches unrelated text)"
                    else:
                        # A rule that doesn't match the message it came from can
                        # never produce a cache hit, so it is rejected too.
                        try:
                            matches_original = re.search(regex_rule, log_message) is not None
                        except re.error:
                            matches_original = False
                        if matches_original:
                            reason = None
                            break
                        reason = "regex does not match the original log message"

                print(f"⚠️ [COMPILE] Attempt {attempt} rejected: {reason}")
                # Retry, telling the model exactly what was wrong with its answer.
                prompt = (
                    base_prompt
                    + f" Your previous answer {regex_rule!r} was rejected: {reason}. "
                    + "Return a corrected pattern in valid Python `re` syntax that matches the original log text."
                )

        if reason is not None:
            await _mark_compile_failed(log_id, f"{reason} | pattern: {regex_rule!r}"[:200])
            return

        async with aiosqlite.connect(DB_PATH, timeout=30) as conn:
            await conn.execute(
                "INSERT INTO compiled_rules_cache (regex_pattern, cached_fix, source_log_id, created_at) VALUES (?, ?, ?, ?)",
                (regex_rule, remediation, log_id, time.time()),
            )
            await conn.commit()
        await _resolve_log(log_id, remediation, "LLM_COMPILED", regex_rule)

        LOCAL_RULE_CACHE[regex_rule] = remediation
        if REDIS_SHARING:
            try:
                await REDIS_CLIENT.hset(REDIS_RULES_KEY, regex_rule, remediation)
            except Exception as e:
                # The rule is safe in SQLite; startup reconciliation copies it to Redis later.
                print(f"⚠️ [RULE COMPILED] Saved to SQLite + L1, Redis write failed ({type(e).__name__})")
        print(f"🧬 [RULE COMPILED] {INSTANCE_ID} saved rule: {regex_rule}")

    except Exception as e:
        reason = f"{type(e).__name__}: {e}"
        print(f"⚠️ [COMPILE] Failed for log {log_id}: {reason}")
        await _mark_compile_failed(log_id, reason[:200])


# =====================================================================
# REQUEST SCHEMA + AUTH
# =====================================================================
class TelemetryPayload(BaseModel):
    service: str = Field(..., min_length=1, description="Name of the service emitting this log")
    level: str = Field(..., min_length=1, description="Log severity, e.g. ERROR, WARN, INFO")
    message: str = Field(..., min_length=1, description="The raw log message text")
    source: str | None = Field(None, max_length=50, description="Where the log came from, e.g. loghub, benchmark")
    event_id: str | None = Field(None, max_length=200, description="Ground-truth template id, for labelled datasets")


INGEST_API_KEY = os.environ.get("INGEST_API_KEY")
if not INGEST_API_KEY:
    # Printed once at startup, not per request: a print on every request
    # would itself slow the ingest path under load.
    print("⚠️ [SECURITY] INGEST_API_KEY is not set -- ingest endpoints are UNPROTECTED.")


async def verify_api_key(x_api_key: str = Header(None)):
    if not INGEST_API_KEY:
        return
    if x_api_key != INGEST_API_KEY:
        raise HTTPException(status_code=401, detail="Invalid or missing API key")


async def _produce_many(records: list[dict]):
    """Stamp each record with its ingest time and hand it to Kafka."""
    now = time.time()
    futures = []
    for record in records:
        value = json.dumps({**record, "ingested_at": now}).encode("utf-8")
        futures.append(await kafka_producer.send(KAFKA_TOPIC, value))
    await asyncio.gather(*futures)
    TELEMETRY_COUNTERS["messages_produced"] += len(records)


@app.post("/v1/telemetry/ingest", status_code=status.HTTP_202_ACCEPTED, dependencies=[Depends(verify_api_key)])
async def ingest_log(payload: TelemetryPayload):
    try:
        await _produce_many([payload.model_dump()])
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Could not hand the log to Kafka: {e}")
    return {"status": "ACCEPTED", "message_broker": "SENT_TO_KAFKA", "topic": KAFKA_TOPIC}


@app.post("/v1/telemetry/ingest-sync", dependencies=[Depends(verify_api_key)])
async def ingest_log_sync_baseline(payload: TelemetryPayload):
    """
    BENCHMARK BASELINE ONLY -- the naive design this project replaces: no
    Kafka, no cache, the LLM is called inline for every log and the client
    waits for it. tools/benchmark.py compares this against /v1/telemetry/ingest.
    Its calls are recorded with mode='sync_baseline' so they never count
    toward the pipeline's own token numbers.
    """
    if not GROQ_API_KEY:
        raise HTTPException(status_code=503, detail="GROQ_API_KEY is not set")
    redacted = await asyncio.to_thread(redact_sensitive_data, payload.message)
    prompt = build_compile_prompt(redacted[:MAX_LOG_MESSAGE_LENGTH_FOR_LLM])
    try:
        async with httpx.AsyncClient() as client:
            _, fix = await _request_rule_from_groq(client, prompt, log_id=None, mode="sync_baseline")
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"LLM call failed: {e}")
    return {"status": "PROCESSED_INLINE", "engineering_fix": fix}


# =====================================================================
# OPENTELEMETRY (OTLP/HTTP) LOG RECEIVER
# =====================================================================
# Lets any OpenTelemetry SDK or Collector push logs straight into LogPulse:
# point an otlp_http exporter at http://<this-host>:8000 and it POSTs to /v1/logs.
OTLP_MIN_SEVERITY = int(os.environ.get("OTLP_MIN_SEVERITY", "13"))  # 13 = WARN in the OTel spec

# When set, every record accepted on /v1/logs is also appended to this file
# (one JSON object per line), so a live session can be replayed later,
# identically, into any configuration: tools/replay_recording.py.
OTLP_RECORD_FILE = os.environ.get("OTLP_RECORD_FILE")


def _append_recording(records: list[dict]) -> None:
    received_at = time.time()
    with open(OTLP_RECORD_FILE, "a", encoding="utf-8") as f:
        for record in records:
            f.write(json.dumps({**record, "received_at": received_at}) + "\n")

_SEVERITY_TEXT_TO_NUMBER = {
    "TRACE": 1, "DEBUG": 5, "INFO": 9, "NOTICE": 10, "WARN": 13, "WARNING": 13,
    "ERROR": 17, "FATAL": 21, "CRITICAL": 21,
}

try:
    from google.protobuf.json_format import MessageToDict
    from opentelemetry.proto.collector.logs.v1.logs_service_pb2 import ExportLogsServiceRequest
    _OTLP_PROTO_AVAILABLE = True
except Exception:
    _OTLP_PROTO_AVAILABLE = False


def _otlp_value_to_str(value: dict) -> str | None:
    """OTLP AnyValue -> plain string."""
    if not isinstance(value, dict):
        return None
    for key in ("stringValue", "intValue", "doubleValue", "boolValue"):
        if key in value:
            return str(value[key])
    for key in ("kvlistValue", "arrayValue"):
        if key in value:
            return json.dumps(value[key])
    return None


def _severity_number(record: dict) -> int:
    try:
        number = int(record.get("severityNumber") or 0)
    except (TypeError, ValueError):
        number = 0
    if not number:
        text = str(record.get("severityText") or "").upper()
        number = _SEVERITY_TEXT_TO_NUMBER.get(text, 0)
    return number


def _severity_label(number: int) -> str:
    if number >= 21:
        return "FATAL"
    if number >= 17:
        return "ERROR"
    if number >= 13:
        return "WARN"
    if number >= 9:
        return "INFO"
    if number >= 5:
        return "DEBUG"
    return "TRACE" if number else "UNSPECIFIED"


def parse_otlp_logs(body: dict, min_severity: int = OTLP_MIN_SEVERITY) -> list[dict]:
    """
    Pure function: an OTLP ExportLogsServiceRequest (as a JSON dict) in,
    LogPulse payloads out. Keeps only records at or above min_severity;
    records with no severity at all are kept only when min_severity is 0.
    """
    payloads = []
    for resource_logs in body.get("resourceLogs") or []:
        attributes = (resource_logs.get("resource") or {}).get("attributes") or []
        resource = {a.get("key"): _otlp_value_to_str(a.get("value") or {}) for a in attributes}
        service = resource.get("service.name") or "unknown-service"
        for scope_logs in resource_logs.get("scopeLogs") or []:
            for record in scope_logs.get("logRecords") or []:
                message = _otlp_value_to_str(record.get("body") or {})
                if not message or not message.strip():
                    continue
                severity = _severity_number(record)
                if severity < min_severity:
                    continue
                level = str(record.get("severityText") or "").upper() or _severity_label(severity)
                payloads.append({"service": service, "level": level, "message": message, "source": "otlp"})
    return payloads


@app.post("/v1/logs", dependencies=[Depends(verify_api_key)])
async def otlp_logs_receiver(request: Request):
    raw = await request.body()
    if request.headers.get("content-encoding", "").lower() == "gzip":
        raw = gzip.decompress(raw)
    content_type = request.headers.get("content-type", "").split(";")[0].strip().lower()

    if content_type == "application/x-protobuf":
        if not _OTLP_PROTO_AVAILABLE:
            raise HTTPException(
                status_code=415,
                detail="Protobuf OTLP needs `pip install opentelemetry-proto`, or set the exporter to encoding: json",
            )
        message = ExportLogsServiceRequest()
        message.ParseFromString(raw)
        body = MessageToDict(message, use_integers_for_enums=True)
    elif content_type == "application/json":
        try:
            body = json.loads(raw or b"{}")
        except json.JSONDecodeError:
            raise HTTPException(status_code=400, detail="Body is not valid JSON")
    else:
        raise HTTPException(status_code=415, detail="Send OTLP as application/json or application/x-protobuf")

    records = parse_otlp_logs(body)
    if records and OTLP_RECORD_FILE:
        await asyncio.to_thread(_append_recording, records)
    if records:
        try:
            await _produce_many(records)
        except Exception as e:
            raise HTTPException(status_code=503, detail=f"Could not hand logs to Kafka: {e}")
    print(f"📥 [OTLP] Accepted {len(records)} log record(s) at or above severity {OTLP_MIN_SEVERITY}")

    # An empty ExportLogsServiceResponse means full success in both encodings.
    if content_type == "application/x-protobuf":
        return Response(content=b"", media_type="application/x-protobuf")
    return Response(content=b"{}", media_type="application/json")


# =====================================================================
# METRICS
# =====================================================================
@app.get("/metrics")
async def get_cluster_metrics():
    return {
        "node_status": "ONLINE",
        "redis_cached_rules_count": len(LOCAL_RULE_CACHE),
        "total_hot_cache_hits": TELEMETRY_COUNTERS["cache_hits"],
        "total_cache_misses": TELEMETRY_COUNTERS["cache_misses"],
        "total_messages_produced_to_kafka": TELEMETRY_COUNTERS["messages_produced"],
        "llm_calls_since_start": TELEMETRY_COUNTERS["llm_calls"],
        "llm_tokens_since_start": TELEMETRY_COUNTERS["llm_tokens"],
        "compile_queue_depth": COMPILE_QUEUE.qsize(),
        "instance_id": INSTANCE_ID,
        "redis_sharing": REDIS_SHARING,
        "kafka_partitions_assigned": sorted(tp.partition for tp in kafka_consumer.assignment()) if kafka_consumer else [],
    }


def _percentile(values: list[float], pct: float) -> float | None:
    """Nearest-rank percentile; None for an empty list."""
    if not values:
        return None
    ordered = sorted(values)
    rank = max(1, min(len(ordered), math.ceil(pct / 100 * len(ordered))))
    return round(ordered[rank - 1], 3)


def _latency_stats(values_ms: list[float]) -> dict:
    return {
        "count": len(values_ms),
        "p50_ms": _percentile(values_ms, 50),
        "p95_ms": _percentile(values_ms, 95),
        "max_ms": round(max(values_ms), 2) if values_ms else None,
    }


def classify_log(routed_by: str | None, ai_analysis: str | None) -> str:
    """cache_hit | llm_compiled | failed | pending | unresolved"""
    if routed_by == "MISS_LEARNING_OFF":
        return "unresolved"
    if ai_analysis == "COMPILING...":
        return "pending"
    if (ai_analysis or "").startswith("COMPILE FAILED"):
        return "failed"
    if routed_by in ("LLM_COMPILED", "KAFKA_ASYNC_PROCESSOR"):
        return "llm_compiled"
    return "cache_hit"


def compute_summary(db_path: str = DB_PATH, since: float = 0.0) -> dict:
    """
    Every number the project claims, computed from what was actually stored.
    `since` (epoch seconds) limits it to one run, e.g. one dataset replay.
    """
    conn = sqlite3.connect(db_path, timeout=30)
    conn.row_factory = sqlite3.Row
    try:
        logs = conn.execute(
            "SELECT id, routed_by, ai_analysis, event_id, matched_rule, ingested_at, consumed_at, resolved_at, instance_id, lookup_ms "
            "FROM system_logs WHERE COALESCE(ingested_at, 0) >= ? OR ? = 0",
            (since, since),
        ).fetchall()
        calls = conn.execute(
            "SELECT mode, ok, prompt_tokens, completion_tokens, total_tokens, latency_ms, instance_id "
            "FROM llm_calls WHERE created_at >= ?",
            (since,),
        ).fetchall()
        rule_sources = conn.execute(
            "SELECT r.regex_pattern, s.event_id FROM compiled_rules_cache r "
            "LEFT JOIN system_logs s ON s.id = r.source_log_id"
        ).fetchall()
        rules_total = conn.execute("SELECT COUNT(*) FROM compiled_rules_cache").fetchone()[0]
    finally:
        conn.close()

    outcome_counts = {"cache_hit": 0, "llm_compiled": 0, "failed": 0, "pending": 0, "unresolved": 0}
    queue_lag, hit_latency, waited_latency, compile_latency = [], [], [], []
    waited_hits = 0
    route_latency: dict[str, list[float]] = {}   # ingest-to-fix time per layer that answered
    lookup_latency: dict[str, list[float]] = {}  # time of the cache lookup alone, by result
    compiled_per_template: dict[str, int] = {}   # LLM compiles per ground-truth template
    instances: dict[str, dict] = {}
    for row in logs:
        outcome = classify_log(row["routed_by"], row["ai_analysis"])
        outcome_counts[outcome] += 1
        inst = instances.setdefault(row["instance_id"] or "unknown",
                                    {"logs": 0, "l1_hits": 0, "l2_hits": 0, "llm_compiled": 0, "llm_calls": 0, "llm_tokens": 0})
        inst["logs"] += 1
        route = row["routed_by"] or ""
        inst["l1_hits"] += route.startswith("L1_")
        inst["l2_hits"] += route.startswith("L2_")
        inst["llm_compiled"] += outcome == "llm_compiled"
        if outcome == "llm_compiled" and row["event_id"]:
            compiled_per_template[row["event_id"]] = compiled_per_template.get(row["event_id"], 0) + 1
        if row["lookup_ms"] is not None:
            key = route if route.startswith(("L1_", "L2_")) and "_AFTER_" not in route else "MISS"
            lookup_latency.setdefault(key, []).append(row["lookup_ms"])
        if row["ingested_at"] and row["resolved_at"]:
            route_latency.setdefault(route, []).append((row["resolved_at"] - row["ingested_at"]) * 1000)
        # A hit "after queue/lock wait" cost no LLM call, but it sat behind
        # compiles first, so its latency is reported apart from instant hits.
        waited = outcome == "cache_hit" and "_AFTER_" in (row["routed_by"] or "")
        waited_hits += waited
        if row["ingested_at"] and row["consumed_at"]:
            queue_lag.append((row["consumed_at"] - row["ingested_at"]) * 1000)
        if row["ingested_at"] and row["resolved_at"]:
            elapsed = (row["resolved_at"] - row["ingested_at"]) * 1000
            if outcome != "cache_hit":
                compile_latency.append(elapsed)
            else:
                (waited_latency if waited else hit_latency).append(elapsed)

    def _calls_summary(mode: str) -> dict:
        rows = [c for c in calls if c["mode"] == mode]
        total = sum(c["total_tokens"] or 0 for c in rows)
        return {
            "calls": len(rows),
            "failed_calls": sum(1 for c in rows if not c["ok"]),
            "prompt_tokens": sum(c["prompt_tokens"] or 0 for c in rows),
            "completion_tokens": sum(c["completion_tokens"] or 0 for c in rows),
            "total_tokens": total,
            "avg_tokens_per_call": round(total / len(rows), 1) if rows else None,
            "latency": _latency_stats([c["latency_ms"] for c in rows if c["latency_ms"] is not None]),
        }

    pipeline = _calls_summary("pipeline")
    baseline = _calls_summary("sync_baseline")
    for call in calls:
        if call["mode"] == "pipeline":
            inst = instances.setdefault(call["instance_id"] or "unknown",
                                        {"logs": 0, "l1_hits": 0, "l2_hits": 0, "llm_compiled": 0, "llm_calls": 0, "llm_tokens": 0})
            inst["llm_calls"] += 1
            inst["llm_tokens"] += call["total_tokens"] or 0

    # The naive design calls the LLM once for every log. Its token cost per
    # call is taken from measured baseline calls when they exist, otherwise
    # from the pipeline's own measured calls.
    logs_handled = len(logs) - outcome_counts["pending"]
    per_call = baseline["avg_tokens_per_call"] or pipeline["avg_tokens_per_call"]
    naive_tokens = round(logs_handled * per_call) if per_call else None
    token_reduction = None
    if naive_tokens:
        token_reduction = round(100 * (1 - pipeline["total_tokens"] / naive_tokens), 1)

    # Ground truth from labelled datasets: did each cache hit use a rule that
    # was learned from a log of the same template?
    rule_template = {pattern: event_id for pattern, event_id in rule_sources}
    labelled = [row for row in logs if row["event_id"]]
    checked = correct = 0
    for row in labelled:
        if classify_log(row["routed_by"], row["ai_analysis"]) != "cache_hit" or not row["matched_rule"]:
            continue
        source_template = rule_template.get(row["matched_rule"])
        if source_template:
            checked += 1
            correct += source_template == row["event_id"]

    templates = {row["event_id"] for row in labelled}
    # Per-template outcome counts, so an experiment can tell "missed a kind
    # of error it was trained on" apart from "never saw this kind in training".
    by_template: dict[str, dict] = {}
    for row in labelled:
        stats = by_template.setdefault(row["event_id"], {"logs": 0, "cache_hit": 0})
        stats["logs"] += 1
        stats["cache_hit"] += classify_log(row["routed_by"], row["ai_analysis"]) == "cache_hit"
    return {
        "since": since,
        "logs": {"total": len(logs), "handled": logs_handled, **outcome_counts,
                 "cache_hit_instant": outcome_counts["cache_hit"] - waited_hits,
                 "cache_hit_after_wait": waited_hits},
        "cache_hit_rate_pct": round(100 * outcome_counts["cache_hit"] / logs_handled, 1) if logs_handled else None,
        "rules_learned_total": rules_total,
        "llm": {"pipeline": pipeline, "sync_baseline": baseline},
        "naive_comparison": {
            "naive_llm_calls": logs_handled,
            "actual_llm_calls": pipeline["calls"],
            "tokens_per_naive_call_used": per_call,
            "naive_tokens_estimate": naive_tokens,
            "actual_tokens": pipeline["total_tokens"],
            "token_reduction_pct": token_reduction,
            "note": "naive = one LLM call per log, at the measured tokens per call",
        },
        "latency": {
            "kafka_queue_lag": _latency_stats(queue_lag),
            "ingest_to_fix_cache_hit": _latency_stats(hit_latency),
            "ingest_to_fix_hit_after_wait": _latency_stats(waited_latency),
            "ingest_to_fix_llm": _latency_stats(compile_latency),
            "ingest_to_fix_by_route": {route: _latency_stats(v) for route, v in sorted(route_latency.items())},
            "cache_lookup_by_result": {key: _latency_stats(v) for key, v in sorted(lookup_latency.items())},
        },
        "instances": instances,
        "duplicate_compiles": {
            "templates_compiled": len(compiled_per_template),
            "llm_compiled_logs": sum(compiled_per_template.values()),
            "extra_compiles_beyond_one_per_template": sum(n - 1 for n in compiled_per_template.values()),
        },
        "dataset_eval": {
            "labelled_logs": len(labelled),
            "distinct_templates": len(templates),
            "cache_hits_checked": checked,
            "cache_hits_same_template": correct,
            "rule_precision_pct": round(100 * correct / checked, 1) if checked else None,
            "by_template": by_template,
        },
    }


class LearningSwitch(BaseModel):
    enabled: bool


@app.get("/admin/learning")
async def get_learning():
    return {"learning_enabled": LEARNING_ENABLED}


@app.post("/admin/learning", dependencies=[Depends(verify_api_key)])
async def set_learning(switch: LearningSwitch):
    """Freeze (False) or unfreeze (True) the rule set; see LEARNING_ENABLED."""
    global LEARNING_ENABLED
    LEARNING_ENABLED = switch.enabled
    print(f"🎛️ [LEARNING] {'ON -- misses go to the LLM' if LEARNING_ENABLED else 'OFF -- rules frozen'}")
    return {"learning_enabled": LEARNING_ENABLED}


@app.get("/metrics/summary")
async def get_metrics_summary(since: float = 0.0):
    """Numbers computed from the database, so they survive restarts."""
    return await asyncio.to_thread(compute_summary, DB_PATH, since)


@app.get("/logs")
async def get_recent_logs(limit: int = 25):
    """Read-only endpoint for the demo UI: recent logs and how they were resolved."""
    async with aiosqlite.connect(DB_PATH, timeout=30) as conn:
        conn.row_factory = aiosqlite.Row
        async with conn.execute(
            "SELECT id, timestamp, service, level, message, ai_analysis, routed_by, source FROM system_logs ORDER BY id DESC LIMIT ?",
            (limit,),
        ) as cursor:
            rows = await cursor.fetchall()
    return {"logs": [dict(row) for row in rows]}


@app.get("/rules")
async def get_compiled_rules():
    """Read-only endpoint for the demo UI: every rule learned so far."""
    async with aiosqlite.connect(DB_PATH, timeout=30) as conn:
        conn.row_factory = aiosqlite.Row
        async with conn.execute(
            "SELECT id, regex_pattern, cached_fix FROM compiled_rules_cache ORDER BY id DESC"
        ) as cursor:
            rows = await cursor.fetchall()
    return {"rules": [dict(row) for row in rows]}


# Serves the demo dashboard at / -- see static/index.html
if os.path.isdir("static"):
    app.mount("/", StaticFiles(directory="static", html=True), name="static")
