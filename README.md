# LogPulse

**Ask the LLM once per kind of incident, not once per log line.**

LogPulse is an asynchronous incident-triage pipeline. Services send their error logs (over OpenTelemetry or HTTP). The first time a new kind of incident appears, an LLM writes two things: a fix, and a regex that recognises that kind of incident. Every later incident that matches the regex gets the saved fix from a cache in milliseconds, with no LLM call.

On a live run against the [OpenTelemetry Demo](https://github.com/open-telemetry/opentelemetry-demo) shop, LogPulse handled **10,345 incidents with 18 LLM calls and 10,931 tokens**. Calling the LLM for every log would have cost about 6.3 million tokens, so that is **99.8% fewer**.

## Why

Production failures repeat. A service that breaks at 2 a.m. writes the same error thousands of times, and only the numbers change: `timeout after 3000ms`, `timeout after 3012ms`, and so on. In the live run, 10,345 error logs held only 14 kinds of problem.

Teams increasingly ask an LLM to explain errors. Calling it on every line has three costs:

- **Money:** you pay per token.
- **Time:** about 1 second or more per call.
- **Rate limits:** the call cap is hit exactly when an incident floods the logs.

Log tools can already group similar lines into patterns (for example the Drain algorithm). Grouping tells you *that* lines are similar, not *what to do* about them. LogPulse combines the two ideas and measures the result end to end: the LLM writes the pattern and the fix once, and the pattern does the recognising from then on.

## How an incident flows

```
 OpenTelemetry services ──OTLP──▶ POST /v1/logs ───────┐
 (SDK or Collector)                                    │
 replay scripts / curl ──▶ POST /v1/telemetry/ingest ──┤  answers 202 at once
                                                       ▼
                                             Kafka topic (1 partition per instance)
                                                       │
                                     consumer ─▶ L1 in-process dict ── hit ─▶ fix   (~0.04 ms)
                                                       │ miss
                                                 L2 Redis (shared) ── hit ─▶ fix, copied into L1   (~3 ms)
                                                       │ miss
                                                 compile queue ─▶ re-check cache ─▶ hit: no LLM call
                                                       │ still a miss; Redis lock so one instance asks
                                                 Groq LLM (PII redacted first) ─▶ regex + fix   (~1.3 s)
                                                       │ regex must compile, pass a ReDoS probe,
                                                       │ match its own incident, and not be too broad
                                                       │ (one retry with the reason, else "compile failed")
                                                 rule saved: SQLite (system of record) + Redis + L1
```

1. **Ingest.** FastAPI keeps warnings and worse (OTLP severity ≥ 13), puts each incident on Kafka, and replies `202 Accepted` at once. The sender never waits for the LLM.
2. **L1.** Each consumer tries the saved regexes held in its own memory.
3. **L2.** On an L1 miss, it checks Redis, which every instance shares, so a rule learned by one instance is free for the others.
4. **Compile.** On a miss in both, the incident joins a compile queue, which checks the cache again before calling the LLM. Another incident of the same kind may have just been learned. A Redis `SET NX` lock stops two instances paying for the same message.
5. **LLM.** Presidio removes personal data (emails, phone numbers, cards, IPs, IBANs, SSNs). Groq then answers in JSON: `{regex_pattern, engineering_fix}`.
6. **Guardrails.** A regex is saved only if it passes four checks:
   - it compiles;
   - it finishes quickly on a stress test run in a separate process (protection against ReDoS);
   - it matches the incident it came from;
   - it is not too generic (for example `.*`).

**Storage.** SQLite holds three tables:
- `system_logs`: every incident, and which layer answered it.
- `compiled_rules_cache`: every rule, and the incident it was learned from.
- `llm_calls`: every LLM request, with Groq's token counts and its latency.

All numbers below are computed from these tables by `GET /metrics/summary`. At startup, the app copies any rule missing from Redis out of SQLite, so a crash between the two writes can't leave the instances out of sync. There is no per-incident SQLite lookup: Redis holds the same rules and persists them (AOF), so a SQLite lookup would never find anything new.

## Tech stack

| Part | Tool | Why this, not something else |
|---|---|---|
| API | Python, FastAPI, uvicorn | Async-native, so one process waits on Kafka, Redis and the LLM concurrently. Request validation comes from Pydantic. Flask isn't async by design; Django is heavier than an API needs. |
| Queue | Apache Kafka (aiokafka) | Durable, replayable, and it splits work across instances through partitions and consumer groups. It is the standard for log pipelines. RabbitMQ is built for tasks; an in-memory queue is lost on a crash. |
| L1 cache | Python dict | No network at all. |
| L2 cache | Redis 7, AOF persistence | Shared by every instance, persistent, and `SET NX` gives a simple single-flight lock. Memcached has no persistence. |
| Store | SQLite, WAL mode (aiosqlite) | Zero setup; the system of record and the source of every metric. Postgres is the upgrade for many machines. |
| LLM | Groq, `openai/gpt-oss-120b` | Fast (median call ~1.3 s), JSON mode, and it reports exact token counts. The API is OpenAI-compatible, so the provider is a one-line swap. |
| Privacy | Microsoft Presidio | Redacts personal data before anything leaves for the LLM. |
| Telemetry in | OpenTelemetry (OTLP/HTTP, JSON or protobuf) | Any service already instrumented with OpenTelemetry can send to LogPulse with a collector config change and no code change. |
| Evaluation | Loghub datasets, Drain3, pytest | Loghub provides real logs with human labels. Drain3 provides an independent answer key for unlabelled demo logs. There are 57 tests. |

## Results

All runs were on one Windows 11 laptop on 2026-11-19, with Docker Desktop for Redis and Kafka. Held-out runs used frozen code (`app.py` fingerprint `6562b17bbba5`). Result files are in [`results/`](results); runs ruined by the laptop sleeping are kept in [`results/invalid/`](results/invalid) with the reason.

This laptop needs ~19 ms for even an empty HTTP request, so absolute latencies are pessimistic. The comparisons (async vs sync, Redis on vs off) are fair, because both sides ran on the same machine.

### 1. Live run on the OpenTelemetry Demo

The demo's collector forwarded its warning-and-worse logs to `/v1/logs` for about 3 hours, while failures were switched on through the demo's feature flags.

| | |
|---|---|
| Incidents | 10,345 |
| Answered from L1 at once | 10,313 |
| Waited behind the compile of the same kind, then reused its rule (no extra LLM call) | 17 |
| Needed a new rule from the LLM | 13 |
| Failed (no safe regex after 2 attempts) | 2 |
| LLM calls / tokens | **18 / 10,931**, vs ~6.28 M tokens at one call per incident (**−99.8%**) |
| Ingest → fix, known incident (p50 / p95) | 46 / 142 ms |
| Ingest → fix, new incident (p50 / p95) | 12.6 / 28 s (mostly queueing behind other new kinds, plus Groq rate limits) |
| Kafka queue lag (p50 / p95) | 40 / 133 ms |
| Agreement with Drain3 (independent labels) | 14 Drain3 kinds vs 13 rules, mapped one to one; **0 wrong groupings** in 10,330 reuses |

Honest context: 95% of the volume (9,865 logs) was the demo collector's own Kafka connection errors. Only 480 logs came from shop services (payment 468, load generator 12). A few very repetitive errors make the hit rate look especially good.

### 2. Redis sharing across 3 instances

Three instances shared one Kafka consumer group. The same 5,000 recorded demo incidents were replayed twice, once with Redis sharing on and once with it off.

| Sharing | LLM calls | Tokens | Duplicate compiles | L2 hits | L1 lookup p50 | L2 lookup p50 | LLM call p50 | Precision |
|---|---|---|---|---|---|---|---|---|
| **on** | **14** | **9,430** | 0 | 11 | 0.04 ms | 3.2 ms | 1,291 ms | 100% |
| off | 32 | 21,002 | 16 | 0 | 0.03 ms | – | 1,156 ms | 100% |

Sharing cut LLM calls by 56% and tokens by 55%. A rule found in Redis costs ~3 ms instead of ~1.2 s for an LLM call. That 3 ms is almost all network round trip to Redis (a bare `PING` takes 1.7 ms here). An earlier run on the Apache dataset showed the same effect: 8 vs 18 calls.

### 3. Async ingestion vs calling the LLM inline

`tools/benchmark.py` compares `/v1/telemetry/ingest` (Kafka, `202`) with `/v1/telemetry/ingest-sync`, the naive design that calls the LLM inside the request.

| Design | Concurrency | Requests | p50 | p95 | Throughput |
|---|---|---|---|---|---|
| **async** | 1 | 300 | **23 ms** | 42 ms | 21.8 req/s |
| sync (LLM inline) | 1 | 20 | 3,145 ms | 5,145 ms | 0.3 req/s |
| **async** | 5 | 300 | **104 ms** | 213 ms | 39.4 req/s |
| sync (LLM inline) | 5 | 20 | 8,712 ms | 12,949 ms | 0.5 req/s |

The sender waits about 100× less with async. The sync design also paid ~490 tokens on every request.

### 4. Does it generalise? Held-out datasets, learning frozen

Each [Loghub](https://github.com/logpai/loghub) dataset was split 70/10/20 in time order. LogPulse learned rules on the first 70% only. On validation and test, learning was switched off (`POST /admin/learning`), so the LLM was never called. A kind never seen in training has no correct rule, so any hit on one is a **false match**.

| Dataset | Kinds (train / test) | Train: LLM calls, tokens | Test hit rate | Test hit rate, known kinds | Test lines of new kinds | False matches on them | Test precision |
|---|---|---|---|---|---|---|---|
| HDFS | 12 / 11 | 26, 16,645 | 99.2% | 99.7% | 2 | 0 | 100% |
| Zookeeper | 40 / 20 | 44, 24,170 | 98.8% | 99.7% | 4 | 0 | 99.5% |
| Linux | 25 / 103 | 24, 13,450 | 71.0% | 100% | 116 | 0 | 81.3% |

Precision is the share of reused test incidents whose rule was learned from an incident of the same labelled kind. Linux's overall hit rate is 71% because 93 of its 103 test kinds never appeared in training. LogPulse correctly left all 116 of those lines unresolved.

What went wrong, so it isn't hidden:

- **Linux: one over-broad rule.** The LLM's SSH `authentication failure` regex stopped before the end of the line. It merged four labelled kinds (no user, `user=root`, `user=guest`, `user=test`) and pulled precision down to 81%. An earlier run of the same frozen code scored 100%, because the LLM doesn't write the same regex twice.
- **Zookeeper, earlier run:** a leader-election rule ignored the `LEADING / LOOKING / FOLLOWING` state and produced 1 false match.
- **Rules are often too narrow.** HDFS needed 26 calls for 12 kinds. One rule wrote `blk_\d+` and missed negative block ids, so a second rule (`blk_-?\d+`) was learned. That costs tokens, but it doesn't give wrong fixes.

### 5. Fix quality (human review)

All 13 rules from the live run were rated by hand in [`results/demo-live-2-review.csv`](results/demo-live-2-review.csv).

| | |
|---|---|
| Rules that matched the right incidents | 11 of 11 reused (2 never reused) |
| Fixes rated useful / vague / wrong | **4 / 9 / 0** |

The fixes are rarely wrong but often generic, such as "ensure the broker service is running and the network configuration is correct". The LLM sees one log line with no context. Several errors come from third-party software, so advice to change the code isn't actionable. Some causes are invisible from the log alone: the payment errors came from the demo's `paymentFailure` feature flag.

## Claims and their status

| Claim | Status | Evidence |
|---|---|---|
| Far fewer LLM tokens than calling the LLM per incident | **Proven** | −99.8% live; −96.9% to −98.3% while learning on datasets |
| Async ingestion keeps senders fast | **Proven** | 23 ms vs 3.1 s p50, same machine |
| Redis sharing avoids repeated LLM calls across instances | **Proven** | 14 vs 32 calls, 0 vs 16 duplicates |
| Rules recognise future incidents of known kinds | **Proven** | 99.7–100% on held-out test data |
| New, never-seen incidents are left alone | **Proven** on these runs | 0 false matches on 122 new-kind test lines (1 in an earlier Zookeeper run) |
| Reused rules give the right fix | **Mostly** | 99.5–100% precision, but 81% on Linux from one over-broad rule |
| The fixes are good advice | **Partly** | 4 useful, 9 vague, 0 wrong of 13 |
| Works on real production telemetry | **Not tested** | OpenTelemetry Demo only |

## Limitations

1. **A rule is checked against one example.** The guardrails confirm that a regex matches the incident it came from, never what else it will match, so an over-broad rule can slip through (Linux, 81%). Production logs have no labels, so nothing would flag it.
2. **Fixes are generic.** The LLM sees one line, with no trace, code, configuration or incident history.
3. **Lookup grows with the number of rules, and rules never expire.** Every incident is tried against every regex in turn, and a saved rule is trusted forever.

Smaller ones:
- Only the log body is read, not the OpenTelemetry attributes.
- Live matching has no regex timeout. The ReDoS probe runs only when a rule is accepted.
- The in-memory compile queue is lost on restart.
- SQLite allows one writer at a time.

## Future work

1. **Context-aware fixes.** Give the LLM the OpenTelemetry attributes, nearby logs, and runbooks or past post-mortems found by retrieval. Add a useful / vague / wrong button to the dashboard and track the share rated useful.
2. **Rules that check themselves.** Before saving a rule, replay it over recently stored incidents and reject it, or ask for a tighter one, if it grabs incidents that Drain3 puts in other groups. Retire rules that drift.
3. **Scale and real traffic.** Compile all rules into one matcher (a combined regex or Hyperscan), index rules by service, move to Postgres and Kubernetes, and run on a real production log stream.

## Repository layout

```
app.py                  the whole service: ingest, Kafka consumer, caches, LLM compile, metrics, dashboard
static/index.html       live dashboard served at /
docker-compose.yml      Redis + Kafka
otel/                   collector config that forwards the OpenTelemetry Demo's logs to LogPulse
tools/                  replay, dataset split, experiments, benchmark, Drain3 labelling, review sheet
tests/                  57 tests (Groq is faked; Redis/Kafka integration tests need docker compose)
results/                every result file quoted above (JSON, CSV, recorded sessions)
```

Loghub datasets are downloaded into `data/` on first use. SQLite databases and app logs from the runs are not committed.

## Running it yourself (Windows, PowerShell)

You need Docker Desktop, Python 3.12+ and a [Groq API key](https://console.groq.com). Run every command from the project folder.

**One-time setup**

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe -m spacy download en_core_web_sm
[Environment]::SetEnvironmentVariable("GROQ_API_KEY", "your-groq-key", "User")   # then open a new PowerShell
```

On Linux or macOS, use `source .venv/bin/activate` and `export GROQ_API_KEY=...`; the rest is the same.

**Every session**

```powershell
docker compose up -d                      # Redis + Kafka
$env:PYTHONIOENCODING = "utf-8"           # the app prints emoji; Windows consoles need this
```

**Tests** (no Groq needed)

```powershell
.\.venv\Scripts\python.exe -m pytest tests -q
```

**Run the service and send a log**

```powershell
.\.venv\Scripts\python.exe -m uvicorn app:app --host 127.0.0.1 --port 8000
# dashboard: http://localhost:8000    numbers: http://localhost:8000/metrics/summary
```

**Experiments**

These commands are the ones behind the results above. Each prints its numbers and saves a JSON file in `results/`.

```powershell
# Live end-to-end: start the app with OTLP_RECORD_FILE set, point the OpenTelemetry Demo's
# collector at it (see otel/otelcol-config-extras.yml), and switch failures on at
# http://localhost:8080/feature. Then label the recording and build the review sheet:
.\.venv\Scripts\python.exe tools\label_with_drain.py results\demo-recording-2.jsonl
.\.venv\Scripts\python.exe tools\make_review_sheet.py results\demo-live-2.db

# Redis sharing on vs off, 3 instances on ports 8001-8003 (stop other app instances first)
.\.venv\Scripts\python.exe tools\multi_instance_experiment.py --recording results\demo-recording-2.labelled.jsonl --limit 5000

# Held-out generalisation (starts its own app on port 8000)
.\.venv\Scripts\python.exe tools\run_heldout.py --datasets HDFS,Zookeeper,Linux

# Async vs sync (app running)
.\.venv\Scripts\python.exe tools\benchmark.py --mode async --requests 300 --concurrency 1
.\.venv\Scripts\python.exe tools\benchmark.py --mode sync  --requests 20  --concurrency 1
```

Configuration is through environment variables: `GROQ_API_KEY`, `INGEST_API_KEY`, `LOGPULSE_DB`, `KAFKA_TOPIC`, `LOGPULSE_REDIS_KEY`, `REDIS_SHARING`, `OTLP_RECORD_FILE` and `PII_ENTITIES`. The Groq free tier caps tokens per day, and the full set of experiments uses roughly 150–200k, so spread them over two days if runs start failing with HTTP 429. Keep the machine awake during long runs: a sleep breaks Kafka timestamps and invalidates the latency numbers.
