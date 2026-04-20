# **Distributed Async Persistent Tasks Queue With Postgres**

> Previous version: [Async Persistent Tasks Queue with Postgres](../async-pg-task-queue/README.md)

---

## Table of Contents

1. [What This Version Is](#1-what-this-version-is)
2. [What We Improved From the Previous Version](#2-what-we-improved-from-the-previous-version)
3. [Why Postgres, Not Redis](#3-why-postgres-not-redis)
4. [Architecture](#4-architecture)
5. [Key Design Decisions](#5-key-design-decisions)
6. [Case Studies — Where We Failed and How We Fixed It](#6-case-studies--where-we-failed-and-how-we-fixed-it)
7. [Performance](#7-performance)
8. [Observability](#8-observability)
9. [Running the System](#9-running-the-system)
10. [What Comes Next](#10-what-comes-next)

---

## 1. What This Version Is

This system processes a queue of AI prompts through the Google Gemini API while staying strictly under a rate limit, 15 requests per minute. It runs multiple worker processes in parallel, all coordinated through a single Postgres database.

The previous version solved crash-safety by persisting task state to Postgres. But the rate limiter was still in-memory, one copy per process. Run two workers and each thought it owned the full budget, so together they fired twice as many requests as allowed and got 429s.

This version solves that, and then keeps going. By the time all the bugs were found and fixed, it also gained: proper run isolation with UUIDs, heartbeat-based orphan detection, a dashboard with per-run filtering, and a claim loop that does three database operations in one network round trip instead of three.

The full list of files:

| File | Role |
|---|---|
| `master.py` | Entry point — creates a run, enqueues tasks, spawns workers, reports timing |
| `worker.py` | Worker process — claims tasks, applies rate limit, calls Gemini, self-exits |
| `db.py` | All database interaction — schema, task lifecycle, rate limit, events, heartbeat |
| `rate_limiter.py` | `PgRateLimiter` — wraps the Postgres `try_acquire_slot()` function |
| `dashboard.py` | Streamlit live dashboard — per-run filtering, dynamic refresh rate |

---

## 2. What We Improved From the Previous Version

The previous version had a persistent task queue but left several real problems unsolved. Here is every improvement made in this version and the exact reason each one was needed.

### 2.1 Distributed rate limiting

**Before:** Each worker process had its own `asyncio.Lock()` and in-memory `deque`. Two workers sharing a 15 RPM limit each believed they had the full 15 slots, together they fired 30 requests per minute.

**After:** The sliding window lives in a `rate_limit_window` Postgres table. A `pg_advisory_xact_lock` inside a stored function serialises all workers through a single critical section. Only one worker can check and insert at a time, across all processes on all machines sharing the same database.

### 2.2 True multi-process execution

**Before:** Workers were coroutines inside a single `asyncio.gather()`, one Python process, one event loop, one CPU core.

**After:** Each `worker.py` is an independent OS process with its own event loop, spawned by `master.py` using `asyncio.create_subprocess_exec()`. The rate limiter coordinates them through Postgres, no shared memory needed.

### 2.3 Run isolation with UUIDs

**Before:** Tasks from different runs shared the same table with no way to tell them apart. Running the script twice mixed results together. `is_complete()` would look at all tasks ever created, not just the current run.

**After:** Every `master.py` invocation creates a row in the `runs` table and gets back a UUID `run_id`. Every task, event, and heartbeat carries that `run_id`. All queries are scoped with `WHERE run_id = $1`. Two runs can execute simultaneously without interfering.

### 2.4 Heartbeat-based orphan recovery (no timeout)

**Before:** A task was considered "stuck" if it had been `running` for more than 30 seconds. This caused legitimate slow tasks (Gemini sometimes takes 40+ seconds) to be incorrectly reset and processed twice.

**After:** A task is considered orphaned only if the worker that owns it has stopped sending heartbeats. Every worker upserts `last_seen` every 5 seconds. A task is only reset if its `worker_id` has no heartbeat row newer than 15 seconds. A slow-but-alive worker's task is never touched — regardless of how long it runs.

### 2.5 Single-round-trip claim loop

**Before:** The worker's main loop made three separate database calls on every iteration:

```
recover_stuck_tasks()  →  round trip 1  (~300ms on NeonDB)
is_complete()          →  round trip 2  (~300ms on NeonDB)
claim_next_task()      →  round trip 3  (~300ms on NeonDB)
```

With 75 tasks this added ~67 seconds of pure network overhead, explaining the observed 5–6 second gap between a task finishing and the next one being claimed.

**After:** One CTE does all three in a single round trip:

```sql
WITH
  recovered AS (UPDATE tasks ... WHERE heartbeat gone ... RETURNING task_id),
  completion AS (SELECT COUNT(*) FROM tasks WHERE status IN ('pending','running')),
  claimed    AS (UPDATE tasks SET status='running' ... FOR UPDATE SKIP LOCKED RETURNING ...)
SELECT recovered_count, incomplete_count, task_id, prompt
```

One network call. The results tell the worker everything it needs to decide what to do next.

### 2.6 Single event loop in master

**Before:** `master.py` called `asyncio.run()` twice, once to enqueue tasks, once to fetch the final run duration. asyncpg's SSL connections are tied to the event loop that created them. When the first loop was destroyed, background tasks were still trying to write on the closed socket, causing `Fatal error on SSL transport`.

**After:** `master.py` is one `async def main()` inside one `asyncio.run()`. The pool is created once and lives for the entire process. No teardown issues.

### 2.7 Dashboard per-run filtering and dynamic refresh

**Before:** The dashboard showed all tasks across all runs with no way to scope by run. Refresh interval was hardcoded at 3 seconds.

**After:** A sidebar run selector filters every panel to the selected `run_id`. A radio button selects 1s / 3s / 5s / 10s refresh. The workers panel joins against the heartbeat table scoped to the selected run.

---

## 3. Why Postgres, Not Redis

The natural question when building a distributed rate limiter is: why not Redis? Redis is built for exactly this, a Lua script with `ZSET` operations is the textbook solution, with sub-millisecond latency and 50,000+ operations per second.

The answer is straightforward: we already have Postgres. Adding Redis means a second service to run, monitor, back up, and pay for. For a system processing 15 requests per minute, 0.25 requests per second, the operational cost of Redis is not justified by any performance gain.

The concrete tradeoff:

| | Redis Lua + ZSET | Postgres advisory lock |
|---|---|---|
| Extra service | Redis server required | None, already have Postgres |
| Latency per check | ~0.5ms | ~3–8ms (NeonDB) |
| Max throughput | ~50,000 RPS | ~200–500 RPS |
| Atomicity mechanism | Lua script | Advisory lock + transaction |
| Audit trail | No, Redis is ephemeral | Yes, `rate_limit_window` is queryable |
| Cost | Additional service | Zero additional cost |

At 15 RPM the 3–8ms overhead is 0.005% of each request's time. It is not measurable. The audit trail, on the other hand, is genuinely useful, you can query `rate_limit_window` at any time to see exactly how many slots are in use and when they expire.

**When to reconsider Redis:** If throughput requirements grow to hundreds of requests per minute and the Postgres advisory lock becomes a bottleneck, profiling will show it clearly. At that point the migration to Redis is a single-file change in `rate_limiter.py`, the rest of the system is unchanged.

---

## 4. Architecture

### 4.1 Component map

```
python master.py --workers 5 --requests 75

  master.py  (one OS process)
  ├── setup_tables()         schema creation — runs once, here only
  ├── create_run()           inserts a row in `runs`, gets back a UUID run_id
  ├── enqueue_tasks(run_id)  inserts 75 rows in `tasks` tagged with run_id
  ├── spawn worker 1 --run-id <uuid> ──┐
  ├── spawn worker 2 --run-id <uuid> ──┤  independent OS processes
  ├── spawn worker 3 --run-id <uuid> ──┤  each with own event loop
  ├── spawn worker N --run-id <uuid> ──┘
  ├── await asyncio.gather(*[p.wait()])   non-blocking wait for all
  ├── finish_run(run_id)     records finished_at on the runs row
  └── log wall time + DB time

  worker.py  (×N, independent OS processes)
  ├── get_pool()             connectivity check only — no DDL
  ├── heartbeat_loop()       upserts (worker_id, run_id) every 5s
  └── main loop:
      ├── claim_loop_step()  ONE round trip:
      │     recover orphans whose worker heartbeat is stale
      │     check if all tasks are terminal
      │     claim next pending task via FOR UPDATE SKIP LOCKED
      └── process_task()
          ├── limiter.wait_if_needed()   Postgres advisory lock gate
          └── ask_gemini()               API call → mark_done / mark_failed
```

### 4.2 Database schema

```
runs
  run_id UUID PK, started_at, finished_at, worker_count, max_calls, window_secs, total_tasks

tasks
  task_id UUID PK, run_id FK→runs, prompt, status, worker_id,
  retries, wait_used, result, created_at, updated_at, claimed_at

events
  event_id UUID PK, run_id FK→runs, task_id FK→tasks,
  worker_id, event_type, detail, ts

rate_limit_window
  id UUID PK, key, ts

worker_heartbeat
  (worker_id, run_id) PK, last_seen
```

### 4.3 Task lifecycle

```
pending ──[claim_loop_step()]──► running ──[mark_done()]──────► done
                                    │
                                    ├──[mark_failed()]─────────► failed
                                    │
                                    └──[heartbeat gone > 15s]──► pending
                                       recover_stuck_tasks()
                                       inside claim_loop_step()
```

---

## 5. Key Design Decisions

### 5.1 `claimed_at` is immutable after claim

When a task is claimed, `claimed_at` is set to `NOW()` and never touched again. `updated_at` is touched by every subsequent operation — retries, state changes, etc.

This matters for the old timeout-based recovery: if we used `updated_at` to detect a stuck task, a task that retried several times before its worker crashed would keep resetting the clock. With `claimed_at` frozen at claim time, the age of the claim is always accurate.

Even though the current version uses heartbeat-based recovery (not `claimed_at`), keeping `claimed_at` immutable is still correct, it gives an accurate record of how long each task actually ran.

### 5.2 `is_complete()` counts everything unconditionally

An earlier version of `is_complete()` tried to ignore `running` rows older than N seconds, treating them as "probably orphaned." This created a race window: if `is_complete()` declared victory before `recover_stuck_tasks()` had actually reset the row, workers would exit with tasks still unprocessed.

The fix: `is_complete()` counts all `pending` and `running` rows with no exceptions. Recovery is `recover_stuck_tasks()`'s job. Completion checking is `is_complete()`'s job. They do not share any threshold or state.

### 5.3 Heartbeat PK is `(worker_id, run_id)`

Workers from different runs never overwrite each other's heartbeat. If run A and run B both have a `worker_id = "1"`, they get separate rows in `worker_heartbeat`. Without the `run_id` in the PK, worker 1 from run B would silently update the heartbeat row belonging to worker 1 from run A, making run A's worker appear alive when it might not be.

### 5.4 `setup_tables()` runs in master only

All DDL (`CREATE TABLE`, `ALTER TABLE`, `CREATE INDEX`) runs in `master.py` before any worker is spawned. Workers only call `get_pool()` to verify connectivity.

When five workers start simultaneously and all call `setup_tables()`, Postgres tries to update the same catalog rows for `ALTER TABLE ADD COLUMN IF NOT EXISTS` and `CREATE INDEX IF NOT EXISTS` in five concurrent transactions. Postgres throws `tuple concurrently updated` on its own internal catalog. Moving DDL to the master, which runs it once before spawning anyone, eliminates this entirely.

---

## 6. Case Studies — Where We Failed and How We Fixed It

These are the actual bugs encountered during development, in the order they appeared.

---

### Case Study 1 — Tasks stuck in pending, workers doing nothing

**What we saw:** Workers started, logged "no pending tasks", and spun forever. Every task stayed `pending` in the database. Nothing was being claimed.

**Why it happened:** Workers were calling `setup_tables()` on startup. When five workers start at the same millisecond, all five hit `ALTER TABLE ADD COLUMN IF NOT EXISTS` simultaneously. Postgres locks the same catalog row for each, five transactions competing for one row. Postgres throws and rolls back the losers. The workers caught the exception and returned early from `main()`, before ever reaching the claim loop.

**What we changed:** `setup_tables()` moved to `master.py` exclusively. Workers call `get_pool()` only. Schema is guaranteed to exist before the first worker spawns.

**The lesson:** DDL is not idempotent under concurrency, even with `IF NOT EXISTS`. Run it exactly once, from a single process, before spawning anything else.

---

### Case Study 2 — Stuck task recovery never firing (120 second timeout)

**What we saw:** When a worker crashed mid-task, the remaining workers would spin on "waiting for in-flight tasks" for up to two minutes before picking up the orphaned task.

**Why it happened:** The original `recover_stuck_tasks()` used `updated_at` to decide if a task was stuck, reset it if `updated_at < NOW() - 120 seconds`. But `increment_retries()` was also updating `updated_at` on every retry attempt. A task that had retried three times before its worker crashed would have a recent `updated_at`, so it would never age past the 120-second threshold.

**What we changed:** Added a `claimed_at` column, set once on claim and never touched again. `recover_stuck_tasks()` was rewritten to use `claimed_at`. The timeout was reduced from 120 seconds to 30 seconds, since `claimed_at` is now a stable reference. Later, the timeout was removed entirely in favour of heartbeat-based detection.

**The lesson:** Use a timestamp that reflects the thing you actually want to measure. `updated_at` measures "when was this row last modified", not "when did this worker claim this task."

---

### Case Study 3 — `is_complete()` race causing premature shutdown

**What we saw:** In the logs: worker 1 logged "all tasks complete — stopping", then worker 4 logged "recovered 1 stuck task", then worker 2 claimed and processed that task. Worker 1 had already exited before the task was done.

**Why it happened:** An earlier version of `is_complete()` accepted a `stuck_threshold_seconds` parameter and ignored `running` rows older than that threshold, treating them as "probably orphaned and about to be recovered." The idea was that `is_complete()` and `recover_stuck_tasks()` would stay in sync by sharing the same threshold.

In practice there was always a window between "`is_complete()` decides to ignore a running row" and "`recover_stuck_tasks()` actually resets it." In that window, `is_complete()` returned `True` and workers started exiting.

**What we changed:** `is_complete()` now counts every `pending` and `running` row unconditionally, with no threshold and no exceptions. `recover_stuck_tasks()` is the only function that makes decisions about stuck rows. The two functions no longer share any state.

**The lesson:** When two functions need to stay "in sync" via a shared constant, that's usually a sign one of them is doing the other's job. Give each function one clear responsibility and remove the coupling.

---

### Case Study 4 — SSL transport crash on second `asyncio.run()`

**What we saw:**
```
Fatal error on SSL transport
RuntimeError: Event loop is closed
```

This appeared after the workers finished, when `master.py` tried to query `run_duration()`.

**Why it happened:** The original `master.py` called `asyncio.run()` twice, once to enqueue tasks, once to fetch the run duration after workers finished. asyncpg opens SSL connections with background tasks tied to the event loop that created them. When the first `asyncio.run()` exits, it closes and destroys that loop. The background tasks then try to write on a closed socket.

The second `asyncio.run()` created a new loop, but the pool was still bound to the dead one. Even resetting `db._pool = None` didn't fully fix it, background SSL tasks from the first loop were still running.

**What we changed:** `master.py` became a single `async def main()` inside one `asyncio.run()`. The pool is created once and lives for the entire process lifetime. Workers are spawned with `asyncio.create_subprocess_exec()` which integrates cleanly with the event loop. The final `run_duration()` query uses the same pool and loop as everything else.

**The lesson:** asyncpg pools are married to the event loop that created them. Never call `asyncio.run()` more than once in a process that uses asyncpg.

---

### Case Study 5 — asyncpg stale cached statement plan

**What we saw:**
```
claim_batch failed: cached statement plan is invalid due to a database schema change
```

**Why it happened:** asyncpg caches prepared statements per connection for performance. When `setup_tables()` ran `ALTER TABLE ADD COLUMN`, Postgres invalidated the cached plan for any query touching the `tasks` table. The connection's cached plan was now stale, and the next query using it failed.

**What we changed:** Added `statement_cache_size=0` to `create_pool()`. This disables prepared statement caching, every query is planned fresh. At 15 RPM the performance cost is completely irrelevant.

**The lesson:** Disable statement caching on pools used in the same process as DDL operations. The cache is built for stable schemas.

---

### Case Study 6 — Slow-but-alive tasks incorrectly recovered (timeout approach)

**What we saw:** Tasks were being reset to `pending` and processed twice. In the logs: worker 3 logged `task=106 done`, then 10 seconds later worker 2 logged `claimed task=106`. The task ran twice and made two Gemini API calls.

**Why it happened:** Gemini calls sometimes take 35–43 seconds. The stuck task timeout was 30 seconds. A task that was legitimately running got its `claimed_at` past the 30-second threshold before it finished — `recover_stuck_tasks()` reset it to `pending` while the original worker was still processing it.

**What we changed:** Dropped the time-based approach entirely. Replaced with heartbeat-based detection: a task is orphaned only if the worker that owns it has stopped sending heartbeats. As long as a worker is alive, its tasks are safe, regardless of how long they run. The `worker_heartbeat` table records `(worker_id, run_id) → last_seen`, updated every 5 seconds.

**The lesson:** Time is the wrong signal for liveness. Heartbeats are the right signal. A 40-second task on an alive worker is not stuck. A 10-second task on a dead worker is.

---

### Case Study 7 — 5–6 second gap between task completion and next claim

**What we saw:** After measuring a run of 75 tasks, the DB time was 495 seconds against a theoretical minimum of 321 seconds (75 ÷ 14 RPM × 60). Inspecting the logs showed a consistent 5–6 second gap between every "task done" line and the next "claimed task" line.

**Why it happened:** The worker's main loop made three separate database calls every iteration — `recover_stuck_tasks()`, `is_complete()`, `claim_next_task()`. Each is a separate network round trip to NeonDB. NeonDB adds ~300–500ms of latency per call. Three calls per iteration × ~500ms each = ~1.5 seconds of overhead per task minimum, and with serialisation under load it accumulated to 5–6 seconds.

**What we changed:** Rewrote the three calls as a single CTE in `claim_loop_step()`. One query does recovery, completion check, and claim atomically, Postgres executes all three steps before sending one result set back. The per-iteration overhead dropped from ~1.5s to ~0.3–0.5s.

**The lesson:** Every async database call is a network round trip. Count your round trips, ask whether they can be one query.

---

## 7. Performance

### 7.1 Theoretical minimum

```
75 tasks ÷ 15 requests/minute = ~300 seconds
```

This is the hard floor set by the API rate limit. No engineering can go below it.


### 7.2 Why sequential is 2× slower

With one sequential worker, the event loop blocks on each Gemini call (5–40 seconds) before claiming the next rate limit slot. During those seconds, available slots expire unused. The window advances but nothing claims the open space.

With 3+ workers, one is always waiting at the rate limit gate. The moment a slot opens, it gets claimed. Idle time drops to near zero.

---

## 8. Observability

### 8.1 Events as audit log

Every state transition writes a row to `events`. This is permanent, persistent across worker restarts, and queryable at any time.

| `event_type` | Meaning |
|---|---|
| `started` | Task claimed by a worker |
| `success` | Task completed successfully |
| `dropped` | Task failed after all retries exhausted |
| `error` | Single attempt failed — retry will follow |
| `api_429` | API rate limit hit — not a failure, worker will sleep and retry |
| `rate_limited` | Worker waiting at the Postgres rate limit gate |

### 8.2 Dashboard

```bash
streamlit run dashboard.py
```

The sidebar lets you select any historical run or view all runs at once. Refresh interval is configurable: 1s / 3s / 5s / 10s. Panels shown: task status metrics, active workers per run, rate limit slot count, event breakdown, timeline chart (5s buckets), full task table, wait time chart, rolling event log, and an all-runs summary table.

### 8.3 Useful queries

```sql
-- All runs with timing
SELECT run_id, started_at, finished_at,
       EXTRACT(EPOCH FROM (finished_at - started_at)) AS seconds,
       total_tasks, worker_count
FROM runs ORDER BY started_at DESC;

-- Per-worker throughput for a specific run
SELECT worker_id,
    COUNT(*) FILTER (WHERE event_type = 'success') AS done,
    COUNT(*) FILTER (WHERE event_type = 'dropped') AS failed
FROM events WHERE run_id = '<uuid>'
GROUP BY worker_id;

-- Tasks still running with their owning worker
SELECT task_id, worker_id, claimed_at, NOW() - claimed_at AS age
FROM tasks WHERE run_id = '<uuid>' AND status = 'running';

-- Is any worker heartbeat stale?
SELECT worker_id, last_seen, NOW() - last_seen AS silence
FROM worker_heartbeat WHERE run_id = '<uuid>'
ORDER BY last_seen;

-- Rate limit slots currently in use
SELECT COUNT(*), MIN(ts), MAX(ts) FROM rate_limit_window;
```

---

## 9. Running the System

### 9.1 Setup

```bash
pip install asyncpg google-genai python-dotenv streamlit
```

```bash
# .env
DATABASE_URL=postgresql://user:pass@host/dbname
GEMINI_API_KEY=your-key-here
```

### 9.2 Run

```bash
# 5 workers, 75 tasks, default 15 RPM
python master.py --workers 5 --requests 75

# Custom rate limit
python master.py --workers 5 --requests 75 --max-calls 14 --window 60

# Live dashboard in a separate terminal
streamlit run dashboard.py
```

### 9.3 CLI reference

| Flag | Default | Description |
|---|---|---|
| `--workers` | `3` | Number of parallel worker OS processes |
| `--requests` | `75` | Number of tasks to enqueue |
| `--max-calls` | `15` | API calls allowed per window |
| `--window` | `60` | Window size in seconds |
| `--retries` | `3` | Max retries on real errors (429s don't count) |
| `--rate-key` | `gemini:rpm` | Shared rate limit key in Postgres |

---

## 10. What Comes Next

**Pub/sub for idle workers.** Right now, when a worker has no pending tasks to claim, it polls every 1 second until either a task becomes available or the run ends. This is unnecessary network traffic. The correct solution is for `mark_done()` to publish a notification on a Postgres channel (`NOTIFY`), and idle workers to `LISTEN` for it, waking instantly when work becomes available instead of polling.

**Redis rate limiter (when justified by profiling).** If throughput requirements grow significantly and the Postgres advisory lock shows up as a bottleneck in profiling, migrating the rate limiter to Redis is a single-file change in `rate_limiter.py`. The audit trail requirement would mean keeping a lightweight event log in Postgres even if the rate limiter moves to Redis. The migration should be driven by measured evidence, not assumption.

**Per-task timeout configuration.** The current heartbeat dead threshold (15 seconds) applies globally. Some task types might legitimately run for 60+ seconds; others should fail fast. A `timeout_seconds` column on `tasks` would let callers configure this per task type.