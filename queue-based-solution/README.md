# Rate Limit Handler - Queue + Exponential Backoff Solution

## Overview

This document covers the design decisions, algorithmic reasoning, and trade-offs behind the Rate Limit Handler. It explains why each component was chosen, how they work together, and where the current implementation has limitations, and how those limitations can be overcome in production.

---

## 1. The Problem

Every external API enforces a rate limit, a ceiling on how many requests a client can make within a time window. When you exceed it, the server responds with a `429 Too Many Requests` (or in GitHub's case, `403 Forbidden`) and stops serving you until the window resets.

Without a handler, your code behaves like this:

```
Request 1  ✓
Request 2  ✓
Request 3  ✓
Request 4  → 429 → unhandled exception → 💥 program crashes
```

The goal of this handler is to make your program **never crash due to rate limits**. It should slow itself down gracefully, recover from failures automatically, and give you full visibility into what's happening.

---

## 2. Why a Sliding Window?

### The alternative: Fixed Window

The simplest approach is a fixed window, reset a counter every N seconds:

```
Window 1: [0s – 60s]   → allow 5 requests, then block
Window 2: [60s – 120s] → reset, allow 5 more
```

This has a critical flaw called the **boundary burst problem**. A client can make 5 requests at t=59s and 5 more at t=61s, 10 requests in 2 seconds, without ever technically violating the rule, because each falls in a different window.

### Why sliding window instead

A sliding window doesn't reset on a fixed clock. It looks backward from the current moment: *how many requests have been made in the last N seconds?*

```
t=0s   task_1 fires  → stamps t=0   → window: [0]          (1/5)
t=7s   task_2 fires  → stamps t=7   → window: [0, 7]        (2/5)
t=14s  task_3 fires  → stamps t=14  → window: [0, 7, 14]    (3/5)
t=21s  task_4 fires  → stamps t=21  → window: [0, 7, 14, 21](4/5)
t=28s  task_5 fires  → stamps t=28  → window: [0,7,14,21,28](5/5)
t=35s  task_6 wants to fire
       → oldest stamp is t=0
       → t=0 expires at t=0+60=60s
       → must wait 60-35 = 25s
       → sleeps 25s, fires at t=60
       → window: [7,14,21,28,60], t=0 naturally evicted
```

This enforces a true continuous rate, you can never burst across a boundary.

### The implementation

```python
def wait_if_needed(self):
    current_time = time.monotonic()

    # Evict timestamps that have fallen outside the window
    while self.tasks and self.tasks[0]["timestamp"] < current_time - self.window:
        self.tasks.popleft()

    # If window is full, sleep exactly until the oldest slot expires
    if len(self.tasks) >= self.max_calls:
        wait_time = self.tasks[0]["timestamp"] + self.window - current_time
        time.sleep(max(wait_time, 0))
```

`time.monotonic()` is used instead of `time.time()` because it is immune to system clock changes (NTP sync, daylight savings, manual adjustments). It always moves forward.

`max(wait_time, 0)` guards against a negative sleep value caused by floating point imprecision in edge cases, `time.sleep()` would raise an error on a negative input.

---

## 3. Why Exponential Backoff?

### The naive approach: fixed retry delay

```python
time.sleep(2)  # always wait 2 seconds between retries
```

This fails under load. If 100 clients all fail at the same time and all retry after exactly 2 seconds, they all hit the server simultaneously again, causing another wave of failures. This is called a **retry storm**.

### Exponential backoff

Each retry waits twice as long as the previous:

```
Attempt 1 fails → wait 2s   (2^1)
Attempt 2 fails → wait 4s   (2^2)
Attempt 3 fails → wait 8s   (2^3)
Attempt 4 fails → wait 16s  (2^4)
Attempt 5 fails → wait 32s  (2^5)
```

This does two things. First, it gives the server increasing amounts of time to recover. Second, it naturally spreads retries out over time so clients don't all collide.

### The two-lane retry logic

A key design decision is separating server-side rate limits from application errors:

```python
except TooManyRequestsError as e:
    # Server said wait, trust it, don't count as a retry
    time.sleep(e.retry_after)

except Exception as e:
    # Application error, apply backoff, count against retries
    retries += 1
    time.sleep(2 ** retries)
```

A `429` from the server means "you're fine, just slow down." It shouldn't consume one of your retry attempts because the request hasn't actually failed, it hasn't even been processed yet. Application errors (500s, timeouts, parsing failures) are real failures and do count.

---

## 4. Why These Two Together?

The rate limiter and retry handler solve different problems:

| Component | Prevents | Mechanism |
|---|---|---|
| Sliding window | Sending too many requests | Sleeps before firing |
| Exponential backoff | Crashing on transient failures | Sleeps after failing |

Without the rate limiter, retries could themselves trigger more rate limit errors. Without retries, a single transient failure drops a request permanently. Together they form a complete resilience layer:

```
Request arrives
      │
      ▼
wait_if_needed()     ← are we going too fast? slow down first
      │
      ▼
attempt the call
      ├─ 200 OK      → stamp timestamp, return result
      ├─ 429         → sleep Retry-After, loop back (no retry count)
      ├─ 5xx/timeout → sleep 2^n, increment retry count, loop back
      └─ retries exhausted → raise, caught by outer for loop
```

---

## 5. Decorator Design

The decorators are applied in a specific order that matters:

```python
@rate_limiter.rate_limited       # outer
@rate_limiter.retry_on_failure   # inner
def ask_gemini(...):
    ...
```

Python applies decorators bottom-up at definition time, but they execute top-down at call time. So `rate_limited` runs first (checks the window), then `retry_on_failure` runs (manages the actual call attempts). Reversing them would mean retries could fire without checking the rate limit, potentially sending a burst of retries that all violate the window.

---

## 6. Limitations of This Implementation

This is a **synchronous, single-process** implementation. It works well for scripting, small batch jobs, and learning, but has real constraints in production environments.

### 6.1 No true concurrency

Every request blocks the entire program. While one request is waiting for a response, nothing else can run. This is the fundamental constraint of synchronous Python.

**Impact:** For 30 requests at 8 seconds each, total runtime is ~240 seconds. A concurrent version could run multiple requests in parallel and finish in a fraction of the time.

**Overcome with:** `asyncio` + `aiohttp` for async I/O, or `concurrent.futures.ThreadPoolExecutor` for thread-based concurrency. The rate limiter's `deque` would need a `threading.Lock()` to be thread-safe.

### 6.2 In-memory state only

The task queue lives in memory. If the process crashes, all pending tasks and timing state are lost.

**Impact:** In a long batch job, a crash mid-run means you don't know which tasks completed, and you may re-run or skip tasks on restart.

**Overcome with:** Persisting task state to a database (SQLite for simple cases, PostgreSQL for production), a message queue (Redis, RabbitMQ), or a task queue framework like Celery.

### 6.3 Single-process rate limit only

The sliding window is local to one process. If you run two instances of the script simultaneously, each has its own window, together they could send twice the allowed requests.

**Impact:** Running parallel workers or deploying to multiple machines breaks the rate limit guarantee entirely.

**Overcome with:** A shared, atomic counter in Redis using the `INCR` + `EXPIRE` pattern, or a distributed rate limiting service. Redis's atomic operations make it safe for concurrent writers.

### 6.4 No `Retry-After` header parsing from Gemini

The Gemini SDK raises a `ClientError` without exposing a `Retry-After` duration directly. The current implementation defaults to 60 seconds when a 429 is caught.

**Impact:** You may wait longer than necessary (if the actual reset is in 10 seconds) or occasionally not long enough.

**Overcome with:** Parsing the error response body or headers from the `ClientError` exception to extract the exact retry delay if the SDK exposes it.

### 6.5 No observability

Logs are written to stdout only. There's no metrics export, no alerting, and no historical record.

**Impact:** You can't build dashboards, set alerts on failure rates, or analyze trends over time.

**Overcome with:** Structured logging (JSON format) to a log aggregator (Datadog, Grafana Loki), or exporting counters to a metrics system (Prometheus, StatsD).

---

## 7. When This Implementation Is the Right Choice

Despite its limitations, this implementation is the correct choice when:

- You're running a **single sequential batch job** (scripts, data pipelines, one-off tasks)
- You want **zero infrastructure dependencies**, no Redis, no databases, no message queues
- You need **simplicity and debuggability**, the entire logic is in one file, easy to trace
- The **volume is low enough** that sequential execution finishes in an acceptable time

For anything requiring real concurrency, multi-process safety, or production reliability, the architecture above scales up naturally, the core logic (sliding window + exponential backoff + two-lane retry) stays identical. Only the infrastructure around it changes.

---
