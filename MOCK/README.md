# Rate Limit Handler

A lightweight, synchronous rate limiter for Python that ensures your code never breaks when hitting API rate limits. It quietly waits, retries, and recovers — all without crashing your program.

---

## The Problem

Most APIs enforce a limit on how many requests you can make in a given time window. If you exceed it, they return a `429 Too Many Requests` error. Without a handler, your code just crashes.

```
Request 1 ✓
Request 2 ✓
Request 3 ✓
Request 4 → 429 Too Many Requests → 💥 crash
```

This script solves that by sitting in front of every request and managing the timing for you.

---

## How It Works

There are two independent mechanisms working together.

### 1. Sliding Window Rate Limiter

Every time a request completes, its timestamp gets recorded in a queue. Before the next request is allowed through, the limiter checks: *how many requests have been made in the last N seconds?*

```
window = 5 seconds, max_calls = 3

t=0s   task_1 ──── completes ──── window: [task_1]         (1/3)
t=1s   task_2 ──── completes ──── window: [task_1, task_2] (2/3)
t=2s   task_3 ──── completes ──── window: [task_1, task_2, task_3] (3/3)
t=3s   task_4 ──── BLOCKED ─────  window full!
                   waits 2s ────── task_1 expires at t=5s
t=5s   task_4 ──── completes ──── window: [task_3, task_4] (2/3)
```

The key is that it doesn't reset on a fixed clock. It calculates the **exact** time until the oldest slot expires and sleeps only that long. That's what makes it a *sliding* window — it moves with time rather than resetting every N seconds.

```python
# The wait is calculated precisely:
wait_time = oldest_timestamp + window - current_time
```

### 2. Retry with Exponential Backoff

When a request fails (network error, server error, etc.), the handler doesn't give up immediately. It retries up to `max_retries` times, waiting longer between each attempt:

```
Attempt 1 fails → wait 2s  (2^1)
Attempt 2 fails → wait 4s  (2^2)
Attempt 3 fails → wait 8s  (2^3)
→ Max retries reached. Drop the request.
```

The increasing delay prevents hammering a server that's already struggling, and gives it time to recover.

---

## Code Structure

```
handler.py
│
├── logger()              Helper to log INFO and ERROR messages
│
├── RateLimiter           Core class
│   ├── __init__()        Sets up max_calls, window, max_retries, and the task queue
│   ├── wait_if_needed()  Checks the window and sleeps if limit is reached
│   ├── add_task()        Stamps a timestamp after a successful request
│   ├── @rate_limited     Decorator: runs wait_if_needed() before every call
│   └── @retry_on_failure Decorator: wraps a call with retry + backoff logic
│
├── parse_args()          CLI argument parser
└── main()                Wires everything together and runs the simulation
```

### Decorator order matters

```python
@rate_limiter.rate_limited       # ← runs first: checks the window
@rate_limiter.retry_on_failure   # ← runs second: handles failures
def make_request():
    ...
```

The execution order for every call is:

```
make_request() called
       │
       ▼
wait_if_needed()      block here if window is full
       │
       ▼
retry_on_failure()    attempt the call
       ├── success → add_task() stamps timestamp → return result
       ├── fail    → wait 2^n seconds → retry
       └── fail    → (retries exhausted) → raise → caught in for loop
```

---

## Running It

```bash
# Install dependencies (only needed for the real API version)
pip install requests

# Run with defaults: 5 calls / 10s window / 3 retries / 10 requests
python handler.py

# Tighter limit to see rate limiting in action
python handler.py --max-calls 3 --window 5 --requests 20
```

### CLI Options

| Flag | Default | Description |
|---|---|---|
| `--max-calls` | `5` | Max requests allowed per window |
| `--window` | `10` | Window size in seconds |
| `--max-retries` | `3` | Retry attempts before dropping a request |
| `--requests` | `10` | Total requests to simulate |

---

## Reading the Output

Each completed task prints a timing summary:

```
[t=8.18s] task_4 | waited=2.22s | exec=1.55s | total=3.76s | window=[task_3, task_4] (2/3)
```

| Field | Meaning |
|---|---|
| `t=` | Seconds elapsed since program started |
| `waited=` | Time spent blocked by the rate limiter |
| `exec=` | Actual request execution time |
| `total=` | `waited + exec` |
| `window=[...]` | Tasks currently alive inside the sliding window |
| `(n/max)` | How full the window is right now |

When `waited > 0`, the rate limiter kicked in. When tasks disappear from `window=[...]`, the sliding window expired their slots and freed up capacity.

---

## Real Output Sample

```
[INFO] - Starting with max_calls=3, window=5s, max_retries=3, total_requests=20
[INFO] - --- Request 1/20 ---
[INFO] - Request successful! Task ID: task_1
[INFO] - [t=1.63s] task_1 | waited=0.00s | exec=1.63s | total=1.63s | window=[task_1] (1/3)
[INFO] - --- Request 4/20 ---
[INFO] - Rate limit exceeded. Waiting for 2.21s... [window has 3/3 tasks]
[INFO] - Request successful! Task ID: task_4
[INFO] - [t=8.18s] task_4 | waited=2.22s | exec=1.55s | total=3.76s | window=[task_3, task_4] (2/3)
[ERROR] - Error: Simulated request failure. Retrying... task_5 (1/3)
[ERROR] - Error: Simulated request failure. Retrying... task_5 (2/3)
[INFO] - Request successful! Task ID: task_5
[INFO] - [t=19.09s] task_5 | waited=0.00s | exec=10.91s | total=10.91s | window=[task_5] (1/3)
```

---

## Limitations

This is a mock/learning script. A production version would additionally need:

- **`429` header handling** — real APIs return a `Retry-After` header telling you exactly how long to wait
- **Thread safety** — the task queue needs a lock if requests run concurrently
- **Persistent queue** — tasks are lost if the script crashes mid-run