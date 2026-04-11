# Async Upgrade

## [What the original sync version had](../queue-based-solution-sync/README.md)

The sync implementation covered the core resilience logic correctly:
- Sliding window rate limiter using a `deque`
- Two-lane retry: 429 never burns a retry, real errors do
- Exponential backoff: `2^n` seconds between real failure retries
- Sequential, single-process, in-memory only

---

## Limitations solved in this async version

### 6.1 No true concurrency → Solved

The original blocked the entire program on every `time.sleep()` and every API call. Nothing else could run while one request was in flight.

The async version fixes this with three changes:

- `client.aio.models.generate_content()`  the Gemini call no longer blocks. While one task waits for a network response, the event loop runs other tasks.
- `await asyncio.sleep()`  rate limit waits and backoff sleeps yield control instead of freezing the process.
- `asyncio.gather()`  all tasks are launched concurrently. The rate limiter gates how many actually fire, but the waiting tasks don't block each other.

Result: 20 tasks at 8s each that previously took ~160s now finish in ~75s (one window of 15, then the remaining 5).

### Race condition introduced by concurrency → Solved

Concurrency created a new problem the sync version never had. Two coroutines could both read the deque, both see a free slot, and both claim it  exceeding the rate limit.
```
Coroutine A: checks deque → count=14 → "ok, slot free"
                                            ↕ event loop switches
Coroutine B: checks deque → count=14 → "ok, slot free"  ← BOTH saw 14!
Coroutine A: appends timestamp → count=15
Coroutine B: appends timestamp → count=16  ← exceeded limit
```
Fixed with `asyncio.Lock()`. Only one coroutine can read and write the deque at a time. Critically, the lock is released before `asyncio.sleep()` so waiting coroutines don't block each other while sleeping.

---
## Timeline of execution for 20 requests

```
t=0        t=8-14s         t=60s      t=62-71s
│          │               │          │
├──[1-15 API calls]────────┤          │
│          └──[done]       │          │
├──[16-20 sleeping ~58s]───┤          │
│                          └──[16-20 API calls]──┤
                                                done
```

The two solved limitations are directly related  concurrency required the lock as a consequence. The four remaining limitations all require external infrastructure (a database, Redis, a metrics system) and are the natural next step if this script needs to run reliably in production.