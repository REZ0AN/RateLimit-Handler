# Async Persistent Tasks Queue with Postgres

## [Async Only Solution](../queue-based-solution-async/README.md)

The async version solved concurrency and the deque race condition, but all task state still lived in memory. A crash mid-run meant losing everything — no record of what finished, no way to resume.

This version adds a NeonDB (Postgres) state log alongside the async rate limiter, giving the script a persistent audit trail without changing any of the core rate limiting or retry logic.

---

## What this version adds

### Two files

**`db.py`** — all database interaction in one place. Responsible for:
- Creating the `tasks` and `events` tables on first run
- Inserting prompts as `pending` tasks before execution starts
- Transitioning task state: `pending → running → done / failed`
- Logging every meaningful event with a server-side timestamp

**`handler.py`** — the main script, unchanged in structure from the async version. It calls `db.*` functions at state transition points but the rate limiter, retry logic, and backoff are identical.

---

## Limitations solved in this version

### 6.2 In-memory state only → Solved

Every task is written to Postgres before execution begins. At every transition point the DB is updated:

```
enqueue()       → status = pending    (before anything runs)
mark_running()  → status = running    (coroutine claims the task)
mark_done()     → status = done       (Gemini responded successfully)
mark_failed()   → status = failed     (max retries exhausted)
add_wait()      → wait_used += Ns     (seconds lost to rate limiting)
log_event()     → events row inserted (full audit trail)
```

If the process crashes at any point, the table reflects exactly where each task was. On restart you can query `WHERE status != 'done'` and resume from that point without re-running completed work.

---

## Case Study - Implementation for Persistent Tasks Queue

### Problem 1 — Invalid state transitions

The first version of `mark_done` was an unguarded UPDATE:

```sql
-- wrong: allows done → running, failed → done, any invalid jump
UPDATE tasks SET status='done' WHERE task_id=$1
```

Any code bug or retry re-entry could corrupt the state machine. Fixed by adding a state guard to every transition:

```sql
-- correct: only valid transition is applied, invalid ones are silently ignored
UPDATE tasks SET status='done'
WHERE task_id=$1 AND status='running'
```

This means the DB enforces the state machine, not just the application code.

### Problem 2 — Double execution / re-entry race

If a coroutine retried and accidentally called `ask_gemini` with the same `task_id`, two coroutines would write to the same row simultaneously. Fixed by making `mark_running` atomic using `FOR UPDATE SKIP LOCKED`:

```sql
SELECT task_id FROM tasks
WHERE task_id = $1 AND status = 'pending'
FOR UPDATE SKIP LOCKED
```

`FOR UPDATE` locks the row for the duration of the transaction. `SKIP LOCKED` means any other caller that tries to claim the same row will get nothing back instead of waiting — it moves on rather than blocking. `mark_running` returns `False` if the row was already claimed, and the coroutine exits cleanly without writing anything.

This also makes the system safe for multi-process execution as a side effect — two separate Python processes running simultaneously will never claim the same task.

### Problem 3 — Thundering herd at the window boundary

When 24 tasks run against a 15 RPM limit, the first 15 fire at `t=0` and finish at staggered times between `t=8–14s`. The remaining 9 all call `await asyncio.sleep(~60s)` at the exact same moment, so they all wake at the exact same moment too.

At `t=60s` this creates a burst:

```
9 rate-limited tasks wake simultaneously:
  → add_wait()            9 DB writes
  → log_event()           9 DB writes

15 first-batch tasks finishing:
  → mark_done()           15 DB writes
  → log_event("success")  15 DB writes

total: 48 concurrent DB operations
pool max_size was 10 → 38 operations dropped silently
```

The result was that `success` events were missing specifically for the rate-limited tasks — they hit the DB at the collision point, not at the staggered times the first-batch tasks had.

Fixed with `asyncio.Semaphore(10)` wrapping every DB call:

```python
async def log_event(...):
    async with get_semaphore():   # max 10 concurrent DB ops
        async with pool.acquire(timeout=10) as conn:
            await conn.execute(...)
```

The semaphore turns the thundering herd into an orderly queue. At `t=60s`, 10 DB operations proceed and 38 wait outside the semaphore. As each finishes, the next enters. All 48 complete with nothing dropped.

**Performance cost:** Each DB operation takes ~5ms on NeonDB. With 48 operations in groups of 10, the extra wait is roughly 5 rounds × 5ms = ~25ms. On a script that takes ~75 seconds total, this is 0.03% overhead — not measurable in practice.

---
