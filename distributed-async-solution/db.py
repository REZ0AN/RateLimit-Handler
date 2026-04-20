"""
db.py — database layer (Stage 7 revised: single-round-trip claim loop).

Changes vs previous Stage 7
-----------------------------
1. claim_loop_step() — NEW
   Replaces three separate calls (recover_stuck_tasks, is_complete,
   claim_next_task) with a single CTE that does all three in one round trip.
   At NeonDB latency (~200-500ms per call), three calls = 600-1500ms overhead
   per task. One call = ~200-500ms. This is the main source of the 5-6s gap
   between task done and next task claimed.

2. rate_limiter signatures fixed for Stage 7
   log_event and add_wait_time now accept (run_id, task_id, ...) with UUID
   strings, matching the Stage 7 schema. The old signatures caused silent
   failures whenever the window was full.

3. recover_stuck_tasks() and is_complete() kept for compatibility but
   worker.py no longer calls them in the main loop — claim_loop_step() handles
   both. They are still useful for diagnostics and dashboard queries.
"""

import os
import uuid
import asyncio
import asyncpg
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

_pool: asyncpg.Pool | None = None
_pool_lock = asyncio.Lock()

HEARTBEAT_DEAD_SECONDS = 15.0


# ---------------------------------------------------------------------------
# Connection pool
# ---------------------------------------------------------------------------

async def get_pool() -> asyncpg.Pool:
    global _pool
    async with _pool_lock:
        if _pool is None:
            _pool = await asyncpg.create_pool(
                DATABASE_URL,
                min_size=2,
                max_size=10,
                statement_cache_size=0,
            )
    return _pool


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

async def setup_tables():
    pool = await get_pool()
    async with pool.acquire() as conn:

        await conn.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto")

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS runs (
                run_id        UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
                started_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
                finished_at   TIMESTAMPTZ,
                worker_count  INT,
                max_calls     INT,
                window_secs   INT,
                total_tasks   INT
            )
        """)

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS tasks (
                task_id    UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
                run_id     UUID         NOT NULL REFERENCES runs(run_id),
                prompt     TEXT         NOT NULL,
                status     TEXT         NOT NULL DEFAULT 'pending',
                worker_id  TEXT,
                retries    INT          NOT NULL DEFAULT 0,
                wait_used  FLOAT        NOT NULL DEFAULT 0,
                result     TEXT,
                created_at TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
                claimed_at TIMESTAMPTZ
            )
        """)
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_tasks_run_status
            ON tasks (run_id, status)
        """)
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_tasks_run_worker
            ON tasks (run_id, worker_id)
        """)

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS events (
                event_id   UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
                run_id     UUID         NOT NULL REFERENCES runs(run_id),
                task_id    UUID         NOT NULL REFERENCES tasks(task_id),
                worker_id  TEXT,
                event_type TEXT         NOT NULL,
                detail     TEXT,
                ts         TIMESTAMPTZ  NOT NULL DEFAULT NOW()
            )
        """)
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_events_run_task
            ON events (run_id, task_id)
        """)
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_events_run_ts
            ON events (run_id, ts DESC)
        """)

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS rate_limit_window (
                id   UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
                key  TEXT         NOT NULL,
                ts   TIMESTAMPTZ  NOT NULL DEFAULT NOW()
            )
        """)
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_rlw_key_ts
            ON rate_limit_window (key, ts)
        """)

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS worker_heartbeat (
                worker_id  TEXT         NOT NULL,
                run_id     UUID         NOT NULL REFERENCES runs(run_id),
                last_seen  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
                PRIMARY KEY (worker_id, run_id)
            )
        """)

        await conn.execute("""
            CREATE OR REPLACE FUNCTION try_acquire_slot(
                p_key            TEXT,
                p_max_calls      INT,
                p_window_seconds INT
            ) RETURNS BOOLEAN AS $$
            DECLARE
                v_count INT;
            BEGIN
                PERFORM pg_advisory_xact_lock(hashtext(p_key));

                DELETE FROM rate_limit_window
                WHERE  key = p_key
                  AND  ts  < NOW() - make_interval(secs => p_window_seconds);

                SELECT COUNT(*) INTO v_count
                FROM   rate_limit_window
                WHERE  key = p_key;

                IF v_count < p_max_calls THEN
                    INSERT INTO rate_limit_window (key) VALUES (p_key);
                    RETURN TRUE;
                ELSE
                    RETURN FALSE;
                END IF;
            END;
            $$ LANGUAGE plpgsql;
        """)


# ---------------------------------------------------------------------------
# Run lifecycle
# ---------------------------------------------------------------------------

async def create_run(worker_count: int, max_calls: int, window_secs: int) -> str:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            INSERT INTO runs (worker_count, max_calls, window_secs)
            VALUES ($1, $2, $3)
            RETURNING run_id
        """, worker_count, max_calls, window_secs)
    return str(row["run_id"])


async def finish_run(run_id: str, total_tasks: int):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("""
            UPDATE runs
            SET    finished_at = NOW(),
                   total_tasks = $2
            WHERE  run_id = $1
        """, uuid.UUID(run_id), total_tasks)


# ---------------------------------------------------------------------------
# Task enqueue
# ---------------------------------------------------------------------------

async def enqueue_tasks(run_id: str, prompts: list[str]) -> list[str]:
    pool = await get_pool()
    rid  = uuid.UUID(run_id)
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            INSERT INTO tasks (run_id, prompt)
            SELECT $1, unnest($2::text[])
            RETURNING task_id
        """, rid, prompts)
    return [str(r["task_id"]) for r in rows]


# ---------------------------------------------------------------------------
# Single-round-trip claim loop step  ← KEY CHANGE
# ---------------------------------------------------------------------------

async def claim_loop_step(run_id: str, worker_id: str) -> dict:
    """
    Perform recover + completion-check + claim in a single Postgres round trip.

    Why one round trip matters
    --------------------------
    The previous design called three separate async functions in the worker
    loop: recover_stuck_tasks(), is_complete(), claim_next_task(). Each is a
    separate network call to NeonDB (~200-500ms each). With 75 tasks that's
    3 × 75 = 225 extra round trips = 45-112 seconds of pure network overhead,
    which directly explains the 5-6 second gap observed between task completion
    and next claim.

    This function issues one query that does all three as a single CTE:

      Step 1 — recover: reset tasks whose worker heartbeat has gone silent.
      Step 2 — check:   count remaining non-terminal tasks.
      Step 3 — claim:   atomically claim the next pending task if any exist.

    Return value
    ------------
    {
        "recovered": int,       # tasks reset to pending (0 in normal operation)
        "incomplete": int,      # pending + running tasks remaining (0 = done)
        "task_id": str | None,  # UUID of claimed task, None if nothing to claim
        "prompt": str | None,   # prompt of claimed task
    }

    The caller (worker main loop) decides what to do:
      - recovered > 0  → log warning
      - incomplete == 0 → all tasks terminal → shutdown
      - task_id is None → another worker has last tasks in flight → sleep 1s
      - task_id is set  → process the task
    """
    pool = await get_pool()
    rid  = uuid.UUID(run_id)

    async with pool.acquire() as conn:
        async with conn.transaction():
            row = await conn.fetchrow("""
                WITH
                -- Step 1: recover tasks whose worker heartbeat has gone silent
                recovered AS (
                    UPDATE tasks t
                    SET    status     = 'pending',
                           worker_id  = NULL,
                           claimed_at = NULL,
                           updated_at = NOW()
                    WHERE  t.run_id = $1
                      AND  t.status = 'running'
                      AND  NOT EXISTS (
                               SELECT 1
                               FROM   worker_heartbeat h
                               WHERE  h.worker_id = t.worker_id
                                 AND  h.run_id    = $1
                                 AND  h.last_seen > NOW() - make_interval(secs => $3)
                           )
                    RETURNING task_id
                ),

                -- Step 2: count tasks still in non-terminal state
                -- (runs after recovery so recovered tasks appear as pending)
                completion AS (
                    SELECT COUNT(*) AS incomplete
                    FROM   tasks
                    WHERE  run_id = $1
                      AND  status IN ('pending', 'running')
                ),

                -- Step 3: claim the next available pending task
                claimed AS (
                    UPDATE tasks
                    SET    status     = 'running',
                           worker_id  = $2,
                           claimed_at = NOW(),
                           updated_at = NOW()
                    WHERE  task_id = (
                        SELECT task_id
                        FROM   tasks
                        WHERE  run_id = $1
                          AND  status = 'pending'
                        ORDER  BY created_at
                        FOR UPDATE SKIP LOCKED
                        LIMIT  1
                    )
                    RETURNING task_id, prompt
                )

                SELECT
                    (SELECT COUNT(*)   FROM recovered)   AS recovered_count,
                    (SELECT incomplete FROM completion)  AS incomplete_count,
                    (SELECT task_id    FROM claimed)     AS task_id,
                    (SELECT prompt     FROM claimed)     AS prompt
            """, rid, worker_id, HEARTBEAT_DEAD_SECONDS)

    task_id = str(row["task_id"]) if row["task_id"] else None
    prompt  = row["prompt"]

    # Log the claim event inside a separate connection (outside the CTE
    # transaction which is already committed at this point).
    if task_id:
        await log_event(run_id, task_id, worker_id, "started", None)

    return {
        "recovered":  int(row["recovered_count"] or 0),
        "incomplete": int(row["incomplete_count"] or 0),
        "task_id":    task_id,
        "prompt":     prompt,
    }


# ---------------------------------------------------------------------------
# Task lifecycle
# ---------------------------------------------------------------------------

async def mark_done(task_id: str, run_id: str, worker_id: str, result: str):
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute("""
                UPDATE tasks
                SET    status     = 'done',
                       result     = $2,
                       worker_id  = NULL,
                       updated_at = NOW()
                WHERE  task_id = $1
                  AND  status  = 'running'
            """, uuid.UUID(task_id), result)
            await _log_event(conn, run_id, task_id, worker_id, "success", result[:200])


async def mark_failed(task_id: str, run_id: str, worker_id: str, reason: str):
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute("""
                UPDATE tasks
                SET    status     = 'failed',
                       worker_id  = NULL,
                       updated_at = NOW()
                WHERE  task_id = $1
                  AND  status  = 'running'
            """, uuid.UUID(task_id))
            await _log_event(conn, run_id, task_id, worker_id, "dropped", reason)


async def increment_retries(task_id: str):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("""
            UPDATE tasks
            SET    retries    = retries + 1,
                   updated_at = NOW()
            WHERE  task_id = $1
        """, uuid.UUID(task_id))


async def add_wait_time(run_id: str, task_id: str, seconds: float):
    """Updated signature: run_id + task_id (both needed for Stage 7 context)."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("""
            UPDATE tasks
            SET    wait_used  = wait_used + $2,
                   updated_at = NOW()
            WHERE  task_id = $1
        """, uuid.UUID(task_id), seconds)


# ---------------------------------------------------------------------------
# Event logging
# ---------------------------------------------------------------------------

async def _log_event(
    conn:       asyncpg.Connection,
    run_id:     str,
    task_id:    str,
    worker_id:  str,
    event_type: str,
    detail:     str | None,
):
    await conn.execute("""
        INSERT INTO events (run_id, task_id, worker_id, event_type, detail)
        VALUES ($1, $2, $3, $4, $5)
    """, uuid.UUID(run_id), uuid.UUID(task_id), worker_id, event_type, detail)


async def log_event(
    run_id:     str,
    task_id:    str,
    worker_id:  str,
    event_type: str,
    detail:     str | None,
):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await _log_event(conn, run_id, task_id, worker_id, event_type, detail)


# ---------------------------------------------------------------------------
# Worker heartbeat
# ---------------------------------------------------------------------------

async def upsert_heartbeat(worker_id: str, run_id: str):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO worker_heartbeat (worker_id, run_id, last_seen)
            VALUES ($1, $2, NOW())
            ON CONFLICT (worker_id, run_id) DO UPDATE
            SET last_seen = NOW()
        """, worker_id, uuid.UUID(run_id))


# ---------------------------------------------------------------------------
# Kept for diagnostics / dashboard — not called in the main worker loop
# ---------------------------------------------------------------------------

async def recover_stuck_tasks(run_id: str) -> int:
    pool = await get_pool()
    rid  = uuid.UUID(run_id)
    async with pool.acquire() as conn:
        result = await conn.fetchval("""
            WITH recovered AS (
                UPDATE tasks t
                SET    status     = 'pending',
                       worker_id  = NULL,
                       claimed_at = NULL,
                       updated_at = NOW()
                WHERE  t.run_id = $1
                  AND  t.status = 'running'
                  AND  NOT EXISTS (
                           SELECT 1
                           FROM   worker_heartbeat h
                           WHERE  h.worker_id = t.worker_id
                             AND  h.run_id    = $1
                             AND  h.last_seen > NOW() - make_interval(secs => $2)
                       )
                RETURNING task_id
            )
            SELECT COUNT(*) FROM recovered
        """, rid, HEARTBEAT_DEAD_SECONDS)
    return int(result or 0)


async def is_complete(run_id: str) -> bool:
    pool = await get_pool()
    async with pool.acquire() as conn:
        incomplete = await conn.fetchval("""
            SELECT COUNT(*)
            FROM   tasks
            WHERE  run_id = $1
              AND  status IN ('pending', 'running')
        """, uuid.UUID(run_id))
    return incomplete == 0


# ---------------------------------------------------------------------------
# Run statistics
# ---------------------------------------------------------------------------

async def run_duration(run_id: str) -> float | None:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT
                MIN(ts) FILTER (WHERE event_type = 'started')             AS started_at,
                MAX(ts) FILTER (WHERE event_type IN ('success','dropped')) AS finished_at
            FROM events
            WHERE run_id = $1
        """, uuid.UUID(run_id))
    if not row or not row["started_at"] or not row["finished_at"]:
        return None
    return (row["finished_at"] - row["started_at"]).total_seconds()


# ---------------------------------------------------------------------------
# Rate limit window helper
# ---------------------------------------------------------------------------

async def oldest_slot_ts(key: str) -> datetime | None:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT ts
            FROM   rate_limit_window
            WHERE  key = $1
            ORDER  BY ts ASC
            LIMIT  1
        """, key)
    return row["ts"] if row else None