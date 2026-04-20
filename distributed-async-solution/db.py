"""
db.py — all database interaction for Stage 6 (master-worker).

Changes vs Stage 5
-------------------
1. claim_batch(worker_id, n) — claim up to N pending tasks in one
   round trip. The master producer calls this instead of claim_next_task()
   so it can fill the asyncio.Queue in one DB call rather than N calls.

2. claim_next_task() kept for compatibility but not used by master.py.

3. run_duration() — returns total elapsed seconds from first 'started'
   event to last terminal event. Used by master to log final stats.

4. claimed_at column + recover_stuck_tasks() / is_complete() unchanged
   from Stage 5 fix (make_interval, float cast, claimed_at reference).
"""

import os
import asyncio
import asyncpg
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

_pool: asyncpg.Pool | None = None
_pool_lock = asyncio.Lock()


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
                statement_cache_size=0,  # prevents stale cached plans after schema changes
            )
    return _pool


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

async def setup_tables():
    """
    Create all tables and the try_acquire_slot() Postgres function.
    Safe to call multiple times — all statements are idempotent.
    """
    pool = await get_pool()
    async with pool.acquire() as conn:

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS tasks (
                task_id    BIGSERIAL    PRIMARY KEY,
                prompt     TEXT         NOT NULL,
                status     TEXT         NOT NULL DEFAULT 'pending',
                retries    INT          NOT NULL DEFAULT 0,
                wait_used  FLOAT        NOT NULL DEFAULT 0,
                result     TEXT,
                created_at TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
                claimed_at TIMESTAMPTZ
            )
        """)

        await conn.execute("""
            ALTER TABLE tasks
            ADD COLUMN IF NOT EXISTS claimed_at TIMESTAMPTZ
        """)

        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_tasks_status_claimed
            ON tasks (status, claimed_at)
        """)

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS events (
                event_id   BIGSERIAL    PRIMARY KEY,
                task_id    BIGINT       REFERENCES tasks(task_id),
                worker_id  TEXT,
                event_type TEXT         NOT NULL,
                detail     TEXT,
                ts         TIMESTAMPTZ  NOT NULL DEFAULT NOW()
            )
        """)

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS rate_limit_window (
                id   BIGSERIAL    PRIMARY KEY,
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
                worker_id TEXT        PRIMARY KEY,
                last_seen TIMESTAMPTZ NOT NULL DEFAULT NOW()
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
# Task lifecycle
# ---------------------------------------------------------------------------

async def enqueue_tasks(prompts: list[str]) -> list[int]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            INSERT INTO tasks (prompt)
            SELECT unnest($1::text[])
            RETURNING task_id
        """, prompts)
    return [r["task_id"] for r in rows]


async def claim_batch(worker_id: str, n: int) -> list[dict]:
    """
    Atomically claim up to N pending tasks in a single transaction.

    Uses the same FOR UPDATE SKIP LOCKED pattern as claim_next_task()
    so concurrent callers never double-claim. Returns a list of dicts
    (possibly empty if no pending tasks exist right now).

    The master producer calls this to fill the asyncio.Queue in one
    round trip rather than making N separate claim calls.
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            rows = await conn.fetch("""
                SELECT task_id, prompt
                FROM   tasks
                WHERE  status = 'pending'
                ORDER  BY task_id
                FOR UPDATE SKIP LOCKED
                LIMIT  $1
            """, n)

            if not rows:
                return []

            task_ids = [r["task_id"] for r in rows]

            await conn.execute("""
                UPDATE tasks
                SET    status     = 'running',
                       claimed_at = NOW(),
                       updated_at = NOW()
                WHERE  task_id = ANY($1)
                  AND  status  = 'pending'
            """, task_ids)

            for row in rows:
                await _log_event(conn, row["task_id"], worker_id, "started", None)

    return [dict(r) for r in rows]


async def claim_next_task(worker_id: str) -> dict | None:
    """Single-task claim — kept for compatibility."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            row = await conn.fetchrow("""
                SELECT task_id, prompt
                FROM   tasks
                WHERE  status = 'pending'
                ORDER  BY task_id
                FOR UPDATE SKIP LOCKED
                LIMIT  1
            """)
            if row is None:
                return None
            await conn.execute("""
                UPDATE tasks
                SET    status     = 'running',
                       claimed_at = NOW(),
                       updated_at = NOW()
                WHERE  task_id = $1
                  AND  status  = 'pending'
            """, row["task_id"])
            await _log_event(conn, row["task_id"], worker_id, "started", None)
    return dict(row)


async def mark_done(task_id: int, worker_id: str, result: str):
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute("""
                UPDATE tasks
                SET    status = 'done', result = $2, updated_at = NOW()
                WHERE  task_id = $1 AND status = 'running'
            """, task_id, result)
            await _log_event(conn, task_id, worker_id, "success", result[:200])


async def mark_failed(task_id: int, worker_id: str, reason: str):
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute("""
                UPDATE tasks
                SET    status = 'failed', updated_at = NOW()
                WHERE  task_id = $1 AND status = 'running'
            """, task_id, reason)
            await _log_event(conn, task_id, worker_id, "dropped", reason)


async def increment_retries(task_id: int):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("""
            UPDATE tasks
            SET    retries    = retries + 1,
                   updated_at = NOW()
            WHERE  task_id = $1
        """, task_id)


async def add_wait_time(task_id: int, seconds: float):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("""
            UPDATE tasks
            SET    wait_used  = wait_used + $2,
                   updated_at = NOW()
            WHERE  task_id = $1
        """, task_id, seconds)


# ---------------------------------------------------------------------------
# Event logging
# ---------------------------------------------------------------------------

async def _log_event(
    conn:       asyncpg.Connection,
    task_id:    int,
    worker_id:  str,
    event_type: str,
    detail:     str | None,
):
    await conn.execute("""
        INSERT INTO events (task_id, worker_id, event_type, detail)
        VALUES ($1, $2, $3, $4)
    """, task_id, worker_id, event_type, detail)


async def log_event(
    task_id:    int,
    worker_id:  str,
    event_type: str,
    detail:     str | None,
):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await _log_event(conn, task_id, worker_id, event_type, detail)


# ---------------------------------------------------------------------------
# Worker heartbeat
# ---------------------------------------------------------------------------

async def upsert_heartbeat(worker_id: str):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO worker_heartbeat (worker_id, last_seen)
            VALUES ($1, NOW())
            ON CONFLICT (worker_id) DO UPDATE
            SET last_seen = NOW()
        """, worker_id)


# ---------------------------------------------------------------------------
# Completion check
# ---------------------------------------------------------------------------

async def is_complete() -> bool:
    """
    Return True when every task is in a terminal state (done or failed).

    Counts ALL pending and running rows with no exceptions.
    recover_stuck_tasks() is solely responsible for moving orphaned
    'running' rows back to 'pending'. is_complete() just checks the count.

    The previous version ignored 'running' rows older than N seconds which
    created a race: is_complete() could return True before recover_stuck_tasks()
    had reset the stuck row, causing workers to exit with tasks unprocessed.
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        incomplete = await conn.fetchval("""
            SELECT COUNT(*)
            FROM   tasks
            WHERE  status IN ('pending', 'running')
        """)
    return incomplete == 0


# ---------------------------------------------------------------------------
# Stuck task recovery
# ---------------------------------------------------------------------------

async def recover_stuck_tasks(timeout_seconds: int = 30) -> int:
    """
    Reset tasks 'running' longer than timeout_seconds back to 'pending'.
    Uses claimed_at (set once, never modified) so retries bumping
    updated_at can never hide a genuinely stuck task.
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        result = await conn.fetchval("""
            WITH recovered AS (
                UPDATE tasks
                SET    status     = 'pending',
                       claimed_at = NULL,
                       updated_at = NOW()
                WHERE  status     = 'running'
                  AND  claimed_at < NOW() - make_interval(secs => $1)
                RETURNING task_id
            )
            SELECT COUNT(*) FROM recovered
        """, float(timeout_seconds))
    return int(result or 0)


# ---------------------------------------------------------------------------
# Run statistics
# ---------------------------------------------------------------------------

async def run_duration() -> float | None:
    """
    Return total elapsed seconds from first 'started' event to last
    terminal event ('success' or 'dropped'). Returns None if the run
    hasn't started or isn't finished yet.
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT
                MIN(ts) FILTER (WHERE event_type = 'started')            AS started_at,
                MAX(ts) FILTER (WHERE event_type IN ('success','dropped')) AS finished_at
            FROM events
        """)
    if not row or not row["started_at"] or not row["finished_at"]:
        return None
    delta = row["finished_at"] - row["started_at"]
    return delta.total_seconds()


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