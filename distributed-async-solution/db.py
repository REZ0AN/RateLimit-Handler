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
            )
    return _pool


# ---------------------------------------------------------------------------
# Schema — setup all tables and the Postgres function on first run
# ---------------------------------------------------------------------------

async def setup_tables():
    pool = await get_pool()
    async with pool.acquire() as conn:

        # tasks — unchanged from Stage 4
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS tasks (
                task_id     BIGSERIAL    PRIMARY KEY,
                prompt      TEXT         NOT NULL,
                status      TEXT         NOT NULL DEFAULT 'pending',
                retries     INT          NOT NULL DEFAULT 0,
                wait_used   FLOAT        NOT NULL DEFAULT 0,
                result      TEXT,
                created_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
                updated_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW()
            )
        """)

        # events — unchanged from Stage 4
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS events (
                event_id    BIGSERIAL    PRIMARY KEY,
                task_id     BIGINT       REFERENCES tasks(task_id),
                worker_id   TEXT,
                event_type  TEXT         NOT NULL,
                detail      TEXT,
                ts          TIMESTAMPTZ  NOT NULL DEFAULT NOW()
            )
        """)

        # rate_limit_window — NEW in Stage 5
        # Each row = one granted API call slot still inside the sliding window.
        # Rows are evicted inside try_acquire_slot() as a side effect of every
        # call, so no separate cron job is needed.
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

        # worker_heartbeat — NEW in Stage 5
        # Workers upsert here every few seconds. The dashboard uses this to
        # show which workers are alive without reading stdout logs.
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS worker_heartbeat (
                worker_id   TEXT         PRIMARY KEY,
                last_seen   TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
                tasks_done  INT          NOT NULL DEFAULT 0,
                tasks_fail  INT          NOT NULL DEFAULT 0
            )
        """)

        # try_acquire_slot() — NEW in Stage 5
        # The core of the distributed rate limiter. Replaces asyncio.Lock() +
        # deque from all previous stages with a Postgres advisory lock.
        #
        # pg_advisory_xact_lock(hashtext(key)) serialises all callers for a
        # given key. Only one worker can be inside this function at a time.
        # The lock releases automatically when the transaction commits.
        #
        # Returns TRUE  → slot granted, caller may fire the API call.
        # Returns FALSE → window full, caller must sleep and retry.
        await conn.execute("""
            CREATE OR REPLACE FUNCTION try_acquire_slot(
                p_key            TEXT,
                p_max_calls      INT,
                p_window_seconds INT
            ) RETURNS BOOLEAN AS $$
            DECLARE
                v_count INT;
            BEGIN
                -- Acquire exclusive advisory lock for this key.
                -- Other workers calling with the same key block here until
                -- this transaction commits or rolls back.
                PERFORM pg_advisory_xact_lock(hashtext(p_key));

                -- Evict timestamps outside the sliding window.
                DELETE FROM rate_limit_window
                WHERE  key = p_key
                  AND  ts  < NOW() - (p_window_seconds || ' seconds')::INTERVAL;

                -- Count remaining slots in the window.
                SELECT COUNT(*) INTO v_count
                FROM   rate_limit_window
                WHERE  key = p_key;

                -- Grant or deny.
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
# Task lifecycle — unchanged from Stage 4
# ---------------------------------------------------------------------------

async def enqueue_tasks(prompts: list[str]) -> list[int]:
    """Insert prompts as pending tasks. Returns list of task_ids."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            INSERT INTO tasks (prompt)
            SELECT unnest($1::text[])
            RETURNING task_id
        """, prompts)
    return [r["task_id"] for r in rows]


async def claim_next_task(worker_id: str) -> dict | None:
    """
    Atomically claim the next pending task for this worker.
    FOR UPDATE SKIP LOCKED ensures two workers never claim the same row.
    Returns None when the queue is empty.
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            row = await conn.fetchrow("""
                SELECT task_id, prompt
                FROM   tasks
                WHERE  status = 'pending'
                ORDER  BY task_id
                FOR UPDATE SKIP LOCKED
                LIMIT 1
            """)
            if row is None:
                return None
            await conn.execute("""
                UPDATE tasks
                SET    status = 'running', updated_at = NOW()
                WHERE  task_id = $1 AND status = 'pending'
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
            SET    retries = retries + 1, updated_at = NOW()
            WHERE  task_id = $1
        """, task_id)


async def add_wait_time(task_id: int, seconds: float):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("""
            UPDATE tasks
            SET    wait_used = wait_used + $2, updated_at = NOW()
            WHERE  task_id = $1
        """, task_id, seconds)


# ---------------------------------------------------------------------------
# Event logging
# ---------------------------------------------------------------------------

async def _log_event(
    conn: asyncpg.Connection,
    task_id: int,
    worker_id: str,
    event_type: str,
    detail: str | None,
):
    """Write an event row inside an existing connection/transaction."""
    await conn.execute("""
        INSERT INTO events (task_id, worker_id, event_type, detail)
        VALUES ($1, $2, $3, $4)
    """, task_id, worker_id, event_type, detail)


async def log_event(
    task_id: int,
    worker_id: str,
    event_type: str,
    detail: str | None,
):
    """Write an event row using a fresh connection from the pool."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        await _log_event(conn, task_id, worker_id, event_type, detail)


# ---------------------------------------------------------------------------
# Worker heartbeat — NEW in Stage 5
# ---------------------------------------------------------------------------

async def upsert_heartbeat(worker_id: str, tasks_done: int, tasks_fail: int):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO worker_heartbeat (worker_id, last_seen, tasks_done, tasks_fail)
            VALUES ($1, NOW(), $2, $3)
            ON CONFLICT (worker_id) DO UPDATE
            SET last_seen  = NOW(),
                tasks_done = EXCLUDED.tasks_done,
                tasks_fail = EXCLUDED.tasks_fail
        """, worker_id, tasks_done, tasks_fail)


# ---------------------------------------------------------------------------
# Rate limit window helper — NEW in Stage 5
# ---------------------------------------------------------------------------

async def oldest_slot_ts(key: str) -> datetime | None:
    """
    Return the timestamp of the oldest slot in the window for `key`.
    Used by PgRateLimiter to calculate how long to sleep when the window is full.
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT ts
            FROM   rate_limit_window
            WHERE  key = $1
            ORDER  BY ts ASC
            LIMIT 1
        """, key)
    return row["ts"] if row else None
