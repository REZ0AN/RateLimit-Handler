import asyncpg
import os
import time
import asyncio
import uuid
from dotenv import load_dotenv

load_dotenv()

_db_semaphore: asyncio.Semaphore | None = None
_pool: asyncpg.Pool | None = None
def get_semaphore() -> asyncio.Semaphore:
    """
    Limits concurrent DB operations to 10 at a time.
    Prevents the burst at t=60s when all rate-limited tasks
    wake simultaneously and flood the connection pool.
    """
    global _db_semaphore
    if _db_semaphore is None:
        _db_semaphore = asyncio.Semaphore(10)
    return _db_semaphore

async def get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(
            os.environ["DATABASE_URL"],
            ssl="require",
            min_size=2,
            max_size=30       # ← match your max concurrency (24 tasks + headroom)
        )
    return _pool


async def init_db():
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS tasks (
                task_id     TEXT PRIMARY KEY,
                prompt      TEXT NOT NULL,
                status      TEXT NOT NULL DEFAULT 'pending',
                retries     INT  DEFAULT 0,
                wait_used   REAL DEFAULT 0,
                created_at  REAL,
                updated_at  REAL
            );
            CREATE TABLE IF NOT EXISTS events (
                id          SERIAL PRIMARY KEY,
                task_id     TEXT,
                event_type  TEXT,
                detail      TEXT,
                ts          REAL DEFAULT extract(epoch from now())
            );
        """)


async def enqueue(prompts: list[str]) -> list[dict]:
    """
    Insert prompts as pending tasks.
    Returns [{task_id, prompt}] — handed directly to coroutines.
    Each task starts as 'pending' and stays that way until the
    coroutine itself transitions it to 'running'.
    """
    pool = await get_pool()
    now  = time.time()
    tasks = []
    async with pool.acquire() as conn:
        for prompt in prompts:
            task_id = "task_" + uuid.uuid4().hex[:6]
            await conn.execute("""
                INSERT INTO tasks (task_id, prompt, status, created_at, updated_at)
                VALUES ($1, $2, 'pending', $3, $3)
            """, task_id, prompt, now)
            tasks.append({"task_id": task_id, "prompt": prompt})
    return tasks


async def mark_running(task_id: str) -> bool:
    """
    Transition pending → running atomically.
    Returns True if this caller won the row, False if already claimed.
    FOR UPDATE SKIP LOCKED makes this safe for concurrent coroutines
    and multiple processes simultaneously.
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            row = await conn.fetchrow("""
                SELECT task_id FROM tasks
                WHERE task_id = $1 AND status = 'pending'
                FOR UPDATE SKIP LOCKED
            """, task_id)
            if not row:
                return False
            await conn.execute("""
                UPDATE tasks
                SET status = 'running', updated_at = extract(epoch from now())
                WHERE task_id = $1
            """, task_id)
            return True


async def mark_done(task_id: str):
    """running → done only."""
    pool = await get_pool()
    async with pool.acquire(timeout=10) as conn:
        await conn.execute("""
            UPDATE tasks
            SET status = 'done', updated_at = extract(epoch from now())
            WHERE task_id = $1 AND status = 'running'
        """, task_id)


async def mark_failed(task_id: str):
    """running → failed only."""
    pool = await get_pool()
    async with pool.acquire(timeout=10) as conn:
        await conn.execute("""
            UPDATE tasks
            SET status = 'failed', retries = retries + 1,
                updated_at = extract(epoch from now())
            WHERE task_id = $1 AND status = 'running'
        """, task_id)


async def add_wait(task_id: str, seconds: float):
    pool = await get_pool()
    async with pool.acquire(timeout=10) as conn:
        await conn.execute(
            "UPDATE tasks SET wait_used = wait_used + $1 WHERE task_id = $2",
            seconds, task_id
        )


async def log_event(task_id: str, event_type: str, detail: str = ""):
    pool = await get_pool()
    try:
        async with pool.acquire(timeout=10) as conn:   # ← timeout instead of hanging
            await conn.execute("""
                INSERT INTO events (task_id, event_type, detail)
                VALUES ($1, $2, $3)
            """, task_id, event_type, detail)
    except Exception as e:
        # log to stdout so failures are visible — never silently drop events
        print(f"[db] log_event failed for {task_id} ({event_type}): {e}")


async def summary():
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT status, count(*) as n FROM tasks GROUP BY status ORDER BY status"
        )
    print("\n── DB summary ──────────────────────")
    for r in rows:
        print(f"  {r['status']:10s}  {r['n']}")
    print("────────────────────────────────────\n")