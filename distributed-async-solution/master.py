"""
master.py — enqueue tasks then launch multiple worker.py processes.

Usage:
    python master.py --workers 3 --requests 75
    python master.py --workers 5 --requests 30 --max-calls 15 --window 60

Steps
-----
1. Enqueue --requests tasks into the DB (before any worker starts).
2. Launch --workers independent worker.py processes.
3. Wait for all workers to finish.
4. Print wall time and DB-measured time.
"""

import argparse
import asyncio
import subprocess
import sys
import time
import logging

import db
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [master] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("master")

PROMPT_TEMPLATE = "Tell me a fun fact about the number {n}."

async def setup_db():
    # --- Connectivity check ---
    # Fail fast with a clear message if DATABASE_URL is wrong or unreachable.
    # Without this, a pool creation failure surfaces only as "no pending tasks"
    # and the worker spins silently forever.
    try:
        await db.setup_tables()
    except Exception as exc:
        log.error("DB setup failed — check DATABASE_URL: %s", exc)
        return 

async def _enqueue(n_requests: int) -> list[int]:
    prompts = [PROMPT_TEMPLATE.format(n=i + 1) for i in range(n_requests)]
    return await db.enqueue_tasks(prompts)


async def _fetch_duration() -> float | None:        
    return await db.run_duration()


async def main(
    n_workers:   int,
    n_requests:  int,
    max_calls:   int,
    window:      int,
    retries:     int,
    rate_key:    str,
):
    # -- Step 0: DB Tables Setup --
    await setup_db()
    
    # --- Step 1: enqueue ---
    log.info("enqueueing %d tasks...", n_requests)
    task_ids = await _enqueue(n_requests)
    log.info("enqueued %d tasks (ids %d-%d)", len(task_ids), task_ids[0], task_ids[-1])

    # --- Step 2: launch workers ---
    processes = []
    for i in range(1, n_workers + 1):
        cmd = [
            sys.executable, "worker.py",
            "--worker-id",  str(i),
            "--max-calls",  str(max_calls),
            "--window",     str(window),
            "--retries",    str(retries),
            "--rate-key",   rate_key,
        ]
        p = subprocess.Popen(cmd)
        log.info("started worker %d (pid=%d)", i, p.pid)
        processes.append(p)

    # --- Step 3: wait ---
    log.info("%d workers running — waiting for completion...", n_workers)
    wall_start = time.monotonic()

    try:
        for p in processes:
            p.wait()
    except KeyboardInterrupt:
        log.info("interrupted — terminating workers...")
        for p in processes:
            p.terminate()
        for p in processes:
            p.wait()

    wall_elapsed = time.monotonic() - wall_start

    # --- Step 4: print times ---
    # asyncio.run() creates a fresh event loop here. db._pool is reset
    # inside _fetch_duration() so it doesn't reuse the dead pool from
    # the enqueue loop above.
    db_elapsed = await _fetch_duration()

    log.info("-" * 50)
    log.info("all workers finished")
    log.info("  wall time : %.1fs", wall_elapsed)
    if db_elapsed:
        log.info("  DB time   : %.1fs  (first claimed -> last done)", db_elapsed)
        log.info("  tasks/min : %.1f", n_requests / (db_elapsed / 60))
    log.info("-" * 50)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Enqueue tasks and launch multiple worker.py processes"
    )
    parser.add_argument("--workers",   type=int, default=3,      help="Number of worker processes (default: 3)")
    parser.add_argument("--requests",  type=int, default=75,     help="Number of tasks to enqueue (default: 75)")
    parser.add_argument("--max-calls", type=int, default=15,     help="API calls per window (default: 15)")
    parser.add_argument("--window",    type=int, default=60,     help="Window size in seconds (default: 60)")
    parser.add_argument("--retries",   type=int, default=3,      help="Max retries on real errors (default: 3)")
    parser.add_argument("--rate-key",  default="gemini:rpm",     help="Shared rate limit key (default: gemini:rpm)")
    args = parser.parse_args()

    asyncio.run(main(
        n_workers  = args.workers,
        n_requests = args.requests,
        max_calls  = args.max_calls,
        window     = args.window,
        retries    = args.retries,
        rate_key   = args.rate_key,
    ))