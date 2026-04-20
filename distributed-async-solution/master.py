"""
master.py — orchestrator (Stage 7: run_id, UUID keys, heartbeat recovery).

Steps
-----
1. setup_tables()     — idempotent schema creation (DDL runs once, here only)
2. create_run()       — insert a runs row, get back a UUID run_id
3. enqueue_tasks()    — insert tasks scoped to this run_id
4. spawn workers      — each receives --run-id so all DB calls are scoped
5. wait              — asyncio.gather on all worker processes
6. finish_run()       — record finished_at on the runs row
7. report            — wall time + DB-measured time for this run

Why run_id matters
------------------
All tasks, events, and heartbeats are tagged with run_id. This means:
  - Multiple master.py invocations can run simultaneously without
    interfering — each sees only its own tasks via WHERE run_id = $1.
  - is_complete() is scoped to the run — finishing run A doesn't
    trigger shutdown in run B.
  - recover_stuck_tasks() only recovers tasks from the current run.
  - Historical runs are fully queryable: SELECT * FROM runs JOIN tasks
    USING (run_id) WHERE run_id = '<old-uuid>'.

Usage
-----
    python master.py --workers 5 --requests 75
    python master.py --workers 3 --requests 30 --max-calls 15 --window 60
"""

import argparse
import asyncio
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


async def main(
    n_workers:   int,
    n_requests:  int,
    max_calls:   int,
    window:      int,
    retries:     int,
    rate_key:    str,
):
    # ── Step 1: schema ────────────────────────────────────────────────────
    # DDL runs here and nowhere else. All workers call get_pool() only —
    # concurrent setup_tables() causes "tuple concurrently updated" on the
    # Postgres catalog when N processes hit ALTER TABLE simultaneously.
    try:
        await db.setup_tables()
    except Exception as exc:
        log.error("DB setup failed: %s", exc)
        return

    # ── Step 2: create run ────────────────────────────────────────────────
    run_id = await db.create_run(
        worker_count = n_workers,
        max_calls    = max_calls,
        window_secs  = window,
    )
    log.info("run_id=%s", run_id)

    # ── Step 3: enqueue tasks ─────────────────────────────────────────────
    log.info("enqueueing %d tasks...", n_requests)
    prompts  = [PROMPT_TEMPLATE.format(n=i + 1) for i in range(n_requests)]
    task_ids = await db.enqueue_tasks(run_id, prompts)
    log.info("enqueued %d tasks", len(task_ids))

    # ── Step 4: spawn workers ─────────────────────────────────────────────
    processes = []
    for i in range(1, n_workers + 1):
        cmd = [
            sys.executable, "worker.py",
            "--worker-id",  str(i),
            "--run-id",     run_id,
            "--max-calls",  str(max_calls),
            "--window",     str(window),
            "--retries",    str(retries),
            "--rate-key",   rate_key,
        ]
        p = await asyncio.create_subprocess_exec(*cmd)
        log.info("started worker %d (pid=%d)", i, p.pid)
        processes.append(p)

    # ── Step 5: wait ──────────────────────────────────────────────────────
    log.info("%d workers running — waiting for completion...", n_workers)
    wall_start = time.monotonic()

    try:
        await asyncio.gather(*[p.wait() for p in processes])
    except asyncio.CancelledError:
        log.info("interrupted — terminating workers...")
        for p in processes:
            p.terminate()
        await asyncio.gather(*[p.wait() for p in processes])

    wall_elapsed = time.monotonic() - wall_start

    # ── Step 6: mark run finished ─────────────────────────────────────────
    await db.finish_run(run_id, total_tasks=n_requests)

    # ── Step 7: report ────────────────────────────────────────────────────
    db_elapsed = await db.run_duration(run_id)

    log.info("-" * 50)
    log.info("run finished  run_id=%s", run_id)
    log.info("  wall time : %.1fs", wall_elapsed)
    if db_elapsed:
        log.info("  DB time   : %.1fs  (first claimed → last done)", db_elapsed)
        log.info("  tasks/min : %.1f", n_requests / (db_elapsed / 60))
    log.info("-" * 50)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Orchestrator — enqueue tasks and launch workers (Stage 7)"
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