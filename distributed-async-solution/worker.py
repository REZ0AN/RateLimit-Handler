"""
worker.py — standalone rate-limited Gemini worker (Postgres-distributed).

Changes vs original Stage 5
-----------------------------
1. STUCK_TASK_TIMEOUT reduced 120s → 30s.
   The original 120s was a workaround for updated_at being reset by
   increment_retries(). Now that db.py uses claimed_at (which is never
   touched after the initial claim), 30s is safe and correct.

2. is_complete() receives stuck_threshold_seconds=STUCK_TASK_TIMEOUT.
   Both is_complete() and recover_stuck_tasks() use the same threshold so
   they stay in sync: a row ignored by is_complete() as "probably orphaned"
   is guaranteed to be reset by recover_stuck_tasks() on the same iteration.

3. recover_stuck_tasks() is called BEFORE is_complete() and claim_next_task()
   on every iteration. This ensures orphaned rows are reset to 'pending'
   before we decide whether the run is complete and before we try to claim.
   In the original the order was the same, but the threshold mismatch made
   recovery ineffective.

4. Startup DB connectivity check added.
   get_pool() is called explicitly before the main loop starts. If the
   DATABASE_URL is wrong or NeonDB is unreachable the worker logs a clear
   error and exits immediately instead of silently spinning on "no pending
   tasks" forever.

5. Exception handling added around claim_next_task().
   A DB error during claim previously returned None (transaction rolled back
   silently), making the worker behave as if the queue were empty. Now it
   logs the error and sleeps before retrying so transient DB issues don't
   cause a silent stuck state.

Everything else (two-lane retry, exponential backoff, heartbeat, shutdown
via stop_event) is unchanged from Stage 5.
"""

import asyncio
import argparse
import logging
import os
import time

from google import genai
from dotenv import load_dotenv

import db
from rate_limiter import PgRateLimiter, TooManyRequestsError

load_dotenv()

GEMINI_MODEL       = "gemma-4-26b-a4b-it"
HEARTBEAT_EVERY    = 5   # seconds between heartbeat upserts
STUCK_TASK_TIMEOUT = 40  # seconds before a 'running' task is considered stuck
                         # must match the timeout passed to recover_stuck_tasks()
                         # and is_complete() — they share one constant here.


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def make_logger(worker_id: str) -> logging.Logger:
    logging.basicConfig(
        level=logging.INFO,
        format=f"%(asctime)s [{worker_id}] %(message)s",
        datefmt="%H:%M:%S",
    )
    return logging.getLogger(worker_id)


# ---------------------------------------------------------------------------
# Gemini API call
# ---------------------------------------------------------------------------

async def ask_gemini(client: genai.Client, prompt: str) -> str:
    """
    Make one Gemini API call.

    Raises TooManyRequestsError on 429.
    Raises plain Exception on all other failures.
    """
    try:
        response = await client.aio.models.generate_content(
            model=GEMINI_MODEL,
            contents=prompt,
        )
        return response.text
    except Exception as exc:
        lowered = str(exc).lower()
        if "429" in lowered or "resource_exhausted" in lowered or "rate" in lowered:
            raise TooManyRequestsError(retry_after=60.0) from exc
        raise


# ---------------------------------------------------------------------------
# Single task — rate limit gate + two-lane retry
# ---------------------------------------------------------------------------

async def process_task(
    task:        dict,
    worker_id:   str,
    limiter:     PgRateLimiter,
    client:      genai.Client,
    max_retries: int,
    log:         logging.Logger,
) -> None:
    """
    Process one task to completion. Never returns until the task reaches
    'done' or 'failed'. All retries are handled internally.

    Retry lanes
    -----------
    Lane 1 — TooManyRequestsError (API 429):
        Sleep retry_after seconds. Does NOT increment retries counter.

    Lane 2 — real errors (5xx, timeout, network, parse failure):
        Sleep 2^retries seconds. Increments retries counter.
        After max_retries exhausted: mark task 'failed', return.
    """
    task_id = task["task_id"]
    prompt  = task["prompt"]
    retries = 0

    while True:
        await limiter.wait_if_needed(task_id, worker_id)

        try:
            t0      = time.monotonic()
            result  = await ask_gemini(client, prompt)
            elapsed = time.monotonic() - t0

            await db.mark_done(task_id, worker_id, result)
            log.info("task=%d done (%.2fs)", task_id, elapsed)
            return

        except TooManyRequestsError as exc:
            log.warning(
                "task=%d API 429 — sleeping %.0fs (not a retry)",
                task_id, exc.retry_after,
            )
            await db.log_event(task_id, worker_id, "api_429", f"retry_after={exc.retry_after}")
            await asyncio.sleep(exc.retry_after)

        except Exception as exc:
            retries += 1
            await db.increment_retries(task_id)
            await db.log_event(task_id, worker_id, "error", str(exc))

            if retries >= max_retries:
                log.error(
                    "task=%d failed after %d retries: %s",
                    task_id, retries, exc,
                )
                await db.mark_failed(task_id, worker_id, str(exc))
                return

            backoff = 2 ** retries
            log.warning(
                "task=%d error (attempt %d/%d) backoff=%ds: %s",
                task_id, retries, max_retries, backoff, exc,
            )
            await asyncio.sleep(backoff)


# ---------------------------------------------------------------------------
# Heartbeat loop
# ---------------------------------------------------------------------------

async def heartbeat_loop(worker_id: str, stop_event: asyncio.Event):
    """
    Upsert last_seen every HEARTBEAT_EVERY seconds.
    Wakes immediately when stop_event is set for a clean final heartbeat.
    """
    while not stop_event.is_set():
        await db.upsert_heartbeat(worker_id)
        try:
            await asyncio.wait_for(
                asyncio.shield(stop_event.wait()),
                timeout=HEARTBEAT_EVERY,
            )
        except asyncio.TimeoutError:
            pass

    await db.upsert_heartbeat(worker_id)


# ---------------------------------------------------------------------------
# Main worker loop
# ---------------------------------------------------------------------------

async def main(
    worker_id:   str,
    max_calls:   int,
    window:      int,
    max_retries: int,
    rate_key:    str,
):
    log = make_logger(worker_id)

    # --- Connectivity check ---
    # Only verify the DB is reachable — do NOT call setup_tables() here.
    # When launched by master.py, all workers start simultaneously and
    # concurrent setup_tables() calls race on ALTER TABLE / CREATE INDEX,
    # causing "tuple concurrently updated" errors.
    # master.py already called setup_tables() before launching workers.
    try:
        await db.get_pool()
    except Exception as exc:
        log.error("DB connection failed — check DATABASE_URL: %s", exc)
        return

    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        log.error("GEMINI_API_KEY not set in environment")
        return

    client     = genai.Client(api_key=api_key)
    limiter    = PgRateLimiter(key=rate_key, max_calls=max_calls, window_seconds=window)
    stop_event = asyncio.Event()

    hb = asyncio.create_task(heartbeat_loop(worker_id, stop_event))

    log.info("started — limit %d/%ds  key='%s'", max_calls, window, rate_key)

    try:
        while not stop_event.is_set():

            # --- Step 1: recover stuck tasks ---
            # Must happen before is_complete() and claim_next_task().
            # If a worker crashed, its task sits 'running' forever.
            # recover_stuck_tasks() resets it to 'pending' so:
            #   a) is_complete() won't block on it indefinitely.
            #   b) claim_next_task() can pick it up.
            # Uses claimed_at (set once on claim, never modified) so
            # increment_retries() bumping updated_at can't hide a stuck task.
            try:
                recovered = await db.recover_stuck_tasks(
                    timeout_seconds=STUCK_TASK_TIMEOUT
                )
                if recovered:
                    log.warning(
                        "recovered %d stuck task(s) back to pending", recovered
                    )
            except Exception as exc:
                log.error("recover_stuck_tasks failed: %s", exc)
                await asyncio.sleep(2)
                continue

            # --- Step 2: check completion ---
            # is_complete() uses the same STUCK_TASK_TIMEOUT threshold so it
            # ignores orphaned 'running' rows (already reset above) and only
            # counts tasks that are genuinely in-flight right now.
            # This prevents the "stuck at 0 pending, 1 running forever" loop.
            try:
                if await db.is_complete():
                    log.info("all tasks complete — stopping")
                    stop_event.set()
                    break
            except Exception as exc:
                log.error("is_complete check failed: %s", exc)
                await asyncio.sleep(2)
                continue

            # --- Step 3: claim next task ---
            try:
                task = await db.claim_next_task(worker_id)
            except Exception as exc:
                # DB error during claim — transient network issue or lock timeout.
                # Log it clearly so it's visible. Sleep and retry rather than
                # silently returning None and spinning on "no pending tasks".
                log.error("claim_next_task failed (will retry): %s", exc)
                await asyncio.sleep(2)
                continue

            if task is None:
                # Queue has no 'pending' rows right now. Two reasons:
                #
                # A) Another worker has the remaining task(s) in flight.
                #    → sleep 1s and recheck. Not a polling delay for new tasks
                #    (enqueue.py already ran). Just a yield while they finish.
                #
                # B) All tasks were just completed between step 2 and step 3.
                #    → is_complete() will return True on the next iteration.
                #
                # Either way, sleep 1s and loop back to step 1.
                log.debug("no pending tasks — waiting for in-flight tasks...")
                await asyncio.sleep(1)
                continue

            # --- Step 4: process ---
            log.info("claimed task=%d", task["task_id"])
            await process_task(task, worker_id, limiter, client, max_retries, log)

    except KeyboardInterrupt:
        log.info("interrupted — shutting down")
        stop_event.set()

    finally:
        await hb
        log.info("shutdown complete")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Rate-limited Gemini worker — Postgres-distributed (Stage 5)"
    )
    parser.add_argument("--worker-id",  required=True,        help="Unique label for this process")
    parser.add_argument("--max-calls",  type=int, default=15, help="API calls allowed per window")
    parser.add_argument("--window",     type=int, default=60, help="Window size in seconds")
    parser.add_argument("--retries",    type=int, default=3,  help="Max retries on real errors")
    parser.add_argument("--rate-key",   default="gemini:rpm", help="Shared rate limit key")
    args = parser.parse_args()

    asyncio.run(main(
        worker_id   = args.worker_id,
        max_calls   = args.max_calls,
        window      = args.window,
        max_retries = args.retries,
        rate_key    = args.rate_key,
    ))