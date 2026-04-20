"""
worker.py — rate-limited Gemini worker (Stage 7 revised: single-round-trip loop).

Key change vs previous Stage 7
--------------------------------
The main loop now calls claim_loop_step() — a single Postgres round trip that
does recovery + completion check + claim atomically via one CTE.

Previous loop (3 round trips per iteration):
    await db.recover_stuck_tasks(run_id)     # round trip 1
    await db.is_complete(run_id)             # round trip 2
    await db.claim_next_task(run_id, ...)    # round trip 3

New loop (1 round trip per iteration):
    step = await db.claim_loop_step(run_id, worker_id)  # round trip 1

At NeonDB latency this reduces per-task overhead from ~600-1500ms to
~200-500ms, eliminating the 5-6 second gap between task completion and
the next claim.

rate_limiter.py also updated:
    wait_if_needed() now passes (run_id, task_id, worker_id) to match
    the Stage 7 db signatures. Previously log_event and add_wait_time
    were called with old Stage 6 signatures causing silent failures.
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

GEMINI_MODEL    = "gemma-4-26b-a4b-it"
HEARTBEAT_EVERY = 5


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
    run_id:      str,
    worker_id:   str,
    limiter:     PgRateLimiter,
    client:      genai.Client,
    max_retries: int,
    log:         logging.Logger,
) -> None:
    task_id = task["task_id"]
    prompt  = task["prompt"]
    retries = 0

    while True:
        await limiter.wait_if_needed(run_id, task_id, worker_id)

        try:
            t0      = time.monotonic()
            result  = await ask_gemini(client, prompt)
            elapsed = time.monotonic() - t0

            await db.mark_done(task_id, run_id, worker_id, result)
            log.info("task=%s done (%.2fs)", task_id[:8], elapsed)
            return

        except TooManyRequestsError as exc:
            log.warning("task=%s 429 — sleeping %.0fs (not a retry)",
                        task_id[:8], exc.retry_after)
            await db.log_event(run_id, task_id, worker_id, "api_429",
                               f"retry_after={exc.retry_after}")
            await asyncio.sleep(exc.retry_after)

        except Exception as exc:
            retries += 1
            await db.increment_retries(task_id)
            await db.log_event(run_id, task_id, worker_id, "error", str(exc))

            if retries >= max_retries:
                log.error("task=%s failed after %d retries: %s",
                          task_id[:8], retries, exc)
                await db.mark_failed(task_id, run_id, worker_id, str(exc))
                return

            backoff = 2 ** retries
            log.warning("task=%s error (attempt %d/%d) backoff=%ds: %s",
                        task_id[:8], retries, max_retries, backoff, exc)
            await asyncio.sleep(backoff)


# ---------------------------------------------------------------------------
# Heartbeat loop
# ---------------------------------------------------------------------------

async def heartbeat_loop(worker_id: str, run_id: str, stop_event: asyncio.Event):
    while not stop_event.is_set():
        try:
            await db.upsert_heartbeat(worker_id, run_id)
        except Exception:
            pass
        try:
            await asyncio.wait_for(
                asyncio.shield(stop_event.wait()),
                timeout=HEARTBEAT_EVERY,
            )
        except asyncio.TimeoutError:
            pass
    try:
        await db.upsert_heartbeat(worker_id, run_id)
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Main worker loop
# ---------------------------------------------------------------------------

async def main(
    worker_id:   str,
    run_id:      str,
    max_calls:   int,
    window:      int,
    max_retries: int,
    rate_key:    str,
):
    log = make_logger(worker_id)

    try:
        await db.get_pool()
    except Exception as exc:
        log.error("DB connection failed: %s", exc)
        return

    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        log.error("GEMINI_API_KEY not set")
        return

    client     = genai.Client(api_key=api_key)
    limiter    = PgRateLimiter(key=rate_key, max_calls=max_calls, window_seconds=window)
    stop_event = asyncio.Event()

    hb = asyncio.create_task(heartbeat_loop(worker_id, run_id, stop_event))

    log.info("started — run=%s  limit=%d/%ds  key='%s'",
             run_id[:8], max_calls, window, rate_key)

    try:
        while not stop_event.is_set():

            # One round trip: recover orphans + check completion + claim next task.
            # Returns a dict with recovered, incomplete, task_id, prompt.
            try:
                step = await db.claim_loop_step(run_id, worker_id)
            except Exception as exc:
                log.error("claim_loop_step failed (will retry): %s", exc)
                await asyncio.sleep(2)
                continue

            if step["recovered"] > 0:
                log.warning("recovered %d orphaned task(s) → pending",
                            step["recovered"])

            # incomplete == 0 means every task is done or failed
            if step["incomplete"] == 0:
                log.info("all tasks complete — stopping")
                stop_event.set()
                break

            # task_id is None when pending=0 but running>0 (other workers in flight)
            if step["task_id"] is None:
                log.debug("no pending tasks — waiting for in-flight tasks...")
                await asyncio.sleep(1)
                continue

            log.info("claimed task=%s", step["task_id"][:8])
            await process_task(
                task        = {"task_id": step["task_id"], "prompt": step["prompt"]},
                run_id      = run_id,
                worker_id   = worker_id,
                limiter     = limiter,
                client      = client,
                max_retries = max_retries,
                log         = log,
            )

    except KeyboardInterrupt:
        log.info("interrupted — shutting down")
        stop_event.set()

    finally:
        await hb
        log.info("shutdown complete")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Rate-limited Gemini worker (Stage 7)"
    )
    parser.add_argument("--worker-id",  required=True,         help="Unique label for this process")
    parser.add_argument("--run-id",     required=True,         help="UUID of the current run")
    parser.add_argument("--max-calls",  type=int, default=15,  help="API calls per window")
    parser.add_argument("--window",     type=int, default=60,  help="Window size in seconds")
    parser.add_argument("--retries",    type=int, default=3,   help="Max retries on real errors")
    parser.add_argument("--rate-key",   default="gemini:rpm",  help="Shared rate limit key")
    args = parser.parse_args()

    asyncio.run(main(
        worker_id   = args.worker_id,
        run_id      = args.run_id,
        max_calls   = args.max_calls,
        window      = args.window,
        max_retries = args.retries,
        rate_key    = args.rate_key,
    ))