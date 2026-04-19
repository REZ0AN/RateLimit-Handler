"""
worker.py — standalone rate-limited Gemini worker (Postgres-distributed).

Each invocation of this script is an independent OS process with its own
event loop. Run as many instances as you want — they share one rate limit
window and one task queue via Postgres. Neither resource will be double-used.

Usage:
    python worker.py --worker-id 1          # terminal 1
    python worker.py --worker-id 2          # terminal 2
    python worker.py --worker-id 3          # terminal 3

All three share the same rate limit budget across all three processes.

How this differs from Stage 4 handler.py
-----------------------------------------
Stage 4:  asyncio.gather(coroutine_1, coroutine_2, ...) inside one process.
Stage 5:  Each worker.py is a separate OS process. The rate limiter is
          PgRateLimiter (Postgres advisory lock) instead of asyncio.Lock().
          Everything else — retry logic, backoff, task claiming — is identical.
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

GEMINI_MODEL     = "gemma-4-26b-a4b-it"
HEARTBEAT_EVERY  = 5   # seconds between heartbeat upserts
QUEUE_POLL_EVERY = 2   # seconds to wait when the queue is empty


# ---------------------------------------------------------------------------
# Logging — binds worker_id into every log line
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

    Raises TooManyRequestsError on 429 — caller sleeps retry_after, does not
    count against the retry budget (request was never processed).

    Raises Exception on all other failures — caller applies exponential
    backoff and counts against the retry budget.
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
# Single task — rate limit gate + two-lane retry (unchanged from Stage 4)
# ---------------------------------------------------------------------------

async def process_task(
    task:        dict,
    worker_id:   str,
    limiter:     PgRateLimiter,
    client:      genai.Client,
    max_retries: int,
    log:         logging.Logger,
) -> bool:
    task_id = task["task_id"]
    prompt  = task["prompt"]
    retries = 0

    while True:

        # --- Gate: wait for a shared rate limit slot ---
        await limiter.wait_if_needed(task_id, worker_id)

        # --- Attempt the API call ---
        try:
            t0      = time.monotonic()
            result  = await ask_gemini(client, prompt)
            elapsed = time.monotonic() - t0

            await db.mark_done(task_id, worker_id, result)
            log.info("task=%d done  (%.2fs)", task_id, elapsed)
            return True

        except TooManyRequestsError as exc:
            # Server-side 429 — do not burn a retry.
            # Sleep the exact Retry-After the API told us, then re-enter the
            # rate limit gate at the top of the loop.
            log.warning("task=%d API 429 — sleeping %.0fs", task_id, exc.retry_after)
            await db.log_event(task_id, worker_id, "api_429", f"retry_after={exc.retry_after}")
            await asyncio.sleep(exc.retry_after)

        except Exception as exc:
            # Real application error — counts against retry budget.
            retries += 1
            await db.increment_retries(task_id)
            await db.log_event(task_id, worker_id, "error", str(exc))

            if retries >= max_retries:
                log.error("task=%d failed after %d retries: %s", task_id, retries, exc)
                await db.mark_failed(task_id, worker_id, str(exc))
                return False

            backoff = 2 ** retries
            log.warning(
                "task=%d error (attempt %d/%d) — backoff %ds: %s",
                task_id, retries, max_retries, backoff, exc,
            )
            await asyncio.sleep(backoff)


# ---------------------------------------------------------------------------
# Heartbeat loop — runs concurrently with the main work loop
# ---------------------------------------------------------------------------

async def heartbeat_loop(worker_id: str, counters: dict):
    while not counters["stop"]:
        await db.upsert_heartbeat(worker_id, counters["done"], counters["fail"])
        await asyncio.sleep(HEARTBEAT_EVERY)


# ---------------------------------------------------------------------------
# Main worker loop
# ---------------------------------------------------------------------------

async def main(
    worker_id:  str,
    max_calls:  int,
    window:     int,
    max_retries: int,
    rate_key:   str,
):
    log = make_logger(worker_id)
    await db.setup_tables()

    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        raise RuntimeError("GEMINI_API_KEY not set")

    client  = genai.Client(api_key=api_key)
    limiter = PgRateLimiter(key=rate_key, max_calls=max_calls, window_seconds=window)

    counters = {"done": 0, "fail": 0, "stop": False}
    hb       = asyncio.create_task(heartbeat_loop(worker_id, counters))

    log.info("started — limit %d/%ds  key='%s'", max_calls, window, rate_key)

    try:
        while True:
            task = await db.claim_next_task(worker_id)

            if task is None:
                log.info("queue empty — polling in %ds", QUEUE_POLL_EVERY)
                await asyncio.sleep(QUEUE_POLL_EVERY)
                continue

            log.info("claimed task=%d", task["task_id"])
            ok = await process_task(task, worker_id, limiter, client, max_retries, log)

            if ok:
                counters["done"] += 1
            else:
                counters["fail"] += 1

    except KeyboardInterrupt:
        log.info("interrupted")
    finally:
        counters["stop"] = True
        hb.cancel()
        await db.upsert_heartbeat(worker_id, counters["done"], counters["fail"])
        log.info("shutdown — done=%d  fail=%d", counters["done"], counters["fail"])


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Rate-limited Gemini worker — Postgres-distributed"
    )
    parser.add_argument("--worker-id",  required=True,         help="Unique label for this process")
    parser.add_argument("--max-calls",  type=int, default=15,  help="Calls allowed per window")
    parser.add_argument("--window",     type=int, default=60,  help="Window size in seconds")
    parser.add_argument("--retries",    type=int, default=3,   help="Max retries on real errors")
    parser.add_argument("--rate-key",   default="gemini:rpm",  help="Shared rate limit key")
    args = parser.parse_args()

    asyncio.run(main(
        worker_id   = args.worker_id,
        max_calls   = args.max_calls,
        window      = args.window,
        max_retries = args.retries,
        rate_key    = args.rate_key,
    ))
