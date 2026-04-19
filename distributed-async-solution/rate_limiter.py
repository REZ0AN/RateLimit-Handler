"""
PgRateLimiter — distributed sliding-window rate limiter backed by Postgres.

Replaces asyncio.Lock() + deque from Stages 2, 3, and 4.

The sliding window now lives in the `rate_limit_window` table shared by all
workers. A Postgres advisory lock serialises all workers through the critical
section: only one worker per key can check-and-insert at a time.

Stage 2 asked: "how many requests have I made?"          (counter)
Stage 3 asked: "how many in the last N seconds?"         (deque, one process)
Stage 5 asks:  "how many in the last N seconds, across   (Postgres, N processes)
                all workers sharing this database?"
"""

import asyncio
import logging
from datetime import datetime, timezone, timedelta

import db

log = logging.getLogger(__name__)


class TooManyRequestsError(Exception):
    """
    Raised when the external API responds with a 429 / rate-limit error.

    Unlike real application errors, a 429 does not consume a retry attempt —
    the request was never processed. The caller sleeps `retry_after` seconds
    then tries again.
    """
    def __init__(self, retry_after: float = 60.0):
        self.retry_after = retry_after
        super().__init__(f"Rate limited by API — retry after {retry_after}s")


class PgRateLimiter:
    """
    Distributed sliding-window rate limiter using Postgres advisory locks.

    All worker processes sharing the same `key` and database will collectively
    never exceed `max_calls` within any `window_seconds` rolling period.

    Usage
    -----
        limiter = PgRateLimiter(key="gemini:rpm", max_calls=15, window_seconds=60)
        wait_used = await limiter.wait_if_needed(task_id, worker_id)
        # safe to fire exactly one API call now

    How it works
    ------------
    wait_if_needed() loops, calling try_acquire_slot() each iteration.

    try_acquire_slot() opens a Postgres transaction and calls the SQL function
    try_acquire_slot(key, max_calls, window_seconds), which:

      1. Acquires pg_advisory_xact_lock(hashtext(key))
         → all other workers calling with the same key block here.
      2. DELETEs expired rows from rate_limit_window (eviction).
      3. COUNTs remaining rows for this key.
      4. INSERTs a new row if count < max_calls  → returns TRUE.
         Otherwise                                → returns FALSE.
      5. Transaction commits → advisory lock released → next worker unblocks.

    On FALSE, the limiter reads the oldest timestamp from the window and sleeps
    exactly until that slot expires, then retries.
    """

    def __init__(
        self,
        key: str = "gemini:rpm",
        max_calls: int = 15,
        window_seconds: int = 60,
    ):
        self.key = key
        self.max_calls = max_calls
        self.window_seconds = window_seconds

    async def wait_if_needed(self, task_id: int, worker_id: str) -> float:
        """
        Block until a slot is available for one API call.
        Returns total seconds spent waiting (recorded in tasks.wait_used).
        """
        total_waited = 0.0

        while True:
            granted = await self._try_acquire()

            if granted:
                return total_waited

            # Window is full — compute exact sleep duration.
            wait_secs = await self._seconds_until_slot_free()
            wait_secs = max(wait_secs, 0) + 0.05  # buffer for clock skew

            log.info(
                "[%s] task=%d window full (%d/%d) — sleeping %.2fs",
                worker_id, task_id, self.max_calls, self.max_calls, wait_secs,
            )
            await db.log_event(task_id, worker_id, "rate_limited", f"sleeping {wait_secs:.2f}s")
            await db.add_wait_time(task_id, wait_secs)
            total_waited += wait_secs
            await asyncio.sleep(wait_secs)

    async def _try_acquire(self) -> bool:
        """
        Call try_acquire_slot() inside a transaction.
        The advisory lock is held for the transaction duration and released
        automatically on commit.
        """
        pool = await db.get_pool()
        async with pool.acquire() as conn:
            async with conn.transaction():
                result = await conn.fetchval(
                    "SELECT try_acquire_slot($1, $2, $3)",
                    self.key,
                    self.max_calls,
                    self.window_seconds,
                )
        return bool(result)

    async def _seconds_until_slot_free(self) -> float:
        """
        How long until the oldest slot in the current window expires.
        Returns 1.0 as a safe fallback if no rows exist (edge case).
        """
        oldest_ts = await db.oldest_slot_ts(self.key)
        if oldest_ts is None:
            return 1.0

        # oldest_ts is TIMESTAMPTZ — already timezone-aware.
        expiry = oldest_ts + timedelta(seconds=self.window_seconds)
        now    = datetime.now(tz=timezone.utc)
        return (expiry - now).total_seconds()
