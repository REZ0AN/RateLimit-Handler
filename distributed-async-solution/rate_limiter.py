"""
PgRateLimiter — distributed sliding-window rate limiter (Stage 7).

Change vs Stage 6
-----------------
wait_if_needed() now takes (run_id, task_id, worker_id) to match the
Stage 7 db.log_event and db.add_wait_time signatures which require run_id.
Previously these were called with old Stage 6 signatures causing silent
failures whenever the window was full — rate_limited events were never
logged and wait_used was never updated.
"""

import asyncio
import logging
from datetime import datetime, timezone, timedelta

import db

log = logging.getLogger(__name__)


class TooManyRequestsError(Exception):
    def __init__(self, retry_after: float = 60.0):
        self.retry_after = retry_after
        super().__init__(f"Rate limited by API — retry after {retry_after}s")


class PgRateLimiter:
    """
    Distributed sliding-window rate limiter using Postgres advisory locks.

    All workers sharing the same key and database collectively never exceed
    max_calls within any window_seconds rolling period.
    """

    def __init__(self, key: str = "gemini:rpm", max_calls: int = 15,
                 window_seconds: int = 60):
        self.key            = key
        self.max_calls      = max_calls
        self.window_seconds = window_seconds

    async def wait_if_needed(
        self,
        run_id:    str,
        task_id:   str,
        worker_id: str,
    ) -> float:
        """
        Block until a slot is available. Returns total seconds waited.
        Logs rate_limited events and updates wait_used with correct signatures.
        """
        total_waited = 0.0

        while True:
            granted = await self._try_acquire()

            if granted:
                return total_waited

            wait_secs = await self._seconds_until_slot_free()
            wait_secs = max(wait_secs, 0) + 0.05  # small buffer for clock skew

            log.info("[%s] task=%s window full — sleeping %.2fs",
                     worker_id, task_id[:8], wait_secs)

            # Both calls now use correct Stage 7 signatures
            await db.log_event(run_id, task_id, worker_id,
                               "rate_limited", f"sleeping {wait_secs:.2f}s")
            await db.add_wait_time(run_id, task_id, wait_secs)

            total_waited += wait_secs
            await asyncio.sleep(wait_secs)

    async def _try_acquire(self) -> bool:
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
        oldest_ts = await db.oldest_slot_ts(self.key)
        if oldest_ts is None:
            return 1.0
        expiry = oldest_ts + timedelta(seconds=self.window_seconds)
        now    = datetime.now(tz=timezone.utc)
        return (expiry - now).total_seconds()