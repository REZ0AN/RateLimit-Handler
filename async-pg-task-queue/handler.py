import asyncio
import time
import argparse
from collections import deque
import logging
from dotenv import load_dotenv
load_dotenv()

from google import genai
from google.genai.errors import ClientError
import db

logging.basicConfig(level=logging.INFO, format='[%(levelname)s] - %(message)s')
logging.getLogger("google_genai").setLevel(logging.WARNING)
logging.getLogger("httpx").setLevel(logging.WARNING)

def logger(message, error=False):
    logging.log(logging.ERROR if error else logging.INFO, message)


# ── rate limiter (unchanged) ──────────────────────────────────────────────────
class RateLimiter:
    def __init__(self, max_calls, window):
        self.max_calls = max_calls
        self.window    = window
        self.tasks     = deque()
        self._lock     = asyncio.Lock()

    async def wait_if_needed(self) -> float:
        while True:
            async with self._lock:
                now = time.monotonic()
                while self.tasks and self.tasks[0] < now - self.window:
                    self.tasks.popleft()
                if len(self.tasks) < self.max_calls:
                    self.tasks.append(now)
                    logger(f"Slot claimed [{len(self.tasks)}/{self.max_calls}]")
                    return 0.0
                wait = self.tasks[0] + self.window - now
                logger(f"Window full [{len(self.tasks)}/{self.max_calls}]. Waiting {wait:.2f}s...")

            t0 = time.monotonic()
            await asyncio.sleep(max(wait, 0.1))
            return time.monotonic() - t0


# ── coroutine: receives task from enqueue, owns the full lifecycle ────────────
async def ask_gemini(client, rate_limiter, task_id, prompt, max_retries=4):
    """
    Flow:
      enqueue() inserts task as 'pending'
      ↓
      ask_gemini() calls mark_running() — transitions pending → running
      ↓
      rate limiter gates the Gemini call
      ↓
      mark_done() or mark_failed() closes the task
    """
    # transition pending → running atomically
    # returns False if another coroutine/process already claimed this task
    claimed = await db.mark_running(task_id)
    if not claimed:
        logger(f"{task_id} → already claimed, skipping", error=True)
        return

    await db.log_event(task_id, "started")
    retries = 0

    while retries < max_retries:

        # wait for a rate-limit slot
        waited = await rate_limiter.wait_if_needed()
        if waited > 0:
            await db.add_wait(task_id, waited)
            await db.log_event(task_id, "rate_limited", f"waited={waited:.1f}s")

        try:
            t0   = time.monotonic()
            resp = await client.aio.models.generate_content(
                model="gemma-4-26b-a4b-it",
                contents=prompt,
                config={"maxOutputTokens": 50}
            )
            elapsed = time.monotonic() - t0

            await db.mark_done(task_id)
            await db.log_event(task_id, "success", f"latency={elapsed:.1f}s")
            logger(f"{task_id} ✓ ({elapsed:.1f}s) → {resp.text.strip()[:60]}")
            return

        except ClientError as e:
            if e.status == 429:
                logger(f"{task_id} → 429. Waiting 60s...", error=True)
                await db.log_event(task_id, "rate_limited", "server 429 wait=60s")
                await db.add_wait(task_id, 60)
                await asyncio.sleep(60)
                # does NOT increment retries — request was never processed

            else:
                retries += 1
                wait = 2 ** retries
                logger(f"{task_id} → error {e.status}. Retry {retries}/{max_retries} in {wait}s", error=True)
                await db.log_event(task_id, "error", f"status={e.status} retry={retries}")
                await asyncio.sleep(wait)

        except Exception as e:
            retries += 1
            wait = 2 ** retries
            logger(f"{task_id} → unexpected: {e}. Retry {retries}/{max_retries} in {wait}s", error=True)
            await db.log_event(task_id, "error", f"{e} retry={retries}")
            await asyncio.sleep(wait)

    await db.mark_failed(task_id)
    await db.log_event(task_id, "dropped", "max retries exhausted")
    logger(f"{task_id} → dropped after {max_retries} retries.", error=True)


# ── main ──────────────────────────────────────────────────────────────────────
async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--api-key",   required=True)
    parser.add_argument("--max-calls", type=int, default=15)
    parser.add_argument("--window",    type=int, default=60)
    parser.add_argument("--requests",  type=int, default=24)
    args = parser.parse_args()

    client       = genai.Client(api_key=args.api_key)
    rate_limiter = RateLimiter(args.max_calls, args.window)

    prompts = [
        "What is a binary search tree? One sentence.",
        "What is a linked list? One sentence.",
        "What is a hash map? One sentence.",
        "What is a queue? One sentence.",
        "What is a stack? One sentence.",
        "What is dynamic programming? One sentence.",
        "What is recursion? One sentence.",
        "What is Big O notation? One sentence.",
        "What is a graph in CS? One sentence.",
        "What is memoization? One sentence.",
        "What is a heap? One sentence.",
        "What is a trie? One sentence.",
        "What is a binary search tree? One sentence.",
        "What is a linked list? One sentence.",
        "What is a hash map? One sentence.",
        "What is a queue? One sentence.",
        "What is a stack? One sentence.",
        "What is dynamic programming? One sentence.",
        "What is recursion? One sentence.",
        "What is Big O notation? One sentence.",
        "What is a graph in CS? One sentence.",
        "What is memoization? One sentence.",
        "What is a heap? One sentence.",
        "What is a trie? One sentence.",
    ]

    await db.init_db()

    # enqueue inserts tasks as 'pending' and returns [{task_id, prompt}]
    # the coroutines themselves transition pending → running via mark_running()
    _prompts = [prompts[i%len(prompts)] for i in range(args.requests)]  # slice prompts to requested number
    tasks = await db.enqueue(_prompts)

    logger(f"Starting {len(tasks)} tasks | limit: {args.max_calls} RPM")
    logger("-" * 60)
    t0 = time.monotonic()

    await asyncio.gather(*[
        ask_gemini(client, rate_limiter, t["task_id"], t["prompt"])
        for t in tasks
    ])

    logger(f"All done in {time.monotonic()-t0:.1f}s")
    await db.summary()

if __name__ == "__main__":
    asyncio.run(main())