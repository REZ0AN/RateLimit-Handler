import asyncio
import time
import argparse
from collections import deque
import logging
from google import genai
from google.genai.errors import ClientError

logging.basicConfig(level=logging.INFO, format='[%(levelname)s] - %(message)s')
logging.getLogger("google_genai").setLevel(logging.WARNING)
logging.getLogger("httpx").setLevel(logging.WARNING)

def logger(message, error=False):
    logging.log(logging.ERROR if error else logging.INFO, message)


class RateLimiter:
    def __init__(self, max_calls, window):
        self.max_calls = max_calls
        self.window    = window
        self.tasks     = deque()
        self._lock     = asyncio.Lock()     # ← ADDED: one coroutine at a time

    async def wait_if_needed(self):
        while True:
            async with self._lock:          # ← ADDED: acquire before reading/writing
                now = time.monotonic()
                while self.tasks and self.tasks[0] < now - self.window:
                    self.tasks.popleft()

                if len(self.tasks) < self.max_calls:
                    # slot is free — claim it and return
                    self.tasks.append(now)
                    logger(f"Slot claimed [{len(self.tasks)}/{self.max_calls}]")
                    return                  # ← releases lock here

                # window full — calculate wait
                wait = self.tasks[0] + self.window - now
                logger(f"Window full [{len(self.tasks)}/{self.max_calls}]. Waiting {wait:.2f}s...")

            # ← lock is released BEFORE sleeping
            # other coroutines can now enter the lock while this one sleeps
            await asyncio.sleep(max(wait, 0.1))
            # after waking, loop back and re-check (window may still be full)


async def ask_gemini(client, rate_limiter, task_id, prompt, max_retries=4):
    retries = 0

    while retries < max_retries:
        await rate_limiter.wait_if_needed()

        try:
            t0   = time.monotonic()
            resp = await client.aio.models.generate_content(
                model="gemma-4-26b-a4b-it",
                contents=prompt,
                config={"maxOutputTokens": 50}
            )
            elapsed = time.monotonic() - t0
            logger(f"{task_id} ✓ ({elapsed:.1f}s) → {resp.text.strip()[:60]}")
            return resp.text.strip()

        except ClientError as e:
            if e.status == 429:
                # server-side rate limit — flat 60s wait, does NOT count as a retry
                # the request was never processed so we shouldn't penalise the task
                logger(f"{task_id} → 429. Waiting 60s (not a retry)...", error=True)
                await asyncio.sleep(60)
                # loop back to wait_if_needed() and try again

            else:
                # real error (500, 503, timeout etc.) — exponential backoff
                retries += 1
                wait = 2 ** retries          # 2s, 4s, 8s, 16s
                logger(
                    f"{task_id} → error {e.status}. "
                    f"Retry {retries}/{max_retries} in {wait}s...",
                    error=True
                )
                await asyncio.sleep(wait)

        except Exception as e:
            # unexpected error — also counts as a retry
            retries += 1
            wait = 2 ** retries
            logger(
                f"{task_id} → unexpected: {e}. "
                f"Retry {retries}/{max_retries} in {wait}s...",
                error=True
            )
            await asyncio.sleep(wait)

    logger(f"{task_id} → dropped after {max_retries} retries.", error=True)


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--api-key",   required=True)
    parser.add_argument("--max-calls", type=int, default=15)
    parser.add_argument("--window",    type=int, default=60)
    parser.add_argument("--requests",  type=int, default=20)
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
    ]

    coroutines = [
        ask_gemini(client, rate_limiter, f"task_{i+1}", prompts[i % len(prompts)])
        for i in range(args.requests)
    ]

    logger(f"Starting {len(coroutines)} tasks | limit: {args.max_calls} RPM")
    logger("-" * 60)
    t0 = time.monotonic()

    await asyncio.gather(*coroutines)

    logger(f"All done in {time.monotonic()-t0:.1f}s")

if __name__ == "__main__":
    asyncio.run(main())