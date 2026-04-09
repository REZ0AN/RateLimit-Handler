import time
import argparse
from collections import deque
from functools import wraps
import logging
from google import genai
from google.genai.errors import ClientError

logging.basicConfig(level=logging.INFO, format='[%(levelname)s] - %(message)s')
# Silence google-genai internal HTTP logs
logging.getLogger("google_genai").setLevel(logging.WARNING)
logging.getLogger("httpx").setLevel(logging.WARNING)

def logger(message, error=False):
    logging.log(logging.ERROR if error else logging.INFO, message)


class TooManyRequestsError(Exception):
    """Raised on 429 from Gemini. Carries retry_after seconds."""
    def __init__(self, retry_after: float):
        self.retry_after = retry_after
        super().__init__(f"Server rate limited. Retry after {retry_after:.2f}s")


class RateLimiter:
    def __init__(self, max_calls: int, window: int, max_retries: int):
        self.max_calls = max_calls
        self.window = window
        self.max_retries = max_retries
        self.tasks = deque()
        self._start_time = time.monotonic()

    def _elapsed(self):
        return time.monotonic() - self._start_time

    def wait_if_needed(self):
        current_time = time.monotonic()
        while self.tasks and self.tasks[0]["timestamp"] < current_time - self.window:
            self.tasks.popleft()
        if len(self.tasks) >= self.max_calls:
            wait_time = self.tasks[0]["timestamp"] + self.window - current_time
            logger(f"Client rate limit reached. Waiting {wait_time:.2f}s... [window {len(self.tasks)}/{self.max_calls}]")
            time.sleep(max(wait_time, 0))

    def add_task(self, task_id, started_at, waited):
        finished_at = time.monotonic()
        self.tasks.append({"task_id": task_id, "timestamp": finished_at})
        now = time.monotonic()
        in_window = [t for t in self.tasks if t["timestamp"] >= now - self.window]
        logger(
            f"[t={self._elapsed():.2f}s] {task_id} | "
            f"waited={waited:.2f}s | "
            f"exec={(finished_at - started_at - waited):.2f}s | "
            f"total={(finished_at - started_at):.2f}s | "
            f"window=[{', '.join(t['task_id'] for t in in_window)}] ({len(in_window)}/{self.max_calls})"
        )

    def rate_limited(self, func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            started_at = time.monotonic()
            wait_start = time.monotonic()
            self.wait_if_needed()
            waited = time.monotonic() - wait_start
            result = func(*args, **kwargs)
            self.add_task(kwargs.get("task_id") or "unknown", started_at, waited)
            return result
        return wrapper

    def retry_on_failure(self, func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            retries = 0
            while retries < self.max_retries:
                try:
                    return func(*args, **kwargs)
                except TooManyRequestsError as e:
                    # Server-side 429: trust the server, don't burn a retry
                    logger(f"Server 429. Waiting {e.retry_after:.2f}s as instructed...", error=True)
                    time.sleep(e.retry_after)
                except Exception as e:
                    logger(f"Error: {e}. Retrying {kwargs.get('task_id')} ({retries + 1}/{self.max_retries})", error=True)
                    retries += 1
                    time.sleep(2 ** retries)
            logger("Max retries reached. Giving up.", error=True)
            raise Exception("Max retries reached")
        return wrapper


def parse_args():
    parser = argparse.ArgumentParser(description="Rate Limiter — Gemini RPM Demo")
    parser.add_argument("--api-key",     type=str, required=True,  help="Gemini API key")
    parser.add_argument("--max-calls",   type=int, default=15,      help="Client-side max calls per window (default: 15)")
    parser.add_argument("--window",      type=int, default=60,     help="Window in seconds (default: 60)")
    parser.add_argument("--max-retries", type=int, default=4,      help="Max retries on failure (default: 4)")
    parser.add_argument("--requests",    type=int, default=20,     help="Total requests to simulate (default: 12)")
    return parser.parse_args()


def main():
    args = parse_args()

    client = genai.Client(api_key=args.api_key)

    logger(f"Starting — Client limit: {args.max_calls} calls/{args.window}s | "
           f"max_retries: {args.max_retries} | total: {args.requests} requests")
    logger(f"Gemini Gemma-4 26B free tier limit: 15 RPM")
    logger("-" * 70)

    rate_limiter = RateLimiter(
        max_calls=args.max_calls,
        window=args.window,
        max_retries=args.max_retries
    )

    prompts = [
        "What is a binary search tree? Three words simply.",
        "What is a linked list? Three words simply.",
        "What is a hash map? Three words simply.",
        "What is a queue? Three words simply.",
        "What is a stack? Three words simply.",
        "What is dynamic programming? Three words simply.",
        "What is recursion? Three words simply.",
        "What is Big O notation? Three words simply.",
        "What is a graph in CS? Three words simply.",
        "What is memoization? Three words simply.",
        "What is a heap? Three words simply.",
        "What is a trie? Three words simply.",
    ]

    @rate_limiter.rate_limited
    @rate_limiter.retry_on_failure
    def ask_gemini(task_id, prompt):
        try:
            response = client.models.generate_content(
                model="gemma-4-26b-a4b-it",
                contents=prompt,
                config={
                    "maxOutputTokens": 10,
                }
            )
            first_five = ' '.join(response.text.strip().split()[:5])
            logger(f"Done — {first_five}...")
        except ClientError as e:
            # Gemini raises ClientError with status 429 when rate limited
            if e.status == 429:
                # Default to 60s if no retry info in the error
                retry_after = 60.0
                raise TooManyRequestsError(retry_after)
            raise  # re-raise other client errors

    for i in range(args.requests):
        logger(f"--- Request {i + 1}/{args.requests} ---")
        try:
            prompt = prompts[i % len(prompts)]
            ask_gemini(task_id=f"task_{i + 1}", prompt=prompt)
        except Exception as e:
            logger(f"Dropped task_{i + 1}: Failed", error=True)


if __name__ == "__main__":
    main()