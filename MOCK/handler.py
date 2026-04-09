import time
import random
import argparse
from collections import deque
from functools import wraps
import logging

logging.basicConfig(level=logging.DEBUG, format='[%(levelname)s] - %(message)s')

def logger(message, error=False):
    log_level = logging.ERROR if error else logging.INFO
    logging.log(log_level, message)


class RateLimiter:
    def __init__(self, max_calls: int, window: int, max_retries: int):
        self.max_calls = max_calls
        self.window = window
        self.max_retries = max_retries
        self.tasks = deque()
        self._start_time = time.monotonic()  # program start reference

    def _elapsed(self):
        return time.monotonic() - self._start_time

    def wait_if_needed(self):
        current_time = time.monotonic()

        while self.tasks and self.tasks[0]["timestamp"] < current_time - self.window:
            self.tasks.popleft()

        if len(self.tasks) >= self.max_calls:
            wait_time = self.tasks[0]["timestamp"] + self.window - current_time
            logger(f"Rate limit exceeded. Waiting for {wait_time:.2f}s... "
                   f"[window has {len(self.tasks)}/{self.max_calls} tasks]")
            time.sleep(max(wait_time, 0))

    def add_task(self, task_id, started_at, waited):
        finished_at = time.monotonic()
        self.tasks.append({"task_id": task_id, "timestamp": finished_at})

        # window snapshot: show which tasks are currently inside the window
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
                except Exception as e:
                    logger(f"Error: {e}. Retrying... {kwargs.get('task_id') or 'unknown'} ({retries + 1}/{self.max_retries})", error=True)
                    retries += 1
                    time.sleep(2 ** retries)
            logger("Max retries reached. Giving up.", error=True)
            raise Exception("Max retries reached")
        return wrapper


def parse_args():
    parser = argparse.ArgumentParser(description="Rate Limiter Demo")
    parser.add_argument("--max-calls",   type=int, default=5,  help="Max requests per window (default: 5)")
    parser.add_argument("--window",      type=int, default=10, help="Window size in seconds (default: 10)")
    parser.add_argument("--max-retries", type=int, default=3,  help="Max retries on failure (default: 3)")
    parser.add_argument("--requests",    type=int, default=10, help="Total requests to simulate (default: 10)")
    return parser.parse_args()


def main():
    args = parse_args()

    logger(f"Starting with max_calls={args.max_calls}, window={args.window}s, "
           f"max_retries={args.max_retries}, total_requests={args.requests}")
    logger("-" * 70)

    rate_limiter = RateLimiter(
        max_calls=args.max_calls,
        window=args.window,
        max_retries=args.max_retries
    )

    @rate_limiter.rate_limited
    @rate_limiter.retry_on_failure
    def make_request(task_id):
        time.sleep(random.uniform(1, 2))
        if random.random() < 0.3:
            raise Exception("Simulated request failure")
        logger(f"Request successful! Task ID: {task_id}")

    for i in range(args.requests):
        logger(f"--- Request {i + 1}/{args.requests} ---")
        try:
            make_request(task_id=f"task_{i + 1}")
        except Exception as e:
            logger(f"Request ultimately failed: {e}", error=True)


if __name__ == "__main__":
    main()