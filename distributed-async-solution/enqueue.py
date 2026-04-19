"""
enqueue.py — populate the tasks table before starting workers.

Separating enqueue from execution means workers can be restarted without
re-submitting tasks. On restart, workers pick up where they left off by
querying WHERE status = 'pending'.

Usage:
    python enqueue.py --requests 30
"""

import asyncio
import argparse
import logging

import db

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [enqueue] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

PROMPTS = [
    "Explain the concept of {i} in one sentence.",
    "What are {i} uses of Python in data science?",
    "Write a haiku about the number {i}.",
    "What would a world with {i} suns look like?",
    "Describe colour #{i} in the spectrum.",
]


def make_prompts(n: int) -> list[str]:
    return [PROMPTS[i % len(PROMPTS)].replace("{i}", str(i + 1)) for i in range(n)]


async def main(n: int):
    await db.setup_tables()
    prompts  = make_prompts(n)
    task_ids = await db.enqueue_tasks(prompts)
    log.info(
        "Enqueued %d tasks  (task_id %d → %d)",
        len(task_ids), task_ids[0], task_ids[-1],
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Enqueue tasks for Postgres-distributed workers")
    parser.add_argument("--requests", type=int, default=20, help="Number of tasks to enqueue")
    args = parser.parse_args()
    asyncio.run(main(args.requests))
