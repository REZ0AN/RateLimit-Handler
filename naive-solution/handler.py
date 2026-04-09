import time
import argparse
import logging
from google import genai

logging.basicConfig(level=logging.INFO, format='[%(levelname)s] - %(message)s')
# Silence google-genai internal HTTP logs
logging.getLogger("google_genai").setLevel(logging.WARNING)
logging.getLogger("httpx").setLevel(logging.WARNING)

def logger(message, error=False):
    logging.log(logging.ERROR if error else logging.INFO, message)


def parse_args():
    parser = argparse.ArgumentParser(description="Naive Rate Limiter Demo")
    parser.add_argument("--api-key",   type=str, required=True)
    parser.add_argument("--max-calls", type=int, default=5,  help="Requests before forced wait (default: 5)")
    parser.add_argument("--wait",      type=int, default=60, help="Fixed wait in seconds (default: 60)")
    parser.add_argument("--requests",  type=int, default=12, help="Total requests (default: 12)")
    return parser.parse_args()


def main():
    args = parse_args()
    client = genai.Client(api_key=args.api_key)

    logger(f"Starting — max_calls={args.max_calls} | fixed_wait={args.wait}s | total={args.requests}")
    logger("-" * 70)

    prompts = [
        "BST? 3 words.",
        "Linked list? 3 words.",
        "Hash map? 3 words.",
        "Queue? 3 words.",
        "Stack? 3 words.",
        "Dynamic programming? 3 words.",
        "Recursion? 3 words.",
        "Big O? 3 words.",
        "Graph in CS? 3 words.",
        "Memoization? 3 words.",
        "Heap? 3 words.",
        "Trie? 3 words.",
    ]

    count = 0  # tracks how many requests have been made

    for i in range(args.requests):
        logger(f"--- Request {i + 1}/{args.requests} ---")

        # if count is divisible by max_calls, force a fixed wait
        if count > 0 and count % args.max_calls == 0:
            logger(f"Reached {args.max_calls} requests. Waiting {args.wait}s...")
            time.sleep(args.wait)

        try:
            prompt = prompts[i % len(prompts)] # cycle through prompts
            response = client.models.generate_content(
                model="gemma-4-26b-a4b-it",
                contents=prompt,
                config={"max_output_tokens": 10}
            )
            first_five = ' '.join(response.text.strip().split()[:5])
            logger(f"Done — {first_five}...")
            count += 1  # only increment on success
        except Exception as e:
            logger(f"Failed: {e}", error=True)


if __name__ == "__main__":
    main()