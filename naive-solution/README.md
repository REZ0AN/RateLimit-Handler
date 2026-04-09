# Rate Limit Handling - MOD solution (Naive Approach)

## What It Does

A simple rate limiter that counts successful requests and forces a fixed pause every time the count hits a multiple of `max_calls`. No queues, no timestamps, no backoff, just a counter and a sleep.

---

## How It Works

The entire logic is one `if` block:

```python
if count > 0 and count % max_calls == 0:
    time.sleep(wait)
```

That's it. Every time `count` is divisible by `max_calls`, the program sleeps for a fixed number of seconds before continuing.

```
max_calls = 5, wait = 60s

Request 1  → count=1  → 1 % 5 = 1  → no wait
Request 2  → count=2  → 2 % 5 = 2  → no wait
Request 3  → count=3  → 3 % 5 = 3  → no wait
Request 4  → count=4  → 4 % 5 = 4  → no wait
Request 5  → count=5  → 5 % 5 = 0  → WAIT 60s ← forced pause
Request 6  → count=6  → 6 % 5 = 1  → no wait
Request 7  → count=7  → 7 % 5 = 2  → no wait
...
Request 10 → count=10 → 10 % 5 = 0 → WAIT 60s ← forced pause again
```

---

## Why This Works (Sometimes)

If your requests are fast (< 1s each), this gives the API a guaranteed breathing room every N requests. It's predictable, easy to debug, and requires zero understanding of timestamps or queues.

---

## Why This Breaks (Most of the Time)

### Problem 1, It ignores actual time

The limiter doesn't care *when* requests were made, only *how many*. If each request takes 8 seconds (like with Gemma), 5 requests already span 40 seconds, you're well within the 60s window. But the code still forces a 60s wait after request 5 even though you haven't come close to the limit.

```
Request 1 at t=0s
Request 2 at t=8s
Request 3 at t=16s
Request 4 at t=24s
Request 5 at t=32s  → forces 60s wait ← unnecessary! only 32s elapsed
Request 6 at t=92s  ← wasted 60 seconds for no reason
```

**Result:** Slower than it needs to be.

### Problem 2, It can still exceed the rate limit

If your requests are very fast (< 1s), you can fire 5 requests in 2 seconds, wait 60 seconds, fire 5 more in 2 seconds, and be within the rule technically. But if the API uses a rolling window instead of a fixed one, requests near the boundary of each wait cycle can collide.

```
t=58s  requests 4 and 5 fire
t=60s  wait ends
t=60s  requests 6 and 7 fire immediately

→ requests 5, 6, 7 all within a 2s span → possible 429
```

**Result:** Still hits rate limits in edge cases.

### Problem 3, No retry logic

If a request fails (503, timeout, network error), the code logs the error and moves on. The request is dropped permanently, no second chance.

```python
except Exception as e:
    logger(f"Failed: {e}", error=True)
    # ← count not incremented, but request is just gone
```

**Result:** Data loss on transient failures.

### Problem 4, The wait is always the same

Whether the API is under heavy load or running perfectly, the sleep is always `wait` seconds. There's no adaptation, it sleeps the same amount after request 5 even if request 5 succeeded in 50ms.

---

## When This Is Acceptable

- Quick throwaway scripts where losing a request doesn't matter
- APIs that are extremely forgiving and rarely fail
- You fully control the request speed and know it's always slow enough

---

## What's Missing (and What Comes Next)

| Gap in naive approach | Solution in advanced version |
|---|---|
| Ignores actual elapsed time | Sliding window tracks real timestamps |
| Fixed sleep wastes time | Sleeps only the exact minimum needed |
| No retry on failure | Exponential backoff retries transient errors |
| 429 crashes the program | `TooManyRequestsError` catches and waits |
| Count resets if process restarts | Timestamp queue survives across the run |

The naive approach answers the question *"how many requests have I made?"*

The sliding window answers the question *"how many requests have I made **in the last N seconds**?"*

That shift from counting to timing is the core difference, and it's what makes the advanced version both faster and more correct.