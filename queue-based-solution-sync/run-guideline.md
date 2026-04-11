# Queue Based Rate Limit Handler

Sliding window approach, tracks real timestamps, retries failures with exponential backoff.

## Setup

```bash
pip install google-genai
```

## Run

```bash
python handler.py --api-key YOUR_KEY
```

## Options

| Flag | Default | Description |
|---|---|---|
| `--api-key` | required | Gemini API key |
| `--max-calls` | `15` | Max requests per window |
| `--window` | `60` | Window size in seconds |
| `--max-retries` | `4` | Retry attempts before dropping |
| `--requests` | `20` | Total requests to run |

## Example

```bash
python handler.py --api-key YOUR_KEY --max-calls 5 --window 60 --max-retries 3 --requests 12
```