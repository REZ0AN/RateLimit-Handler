# Naive Rate Limit Handler

Counter-based approach — sleeps every N requests with a fixed wait.

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
| `--max-calls` | `5` | Requests before forced wait |
| `--wait` | `60` | Fixed wait in seconds |
| `--requests` | `12` | Total requests to run |

## Example

```bash
python handler.py --api-key YOUR_KEY --max-calls 5 --wait 60 --requests 12
```