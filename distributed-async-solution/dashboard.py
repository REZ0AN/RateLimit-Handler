"""
dashboard.py — live Streamlit dashboard for the Postgres-distributed worker.

Reads from tasks, events, worker_heartbeat, and rate_limit_window.
Refreshes every 3 seconds. Run alongside workers:

    streamlit run dashboard.py

Fix vs original
---------------
active_workers() no longer references tasks_done / tasks_fail columns on
worker_heartbeat (those columns don't exist — counts live in the events
table). Counts are now derived from events via a LEFT JOIN so the dashboard
shows accurate per-worker done/fail totals without any schema change.
"""

import os
import time
import asyncio
import asyncpg
import pandas as pd
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL     = os.getenv("DATABASE_URL")
REFRESH_INTERVAL = 3   # seconds


# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="Rate Limit Handler — Stage 5",
    layout="wide",
    initial_sidebar_state="collapsed",
)


# ---------------------------------------------------------------------------
# DB helpers (sync wrappers for Streamlit's sync context)
# ---------------------------------------------------------------------------

def _run(coro):
    try:
        loop = asyncio.get_event_loop()
        if loop.is_closed():
            raise RuntimeError
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    return loop.run_until_complete(coro)


async def _fetch(sql: str) -> list[dict]:
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        rows = await conn.fetch(sql)
        return [dict(r) for r in rows]
    finally:
        await conn.close()


def query(sql: str) -> pd.DataFrame:
    rows = _run(_fetch(sql))
    return pd.DataFrame(rows) if rows else pd.DataFrame()


# ---------------------------------------------------------------------------
# Data fetchers
# ---------------------------------------------------------------------------

def task_counts() -> dict:
    df = query("SELECT status, COUNT(*) AS n FROM tasks GROUP BY status")
    return dict(zip(df["status"], df["n"])) if not df.empty else {}


def task_totals() -> dict:
    df = query("""
        SELECT COUNT(*)        AS total,
               SUM(wait_used)  AS wait,
               SUM(retries)    AS retries
        FROM tasks
    """)
    return df.iloc[0].to_dict() if not df.empty else {}


def active_workers() -> pd.DataFrame:
    # tasks_done / tasks_fail are NOT columns on worker_heartbeat.
    # They are derived from the events table where event_type = 'success'
    # or 'dropped'. This is the source of truth — accurate and persistent
    # across worker restarts, unlike in-memory session counters.
    return query("""
        SELECT
            h.worker_id,
            COALESCE(e.tasks_done, 0) AS tasks_done,
            COALESCE(e.tasks_fail, 0) AS tasks_fail,
            ROUND(EXTRACT(EPOCH FROM (NOW() - h.last_seen))::numeric, 1) AS secs_ago,
            CASE WHEN h.last_seen > NOW() - INTERVAL '15 seconds'
                 THEN 'active' ELSE 'idle' END AS state
        FROM worker_heartbeat h
        LEFT JOIN (
            SELECT
                worker_id,
                COUNT(*) FILTER (WHERE event_type = 'success') AS tasks_done,
                COUNT(*) FILTER (WHERE event_type = 'dropped') AS tasks_fail
            FROM events
            GROUP BY worker_id
        ) e ON e.worker_id = h.worker_id
        ORDER BY h.worker_id
    """)


def window_size() -> int:
    df = query("SELECT COUNT(*) AS n FROM rate_limit_window")
    return int(df["n"].iloc[0]) if not df.empty else 0


def event_breakdown() -> pd.DataFrame:
    return query("""
        SELECT event_type, COUNT(*) AS n
        FROM   events
        GROUP  BY event_type
        ORDER  BY n DESC
    """)


def events_timeline() -> pd.DataFrame:
    return query("""
        SELECT
            date_trunc('second', ts) -
            (EXTRACT(second FROM ts)::int % 5) * INTERVAL '1 second' AS bucket,
            event_type,
            COUNT(*) AS n
        FROM   events
        GROUP  BY 1, 2
        ORDER  BY 1
    """)


def task_table() -> pd.DataFrame:
    return query("""
        SELECT
            task_id,
            status,
            retries,
            ROUND(wait_used::numeric, 2)  AS wait_secs,
            LEFT(prompt, 55)              AS prompt,
            LEFT(result, 65)              AS result_preview,
            updated_at
        FROM tasks
        ORDER BY task_id
    """)


def wait_per_task() -> pd.DataFrame:
    return query("""
        SELECT task_id, ROUND(wait_used::numeric, 2) AS wait_secs
        FROM   tasks
        ORDER  BY task_id
    """)


def recent_events(n: int = 100) -> pd.DataFrame:
    return query(f"""
        SELECT ts, worker_id, event_type, task_id, LEFT(detail, 80) AS detail
        FROM   events
        ORDER  BY ts DESC
        LIMIT  {n}
    """)


# ---------------------------------------------------------------------------
# Layout
# ---------------------------------------------------------------------------

st.title("Rate Limit Handler — Stage 5: Postgres-distributed")
st.caption(f"Auto-refreshes every {REFRESH_INTERVAL}s  ·  source: Postgres")

counts = task_counts()
totals = task_totals()

# Row 1 — task counters
c1, c2, c3, c4, c5, c6 = st.columns(6)
c1.metric("Total",        int(totals.get("total",   0) or 0))
c2.metric("Done",         counts.get("done",    0))
c3.metric("Running",      counts.get("running", 0))
c4.metric("Pending",      counts.get("pending", 0))
c5.metric("Failed",       counts.get("failed",  0))
c6.metric("Wait (s)",     round(float(totals.get("wait", 0) or 0), 1))

st.divider()

# Row 2 — workers + rate limit window
col_w, col_rl = st.columns([2, 1])

with col_w:
    st.subheader("Active workers")
    wdf = active_workers()
    if wdf.empty:
        st.info("No workers have sent a heartbeat yet.")
    else:
        st.dataframe(wdf, use_container_width=True, hide_index=True)

with col_rl:
    st.subheader("Rate limit window")
    n = window_size()
    st.metric("Slots in use (live)", n)
    st.caption(
        "Each row = one granted API call still inside the sliding window. "
        "Rows are evicted by try_acquire_slot() automatically."
    )

st.divider()

# Row 3 — event breakdown + timeline
col_ev, col_tl = st.columns([1, 2])

with col_ev:
    st.subheader("Events")
    ev = event_breakdown()
    if not ev.empty:
        st.dataframe(ev, use_container_width=True, hide_index=True)

with col_tl:
    st.subheader("Timeline (5s buckets)")
    tl = events_timeline()
    if not tl.empty:
        pivot = tl.pivot(index="bucket", columns="event_type", values="n").fillna(0)
        st.line_chart(pivot)

st.divider()

# Row 4 — task table
st.subheader("Tasks")
tdf = task_table()
if not tdf.empty:
    st.dataframe(tdf, use_container_width=True, hide_index=True)

st.divider()

# Row 5 — wait time chart + rolling event log
col_bar, col_log = st.columns([1, 1])

with col_bar:
    st.subheader("Wait time per task (s)")
    wpt = wait_per_task()
    if not wpt.empty:
        st.bar_chart(wpt.set_index("task_id")["wait_secs"])

with col_log:
    st.subheader("Recent events")
    rev = recent_events()
    if not rev.empty:
        st.dataframe(rev, use_container_width=True, hide_index=True)

# Auto-refresh
time.sleep(REFRESH_INTERVAL)
st.rerun()