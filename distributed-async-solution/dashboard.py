"""
dashboard.py — live Streamlit dashboard (Stage 7: run_id filtering, dynamic refresh).

Changes vs previous
--------------------
1. Run selector in sidebar — lists all runs ordered by started_at DESC.
   Every query is scoped to the selected run_id via WHERE run_id = $1.
   "All runs" option available for global views.

2. Dynamic refresh interval — sidebar radio: 1s / 3s / 5s / 10s.

3. All queries updated for Stage 7 schema:
   - UUID primary keys (task_id, event_id, run_id)
   - events.run_id column (direct filter, no join through tasks)
   - worker_heartbeat PK is (worker_id, run_id)
   - tasks.worker_id column for ownership display
   - runs table shown in sidebar with status and timing

4. Run summary panel — shows run metadata (workers, rate limit, duration)
   pulled directly from the runs table + events aggregate.
"""

import os
import time
import asyncio
import asyncpg
import pandas as pd
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="Rate Limit Handler",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# DB helpers
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


async def _fetch(sql: str, *args) -> list[dict]:
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        rows = await conn.fetch(sql, *args)
        return [dict(r) for r in rows]
    finally:
        await conn.close()


async def _fetchval(sql: str, *args):
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        return await conn.fetchval(sql, *args)
    finally:
        await conn.close()


def query(sql: str, *args) -> pd.DataFrame:
    rows = _run(_fetch(sql, *args))
    return pd.DataFrame(rows) if rows else pd.DataFrame()


def scalar(sql: str, *args):
    return _run(_fetchval(sql, *args))


# ---------------------------------------------------------------------------
# Sidebar — run selector + refresh
# ---------------------------------------------------------------------------

with st.sidebar:
    st.title("⚙️ Controls")

    # Refresh interval
    st.subheader("Refresh interval")
    refresh_interval = st.radio(
        label="refresh",
        options=[1, 3, 5, 10],
        index=1,
        format_func=lambda x: f"{x}s",
        horizontal=True,
        label_visibility="collapsed",
    )

    st.divider()

    # Run selector
    st.subheader("Run")

    runs_df = query("""
        SELECT
            run_id::text                                          AS run_id,
            TO_CHAR(started_at, 'MM-DD HH24:MI:SS')             AS started,
            CASE
                WHEN finished_at IS NOT NULL
                THEN ROUND(EXTRACT(EPOCH FROM (finished_at - started_at))::numeric, 1)::text || 's'
                ELSE 'running'
            END                                                   AS duration,
            total_tasks,
            worker_count
        FROM runs
        ORDER BY started_at DESC
        LIMIT 20
    """)

    if runs_df.empty:
        st.info("No runs yet.")
        st.stop()

    # Build display labels
    run_options = {"All runs": None}
    for _, row in runs_df.iterrows():
        label = f"{row['started']}  ({row['duration']})"
        run_options[label] = row["run_id"]

    selected_label = st.selectbox(
        "Select run",
        options=list(run_options.keys()),
        label_visibility="collapsed",
    )
    selected_run_id = run_options[selected_label]

    # Show run metadata if a specific run is selected
    if selected_run_id:
        meta = runs_df[runs_df["run_id"] == selected_run_id].iloc[0]
        st.caption(f"**run_id:** `{selected_run_id[:18]}…`")
        st.caption(f"**workers:** {meta['worker_count']}  |  **tasks:** {meta['total_tasks']}")
        st.caption(f"**started:** {meta['started']}  |  **duration:** {meta['duration']}")

    st.divider()
    st.caption(f"Auto-refreshing every {refresh_interval}s")


# ---------------------------------------------------------------------------
# Scoped query helpers
# ---------------------------------------------------------------------------

def scoped_task_counts() -> dict:
    if selected_run_id:
        df = query("""
            SELECT status, COUNT(*) AS n
            FROM   tasks
            WHERE  run_id = $1::uuid
            GROUP  BY status
        """, selected_run_id)
    else:
        df = query("SELECT status, COUNT(*) AS n FROM tasks GROUP BY status")
    return dict(zip(df["status"], df["n"])) if not df.empty else {}


def scoped_task_totals() -> dict:
    if selected_run_id:
        df = query("""
            SELECT COUNT(*)       AS total,
                   SUM(wait_used) AS wait,
                   SUM(retries)   AS retries
            FROM   tasks
            WHERE  run_id = $1::uuid
        """, selected_run_id)
    else:
        df = query("""
            SELECT COUNT(*)       AS total,
                   SUM(wait_used) AS wait,
                   SUM(retries)   AS retries
            FROM   tasks
        """)
    return df.iloc[0].to_dict() if not df.empty else {}


def scoped_run_duration() -> float | None:
    if not selected_run_id:
        return None
    val = scalar("""
        SELECT EXTRACT(EPOCH FROM (
            MAX(ts) FILTER (WHERE event_type IN ('success','dropped'))
          - MIN(ts) FILTER (WHERE event_type = 'started')
        ))
        FROM events
        WHERE run_id = $1::uuid
    """, selected_run_id)
    return float(val) if val else None


def scoped_active_workers() -> pd.DataFrame:
    if selected_run_id:
        return query("""
            SELECT
                h.worker_id,
                COALESCE(e.tasks_done, 0)                                      AS done,
                COALESCE(e.tasks_fail, 0)                                      AS failed,
                ROUND(EXTRACT(EPOCH FROM (NOW() - h.last_seen))::numeric, 1)   AS secs_ago,
                CASE WHEN h.last_seen > NOW() - INTERVAL '15 seconds'
                     THEN '🟢 active' ELSE '🔴 idle' END                       AS state
            FROM worker_heartbeat h
            LEFT JOIN (
                SELECT
                    worker_id,
                    COUNT(*) FILTER (WHERE event_type = 'success') AS tasks_done,
                    COUNT(*) FILTER (WHERE event_type = 'dropped') AS tasks_fail
                FROM   events
                WHERE  run_id = $1::uuid
                GROUP  BY worker_id
            ) e ON e.worker_id = h.worker_id
            WHERE h.run_id = $1::uuid
            ORDER BY h.worker_id
        """, selected_run_id)
    else:
        return query("""
            SELECT
                h.worker_id,
                h.run_id::text                                                     AS run_id,
                COALESCE(e.tasks_done, 0)                                          AS done,
                COALESCE(e.tasks_fail, 0)                                          AS failed,
                ROUND(EXTRACT(EPOCH FROM (NOW() - h.last_seen))::numeric, 1)       AS secs_ago,
                CASE WHEN h.last_seen > NOW() - INTERVAL '15 seconds'
                     THEN '🟢 active' ELSE '🔴 idle' END                           AS state
            FROM worker_heartbeat h
            LEFT JOIN (
                SELECT worker_id, run_id,
                    COUNT(*) FILTER (WHERE event_type = 'success') AS tasks_done,
                    COUNT(*) FILTER (WHERE event_type = 'dropped') AS tasks_fail
                FROM   events
                GROUP  BY worker_id, run_id
            ) e ON e.worker_id = h.worker_id AND e.run_id = h.run_id
            ORDER BY h.run_id DESC, h.worker_id
        """)


def window_size() -> int:
    val = scalar("SELECT COUNT(*) FROM rate_limit_window")
    return int(val) if val else 0


def scoped_event_breakdown() -> pd.DataFrame:
    if selected_run_id:
        return query("""
            SELECT event_type, COUNT(*) AS n
            FROM   events
            WHERE  run_id = $1::uuid
            GROUP  BY event_type
            ORDER  BY n DESC
        """, selected_run_id)
    else:
        return query("""
            SELECT event_type, COUNT(*) AS n
            FROM   events
            GROUP  BY event_type
            ORDER  BY n DESC
        """)


def scoped_events_timeline() -> pd.DataFrame:
    if selected_run_id:
        return query("""
            SELECT
                date_trunc('second', ts) -
                (EXTRACT(second FROM ts)::int % 5) * INTERVAL '1 second' AS bucket,
                event_type,
                COUNT(*) AS n
            FROM   events
            WHERE  run_id = $1::uuid
            GROUP  BY 1, 2
            ORDER  BY 1
        """, selected_run_id)
    else:
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


def scoped_task_table() -> pd.DataFrame:
    if selected_run_id:
        return query("""
            SELECT
                LEFT(task_id::text, 8)               AS task_id,
                status,
                worker_id,
                retries,
                ROUND(wait_used::numeric, 2)          AS wait_secs,
                LEFT(prompt, 50)                      AS prompt,
                LEFT(result, 60)                      AS result_preview,
                updated_at
            FROM tasks
            WHERE run_id = $1::uuid
            ORDER BY created_at
        """, selected_run_id)
    else:
        return query("""
            SELECT
                LEFT(task_id::text, 8)               AS task_id,
                LEFT(run_id::text, 8)                AS run_id,
                status,
                worker_id,
                retries,
                ROUND(wait_used::numeric, 2)          AS wait_secs,
                LEFT(prompt, 45)                      AS prompt,
                updated_at
            FROM tasks
            ORDER BY created_at DESC
            LIMIT 200
        """)


def scoped_wait_chart() -> pd.DataFrame:
    if selected_run_id:
        return query("""
            SELECT
                LEFT(task_id::text, 8)              AS task_id,
                ROUND(wait_used::numeric, 2)         AS wait_secs
            FROM   tasks
            WHERE  run_id = $1::uuid
            ORDER  BY created_at
        """, selected_run_id)
    else:
        return query("""
            SELECT
                LEFT(task_id::text, 8)              AS task_id,
                ROUND(wait_used::numeric, 2)         AS wait_secs
            FROM   tasks
            ORDER  BY created_at DESC
            LIMIT  200
        """)


def scoped_recent_events(n: int = 100) -> pd.DataFrame:
    if selected_run_id:
        return query(f"""
            SELECT
                ts,
                worker_id,
                event_type,
                LEFT(task_id::text, 8)    AS task_id,
                LEFT(detail, 80)          AS detail
            FROM   events
            WHERE  run_id = $1::uuid
            ORDER  BY ts DESC
            LIMIT  {n}
        """, selected_run_id)
    else:
        return query(f"""
            SELECT
                ts,
                LEFT(run_id::text, 8)     AS run_id,
                worker_id,
                event_type,
                LEFT(task_id::text, 8)    AS task_id,
                LEFT(detail, 80)          AS detail
            FROM   events
            ORDER  BY ts DESC
            LIMIT  {n}
        """)


def scoped_runs_table() -> pd.DataFrame:
    return query("""
        SELECT
            LEFT(run_id::text, 18) || '…'            AS run_id,
            TO_CHAR(started_at, 'MM-DD HH24:MI:SS')  AS started_at,
            CASE
                WHEN finished_at IS NOT NULL
                THEN ROUND(EXTRACT(EPOCH FROM (finished_at - started_at))::numeric, 1)::text || 's'
                ELSE '⏳ running'
            END                                        AS duration,
            worker_count                               AS workers,
            max_calls,
            window_secs,
            total_tasks
        FROM runs
        ORDER BY started_at DESC
        LIMIT 20
    """)


# ---------------------------------------------------------------------------
# Layout
# ---------------------------------------------------------------------------

scope_label = f"run `{selected_run_id[:8]}…`" if selected_run_id else "all runs"
st.title("Rate Limit Handler")
st.caption(f"Showing: **{scope_label}**  ·  refreshes every {refresh_interval}s  ·  source: Postgres")

counts = scoped_task_counts()
totals = scoped_task_totals()
db_elapsed = scoped_run_duration()

# ── Row 1: metrics ────────────────────────────────────────────────────────────
c1, c2, c3, c4, c5, c6, c7 = st.columns(7)
c1.metric("Total",      int(totals.get("total",   0) or 0))
c2.metric("Done",       counts.get("done",    0))
c3.metric("Running",    counts.get("running", 0))
c4.metric("Pending",    counts.get("pending", 0))
c5.metric("Failed",     counts.get("failed",  0))
c6.metric("Wait (s)",   round(float(totals.get("wait", 0) or 0), 1))
c7.metric("DB time (s)", round(db_elapsed, 1) if db_elapsed else "—")

st.divider()

# ── Row 2: workers + rate limit window ───────────────────────────────────────
col_w, col_rl = st.columns([3, 1])

with col_w:
    st.subheader("Workers")
    wdf = scoped_active_workers()
    if wdf.empty:
        st.info("No workers have sent a heartbeat yet.")
    else:
        st.dataframe(wdf, use_container_width=True, hide_index=True)

with col_rl:
    st.subheader("Rate limit window")
    st.metric("Slots in use (live)", window_size())
    st.caption("One row per granted API call inside the sliding window. "
               "Evicted automatically by try_acquire_slot().")

st.divider()

# ── Row 3: events breakdown + timeline ───────────────────────────────────────
col_ev, col_tl = st.columns([1, 2])

with col_ev:
    st.subheader("Events")
    ev = scoped_event_breakdown()
    if not ev.empty:
        st.dataframe(ev, use_container_width=True, hide_index=True)

with col_tl:
    st.subheader("Timeline (5s buckets)")
    tl = scoped_events_timeline()
    if not tl.empty:
        pivot = tl.pivot(index="bucket", columns="event_type", values="n").fillna(0)
        st.line_chart(pivot)

st.divider()

# ── Row 4: task table ─────────────────────────────────────────────────────────
st.subheader("Tasks")
tdf = scoped_task_table()
if not tdf.empty:
    st.dataframe(tdf, use_container_width=True, hide_index=True)
else:
    st.info("No tasks for this selection.")

st.divider()

# ── Row 5: wait chart + recent events ────────────────────────────────────────
col_bar, col_log = st.columns([1, 1])

with col_bar:
    st.subheader("Wait time per task (s)")
    wpt = scoped_wait_chart()
    if not wpt.empty:
        st.bar_chart(wpt.set_index("task_id")["wait_secs"])

with col_log:
    st.subheader("Recent events")
    rev = scoped_recent_events()
    if not rev.empty:
        st.dataframe(rev, use_container_width=True, hide_index=True)

st.divider()

# ── Row 6: runs table (always global) ────────────────────────────────────────
st.subheader("All runs")
rdf = scoped_runs_table()
if not rdf.empty:
    st.dataframe(rdf, use_container_width=True, hide_index=True)

# Auto-refresh
time.sleep(refresh_interval)
st.rerun()