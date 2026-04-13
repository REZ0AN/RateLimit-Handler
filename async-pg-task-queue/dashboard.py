import os
import time
import asyncio
import asyncpg
import streamlit as st
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

# ── page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Rate Limiter Dashboard",
    layout="wide",
    page_icon="⚡"
)

DB_URL = os.environ["DATABASE_URL"]

# ── db helpers (sync wrappers around asyncpg) ─────────────────────────────────

async def _fetch(sql: str, *args):
    conn = await asyncpg.connect(DB_URL, ssl="require")
    try:
        return await conn.fetch(sql, *args)
    finally:
        await conn.close()

def query(sql: str, *args) -> list[dict]:
    """Run a query and return list of dicts. Creates a fresh event loop each call."""
    rows = asyncio.run(_fetch(sql, *args))
    return [dict(r) for r in rows]

# ── data fetchers ─────────────────────────────────────────────────────────────

def get_task_summary() -> pd.DataFrame:
    rows = query("SELECT status, count(*) as n FROM tasks GROUP BY status ORDER BY status")
    return pd.DataFrame(rows) if rows else pd.DataFrame(columns=["status", "n"])

def get_tasks() -> pd.DataFrame:
    rows = query("""
        SELECT task_id, prompt, status, retries,
               round(wait_used::numeric, 1) as wait_used_s,
               to_timestamp(created_at) as created,
               to_timestamp(updated_at) as updated
        FROM tasks
        ORDER BY created_at
    """)
    return pd.DataFrame(rows) if rows else pd.DataFrame()

def get_events() -> pd.DataFrame:
    rows = query("""
        SELECT id, task_id, event_type, detail,
               to_timestamp(ts) as time
        FROM events
        ORDER BY id DESC
        LIMIT 100
    """)
    return pd.DataFrame(rows) if rows else pd.DataFrame()

def get_event_counts() -> pd.DataFrame:
    rows = query("""
        SELECT event_type, count(*) as n
        FROM events
        GROUP BY event_type
        ORDER BY n DESC
    """)
    return pd.DataFrame(rows) if rows else pd.DataFrame()

def get_timeline() -> pd.DataFrame:
    """Events bucketed into 5s intervals for the bar chart."""
    rows = query("""
        SELECT
            date_trunc('minute', to_timestamp(ts)) +
            (floor(extract(second from to_timestamp(ts)) / 5) * interval '5 seconds') as bucket,
            event_type,
            count(*) as n
        FROM events
        GROUP BY bucket, event_type
        ORDER BY bucket
    """)
    return pd.DataFrame(rows) if rows else pd.DataFrame()

def get_wait_by_task() -> pd.DataFrame:
    rows = query("""
        SELECT task_id, wait_used
        FROM tasks
        WHERE wait_used > 0
        ORDER BY wait_used DESC
    """)
    return pd.DataFrame(rows) if rows else pd.DataFrame()

def get_totals() -> dict:
    rows = query("""
        SELECT
            count(*)                            as total,
            count(*) filter (where status='done')    as done,
            count(*) filter (where status='running') as running,
            count(*) filter (where status='pending') as pending,
            count(*) filter (where status='failed')  as failed,
            coalesce(sum(wait_used), 0)          as total_wait,
            coalesce(sum(retries), 0)            as total_retries
        FROM tasks
    """)
    return dict(rows[0]) if rows else {}

# ── layout ────────────────────────────────────────────────────────────────────

st.title("⚡ Rate Limiter — Live Dashboard")
st.caption("Auto-refreshes every 3 seconds. Run `handler.py` in another terminal to see live updates.")

# refresh control
col_refresh, col_stop = st.columns([1, 5])
with col_refresh:
    refresh_rate = st.selectbox("Refresh", [3, 5, 10], index=0, label_visibility="collapsed")

# main refresh loop
placeholder = st.empty()

while True:
    totals    = get_totals()
    df_tasks  = get_tasks()
    df_events = get_events()
    df_counts = get_event_counts()
    df_tl     = get_timeline()
    df_wait   = get_wait_by_task()

    with placeholder.container():

        # ── row 1: KPI cards ──────────────────────────────────────────────────
        k1, k2, k3, k4, k5, k6, k7 = st.columns(7)
        k1.metric("Total",    totals.get("total", 0))
        k2.metric("✅ Done",  totals.get("done", 0))
        k3.metric("🔄 Running", totals.get("running", 0))
        k4.metric("⏳ Pending", totals.get("pending", 0))
        k5.metric("❌ Failed",  totals.get("failed", 0))
        k6.metric("⏱ Wait (s)", round(float(totals.get("total_wait", 0)), 1))
        k7.metric("↩ Retries",  totals.get("total_retries", 0))

        st.divider()

        # ── row 2: event type breakdown + timeline ────────────────────────────
        c_left, c_right = st.columns([1, 2])

        with c_left:
            st.subheader("Event breakdown")
            if not df_counts.empty:
                # colour map
                colours = {
                    "success":      "#1D9E75",
                    "started":      "#378ADD",
                    "rate_limited": "#BA7517",
                    "error":        "#E24B4A",
                    "dropped":      "#888780",
                }
                for _, row in df_counts.iterrows():
                    et   = row["event_type"]
                    n    = int(row["n"])
                    col  = colours.get(et, "#888780")
                    pct  = n / max(df_counts["n"].sum(), 1)
                    st.markdown(
                        f"""<div style="display:flex;align-items:center;gap:10px;margin-bottom:6px">
                            <div style="width:12px;height:12px;border-radius:2px;background:{col}"></div>
                            <span style="flex:1;font-size:13px">{et}</span>
                            <span style="font-size:13px;font-weight:500">{n}</span>
                            <div style="width:80px;height:8px;background:var(--secondary-background-color);border-radius:4px;overflow:hidden">
                              <div style="width:{pct*100:.0f}%;height:100%;background:{col};border-radius:4px"></div>
                            </div>
                        </div>""",
                        unsafe_allow_html=True
                    )
            else:
                st.info("No events yet.")

        with c_right:
            st.subheader("Events over time (5s buckets)")
            if not df_tl.empty:
                pivot = df_tl.pivot_table(
                    index="bucket", columns="event_type", values="n", fill_value=0
                ).reset_index()
                pivot = pivot.set_index("bucket")
                st.bar_chart(pivot, height=220)
            else:
                st.info("Waiting for events...")

        st.divider()

        # ── row 3: task table ─────────────────────────────────────────────────
        st.subheader("Task queue")
        if not df_tasks.empty:
            STATUS_ICON = {
                "pending": "⏳", "running": "🔄",
                "done": "✅", "failed": "❌"
            }
            display = df_tasks.copy()
            display["status"] = display["status"].map(
                lambda s: f"{STATUS_ICON.get(s, '')} {s}"
            )
            display["prompt"] = display["prompt"].str[:60]
            st.dataframe(
                display[["task_id", "status", "retries", "wait_used_s", "prompt"]],
                use_container_width=True,
                hide_index=True
            )
        else:
            st.info("No tasks yet.")

        st.divider()

        # ── row 4: wait time per task ─────────────────────────────────────────
        c_wait, c_log = st.columns([1, 2])

        with c_wait:
            st.subheader("Wait time per task (s)")
            if not df_wait.empty:
                st.bar_chart(
                    df_wait.set_index("task_id"),
                    height=200
                )
            else:
                st.info("No rate-limit waits recorded.")

        with c_log:
            st.subheader("Event log (latest 100)")
            if not df_events.empty:
                log = df_events.copy()
                ICON = {
                    "started":      "🟡",
                    "success":      "✅",
                    "rate_limited": "🟠",
                    "error":        "⚠️",
                    "dropped":      "💀",
                }
                log["event_type"] = log["event_type"].map(
                    lambda e: f"{ICON.get(e, '')} {e}"
                )
                log["time"] = log["time"].astype(str).str[11:19]  # HH:MM:SS
                st.dataframe(
                    log[["time", "task_id", "event_type", "detail"]],
                    use_container_width=True,
                    hide_index=True,
                    height=220
                )
            else:
                st.info("No events logged yet.")

    time.sleep(refresh_rate)