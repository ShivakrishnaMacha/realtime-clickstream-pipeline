"""Live replay dashboard for the real-time clickstream pipeline.

Two modes:
  * Live replay — streams data/clickstream_30min.jsonl through the
    StreamingProcessor N events per tick, with adjustable replay speed.
  * Offline — loads out/aggregates.jsonl checkpoints (written by
    `python -m clickstream.stream --topic ... --out out`).
"""

from __future__ import annotations

import json
import os
import pathlib
import sys
import time

import pandas as pd
import plotly.express as px
import streamlit as st

sys.path.insert(0, str(pathlib.Path(__file__).parent / "src"))
from clickstream.stream import StreamingProcessor  # noqa: E402

ROOT = pathlib.Path(__file__).parent
TOPIC = ROOT / "data" / "clickstream_30min.jsonl"
AGG = ROOT / "out" / "aggregates.jsonl"
SUMMARY = ROOT / "out" / "summary.json"

st.set_page_config(page_title="Clickstream Pipeline — Live", layout="wide")
st.title("Real-time Clickstream Analytics Pipeline")
st.caption("Simulated Kafka topic → tumbling windows → sessionization → "
           "watermark + dedupe → this dashboard")


@st.cache_data(show_spinner=False)
def load_topic() -> list[dict]:
    events = []
    with open(TOPIC, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                events.append(json.loads(line))
    return events


@st.cache_data(show_spinner=False)
def load_offline():
    if not AGG.exists():
        return None, None
    wins = [json.loads(l) for l in AGG.read_text().splitlines() if l.strip()]
    summary = json.loads(SUMMARY.read_text()) if SUMMARY.exists() else {}
    return wins, summary


mode = st.sidebar.radio("Mode", ["Live replay", "Offline (aggregates)"], index=0)

# ---------------------------------------------------------------- live replay
if mode == "Live replay":
    if not TOPIC.exists():
        st.error("Demo topic not found. Run `python data/make_demo.py` first.")
        st.stop()
    events = load_topic()

    if "proc" not in st.session_state:
        st.session_state.proc = StreamingProcessor(out_dir=None)
        st.session_state.idx = 0
        st.session_state.playing = False

    proc: StreamingProcessor = st.session_state.proc

    col1, col2, col3 = st.sidebar.columns(3)
    if col1.button("▶ Play"):
        st.session_state.playing = True
    if col2.button("⏸ Pause"):
        st.session_state.playing = False
    if col3.button("↺ Reset"):
        st.session_state.proc = StreamingProcessor(out_dir=None)
        st.session_state.idx = 0
        st.session_state.playing = False
        st.rerun()

    per_tick = st.sidebar.slider("Events per tick", 10, 2000, 250, step=10)
    tick_secs = st.sidebar.slider("Tick interval (sec)", 0.0, 2.0, 0.25, step=0.05)

    idx = st.session_state.idx
    if st.session_state.playing and idx < len(events):
        batch = events[idx: idx + per_tick]
        for e in batch:
            proc.process_event(e)
        st.session_state.idx = idx + len(batch)
        if st.session_state.idx >= len(events):
            st.session_state.playing = False
            proc.flush()
        progress = st.session_state.idx / len(events)
        st.progress(progress, text=f"replayed {st.session_state.idx:,}/{len(events):,} events")
        if st.session_state.playing:
            time.sleep(tick_secs)
            st.rerun()

    closed = proc.closed_aggs
    # windows still open contribute partial aggregates
    live = [w.to_dict() for w in proc.windows.values()]
    wins = closed + live

    if not wins:
        st.info("Press ▶ Play to start the stream.")
        st.stop()

    df = pd.DataFrame(wins).sort_values("window_start")
    df["window"] = pd.to_datetime(df["window_start"], unit="s")

    # KPIs
    total_events = int(df["events"].sum())
    total_rev = float(df["revenue"].sum())
    minutes = max(len(df), 1)
    conv = (df["funnel"].apply(lambda f: f["purchase"]).sum() /
            max(df["funnel"].apply(lambda f: f["page_view"]).sum(), 1))
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Events/min", f"{total_events / minutes:,.0f}")
    k2.metric("Revenue/min", f"${total_rev / minutes:,.2f}")
    k3.metric("Active sessions", f"{int(df['active_sessions'].sum()):,}")
    k4.metric("View → purchase", f"{conv:.2%}")

    c1, c2 = st.columns(2)
    with c1:
        st.subheader("Events per minute")
        st.plotly_chart(px.area(df, x="window", y="events"), use_container_width=True)
    with c2:
        st.subheader("Revenue per minute")
        st.plotly_chart(px.bar(df, x="window", y="revenue"), use_container_width=True)

    c3, c4 = st.columns(2)
    with c3:
        st.subheader("Conversion funnel (totals)")
        funnel = pd.DataFrame({
            "stage": ["page_view", "add_to_cart", "purchase"],
            "count": [
                int(df["funnel"].apply(lambda f: f["page_view"]).sum()),
                int(df["funnel"].apply(lambda f: f["add_to_cart"]).sum()),
                int(df["funnel"].apply(lambda f: f["purchase"]).sum()),
            ],
        })
        st.plotly_chart(px.bar(funnel, x="stage", y="count", text="count"),
                        use_container_width=True)
    with c4:
        st.subheader("Top pages")
        pages = pd.DataFrame(
            sorted(proc.page_counts.items(), key=lambda kv: kv[1], reverse=True)[:10],
            columns=["page", "views"],
        )
        st.dataframe(pages, use_container_width=True, hide_index=True)

    st.subheader("Top products by revenue")
    prods = pd.DataFrame(
        sorted(proc.product_revenue.items(), key=lambda kv: kv[1], reverse=True)[:10],
        columns=["product_id", "revenue"],
    )
    st.dataframe(prods, use_container_width=True, hide_index=True)

    with st.expander("Data-quality counters"):
        st.json(proc.counters.as_dict())

# ---------------------------------------------------------------- offline
else:
    wins, summary = load_offline()
    if wins is None and TOPIC.exists():
        # First run on a fresh deploy (e.g. Streamlit Cloud): the out/
        # checkpoints are gitignored, so build them once from the topic.
        with st.spinner("Building window checkpoints from the demo topic…"):
            from clickstream.stream import run_topic
            (ROOT / "out").mkdir(exist_ok=True)
            run_topic(str(TOPIC), str(ROOT / "out"), replay_speed=0)
        load_offline.clear()
        wins, summary = load_offline()
    if wins is None:
        st.warning("No checkpoints found and no demo topic to build them from. "
                   "Run `python data/make_demo.py` first, then:\n\n"
                   "`PYTHONPATH=src python -m clickstream.stream "
                   "--topic data/clickstream_30min.jsonl --out out`")
        st.stop()

    df = pd.DataFrame(wins).sort_values("window_start")
    df["window"] = pd.to_datetime(df["window_start"], unit="s")
    st.subheader(f"{len(df)} closed 1-minute windows")

    total_events = int(df["events"].sum())
    total_rev = float(df["revenue"].sum())
    conv = (df["funnel"].apply(lambda f: f["purchase"]).sum() /
            max(df["funnel"].apply(lambda f: f["page_view"]).sum(), 1))
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Total events", f"{total_events:,}")
    k2.metric("Total revenue", f"${total_rev:,.2f}")
    k3.metric("Unique users", f"{int(df['unique_users'].sum()):,}")
    k4.metric("View → purchase", f"{conv:.2%}")

    c1, c2 = st.columns(2)
    with c1:
        st.subheader("Events per minute")
        st.plotly_chart(px.area(df, x="window", y="events"), use_container_width=True)
    with c2:
        st.subheader("Revenue per minute")
        st.plotly_chart(px.bar(df, x="window", y="revenue"), use_container_width=True)

    with st.expander("Data-quality counters (from summary.json)"):
        st.json(summary.get("quality", {}))
