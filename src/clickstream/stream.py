"""Streaming processor for the simulated clickstream topic.

Reads events (JSONL "topic") in stream order and maintains:

* 1-minute tumbling windows with per-window metrics
  (events, unique users, revenue, conversion funnel view->cart->purchase)
* sessionization: a new derived session starts after a 30-min gap per user
* watermark: events arriving more than 5 minutes behind the max seen
  timestamp are dropped as late
* dedupe on event_id
* schema validation via data_quality (malformed rows -> quarantine)

Windows close when the watermark passes window_end + allowed lateness;
closed windows are checkpointed to out/aggregates.jsonl as they close.
A final summary is written to out/summary.json.

CLI: python -m clickstream.stream --topic ... --out ... --replay-speed 0
(replay_speed = events/sec; 0 = as fast as possible)
"""

from __future__ import annotations

import argparse
import json
import os
import time
from dataclasses import dataclass, field

from .data_quality import QualityCounters, Quarantine, validate

LATE_SECONDS = 5 * 60      # watermark: drop events >5 min late
SESSION_GAP = 30 * 60      # new session after 30-min gap per user
WINDOW_SECONDS = 60        # 1-minute tumbling windows


@dataclass
class WindowAgg:
    window_start: int
    events: int = 0
    users: set = field(default_factory=set)
    sessions: set = field(default_factory=set)
    revenue: float = 0.0
    page_view: int = 0
    search: int = 0
    add_to_cart: int = 0
    purchase: int = 0

    def to_dict(self) -> dict:
        views = self.page_view
        return {
            "window_start": self.window_start,
            "window_end": self.window_start + WINDOW_SECONDS,
            "events": self.events,
            "unique_users": len(self.users),
            "active_sessions": len(self.sessions),
            "revenue": round(self.revenue, 2),
            "funnel": {
                "page_view": views,
                "add_to_cart": self.add_to_cart,
                "purchase": self.purchase,
            },
            "conversion_view_to_cart": round(self.add_to_cart / views, 4) if views else 0.0,
            "conversion_view_to_purchase": round(self.purchase / views, 4) if views else 0.0,
        }


class StreamingProcessor:
    """Stateful streaming operator. Feed it events one at a time."""

    def __init__(self, out_dir: str | None = None,
                 late_seconds: int = LATE_SECONDS,
                 session_gap: int = SESSION_GAP):
        self.late_seconds = late_seconds
        self.session_gap = session_gap
        self.seen_ids: set[str] = set()
        self.max_ts: int | float | None = None
        self.counters = QualityCounters()
        self.windows: dict[int, WindowAgg] = {}
        self.user_last_ts: dict[str, int | float] = {}
        self.user_session_n: dict[str, int] = {}
        self.page_counts: dict[str, int] = {}
        self.product_revenue: dict[str, float] = {}
        self.closed_starts: list[int] = []  # window starts closed so far, in order
        self.closed_aggs: list[dict] = []  # their aggregate dicts, in close order

        self.out_dir = out_dir
        self._agg_fh = None
        self.quarantine = Quarantine(
            os.path.join(out_dir, "quarantine.jsonl") if out_dir else None
        )
        if out_dir:
            os.makedirs(out_dir, exist_ok=True)
            self._agg_fh = open(os.path.join(out_dir, "aggregates.jsonl"),
                                "w", encoding="utf-8")

    # ---------------- core operator ----------------
    def process_event(self, event: dict) -> str:
        """Process one event. Returns 'ok' | 'duplicate' | 'late' | 'malformed'."""
        self.counters.processed += 1

        errors = validate(event)
        if errors:
            self.counters.malformed_quarantined += 1
            self.quarantine.add(event, errors)
            return "malformed"

        eid = event["event_id"]
        if eid in self.seen_ids:
            self.counters.duplicates_dropped += 1
            return "duplicate"
        self.seen_ids.add(eid)

        ts = event["ts"]
        if self.max_ts is not None and ts < self.max_ts - self.late_seconds:
            self.counters.late_dropped += 1
            return "late"

        self.counters.valid += 1
        if self.max_ts is None or ts > self.max_ts:
            self.max_ts = ts

        # sessionization
        user = event["user_id"]
        last = self.user_last_ts.get(user)
        if last is None or ts - last > self.session_gap:
            self.user_session_n[user] = self.user_session_n.get(user, 0) + 1
        session_key = f"{user}#s{self.user_session_n[user]}"
        self.user_last_ts[user] = ts

        # window assignment
        wstart = int(ts // WINDOW_SECONDS * WINDOW_SECONDS)
        w = self.windows.get(wstart)
        if w is None:
            w = self.windows[wstart] = WindowAgg(window_start=wstart)
        w.events += 1
        w.users.add(user)
        w.sessions.add(session_key)
        et = event["event_type"]
        if et == "purchase":
            w.purchase += 1
            price = event.get("price") or 0.0
            w.revenue += float(price)
            pid = event.get("product_id")
            if pid:
                self.product_revenue[pid] = self.product_revenue.get(pid, 0.0) + float(price)
        elif et == "add_to_cart":
            w.add_to_cart += 1
        elif et == "page_view":
            w.page_view += 1
        elif et == "search":
            w.search += 1
        page = event.get("page")
        if page:
            self.page_counts[page] = self.page_counts.get(page, 0) + 1

        self._close_ready_windows()
        return "ok"

    def _close_ready_windows(self) -> None:
        """Checkpoint windows the watermark has passed."""
        if self.max_ts is None:
            return
        horizon = self.max_ts - self.late_seconds
        for wstart in sorted(self.windows):
            if wstart + WINDOW_SECONDS <= horizon:
                w = self.windows.pop(wstart)
                agg = w.to_dict()
                if self._agg_fh is not None:
                    self._agg_fh.write(json.dumps(agg, separators=(",", ":")) + "\n")
                    self._agg_fh.flush()
                self.closed_starts.append(wstart)
                self.closed_aggs.append(agg)
            else:
                break

    # ---------------- lifecycle ----------------
    def flush(self) -> None:
        """Close all open windows and write the final summary."""
        for wstart in sorted(self.windows):
            w = self.windows[wstart]
            agg = w.to_dict()
            if self._agg_fh is not None:
                self._agg_fh.write(json.dumps(agg, separators=(",", ":")) + "\n")
            self.closed_starts.append(wstart)
            self.closed_aggs.append(agg)
        self.windows.clear()
        if self._agg_fh is not None:
            self._agg_fh.close()
            self._agg_fh = None
        self.quarantine.close()

    def full_summary(self) -> dict:
        """Aggregate across all closed windows (call after flush)."""
        return {
            "quality": self.counters.as_dict(),
            "windows_emitted": len(self.closed_starts),
            "max_ts": self.max_ts,
        }


def run_topic(topic: str, out_dir: str, replay_speed: float = 0.0) -> dict:
    """Drive the processor over a JSONL topic file. Returns the summary."""
    proc = StreamingProcessor(out_dir=out_dir)
    delay = 1.0 / replay_speed if replay_speed and replay_speed > 0 else 0.0
    with open(topic, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                proc.counters.processed += 1
                proc.counters.malformed_quarantined += 1
                proc.quarantine.add({"_raw": line[:200]}, ["json_decode_error"])
                continue
            proc.process_event(event)
            if delay:
                time.sleep(delay)
    proc.flush()

    summary = proc.full_summary()
    summary_path = os.path.join(out_dir, "summary.json")
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)
    return summary


def main() -> None:
    ap = argparse.ArgumentParser(description="Run the clickstream streaming processor")
    ap.add_argument("--topic", required=True, help="input JSONL topic file")
    ap.add_argument("--out", default="out", help="output dir for checkpoints")
    ap.add_argument("--replay-speed", type=float, default=0.0,
                    help="events/sec; 0 = as fast as possible")
    args = ap.parse_args()

    summary = run_topic(args.topic, args.out, args.replay_speed)
    q = summary["quality"]
    print(f"processed={q['processed']} valid={q['valid']} "
          f"dup_dropped={q['duplicates_dropped']} late_dropped={q['late_dropped']} "
          f"malformed={q['malformed_quarantined']} windows={summary['windows_emitted']}")


if __name__ == "__main__":
    main()
