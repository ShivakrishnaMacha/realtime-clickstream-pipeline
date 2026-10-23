"""Streaming processor tests: dedupe, watermark, sessionization, funnel."""

from clickstream.stream import SESSION_GAP, StreamingProcessor


def _ev(eid, user, ts, etype="page_view", price=None, page="/", product_id=None):
    return {
        "event_id": eid,
        "user_id": user,
        "session_id": "s1",
        "ts": ts,
        "event_type": etype,
        "page": page,
        "product_id": product_id,
        "price": price,
        "utm_source": "direct",
        "device": "desktop",
        "country": "US",
    }


def _proc(**kw):
    return StreamingProcessor(out_dir=None, **kw)


def test_dedupe_drops_duplicate_event_id():
    p = _proc()
    assert p.process_event(_ev("id-1", "u1", 1000)) == "ok"
    assert p.process_event(_ev("id-1", "u1", 1001)) == "duplicate"
    assert p.counters.duplicates_dropped == 1
    assert p.counters.valid == 1
    p.flush()


def test_watermark_drops_late_events():
    p = _proc(late_seconds=300)
    p.process_event(_ev("id-1", "u1", 1000))
    p.process_event(_ev("id-2", "u1", 1300))   # max_ts = 1300
    # 800 < 1300 - 300 = 1000 -> late
    assert p.process_event(_ev("id-3", "u1", 800)) == "late"
    # exactly at the boundary (1000) is NOT late
    assert p.process_event(_ev("id-4", "u1", 1000)) == "ok"
    assert p.counters.late_dropped == 1
    p.flush()


def test_sessionization_splits_on_gap():
    p = _proc(session_gap=SESSION_GAP)
    p.process_event(_ev("a", "u1", 1000))
    p.process_event(_ev("b", "u1", 1100))               # same session
    p.process_event(_ev("c", "u1", 1100 + SESSION_GAP + 1))  # >30 min after last -> new session
    assert p.user_session_n["u1"] == 2
    p.flush()


def test_funnel_math_on_fixture():
    p = _proc()
    base = 1_700_000_000
    wid = base // 60 * 60
    events = []
    for i in range(10):
        events.append(_ev(f"v{i}", f"u{i}", wid + i, "page_view"))
    for i in range(4):
        events.append(_ev(f"c{i}", f"u{i}", wid + 20 + i, "add_to_cart"))
    for i in range(2):
        events.append(_ev(f"p{i}", f"u{i}", wid + 40 + i, "purchase",
                           price=50.0, product_id="SKU-0001"))
    for e in events:
        assert p.process_event(e) == "ok"
    w = p.windows[wid]
    assert (w.page_view, w.add_to_cart, w.purchase) == (10, 4, 2)
    assert w.revenue == 100.0
    assert len(w.users) == 10
    d = w.to_dict()
    assert d["conversion_view_to_cart"] == 0.4
    assert d["conversion_view_to_purchase"] == 0.2
    p.flush()


def test_malformed_goes_to_quarantine(tmp_path):
    p = StreamingProcessor(out_dir=str(tmp_path))
    bad = {"event_id": "x", "user_id": "u1", "ts": "nope", "event_type": "page_view"}
    assert p.process_event(bad) == "malformed"
    assert p.counters.malformed_quarantined == 1
    assert p.counters.valid == 0
    p.flush()
    qpath = tmp_path / "quarantine.jsonl"
    assert qpath.exists() and qpath.stat().st_size > 0
