"""Generator determinism: same (seed, n, minutes) -> byte-identical file."""

import filecmp
import os

from clickstream.generator import generate_events, write_jsonl


def _write(tmp_path, name, n=1500, minutes=30, seed=42):
    p = os.path.join(str(tmp_path), name)
    write_jsonl(generate_events(n, minutes, seed), p)
    return p


def test_same_seed_same_bytes(tmp_path):
    a = _write(tmp_path, "a.jsonl")
    b = _write(tmp_path, "b.jsonl")
    assert filecmp.cmp(a, b, shallow=False)
    assert os.path.getsize(a) > 0


def test_different_seed_different_bytes(tmp_path):
    a = _write(tmp_path, "a.jsonl", seed=1)
    b = _write(tmp_path, "b.jsonl", seed=2)
    assert not filecmp.cmp(a, b, shallow=False)


def test_event_schema(tmp_path):
    events = generate_events(500, 30, seed=7)
    required = {"event_id", "user_id", "session_id", "ts", "event_type",
                "page", "product_id", "price", "utm_source", "device", "country"}
    good = [e for e in events if e.get("event_type") in
            ("page_view", "search", "add_to_cart", "purchase")]
    assert good, "generator must emit mostly valid events"
    for e in good:
        assert required.issubset(e.keys())
        assert isinstance(e["ts"], int)
