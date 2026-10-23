"""Data-quality validation tests."""

from clickstream.data_quality import QualityCounters, validate


def _good():
    return {
        "event_id": "e1",
        "user_id": "u1",
        "session_id": "s1",
        "ts": 1000,
        "event_type": "purchase",
        "page": "/checkout",
        "product_id": "SKU-0001",
        "price": 49.99,
        "utm_source": "google",
        "device": "mobile",
        "country": "US",
    }


def test_valid_event_has_no_errors():
    assert validate(_good()) == []


def test_missing_field_flagged():
    e = _good()
    del e["event_type"]
    assert any(err.startswith("missing:") for err in validate(e))


def test_bad_ts_type_flagged():
    e = _good()
    e["ts"] = "yesterday"
    assert "bad_type:ts" in validate(e)


def test_unknown_event_type_flagged():
    e = _good()
    e["event_type"] = "frobnicate"
    assert any(err.startswith("bad_value:event_type") for err in validate(e))


def test_counters_roundtrip():
    c = QualityCounters(processed=10, valid=7, duplicates_dropped=1,
                        late_dropped=1, malformed_quarantined=1)
    d = c.as_dict()
    assert d["processed"] == 10 and d["valid"] == 7
