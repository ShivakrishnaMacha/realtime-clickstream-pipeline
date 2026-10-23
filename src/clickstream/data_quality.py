"""Schema validation + quarantine for clickstream events.

Bad rows never enter the pipeline: they are appended to a quarantine
file with the validation errors attached, and counted in QualityCounters.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field

REQUIRED_FIELDS = ("event_id", "user_id", "ts", "event_type")
ALLOWED_EVENT_TYPES = ("page_view", "search", "add_to_cart", "purchase")


@dataclass
class QualityCounters:
    processed: int = 0
    valid: int = 0
    duplicates_dropped: int = 0
    late_dropped: int = 0
    malformed_quarantined: int = 0

    def as_dict(self) -> dict:
        return {
            "processed": self.processed,
            "valid": self.valid,
            "duplicates_dropped": self.duplicates_dropped,
            "late_dropped": self.late_dropped,
            "malformed_quarantined": self.malformed_quarantined,
        }


def validate(event: dict) -> list[str]:
    """Return a list of validation error strings (empty = valid)."""
    errors: list[str] = []
    if not isinstance(event, dict):
        return ["not_a_dict"]
    for f in REQUIRED_FIELDS:
        if f not in event or event[f] is None or event[f] == "":
            errors.append(f"missing:{f}")
    ts = event.get("ts")
    if ts is not None and not isinstance(ts, (int, float)):
        errors.append("bad_type:ts")
    elif isinstance(ts, (int, float)) and ts < 0:
        errors.append("bad_value:ts")
    et = event.get("event_type")
    if et is not None and et not in ALLOWED_EVENT_TYPES:
        errors.append(f"bad_value:event_type:{et}")
    price = event.get("price")
    if price is not None and not isinstance(price, (int, float)):
        errors.append("bad_type:price")
    return errors


class Quarantine:
    """Append-only sink for malformed rows."""

    def __init__(self, path: str | None):
        self.path = path
        self._fh = open(path, "w", encoding="utf-8") if path else None

    def add(self, event: dict, errors: list[str]) -> None:
        if self._fh is None:
            return
        self._fh.write(
            json.dumps({"_errors": errors, "_event": event},
                       separators=(",", ":"), default=str) + "\n"
        )

    def close(self) -> None:
        if self._fh is not None:
            self._fh.close()
            self._fh = None
