"""Deterministic seeded synthetic clickstream event generator.

Emits JSONL events for: page_view, search, add_to_cart, purchase.
Occasionally injects real-world messiness: duplicate event_ids,
out-of-order delivery, late events, and malformed rows — so the
streaming processor's data-quality path has something to do.

Determinism: same (seed, n, minutes) -> byte-identical output file.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import uuid

import numpy as np

EVENT_TYPES = ("page_view", "search", "add_to_cart", "purchase")
PAGES = (
    "/",
    "/products",
    "/category/electronics",
    "/category/fashion",
    "/category/home",
    "/product",
    "/cart",
    "/checkout",
    "/deals",
    "/search",
)
UTM_SOURCES = ("organic", "google", "email", "social", "direct", "referral", "ads")
DEVICES = ("desktop", "mobile", "tablet")
COUNTRIES = ("US", "US", "US", "IN", "GB", "CA", "DE", "AU", "BR")  # weighted
PRODUCT_IDS = [f"SKU-{i:04d}" for i in range(1, 121)]

# Fixed epoch anchor (2026-10-23 00:00:00 UTC) so output is stable across runs.
EPOCH_ANCHOR = 1_792_224_000


def _new_event_id(rng: np.random.Generator) -> str:
    return str(uuid.UUID(bytes=rng.bytes(16)))


def _product_price(rng: np.random.Generator, product_id: str) -> float:
    # Deterministic price per product so replays agree (stable md5, not salted hash()).
    digest = hashlib.md5(product_id.encode()).hexdigest()
    return round(float(9.99 + (int(digest, 16) % 49000) / 100.0), 2)


def generate_events(n: int, minutes: int, seed: int) -> list[dict]:
    """Build the raw event list (dicts), anomalies included, in stream order."""
    rng = np.random.default_rng(seed)
    window = minutes * 60
    n_users = max(50, n // 3)

    events: list[dict] = []

    # ---- base traffic: funnel-coherent user sessions ----
    while len(events) < n:
        user_id = f"u{rng.integers(1, n_users + 1):05d}"
        session_id = _new_event_id(rng)[:8]
        t = EPOCH_ANCHOR + int(rng.integers(0, max(1, window - 600)))
        device = rng.choice(DEVICES, p=(0.45, 0.45, 0.10))
        utm = rng.choice(UTM_SOURCES)
        country = rng.choice(COUNTRIES)

        n_views = int(rng.integers(1, 7))
        product_id = rng.choice(PRODUCT_IDS)
        page = f"/product/{product_id}"

        for _ in range(n_views):
            t += int(rng.integers(5, 121))
            events.append(
                _event(
                    rng, t, user_id, session_id, "page_view",
                    page=rng.choice(PAGES), utm=utm, device=device, country=country,
                )
            )
        if rng.random() < 0.15:  # search
            t += int(rng.integers(5, 60))
            events.append(
                _event(rng, t, user_id, session_id, "search",
                       page="/search", utm=utm, device=device, country=country)
            )
        if rng.random() < 0.35:  # add to cart
            t += int(rng.integers(5, 90))
            events.append(
                _event(rng, t, user_id, session_id, "add_to_cart",
                       page="/cart", product_id=product_id, utm=utm,
                       device=device, country=country)
            )
            if rng.random() < 0.45:  # purchase
                t += int(rng.integers(10, 180))
                events.append(
                    _event(rng, t, user_id, session_id, "purchase",
                           page="/checkout", product_id=product_id,
                           price=_product_price(rng, product_id),
                           utm=utm, device=device, country=country)
                )

    events = events[:n]
    events.sort(key=lambda e: e["ts"])

    # ---- anomalies: duplicates (1%), out-of-order (2%), late (0.5%) ----
    def later_pos(i: int) -> int:
        return min(len(events), i + int(rng.integers(5, 60)))

    for i in rng.choice(len(events), size=max(1, n // 100), replace=False):
        dup = dict(events[i])  # exact copy, same event_id
        events.insert(later_pos(i), dup)

    for i in rng.choice(len(events), size=max(1, n // 50), replace=False):
        j = max(0, i - int(rng.integers(1, 30)))
        events[i], events[j] = events[j], events[i]

    tail = len(events)
    for i in rng.choice(len(events), size=max(1, n // 200), replace=False):
        ev = events.pop(i)  # old ts, re-inserted near the end -> "late"
        events.insert(min(len(events), tail - int(rng.integers(0, 20))), ev)

    # ---- malformed rows (0.3%) ----
    for i in rng.choice(len(events), size=max(1, n // 300), replace=False):
        kind = int(rng.integers(0, 3))
        if kind == 0:
            events[i].pop("event_type", None)
        elif kind == 1:
            events[i]["ts"] = "not-a-timestamp"
        else:
            events[i]["event_type"] = "frobnicate"

    return events


def _event(rng, ts, user_id, session_id, event_type, page="/", product_id=None,
           price=None, utm="direct", device="desktop", country="US") -> dict:
    return {
        "event_id": _new_event_id(rng),
        "user_id": user_id,
        "session_id": session_id,
        "ts": int(ts),
        "event_type": event_type,
        "page": page,
        "product_id": product_id,
        "price": price,
        "utm_source": utm,
        "device": device,
        "country": country,
    }


def write_jsonl(events: list[dict], out_path: str) -> None:
    with open(out_path, "w", encoding="utf-8") as f:
        for e in events:
            f.write(json.dumps(e, separators=(",", ":")) + "\n")


def main() -> None:
    ap = argparse.ArgumentParser(description="Generate seeded clickstream events")
    ap.add_argument("--n", type=int, default=4000, help="number of events")
    ap.add_argument("--seed", type=int, default=7, help="random seed")
    ap.add_argument("--out", default="data/clickstream_30min.jsonl")
    ap.add_argument("--minutes", type=int, default=30, help="simulated window")
    args = ap.parse_args()

    events = generate_events(args.n, args.minutes, args.seed)
    write_jsonl(events, args.out)
    print(f"wrote {len(events)} events -> {args.out} (seed={args.seed})")


if __name__ == "__main__":
    main()
