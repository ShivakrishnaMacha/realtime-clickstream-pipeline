"""Generate the committed demo dataset (deterministic).

Default size keeps data/clickstream_30min.jsonl under 1 MB so it can be
committed to the repo; pass --n for a bigger local run (e.g. 50k events).
"""

from __future__ import annotations

import argparse
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from clickstream.generator import generate_events, write_jsonl

DEFAULT_N = 4000
DEFAULT_MINUTES = 30
DEFAULT_SEED = 7


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=DEFAULT_N)
    ap.add_argument("--minutes", type=int, default=DEFAULT_MINUTES)
    ap.add_argument("--seed", type=int, default=DEFAULT_SEED)
    ap.add_argument("--out", default=os.path.join(
        os.path.dirname(__file__), "clickstream_30min.jsonl"))
    args = ap.parse_args()

    events = generate_events(args.n, args.minutes, args.seed)
    write_jsonl(events, args.out)
    size_kb = os.path.getsize(args.out) / 1024
    print(f"wrote {len(events)} events -> {args.out} "
          f"({size_kb:.0f} KB, seed={args.seed})")


if __name__ == "__main__":
    main()
