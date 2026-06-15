#!/usr/bin/env python3
"""Refresh the Hyperliquid perpConciseAnnotations seed.

Single-shot fetch from the public Hyperliquid info endpoint, written
to `hyperliquid_seeds/perp_annotations.json`. Used by:

  1. One-time bootstrap (manual)
  2. Weekly GitHub Actions cron (see
     `.github/workflows/hyperliquid_categories.yml`) — cron commits the
     file back to the repo if it changed, so the dashboard always
     reads a fresh seed without ever calling Hyperliquid at render
     time.

Exit code:
  0 — fetch succeeded and seed written (whether or not it changed on disk)
  1 — fetch failed; existing seed untouched

Run from the repo root:
    python scripts/refresh_hyperliquid_seed.py
"""
from __future__ import annotations

import os
import sys

# Allow running from anywhere — add repo root to sys.path so the
# `hyperliquid` and `seed_cache` modules import cleanly regardless
# of CWD (matters when cron CDs to a checkout dir).
_HERE = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.dirname(_HERE)
sys.path.insert(0, _REPO_ROOT)

import hyperliquid  # noqa: E402  — must follow sys.path tweak


def main() -> int:
    annotations, err = hyperliquid.fetch_perp_annotations()
    if err:
        print(f"FAILED: {err}", file=sys.stderr)
        return 1
    if not annotations:
        print("FAILED: empty response", file=sys.stderr)
        return 1
    ok = hyperliquid.write_seed(annotations)
    if not ok:
        print("FAILED: write_seed returned False", file=sys.stderr)
        return 1
    # Print a per-prefix breakdown for the cron log — makes it easy
    # to spot a sudden drop (e.g. new dex appeared, old one delisted).
    from collections import Counter
    by_prefix = Counter(
        t.split(":")[0] if ":" in t else "(none)"
        for t, _ in annotations)
    by_category = Counter(
        (meta or {}).get("category", "(none)") for _, meta in annotations)
    print(f"wrote {len(annotations)} annotations")
    print("  prefixes:", dict(by_prefix.most_common()))
    print("  categories:", dict(by_category.most_common()))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
