"""One-shot snapshotter for past-month Hyperliquid per-market queries.

Run this when a month rolls over to add a new past-month CSV to
`allium_seeds/`. After the new month's CSV is committed, the dashboard
loads it from disk instead of re-hitting Allium every 4h — the seed
pattern this codebase uses for ULTRA market caps + foreign-L1 tokens
+ Solana commodities.

Usage (from repo root, with ALLIUM_API_KEY in env or .env):

    python3 scripts/snapshot_allium_hl.py             # snapshot all months
    python3 scripts/snapshot_allium_hl.py 2026-07     # snapshot one month

The current month is intentionally NOT snapshotted by the default run —
it's still mutating and the dashboard handles it live. Pass an explicit
month to force-snapshot it anyway (useful when you want a static export
for offline analysis).

After running, `git add allium_seeds/` + commit.
"""
from __future__ import annotations

import os
import sys
import types
from datetime import datetime

# Stub streamlit so allium.py's @st.cache_data is a no-op outside a
# Streamlit context. Same shim used by other one-shot scripts here.
_st = types.ModuleType("streamlit")

def _noop_dec(*a, **kw):
    def deco(f): return f
    if a and callable(a[0]):
        return a[0]
    return deco

_st.cache_data = _noop_dec


class _Secrets:
    def get(self, k, d=None): return d


_st.secrets = _Secrets()
sys.modules["streamlit"] = _st

# Now safe to import allium
sys.path.insert(0, os.path.dirname(os.path.dirname(
    os.path.abspath(__file__))))
import allium  # noqa: E402


# Same lists as in stocks_dashboard.py — keep in sync when adding
# new monthly queries on Allium.
OI_MONTHS: list[tuple[str, str]] = [
    ("2025-10", "X245492iOjb732UetWSB"),
    ("2025-11", "Y0dQpQr4hlIAB3OcHskc"),
    ("2025-12", "5o5BxYqNBZ7ilym8IrVD"),
    ("2026-01", "VTDbrmTWDpLD17FNJ75a"),
    ("2026-02", "vTVCW5klGQZB7yPhMfFX"),
    ("2026-03", "l6qTddnDzjghkopXkHnG"),
    ("2026-04", "AfTgrxI4iz8f6bLzGTyN"),
    ("2026-05", "nbXTSEgW4BengtO1yo9Y"),
    ("2026-06", "3EFdKoYQbnF6SUe5tKxM"),
]
VOL_MONTHS: list[tuple[str, str]] = [
    ("2025-10", "ha8KI7UgIYErrRX7FVRb"),
    ("2025-11", "bjteVj88hcZ2J7aMYTLL"),
    ("2025-12", "yQwb2E1DbTXBMUQOxv91"),
    ("2026-01", "Y3dnh1cetFe4Mvk71RXw"),
    ("2026-02", "rn6de7jubz6UJqmboI3C"),
    ("2026-03", "GTUOinJ1mGqFg05Anf5E"),
    ("2026-04", "3f15H7ewiru0jSSg1pA0"),
    ("2026-05", "c4RdLzLIA2vLooZpI2jV"),
    ("2026-06", "4llrBGWuqueGZmjIc96n"),
]

SEED_DIR = os.path.join(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__))), "allium_seeds")


def _snapshot_one(metric: str, month: str, qid: str) -> int:
    """Pull one month's query and write the CSV. Returns row count."""
    path = os.path.join(SEED_DIR, f"allium_hl_{metric}_{month}.csv")
    df, err = allium.fetch_allium_query_results(qid)
    if err:
        print(f"  {metric} {month}: ERR {err}")
        return 0
    if df.empty:
        print(f"  {metric} {month}: empty (skipped)")
        return 0
    df.to_csv(path, index=False)
    coins = df["coin"].nunique() if "coin" in df.columns else "?"
    print(f"  {metric} {month}: rows={len(df)}, coins={coins} → {path}")
    return len(df)


def main() -> int:
    os.makedirs(SEED_DIR, exist_ok=True)
    # When an explicit month is passed, snapshot only that one (even
    # if it's the current month — caller's choice).
    target_month: str | None = sys.argv[1] if len(sys.argv) > 1 else None
    current_month = datetime.utcnow().strftime("%Y-%m")
    total = 0
    for metric, months in [("oi", OI_MONTHS), ("vol", VOL_MONTHS)]:
        for month, qid in months:
            if target_month is None:
                # Default: skip the current month so we don't snapshot
                # an in-progress month with partial data.
                if month == current_month:
                    print(f"  {metric} {month}: SKIP "
                          f"(current month — runs live)")
                    continue
            elif month != target_month:
                continue
            total += _snapshot_one(metric, month, qid)
    print(f"\nTotal: {total} rows snapshotted")
    return 0


if __name__ == "__main__":
    sys.exit(main())
