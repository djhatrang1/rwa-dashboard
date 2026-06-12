#!/usr/bin/env python3
"""One-time bootstrap of the SoSoValue SOL-ETF seed CSV from a
manually-saved website JSON dump.

Why: the SoSoValue API key plan limits `summary-history` queries to a
30-day rolling window (HTTP 403 with `code:400301, "date range limited
to last 30 days for your API key plan"`). The public website UI shows
the full launch-to-today history; this script ingests a snapshot of
that UI payload to backfill the seed with everything from the SOL ETF
launch date (2025-10-28) onwards.

Source file format:
  sosovalue_seeds/_bootstrap_sol_us_history.json
A JSON array of dicts. Each row mirrors the SoSoValue `_next/data`
endpoint's `pageProps.historyData.list[]` shape, with the fields the
seed CSV needs:
    {dataDate, totalNetInflow, totalVolume, totalNetAssets,
     cumNetInflow}
The full upstream blob is much larger; only this slice is preserved
because it's all the dashboards read.

What this writes:
  sosovalue_seeds/summary_history_sol_us.csv
Same five columns the live `sosovalue._fetch_summary_history` emits:
  date, total_net_inflow, total_value_traded,
  total_net_assets, cum_net_inflow

If the seed CSV already exists, this script MERGES — bootstrap rows
plus current seed rows, deduped by date, latest write wins. That
preserves any newer rows the live fetcher captured since the manual
dump was taken.

Run from the repo root:
    python scripts/bootstrap_sosovalue_seed.py
"""
from __future__ import annotations

import json
import os
import sys

# Allow running from anywhere by adding the repo root to sys.path so
# we can import seed_cache. The repo root is one level up from /scripts.
_HERE = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.dirname(_HERE)
sys.path.insert(0, _REPO_ROOT)

import pandas as pd  # noqa: E402  — must follow sys.path tweak

import seed_cache  # noqa: E402


_SEEDS_DIR = "sosovalue_seeds"
_BOOTSTRAP_FN = "_bootstrap_sol_us_history.json"
_SEED_FN = "summary_history_sol_us.csv"


def _load_bootstrap_rows() -> list[dict]:
    """Read the saved website-JSON slice. Raises if missing — without
    it there's nothing to bootstrap from."""
    path = os.path.join(_REPO_ROOT, _SEEDS_DIR, _BOOTSTRAP_FN)
    if not os.path.exists(path):
        raise SystemExit(
            f"missing bootstrap source: {path}\n"
            f"save the SoSoValue website's `pageProps.historyData.list` "
            f"array to that file first.")
    with open(path) as f:
        rows = json.load(f)
    if not isinstance(rows, list) or not rows:
        raise SystemExit(f"{path}: expected non-empty JSON array")
    return rows


def _to_seed_df(rows: list[dict]) -> pd.DataFrame:
    """Map the website-JSON row shape to the seed CSV column names.
    Drops the `_HH:MM:SS` suffix on `dataDate` so dates stay ISO-clean.
    Type coercion mirrors `sosovalue._fetch_summary_history`."""
    df = pd.DataFrame(rows)
    # Field renaming — website JSON uses camelCase, our seed uses snake_case.
    df = df.rename(columns={
        "dataDate": "date",
        "totalNetInflow": "total_net_inflow",
        "totalVolume": "total_value_traded",
        "totalNetAssets": "total_net_assets",
        "cumNetInflow": "cum_net_inflow",
    })
    # Strip the time component — seed CSVs store YYYY-MM-DD strings.
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"])
    for col in ("total_net_inflow", "total_value_traded",
                 "total_net_assets", "cum_net_inflow"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    keep = ["date", "total_net_inflow", "total_value_traded",
            "total_net_assets", "cum_net_inflow"]
    return df[keep]


def main() -> None:
    boot = _to_seed_df(_load_bootstrap_rows())
    print(f"bootstrap rows: {len(boot)} "
          f"({boot['date'].min().date()} → {boot['date'].max().date()})")

    # Merge with current seed if present — latest write wins per date.
    # The on-disk seed may have fresher rows than the manual dump (e.g.
    # if the cron ran after the user grabbed the JSON), so we keep
    # those over the bootstrap copy when they conflict.
    existing = seed_cache.read_seed_df(
        _SEEDS_DIR, _SEED_FN, parse_dates=["date"])
    if not existing.empty:
        print(f"existing seed rows: {len(existing)} "
              f"({existing['date'].min().date()} → "
              f"{existing['date'].max().date()})")
        # Concat existing AFTER bootstrap so dedupe keeps existing-side
        # when there's a same-date conflict. drop_duplicates keeps the
        # LAST occurrence by default — flip via `keep="last"`.
        merged = (pd.concat([boot, existing], ignore_index=True)
                    .sort_values("date")
                    .drop_duplicates(subset="date", keep="last")
                    .reset_index(drop=True))
    else:
        print("no existing seed — writing bootstrap as-is")
        merged = boot.sort_values("date").reset_index(drop=True)

    seed_cache.write_seed_df(merged, _SEEDS_DIR, _SEED_FN)
    print(f"final seed rows: {len(merged)} "
          f"({merged['date'].min().date()} → {merged['date'].max().date()})")


if __name__ == "__main__":
    main()
