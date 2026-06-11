"""rwa.xyz historical seed loader.

The multi-year stacked-area chart on each rwa.xyz category page is
rendered client-side from rwa.xyz's enterprise REST API — the daily
time series isn't exposed through any public endpoint. Users export
the chart's underlying data via the "Download CSV" button on
app.rwa.xyz and drop the CSV into `rwa_seeds/`. This module reads
those CSVs into a wide DataFrame ready for stacked-area plotting.

Same seed model as `allium_seeds/*.csv` and `mc_seed_*.json` — disk
snapshot for immutable past data, refreshed manually when the user
wants to extend the time axis.

A live-scrape companion (snapshot of current aggregates, top-50 asset
list, league tables, etc.) lives in a separate uncommitted change —
this module ships seed-loading only.

Source CSV shape (verified on rwa-xyz-credit-market-caps.csv):
  Timestamp,Date,Measure,<asset_1>,<asset_2>,...,All Others (N items)
- Timestamp: ms-since-epoch (redundant with Date)
- Date:      ISO YYYY-MM-DD
- Measure:   typically 'Bridged Token Value (Dollar)'
- One column per named asset, NaN where the asset hadn't launched
- "All Others (N items)" rolls up the long-tail (beyond top 50)
"""
from __future__ import annotations

import logging
import os

import pandas as pd
import streamlit as st


log = logging.getLogger(__name__)

# Directory housing rwa.xyz historical seed CSVs (exported manually
# from the chart UI on app.rwa.xyz/<slug>). Resolved relative to this
# module so the path works both from `streamlit run` (CWD = repo
# root) and any cron script that imports `rwa_xyz` from a different
# CWD.
_SEED_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "rwa_seeds")


@st.cache_data(ttl=14_400, show_spinner=False)
def load_history_seed(slug: str = "credit") -> pd.DataFrame:
    """Load a category-level historical time series from an exported
    rwa.xyz CSV in `rwa_seeds/<slug>_market_caps.csv`.

    Returns a wide DataFrame with a `date` column (datetime) and one
    column per asset (display name verbatim from the CSV header) —
    the shape a stacked-area chart wants. The "All Others (N items)"
    rollup column is preserved as-is. NaN-fill is intact so each
    asset only plots from its inception (rwa.xyz's chart behaves
    the same).

    Returns an empty DataFrame if the seed file is missing or
    unreadable — caller surfaces that with an st.info.

    `slug` ∈ {"credit", "commodities", "treasuries", ...} — drives
    the filename lookup. New slugs auto-resolve once the user drops
    the matching CSV in.
    """
    filename = f"{slug}_market_caps.csv"
    path = os.path.join(_SEED_DIR, filename)
    if not os.path.exists(path):
        log.warning("rwa.xyz seed missing: %s", path)
        return pd.DataFrame()
    try:
        df = pd.read_csv(path)
    except Exception as exc:
        log.warning("rwa.xyz seed read failed for %s: %s", path, exc)
        return pd.DataFrame()
    if "Date" not in df.columns:
        log.warning("rwa.xyz seed %s lacks a Date column", path)
        return pd.DataFrame()
    df = df.copy()
    df["date"] = pd.to_datetime(df["Date"], errors="coerce")
    drop_cols = [c for c in ("Timestamp", "Date", "Measure")
                  if c in df.columns]
    df = df.drop(columns=drop_cols)
    # `date` first so downstream chart helpers can `df["date"]` without
    # worrying about column order.
    cols = ["date"] + [c for c in df.columns if c != "date"]
    df = df[cols].sort_values("date").reset_index(drop=True)
    log.info(
        "rwa.xyz seed loaded: %s — %d rows, %d series, %s → %s",
        filename, len(df), len(df.columns) - 1,
        df["date"].min(), df["date"].max())
    return df


def load_credit_history_seed() -> pd.DataFrame:
    """Convenience wrapper for the Tokenized Credit historical CSV."""
    return load_history_seed("credit")
