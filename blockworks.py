"""Blockworks Research perp-DEX data fetcher (Solana).

Hits Blockworks' newer public REST endpoints on `blockworks.com` to pull
the same chart data the analytics dashboard renders. Falls back to
on-disk CSV seeds under `blockworks_seeds/` when the live endpoints
fail — so the dashboard keeps rendering historical data even if
Blockworks restructures the page or temporarily breaks the API.

API surface (both unauthenticated, returned JSON):

  GET https://blockworks.com/api/studio/dashboards?ids=905
      One-shot discovery. Returns:
        { "data": [ { ..., "visualizations": [ ... ] } ] }
      Each visualization has: id, type, slug, title, description,
      queryId, lastExecutionId, executionState, ... — everything we
      need to map a chart slot to a query.

  GET https://blockworks.com/api/studio/dashboard/905/visualization/{viz_id}/execution?limit=50000&page=1
      Per-viz row fetch. Returns:
        { "data": [ row, ... ], "metadata": { "execution": {...} },
          "page": 1, "total": N }
      The metadata.execution carries the same query_id we used to key
      DataFrames on in the old API.

Dashboard 905 has 23 visualizations covering 11 unique query_ids
(several queries — notably 4594 — power multiple chart slots that show
different cuts of the same underlying table). We fetch one viz per
unique query_id and de-dupe the results.

Prior history (kept for context — the old codepath this replaces):

The earlier implementation scraped the SSR HTML of
`blockworks.com/analytics/solana/perp-dexs-solana` for ephemeral
`run_<id>` execution IDs, then hit
`rest.blockworksresearch.com/v1/internal/studio/queries/executions/{run_id}/rows`.
Around 2026-06-10 Blockworks moved data fetching fully client-side,
the SSR HTML stopped embedding `run_<id>` IDs, and the SSR-scrape path
broke. The new `blockworks.com/api/studio/...` endpoints are what the
React app calls now — so we're calling exactly what the browser calls,
no scraping, no auth.

Map of integer query_id → chart purpose (unchanged from the old
implementation; the same query IDs survived the platform migration):
  4594 — daily volume / OI / trade count / liquidations across every
         dimension (Total / Crypto / FX / Commodity / Stock,
         per-exchange, per-symbol). The "everything" table; 8 chart
         slots read different columns from this one query.
  4595 — leader-board ranking snapshot (exchange / market / category)
  4600 — per-DEX KPI deltas snapshot (vol / OI / TC)
  4601 — per-symbol/asset KPI deltas snapshot
  4617 — number-of-markets per exchange, daily timeseries
  4625 — commodities perp daily (XAU/WTI/PAXG/XAG)
  4626 — equities perp daily (SPCX/SKHYNIX/TSLA/SAMSUNG/…)
  4627 — index perp daily (SP500/SPY/URNM/QQQ)
  4628 — FX perp daily (AUD/EUR/CHF/JPY)
  4630 — fees + revenue per DEX, daily timeseries
  4631 — per-DEX fee/rev deltas snapshot
"""
from __future__ import annotations

import os
import logging
from typing import Optional

import pandas as pd
import requests
import streamlit as st


log = logging.getLogger(__name__)

# Dashboard 905 = the public "Solana Perp DEXs" analytics page.
DASHBOARD_ID = 905
DASHBOARD_SLUG = "perp-dexs-solana"   # for reference only

API_BASE = "https://blockworks.com"

# Public dashboard URL — kept for downstream call sites that link to
# the source page in chart captions. Same value the prior
# implementation exported.
DASHBOARD_URL = (
    f"https://blockworks.com/analytics/solana/{DASHBOARD_SLUG}")
_HTTP_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Origin": "https://blockworks.com",
    "Referer": f"https://blockworks.com/analytics/solana/{DASHBOARD_SLUG}",
    "Accept": "application/json, text/plain, */*",
}

# Where seed CSVs live. Resolved relative to this module so the path
# works both from `streamlit run` (CWD = repo root) and from a cron
# script that imports `blockworks` with a different CWD.
_SEED_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "blockworks_seeds")


# Human-readable label per integer query_id — used in chart titles +
# raw-data download filenames. Order matches the on-disk seed CSV
# naming convention (`blockworks_query_<qid>_<label>.csv`).
DASHBOARD_QUERY_LABELS = {
    4594: "all_metrics_daily",
    4595: "leaderboard_snapshot",
    4600: "per_dex_kpi_snapshot",
    4601: "per_symbol_kpi_snapshot",
    4617: "num_markets_daily",
    4625: "commodity_perps_daily",
    4626: "equity_perps_daily",
    4627: "index_perps_daily",
    4628: "fx_perps_daily",
    4630: "fees_revenue_daily",
    4631: "per_dex_fees_snapshot",
}


def _normalize_time_col(df: pd.DataFrame) -> pd.DataFrame:
    """Rename `dt` → `date` if present, parse to naive UTC datetime.
    Returns the DataFrame in-place-modified shape (defensive copy).
    Matches the column shape every other puller in this codebase
    exposes downstream."""
    if df.empty:
        return df
    for tcol in ("dt", "date"):
        if tcol in df.columns:
            df[tcol] = pd.to_datetime(df[tcol], errors="coerce", utc=True)
            df[tcol] = df[tcol].dt.tz_localize(None)
            if tcol != "date":
                df = df.rename(columns={tcol: "date"})
            break
    return df


@st.cache_data(ttl=14_400, show_spinner=False)
def _discover_visualizations() -> list[dict]:
    """Hit the dashboard-list endpoint and return the list of viz
    dicts for dashboard 905. Each dict has at least:
      id, type, slug, title, queryId, lastExecutionId, executionState

    Cached 4h. RAISES on failure — outer wrapper catches and routes
    to the seed-CSV fallback so st.cache_data doesn't pin error
    states for 4h."""
    url = f"{API_BASE}/api/studio/dashboards?ids={DASHBOARD_ID}"
    r = requests.get(url, headers=_HTTP_HEADERS, timeout=30)
    r.raise_for_status()
    j = r.json()
    data = j.get("data") or []
    if not data:
        raise RuntimeError(
            f"dashboards?ids={DASHBOARD_ID} returned empty data array")
    vizs = (data[0] or {}).get("visualizations") or []
    log.info("Blockworks discovery: %d visualizations on dashboard %d",
              len(vizs), DASHBOARD_ID)
    return vizs


@st.cache_data(ttl=14_400, show_spinner=False)
def _fetch_viz_rows(viz_id: str) -> tuple[Optional[int], pd.DataFrame]:
    """Fetch one visualization's rows. Returns (query_id, DataFrame).
    query_id is None on failure; DataFrame is empty on failure.
    Cached 4h per viz_id.

    limit=50000 is the API's hard cap. The biggest query in this
    dashboard (4594 / "all_metrics_daily") returns ~26k rows so it
    fits comfortably."""
    url = (f"{API_BASE}/api/studio/dashboard/{DASHBOARD_ID}/visualization/"
           f"{viz_id}/execution?limit=50000&page=1")
    try:
        r = requests.get(url, headers=_HTTP_HEADERS, timeout=120)
        r.raise_for_status()
        j = r.json()
    except Exception as exc:
        log.warning("Blockworks viz %s fetch failed: %s", viz_id, exc)
        return None, pd.DataFrame()
    meta = j.get("metadata", {}).get("execution", {})
    query_id = meta.get("query_id")
    df = pd.DataFrame(j.get("data") or [])
    df = _normalize_time_col(df)
    return query_id, df


def _load_seed(query_id: int) -> pd.DataFrame:
    """Load the on-disk CSV for one query_id. Filename format:
      blockworks_seeds/blockworks_query_<query_id>_<label>.csv
    Returns empty DataFrame if no seed file exists (or read fails).

    Seeds are immutable historical snapshots — pulled once per
    snapshot script run (see `scripts/snapshot_blockworks.py`) and
    committed to the repo. The dashboard falls back to them whenever
    the live API can't be reached, so the chart never goes blank
    just because Blockworks restructured a page."""
    if not os.path.isdir(_SEED_DIR):
        return pd.DataFrame()
    prefix = f"blockworks_query_{query_id}_"
    candidates = [f for f in os.listdir(_SEED_DIR)
                   if f.startswith(prefix) and f.endswith(".csv")]
    if not candidates:
        return pd.DataFrame()
    # If multiple snapshots exist for the same query (older + newer
    # naming), pick the most recently modified file.
    candidates.sort(
        key=lambda f: os.path.getmtime(os.path.join(_SEED_DIR, f)),
        reverse=True)
    path = os.path.join(_SEED_DIR, candidates[0])
    try:
        df = pd.read_csv(path)
    except Exception as exc:
        log.warning("Seed CSV read failed for %s: %s", path, exc)
        return pd.DataFrame()
    df = _normalize_time_col(df)
    log.info("Loaded seed for query %d: %s (%d rows)",
              query_id, candidates[0], len(df))
    return df


@st.cache_data(ttl=14_400, show_spinner="Loading Blockworks perp data…")
def fetch_perp_dex_data() -> dict[int, pd.DataFrame]:
    """One-shot fetcher. Returns `{query_id: DataFrame}` for the
    dashboard. Cached 4h.

    Resolution order per query_id:
      1. Live API — discovery + viz fetch via the public
         `blockworks.com/api/studio/...` endpoints
      2. On-disk CSV seed under `blockworks_seeds/` — committed
         historical snapshot, used when the live call fails for any
         reason (network, auth flip, JSON shape change, etc.)

    The fallback is per-query, not all-or-nothing: if 9 of the 11
    queries succeed live but 2 fail, the dashboard renders 9 with
    live data + 2 with seeded data (possibly stale by however long
    since the last `snapshot_blockworks.py` run).

    The renderers downstream don't see (or care about) which path
    each query came from — same dict shape, same DataFrame schema."""
    out: dict[int, pd.DataFrame] = {}

    # Step 1: discover what visualizations exist on the dashboard.
    try:
        viz_list = _discover_visualizations()
    except Exception as exc:
        log.warning("Blockworks discovery failed (%s) — "
                     "all queries will load from disk seeds", exc)
        viz_list = []

    # Step 2: fetch one viz per unique query_id. Prefer timeseries
    # over table/counter so the row schema we cache for query 4594
    # (read by 8 different chart slots) is the canonical long form.
    #
    # NOTE: the discovery endpoint returns `queryId` as a STRING
    # ("4594"), but our public-facing dict is keyed by INT (matching
    # DASHBOARD_QUERY_LABELS and the original implementation). We
    # coerce here so the dedupe set + the downstream call sites
    # don't have to worry about str-vs-int mismatches.
    def _to_int(x):
        try:
            return int(x)
        except (TypeError, ValueError):
            return None

    live_qids: set[int] = set()
    if viz_list:
        viz_sorted = sorted(
            viz_list,
            key=lambda v: 0 if v.get("type") == "timeseries" else 1)
        for v in viz_sorted:
            qid = _to_int(v.get("queryId"))
            if qid is None or qid in out:
                continue
            viz_id = v.get("id")
            if not viz_id:
                continue
            _, df = _fetch_viz_rows(viz_id)
            if not df.empty:
                out[qid] = df
                live_qids.add(qid)

    # Step 3: for every query we expect, fall back to disk seed if
    # the live fetch didn't fill it.
    seed_qids: set[int] = set()
    for qid in DASHBOARD_QUERY_LABELS:
        if qid in out and not out[qid].empty:
            continue
        seed = _load_seed(qid)
        if not seed.empty:
            out[qid] = seed
            seed_qids.add(qid)

    log.info(
        "Blockworks perp data: %d queries total (%d live, %d seed-fallback)",
        len(out), len(live_qids), len(seed_qids))
    return out
