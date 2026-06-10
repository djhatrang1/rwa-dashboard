"""Blockworks Research perp-DEX data fetcher (Solana).

Scrapes the Blockworks Analytics dashboard page for current execution
IDs (the dashboard's React app embeds them into the SSR HTML, one per
chart), then pulls the cached query results from
`rest.blockworksresearch.com/v1/internal/studio/queries/executions`
— the public read-side of their Dune-clone "Studio" backend.

Why scrape vs use a stable query ID?
- The execution IDs are ephemeral (`run_*` UUIDs that change every
  refresh cycle) so we can't hardcode them.
- The integer query IDs are stable (e.g. 4594 = main volume/OI table,
  4630 = fees+rev, 4617 = market count, …) but the endpoint that
  resolves "query_id → latest execution" requires authentication
  (401 Unauthorized).
- The SSR-rendered dashboard page DOES embed the current `run_*`
  ids for every chart, so scraping the HTML is the no-auth path.

The map of integer query_id → chart purpose (derived from row
inspection — see schema notes in DASHBOARD_QUERY_LABELS below):
  4594 — daily volume / OI / trade count / liquidations across
         every dimension (Total / Crypto / FX / Commodity / Stock,
         per-exchange, per-symbol). The "everything" table.
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

All endpoints public, no auth, ~30 MB total payload across 11 queries
(cached for 4 h by us + by Blockworks' own scheduler).
"""
from __future__ import annotations

import re
import logging
from typing import Optional

import pandas as pd
import requests
import streamlit as st


log = logging.getLogger(__name__)

DASHBOARD_URL = "https://blockworks.com/analytics/solana/perp-dexs-solana"
REST_BASE = (
    "https://rest.blockworksresearch.com/v1/internal/studio/queries/executions"
)
_HTTP_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Origin": "https://blockworks.com",
    "Referer": DASHBOARD_URL,
    "Accept": "application/json, text/plain, */*",
}
_RUN_ID_RE = re.compile(r"run_[a-z0-9]{20,30}")


# Human-readable label per integer query_id (used in chart titles +
# raw-data download filenames). Derived from inspecting each query's
# row schema — see module docstring for the full mapping.
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


@st.cache_data(ttl=14_400, show_spinner=False)
def _scrape_run_ids(url: str = DASHBOARD_URL) -> list[str]:
    """Pull the dashboard page's SSR HTML and extract every `run_*`
    execution ID embedded in it (one per visualization). Cached 4 h."""
    try:
        r = requests.get(url, headers=_HTTP_HEADERS, timeout=30)
        r.raise_for_status()
    except Exception as exc:
        log.warning("Blockworks dashboard scrape failed for %s: %s",
                    url, exc)
        return []
    ids = sorted(set(_RUN_ID_RE.findall(r.text)))
    log.info("Scraped %d execution IDs from %s", len(ids), url)
    return ids


@st.cache_data(ttl=14_400, show_spinner=False)
def _fetch_execution_rows(run_id: str) -> tuple[Optional[int],
                                                pd.DataFrame]:
    """Fetch one execution's rows. Returns (query_id, DataFrame).
    query_id is None on failure; DataFrame is empty on failure.
    Cached 4 h per run_id."""
    # limit=50000 is the API's hard cap — values above return 400.
    # No query in this dashboard exceeds ~26k rows so this is safe.
    url = f"{REST_BASE}/{run_id}/rows?limit=50000"
    try:
        r = requests.get(url, headers=_HTTP_HEADERS, timeout=60)
        r.raise_for_status()
        j = r.json()
    except Exception as exc:
        log.warning("Blockworks execution fetch failed for %s: %s",
                    run_id, exc)
        return None, pd.DataFrame()
    meta = j.get("metadata", {}).get("execution", {})
    query_id = meta.get("query_id")
    rows = j.get("data") or []
    df = pd.DataFrame(rows)
    # Normalise the time column — different queries use 'dt' or 'date'.
    for tcol in ("dt", "date"):
        if tcol in df.columns:
            df[tcol] = pd.to_datetime(df[tcol], errors="coerce", utc=True)
            df[tcol] = df[tcol].dt.tz_localize(None)
            if tcol != "date":
                df = df.rename(columns={tcol: "date"})
            break
    return query_id, df


@st.cache_data(ttl=14_400, show_spinner="Loading Blockworks perp data…")
def fetch_perp_dex_data() -> dict[int, pd.DataFrame]:
    """One-shot fetcher: scrape current execution IDs + pull each
    query's rows. Returns {query_id: DataFrame}. Cached 4 h.

    Empty dict if the scrape returned nothing (network error etc.).
    Individual queries that fail are silently skipped — the renderer
    handles missing keys with st.info placeholders.
    """
    run_ids = _scrape_run_ids()
    if not run_ids:
        return {}
    out: dict[int, pd.DataFrame] = {}
    for rid in run_ids:
        qid, df = _fetch_execution_rows(rid)
        if qid is None or df.empty:
            continue
        # If two run_ids resolve to the same query_id (shouldn't
        # happen but be defensive), keep the larger one — more recent.
        existing = out.get(qid)
        if existing is None or len(df) > len(existing):
            out[qid] = df
    log.info("Blockworks perp data: fetched %d queries (%s)",
             len(out), sorted(out.keys()))
    return out
