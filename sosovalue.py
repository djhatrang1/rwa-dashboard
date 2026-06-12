"""SoSoValue API fetcher (ETF data).

Wraps the public ETF endpoints on https://openapi.sosovalue.com/openapi/v1
into typed fetchers returning DataFrames. Auth: `x-soso-api-key` header.

Key resolution: `st.secrets["SOSOVALUE_API_KEY"]` first (Cloud), env
fallback (local). Same shape as `paymentscan.py`.

Endpoints used:
  GET /etfs
      List ETF tickers under an asset symbol (one row per ETF).
  GET /etfs/summary-history?symbol=...&country_code=US&limit=300
      Per-day aggregate across all ETFs for the asset:
        date, total_net_inflow, total_value_traded,
        total_net_assets, cum_net_inflow
      The API returns 1–N rows per date (data revisions during the
      day). cum_net_inflow is identical across revisions; the flow
      fields can differ. We dedupe by `date` and keep the LATEST
      revision — the rows arrive newest-first, so groupby+first gets
      the freshest read. Older revisions are discarded.

Disk-snapshot fallback (sosovalue_seeds/) — same pattern as
paymentscan.py and defillama.py. On API failure, falls back to the
most recent CSV snapshot so the dashboard keeps rendering during a
SoSoValue outage.

Aggregate vs per-ticker: the `summary-history` endpoint already
delivers what the SOL ETF charts need (aggregated across all 8 SOL
ETFs). The per-ticker history endpoint `/etfs/{ticker}/history` is
restricted to a 1-month lookback window — fine for daily snapshots
but we don't need it for the aggregate dashboard charts.
"""
from __future__ import annotations

import logging
import os
from typing import Optional

import pandas as pd
import requests
import streamlit as st

import seed_cache


log = logging.getLogger(__name__)

_BASE = "https://openapi.sosovalue.com/openapi/v1"
_SEEDS_DIR = "sosovalue_seeds"


def _key() -> str:
    """Resolve SOSOVALUE_API_KEY — st.secrets first (Cloud), env fallback."""
    try:
        k = st.secrets.get("SOSOVALUE_API_KEY", "")
    except Exception:
        k = ""
    if not k:
        k = os.environ.get("SOSOVALUE_API_KEY", "")
    return k


@st.cache_data(ttl=14_400, show_spinner="Fetching SoSoValue ETF data…")
def _fetch_summary_history(symbol: str, country_code: str = "US",
                           limit: int = 300,
                           revision: str = "v1") -> pd.DataFrame:
    """Internal — RAISES on failure (key missing, HTTP error). The
    public `fetch_etf_summary_history` wrapper catches and routes to
    the seed-CSV fallback so st.cache_data doesn't pin error states.

    `revision` is a cache-bust knob — bump v1→v2 to force a refresh
    inside the 4h TTL window when schema changes ship."""
    key = _key()
    if not key:
        raise RuntimeError("SOSOVALUE_API_KEY missing")
    url = f"{_BASE}/etfs/summary-history"
    params = {
        "symbol": symbol,
        "country_code": country_code,
        # limit=300 is the API max and covers >1y of trading days,
        # more than enough for the SOL ETF history (launched mid-2026).
        "limit": limit,
    }
    r = requests.get(url, headers={"x-soso-api-key": key},
                      params=params, timeout=30)
    r.raise_for_status()
    body = r.json() or {}
    rows = body.get("data") or []
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    # Normalize types: date → datetime64; flows → float.
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    for col in ("total_net_inflow", "total_value_traded",
                 "total_net_assets", "cum_net_inflow"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=["date"])
    # Dedupe: rows arrive newest-revision-first per the API. The
    # cum_net_inflow value is identical across revisions of the same
    # date but the flow/volume fields can differ — keep the freshest
    # revision (first occurrence after sort-by-date-desc).
    df = (df.sort_values("date", ascending=False)
              .drop_duplicates(subset="date", keep="first")
              .sort_values("date")
              .reset_index(drop=True))
    log.info("SoSoValue %s summary-history: %d rows after dedupe",
              symbol, len(df))
    return df


def fetch_etf_summary_history(symbol: str = "SOL",
                              country_code: str = "US",
                              revision: str = "v1"
                              ) -> tuple[pd.DataFrame, Optional[str]]:
    """Public entry. Returns (DataFrame, error_message).
    DataFrame columns: date, total_net_inflow, total_value_traded,
    total_net_assets, cum_net_inflow.

    On success err is None and the returned frame is the MERGED view
    (live API rows ∪ existing seed). The seed is written back as the
    same merged frame so historical bootstrap data (see
    `scripts/bootstrap_sosovalue_seed.py`) survives — the API plan
    only returns ~30 days, but the seed accumulates every row we've
    ever seen.

    On failure the live DataFrame is empty and we serve the seed as-is;
    err is `seed-fallback (...)` when the seed served the response."""
    fn = f"summary_history_{symbol.lower()}_{country_code.lower()}.csv"
    try:
        live = _fetch_summary_history(symbol, country_code,
                                       revision=revision)
        if live.empty:
            # Treat empty live response the same as a failure — fall
            # back to the seed rather than overwriting it with nothing.
            fallback = seed_cache.read_seed_df(
                _SEEDS_DIR, fn, parse_dates=["date"])
            if not fallback.empty:
                return fallback, "seed-fallback (empty live response)"
            return live, None
        # Concat-and-dedupe with the existing seed. Live rows are the
        # freshest reading for any overlapping date, so we keep those
        # (`keep="last"` after putting live AFTER seed in the concat).
        existing = seed_cache.read_seed_df(
            _SEEDS_DIR, fn, parse_dates=["date"])
        if not existing.empty:
            merged = (pd.concat([existing, live], ignore_index=True)
                        .sort_values("date")
                        .drop_duplicates(subset="date", keep="last")
                        .reset_index(drop=True))
        else:
            merged = live
        seed_cache.write_seed_df(merged, _SEEDS_DIR, fn)
        return merged, None
    except Exception as exc:
        msg = f"{type(exc).__name__}: {exc}"
        log.warning("SoSoValue summary-history(%s) failed: %s",
                     symbol, msg)
        fallback = seed_cache.read_seed_df(_SEEDS_DIR, fn,
                                            parse_dates=["date"])
        if not fallback.empty:
            return fallback, f"seed-fallback ({msg})"
        return pd.DataFrame(), msg


@st.cache_data(ttl=14_400, show_spinner=False)
def _fetch_etfs(symbol: str, country_code: str = "US",
                revision: str = "v1") -> pd.DataFrame:
    """Internal — list of ETF tickers under an asset symbol. Raises
    on failure; the only public wrapper just catches to an empty df."""
    key = _key()
    if not key:
        raise RuntimeError("SOSOVALUE_API_KEY missing")
    r = requests.get(f"{_BASE}/etfs",
                      headers={"x-soso-api-key": key},
                      params={"symbol": symbol,
                              "country_code": country_code},
                      timeout=30)
    r.raise_for_status()
    rows = (r.json() or {}).get("data") or []
    return pd.DataFrame(rows)


def fetch_etfs_list(symbol: str = "SOL",
                    country_code: str = "US"
                    ) -> tuple[pd.DataFrame, Optional[str]]:
    """Public list-of-ETFs entry. DataFrame[ticker, name, exchange].
    Same seed-fallback as summary-history. Used to show the catalog
    of SOL ETFs included in the aggregate charts."""
    fn = f"etfs_{symbol.lower()}_{country_code.lower()}.csv"
    try:
        df = _fetch_etfs(symbol, country_code)
        if not df.empty:
            seed_cache.write_seed_df(df, _SEEDS_DIR, fn)
        return df, None
    except Exception as exc:
        msg = f"{type(exc).__name__}: {exc}"
        log.warning("SoSoValue /etfs(%s) failed: %s", symbol, msg)
        fallback = seed_cache.read_seed_df(_SEEDS_DIR, fn)
        if not fallback.empty:
            return fallback, f"seed-fallback ({msg})"
        return pd.DataFrame(), msg
