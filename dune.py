"""Dune Analytics REST fetcher (read-only `/results` endpoint).

Wraps the public Dune REST API into a single typed fetcher that
returns a normalised pandas DataFrame. Same shape conventions as
`paymentscan.py` / `sosovalue.py`:

- Streamlit-cached at the helper level (4 h TTL) so multiple chart
  renders inside one autorefresh tick reuse one HTTP round-trip.
- Key resolution: `st.secrets["DUNE_API_KEY"]` first (Cloud), env
  fallback (local). Empty frame on auth/network failure (caller
  shows an info placeholder).
- Uses `/results` (the latest materialised execution), NOT `/execute`
  — we consume the upstream-cached snapshot rather than triggering a
  fresh run, which would cost credits and take minutes.

Timestamp normalisation:
- Coalesces `day` / `date` / `Date` / `hour` / `time` / `Time` /
  `timestamp` into a single `day` column (datetime64[ns], tz-naive
  UTC). Dune query authors use whichever they prefer; the renderer
  only needs to know one column name.
- Tz-aware → tz-naive via `to_datetime(utc=True).dt.tz_localize(None)`
  so comparisons across queries with different timezone modes never
  raise.

Caller is responsible for renaming / casting non-timestamp columns.
"""
from __future__ import annotations

import os

import pandas as pd
import requests
import streamlit as st


@st.cache_data(ttl=14_400, show_spinner=False)
def fetch_dune_query_results(query_id: int,
                              revision: str = "v1") -> pd.DataFrame:
    """Pull the latest-cached results for a Dune query id via the
    public REST endpoint.

    `revision` is a cache-bust knob — bump v1 → v2 inside the 4h TTL
    window when the upstream query has been edited and you need the
    next page-load to re-fetch instead of waiting for TTL expiry.
    Stored in the cache key but otherwise unused.

    Returns an empty DataFrame on any failure (key missing, network
    error, 5xx, empty response). The renderer is expected to handle
    the empty case with an info placeholder."""
    try:
        key = st.secrets.get("DUNE_API_KEY", "")
    except Exception:
        key = ""
    if not key:
        key = os.environ.get("DUNE_API_KEY", "")
    if not key:
        return pd.DataFrame()
    try:
        r = requests.get(
            f"https://api.dune.com/api/v1/query/{query_id}/results",
            headers={"X-Dune-API-Key": key},
            # Generous cap; community Dune queries typically return
            # ≤ a few hundred rows. The cap protects us from a
            # mistakenly unbounded query author would-be DoS'ing us.
            params={"limit": 5000},
            timeout=30,
        )
        r.raise_for_status()
        rows = (r.json().get("result") or {}).get("rows") or []
    except Exception:
        return pd.DataFrame()
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    # Normalise the timestamp column to `day`. Dune queries on this
    # dashboard use a mix of `day` / `date` / `Date` / `hour` /
    # `time` / `Time` / `timestamp` depending on the author; coalesce
    # them all to one column name so renderers never need to guess.
    # First match wins; if none of the candidates exist the frame
    # passes through unchanged and the caller's missing-column guard
    # catches it.
    for _cand in ("day", "date", "Date",
                   "hour", "time", "Time", "timestamp"):
        if _cand in df.columns:
            df = df.rename(columns={_cand: "day"})
            # Force tz-naive UTC. Dune queries mix `timestamp(3) with
            # time zone` (most queries) and bare `timestamp(3)` (a
            # handful of older ones). Comparing a tz-aware Timestamp
            # with a tz-naive one — e.g. inside `max(asof_candidates)`
            # for headline KPIs — raises `Cannot compare tz-naive and
            # tz-aware`. Coercing via utc=True then dropping tz gives
            # one naive-UTC dtype that compares cleanly across queries.
            df["day"] = (pd.to_datetime(df["day"], utc=True)
                            .dt.tz_localize(None))
            df = df.sort_values("day").reset_index(drop=True)
            break
    return df
