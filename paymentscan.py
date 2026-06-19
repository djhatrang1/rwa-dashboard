"""Paymentscan API fetcher (shared by both dashboards).

Wraps https://paymentscan.xyz/api/v1/ into typed fetchers returning
DataFrames. All endpoints support daily / weekly / monthly aggregation.

Auth: Bearer token in the Authorization header. Resolved from
`st.secrets["PAYMENTSCAN_API_KEY"]` first, env fallback.

Same shape as `allium.py`:
  - Cached inner (`_fetch_cached`) RAISES on failure so st.cache_data
    doesn't pin error states.
  - Public wrapper returns `(df, error_message)` — error is None on
    success; a short string otherwise so renderers can surface the
    actual cause instead of a generic "no data".
  - `revision` kwarg is a cache-bust knob (bump v1 → v2 to force a
    refresh inside the 4h TTL window).

Endpoints (see https://paymentscan.xyz/api-docs):
  projects/{period}      — per-card metrics
  chains/{period}        — by settlement chain
  currencies/{period}    — by settlement currency  [requires full.read]
  networks/{period}      — by payment network      [requires full.read]
  infra/{period}         — by card provider        [requires full.read]
"""
from __future__ import annotations

import logging
import os
import time
from typing import Literal

import pandas as pd
import requests
import streamlit as st

import seed_cache


log = logging.getLogger(__name__)

_BASE = "https://paymentscan.xyz/api/v1"

# On-disk fallback dir. The live fetcher writes its DataFrame here on
# every success; on failure we read the most recent snapshot back so
# the dashboard keeps rendering during Paymentscan outages or
# rate-limiting. Same seed pattern as `allium_seeds/`,
# `blockworks_seeds/`, etc.
_SEEDS_DIR = "paymentscan_seeds"


def _seed_filename(endpoint: str, period: str,
                    include_topups: bool,
                    include_offchain: bool) -> str:
    """Seed file naming: `<endpoint>_<period>[_no_topups][_no_offchain].csv`.
    The flag suffixes only appear when set non-default — so the
    common case (both true) writes the cleanest filename. Keeps the
    seed dir readable in the repo listing."""
    parts = [endpoint, period]
    if not include_topups:
        parts.append("no_topups")
    if not include_offchain:
        parts.append("no_offchain")
    return "_".join(parts) + ".csv"

Period = Literal["daily", "weekly", "monthly"]
Endpoint = Literal["projects", "chains", "currencies", "networks", "infra"]

_VALID_ENDPOINTS = {"projects", "chains", "currencies", "networks", "infra"}
_VALID_PERIODS = {"daily", "weekly", "monthly"}


def _key() -> str:
    """Resolve PAYMENTSCAN_API_KEY — st.secrets first (Cloud), env fallback."""
    try:
        k = st.secrets.get("PAYMENTSCAN_API_KEY", "")
    except Exception:
        k = ""
    if not k:
        k = os.environ.get("PAYMENTSCAN_API_KEY", "")
    return k


def _request(url: str, headers: dict, params: dict | None = None,
             timeout: int = 30, max_attempts: int = 6) -> requests.Response:
    """GET wrapper with 429 backoff honoring Retry-After header.
    Sleep clamped to [1, 30] seconds per retry."""
    backoff = 2
    for _ in range(max_attempts):
        r = requests.get(url, headers=headers, params=params, timeout=timeout)
        if r.status_code != 429:
            return r
        wait = r.headers.get("Retry-After")
        try:
            wait_s = int(wait) if wait else backoff
        except ValueError:
            wait_s = backoff
        wait_s = min(max(wait_s, 1), 30)
        time.sleep(wait_s)
        backoff = min(backoff * 2, 30)
    return r


@st.cache_data(ttl=14_400,
               show_spinner="Fetching Paymentscan data…")
def _fetch_cached(endpoint: str, period: str,
                  include_topups: bool = False,
                  include_offchain: bool = True,
                  revision: str = "v1") -> pd.DataFrame:
    """Cached inner — RAISES on failure (key missing, HTTP error, bad
    schema). Raising matters because @st.cache_data caches return
    values but NOT exceptions, so a failed call won't get pinned for
    4h. The outer wrapper catches + returns an empty DataFrame."""
    if endpoint not in _VALID_ENDPOINTS:
        raise ValueError(
            f"Paymentscan: unknown endpoint {endpoint!r} "
            f"(want one of {sorted(_VALID_ENDPOINTS)})")
    if period not in _VALID_PERIODS:
        raise ValueError(
            f"Paymentscan: unknown period {period!r} "
            f"(want one of {sorted(_VALID_PERIODS)})")
    key = _key()
    if not key:
        raise RuntimeError("PAYMENTSCAN_API_KEY missing")

    url = f"{_BASE}/{endpoint}/{period}"
    headers = {"Authorization": f"Bearer {key}"}
    # API docs spell these as camelCase query params.
    params = {
        "includeTopups": "true" if include_topups else "false",
        "includeOffchainData": "true" if include_offchain else "false",
    }
    r = _request(url, headers=headers, params=params)
    r.raise_for_status()
    body = r.json() or {}
    rows = body.get("data") or []
    df = pd.DataFrame(rows)
    log.info("Paymentscan %s/%s: %d rows", endpoint, period, len(df))
    # On live success, persist to disk so future failures have a
    # snapshot to fall back on. write_seed_df no-ops on empty frames
    # so a degraded live response can't overwrite a good prior seed.
    if not df.empty:
        seed_cache.write_seed_df(
            df, _SEEDS_DIR,
            _seed_filename(endpoint, period,
                            include_topups, include_offchain))
    return df


def fetch(endpoint: Endpoint, period: Period = "daily",
          include_topups: bool = False,
          include_offchain: bool = True,
          revision: str = "v4") -> tuple[pd.DataFrame, str | None]:
    """Public entry. Returns (DataFrame, error_message).
    On success the error is None; on failure the DataFrame is empty
    and the error is a short string (key missing, 401, 429, schema
    error, etc.) so renderers can surface the actual cause.

    `include_topups` defaults to False. Confirmed with the
    Paymentscan team that the API filter is correct and the UI
    toggle is buggy:

      • API includeTopups=false → returns ONLY rows that
        represent actual user-to-merchant card payments. Chains
        whose entire activity is top-ups (TRON, BSC) drop out
        because there's no payment activity to report there —
        users load USDT cheaply via TRON/BSC and spend through
        L2s / Solana. This is the correct view of "card payment
        volume."
      • API includeTopups=true → gross flow including top-ups.
        TRON/BSC dominate because that's where top-up volume
        sits. Useful for a diagnostic "what's the funding leg
        doing" view, but NOT what users mean by "card payment
        volume."
      • Paymentscan UI "INCLUDE TOP-UPS = false" toggle → looks
        like topups are being subtracted but actually just
        shows the same data as `=true`. UI bug; the team
        acknowledged it.

    Earlier docstring revisions in this file misdiagnosed the
    direction of the disagreement; this is now the canonical
    behaviour. Caches bumped to v4 with every flip during
    investigation.

    Pass `include_topups=True` only when you specifically want a
    diagnostic view of gross flow including funding events.

    `revision` is a cache-bust knob: bump the string (v1 → v2) when
    Paymentscan ships a schema change and you want the next page-
    load to fetch fresh data instead of waiting up to 4h for the
    TTL to expire. The value goes into the @st.cache_data key but
    is otherwise unused — pure invalidation."""
    try:
        df = _fetch_cached(endpoint, period,
                           include_topups=include_topups,
                           include_offchain=include_offchain,
                           revision=revision)
        return df, None
    except Exception as exc:
        msg = f"{type(exc).__name__}: {exc}"
        log.warning("Paymentscan %s/%s fetch failed: %s",
                    endpoint, period, msg)
        # Live API down — try the disk snapshot before giving up.
        # Successful reads return an err of `seed:<msg>` so the caller
        # can tell the difference between "live" and "fell back to
        # disk" if it wants to surface that to the user, while non-
        # falsy err still preserves the original failure mode for
        # call sites that bail on any error.
        fallback = seed_cache.read_seed_df(
            _SEEDS_DIR,
            _seed_filename(endpoint, period,
                            include_topups, include_offchain))
        if not fallback.empty:
            return fallback, f"seed-fallback ({msg})"
        return pd.DataFrame(), msg
