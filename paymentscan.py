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


log = logging.getLogger(__name__)

_BASE = "https://paymentscan.xyz/api/v1"

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
                  include_topups: bool = True,
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
    return df


def fetch(endpoint: Endpoint, period: Period = "daily",
          include_topups: bool = True,
          include_offchain: bool = True,
          revision: str = "v1") -> tuple[pd.DataFrame, str | None]:
    """Public entry. Returns (DataFrame, error_message).
    On success the error is None; on failure the DataFrame is empty
    and the error is a short string (key missing, 401, 429, schema
    error, etc.) so renderers can surface the actual cause.

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
        return pd.DataFrame(), msg
