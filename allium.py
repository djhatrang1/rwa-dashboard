"""Allium query fetcher (shared by both dashboards).

Wraps the Allium Explorer async-run pattern (POST run-async → poll run-
state → GET results) into a single `fetch_allium_query_results(qid)`
call returning a DataFrame. Cached 4h via @st.cache_data + raises-on-
failure inner so Streamlit's cache doesn't pin error states.

Reads ALLIUM_API_KEY from st.secrets first, env fallback.

Why a separate module: the original fetcher lived inside
solana_dashboard.py for the Jupiter-vs-DFlow comparison chart;
stocks_dashboard.py needs the same fetcher for the Stablecoin
payments asset vertical, but stocks_dashboard CAN'T import from
solana_dashboard (the dependency is one-way: solana imports stocks
as `sd`). Hoisting the fetcher here lets both dashboards reuse one
implementation without duplication or circular imports.
"""
from __future__ import annotations

import logging
import os
import time

import pandas as pd
import requests
import streamlit as st


log = logging.getLogger(__name__)

_BASE = "https://api.allium.so/api/v1/explorer/queries"


def _allium_key() -> str:
    """Resolve ALLIUM_API_KEY — st.secrets first (Streamlit Cloud),
    env fallback (local dev via .env)."""
    try:
        k = st.secrets.get("ALLIUM_API_KEY", "")
    except Exception:
        k = ""
    if not k:
        k = os.environ.get("ALLIUM_API_KEY", "")
    return k


def _allium_request(method: str, url: str, headers: dict,
                    json_body: dict | None = None,
                    timeout: int = 30,
                    max_attempts: int = 6) -> requests.Response:
    """HTTP wrapper retrying on 429 with backoff that honors
    Retry-After. Allium's starter tier rate-limits aggressively
    (~10 req/min); without this every other run errors out on the
    first 429. Sleep clamped to [1, 30] seconds per retry."""
    backoff = 2
    for _ in range(max_attempts):
        if json_body is not None:
            r = requests.post(url, json=json_body, headers=headers,
                              timeout=timeout)
        else:
            r = requests.get(url, headers=headers, timeout=timeout)
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
               show_spinner="Running Allium query (≈1–2 min)…")
def _fetch_cached(query_id: str, run_limit: int = 50_000,
                  poll_seconds: int = 10,
                  initial_wait_seconds: int = 15,
                  max_wait_seconds: int = 300) -> pd.DataFrame:
    """Cached inner — RAISES on failure (key missing, async timeout,
    run error). Raising matters because @st.cache_data caches return
    values but NOT exceptions, so a failed run won't get pinned for
    4h. Outer wrapper `fetch_allium_query_results` catches + returns
    empty so callers see a clean DataFrame."""
    key = _allium_key()
    if not key:
        raise RuntimeError("ALLIUM_API_KEY missing")
    H = {"X-API-KEY": key}

    # ── Step 1: kick off async run ─────────────────────────────────────
    r = _allium_request(
        "POST",
        f"{_BASE}/{query_id}/run-async",
        headers=H,
        json_body={"parameters": {}, "run_config": {"limit": run_limit}},
    )
    r.raise_for_status()
    run_id = r.json().get("run_id")
    if not run_id:
        raise RuntimeError(f"Allium {query_id}: no run_id returned")

    # ── Step 2: poll until done or timed out ───────────────────────────
    # Poll endpoint is `/explorer/query-runs/<run_id>` — keyed by run_id
    # alone (NOT by query_id like the run-async POST). Originally I had
    # this wrong (`/queries/{qid}/run-status/{run_id}`) and the fetcher
    # silently hung past max_wait because the URL 404'd / returned a
    # different shape, never matched `status=="success"`.
    time.sleep(initial_wait_seconds)
    elapsed = initial_wait_seconds
    state = ""
    while elapsed < max_wait_seconds:
        try:
            r = _allium_request(
                "GET",
                f"https://api.allium.so/api/v1/explorer/query-runs/{run_id}",
                headers=H,
            )
            r.raise_for_status()
            state = ((r.json() or {}).get("status") or "").lower()
        except Exception:
            state = "unknown"
        if state == "success":
            break
        if state in ("failed", "cancelled", "canceled", "error"):
            raise RuntimeError(
                f"Allium {query_id}: run ended with status={state}")
        time.sleep(poll_seconds)
        elapsed += poll_seconds
    else:
        raise RuntimeError(
            f"Allium {query_id}: run timed out after {max_wait_seconds}s"
        )

    # ── Step 3: fetch result rows ──────────────────────────────────────
    r = _allium_request(
        "GET",
        f"https://api.allium.so/api/v1/explorer/query-runs/{run_id}/results",
        headers=H,
    )
    r.raise_for_status()
    rows = (r.json() or {}).get("data") or []
    df = pd.DataFrame(rows)
    log.info("Allium %s: %d rows fetched", query_id, len(df))
    return df


def fetch_allium_query_results(
    query_id: str, run_limit: int = 50_000,
) -> tuple[pd.DataFrame, str | None]:
    """Public entry. Returns (DataFrame, error_message). On success the
    error is None; on failure the DataFrame is empty and the error is
    a short string describing what went wrong (key missing, timeout,
    429, schema error, etc.) so renderers can surface the actual
    cause instead of a generic "no data".

    The inner is cached + RAISES on failure (matters because
    @st.cache_data caches values but NOT exceptions — so a failed
    run isn't pinned for 4h and the next page-load auto-retries).
    """
    try:
        df = _fetch_cached(query_id, run_limit=run_limit)
        return df, None
    except Exception as exc:
        msg = f"{type(exc).__name__}: {exc}"
        log.warning("Allium %s fetch failed: %s", query_id, msg)
        return pd.DataFrame(), msg
