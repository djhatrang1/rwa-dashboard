"""Birdeye Hyperliquid perps fetcher.

Wraps the `/perps/v1/...` endpoint family on `public-api.birdeye.so`
into typed fetchers returning Python dicts / lists. Authentication is
the same `BIRDEYE_API_KEY` we already use for spot data, plus the
`x-perp: hyperliquid` header that scopes the request to Hyperliquid
(the only supported perps DEX as of this writing per their OpenAPI
spec).

Same shape as `allium.py` / `paymentscan.py`:
  - Cached inner (`_fetch_cached`) RAISES on failure so st.cache_data
    doesn't pin error states.
  - Public wrappers return `(data, error_message)` — error is None on
    success; a short string otherwise so renderers can surface the
    actual cause instead of a generic "no data".
  - `revision` kwarg is a cache-bust knob (bump v1 → v2 inside the
    4h TTL window).

Endpoints (per https://docs.birdeye.so/reference/get-perps-v1-token-list):
  /perps/v1/token/list            — all perp markets w/ OI/long/short
  /perps/v1/token/overview        — per-token detail (price, OI,
                                    1h/4h/1d/7d liquidations)
  /perps/v1/token/open_positions  — top wallets by position value
  /perps/v1/token/liquidation_map — price ladder of liquidation levels

RWA markets carry the `xyz:` prefix (xyz:SP500, xyz:GOLD, xyz:NVDA,
…). 70 markets, ~\$2.84B total OI at the time of writing.

API quirks discovered:
  - Path is `/token/list` (slash) not `/token-list` (dash) — the
    Birdeye docs slug format is dashed, the actual API path uses
    slashes
  - Header is `x-perp` (singular) — `x-chain` is rejected as
    "Invalid perp input" with 422
  - `limit` query param max is 20 (despite the OpenAPI spec saying
    50) — paginating with limit=20 + offset=0,20,40,…
"""
from __future__ import annotations

import logging
import os
import time

import pandas as pd
import requests
import streamlit as st


log = logging.getLogger(__name__)

_BASE = "https://public-api.birdeye.so/perps/v1"
_PER_PAGE_LIMIT = 20   # hardcoded max per page (server-enforced)


def _api_key() -> str:
    """Resolve BIRDEYE_API_KEY — st.secrets first (Cloud), env fallback."""
    try:
        k = st.secrets.get("BIRDEYE_API_KEY", "")
    except Exception:
        k = ""
    if not k:
        k = os.environ.get("BIRDEYE_API_KEY", "")
    return k


def _headers() -> dict:
    """Standard header set: API key, accept-json, and the perps-DEX
    scoping header (currently only `hyperliquid` is supported per the
    Birdeye OpenAPI enum)."""
    return {
        "X-API-KEY": _api_key(),
        "accept": "application/json",
        "x-perp": "hyperliquid",
    }


def _request(url: str, params: dict | None = None,
             timeout: int = 30, max_attempts: int = 5) -> requests.Response:
    """GET with 429 backoff honoring Retry-After.
    Sleep clamped to [1, 30] seconds per retry."""
    backoff = 2
    for _ in range(max_attempts):
        r = requests.get(url, headers=_headers(), params=params,
                          timeout=timeout)
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
               show_spinner="Fetching Hyperliquid perps…")
def _fetch_token_list_cached(time_frame: str = "all",
                              sort_by: str = "open_interest",
                              sort_type: str = "desc",
                              revision: str = "v1") -> list[dict]:
    """Paginated /token/list — returns ALL perp markets across pages.

    The server caps `limit` at 20 (despite docs saying 50), so we
    page until empty. Caching as one big list de-dupes the 15+
    HTTP calls behind a single 4h-cached object.

    RAISES on failure — outer wrapper catches and returns ([], err).
    """
    key = _api_key()
    if not key:
        raise RuntimeError("BIRDEYE_API_KEY missing")
    all_items: list[dict] = []
    seen_tokens: set[str] = set()
    # Hyperliquid has ~300 perps total; cap at 500 offsets just to
    # avoid an infinite loop if the API ever changes pagination.
    for offset in range(0, 500, _PER_PAGE_LIMIT):
        r = _request(f"{_BASE}/token/list", params={
            "time_frame": time_frame,
            "sort_by":    sort_by,
            "sort_type":  sort_type,
            "offset":     offset,
            "limit":      _PER_PAGE_LIMIT,
        })
        r.raise_for_status()
        body = r.json() or {}
        items = body.get("data") or []
        if not items:
            break
        fresh = [t for t in items
                  if (n := t.get("token")) and n not in seen_tokens]
        if not fresh:
            # No new tokens on this page — server returned a duplicate
            # page (some APIs repeat the last page when offset exceeds
            # total). Stop iterating.
            break
        all_items.extend(fresh)
        seen_tokens.update(t.get("token") for t in fresh)
    log.info("Birdeye perps token/list: %d unique markets", len(all_items))
    return all_items


def fetch_token_list(time_frame: str = "all",
                     sort_by: str = "open_interest",
                     sort_type: str = "desc",
                     revision: str = "v1"
                     ) -> tuple[list[dict], str | None]:
    """Public entry. Returns (list_of_dicts, error_message).
    Each dict has these keys per the API response:
      token, long_io, short_io, open_interest, margin, margin_used,
      entry_margin, unrealized_pnl, bias, leverage, bias_text
    """
    try:
        items = _fetch_token_list_cached(
            time_frame=time_frame, sort_by=sort_by,
            sort_type=sort_type, revision=revision)
        return items, None
    except Exception as exc:
        msg = f"{type(exc).__name__}: {exc}"
        log.warning("Birdeye perps token/list failed: %s", msg)
        return [], msg


@st.cache_data(ttl=14_400, show_spinner=False)
def _fetch_token_overview_cached(token: str,
                                  revision: str = "v1") -> dict:
    """Per-token detail. Returns price, position_count, open_interest,
    long/short_liquidation_{1h,4h,1d,7d}. Empty dict if not found."""
    key = _api_key()
    if not key:
        raise RuntimeError("BIRDEYE_API_KEY missing")
    r = _request(f"{_BASE}/token/overview", params={"token": token})
    r.raise_for_status()
    return (r.json() or {}).get("data") or {}


def fetch_token_overview(token: str,
                          revision: str = "v1"
                          ) -> tuple[dict, str | None]:
    """Public entry. Returns (overview_dict, error_message)."""
    try:
        return _fetch_token_overview_cached(token, revision=revision), None
    except Exception as exc:
        return {}, f"{type(exc).__name__}: {exc}"


@st.cache_data(ttl=14_400, show_spinner=False)
def _fetch_open_positions_cached(token: str,
                                  sort_by: str = "position_value",
                                  sort_type: str = "desc",
                                  limit: int = 20,
                                  revision: str = "v1") -> list[dict]:
    """Top open positions for a token (whale concentration view).
    Each item has wallet, leverage_value, entry/mark price,
    position_value, unrealized_pnl, open_time."""
    key = _api_key()
    if not key:
        raise RuntimeError("BIRDEYE_API_KEY missing")
    r = _request(f"{_BASE}/token/open_positions", params={
        "token":     token,
        "sort_by":   sort_by,
        "sort_type": sort_type,
        "offset":    0,
        "limit":     limit,
    })
    r.raise_for_status()
    return (r.json() or {}).get("data") or []


def fetch_open_positions(token: str, limit: int = 20,
                          revision: str = "v1"
                          ) -> tuple[list[dict], str | None]:
    """Public entry. Returns (list_of_position_dicts, error_message)."""
    try:
        return _fetch_open_positions_cached(token, limit=limit,
                                              revision=revision), None
    except Exception as exc:
        return [], f"{type(exc).__name__}: {exc}"


# ── RWA categorization helpers ──────────────────────────────────────
# 70 RWA perps on Hyperliquid carry the `xyz:` prefix. Categorize by
# the underlying asset class for the chart's grouping. Symbols not
# matched fall through to "Other RWA" — currently includes a handful
# of pre-IPO / exotic tickers (SPCX = SpaceX, SKHX, CBRS, QNT, etc.)

_RWA_PREFIX = "xyz:"

# Curated taxonomy — kept manually rather than parsed from a TLD/
# ticker registry because Hyperliquid's symbol set is small (~70) and
# evolves slowly. New listings get an "Other" bucket until classified.
_RWA_CATEGORIES: dict[str, set[str]] = {
    "Commodities": {
        "GOLD", "SILVER", "COPPER", "PLATINUM", "PALLADIUM",
        "BRENTOIL", "CL", "NATGAS",
    },
    "Indices": {
        "SP500", "XYZ100", "JP225", "KR200",
        "EWY", "EWJ", "EWT", "EWZ", "XLE",
    },
    "FX": {
        "EUR", "JPY", "GBP",
    },
    # US single-name equities — explicitly enumerated so additions
    # (when Hyperliquid lists new tickers) don't silently land in
    # "Other" until someone notices.
    "US Equities": {
        "NVDA", "MSFT", "AAPL", "GOOGL", "AMZN", "META", "TSLA",
        "AMD", "AVGO", "MU", "INTC", "ORCL", "LLY", "NFLX", "NOW",
        "ARM", "COST", "BABA", "RIVN", "COIN", "MSTR", "HOOD",
        "PLTR", "GME", "HIMS", "RKLB", "DKNG", "ZM", "DELL", "IBM",
        "USAR", "URNM", "BB", "CRWV", "CRCL", "NBIS", "BIRD",
        "DRAM", "LITE", "ASML", "TSM", "SMSN", "HYUNDAI", "MRVL",
        "SNDK",
    },
    # Pre-IPO / exotic / ETFs that don't fit elsewhere — name kept
    # generic so the chart's legend reads cleanly.
    "Other RWA": {
        "SPCX",   # SpaceX pre-IPO
        "SKHX",   # opaque ticker
        "CBRS",   # opaque ticker
        "QNT",    # likely Quant Network or similar
        "PURRDAT","BIRD",
    },
}


def categorize(token: str) -> str:
    """Return the RWA category for an xyz:* token; None for non-RWA."""
    if not token.startswith(_RWA_PREFIX):
        return None
    bare = token[len(_RWA_PREFIX):]
    for cat, members in _RWA_CATEGORIES.items():
        if bare in members:
            return cat
    return "Other RWA"


def is_rwa(token: str) -> bool:
    """True if the token symbol carries the `xyz:` prefix (= one of
    Hyperliquid's RWA / tokenized-asset perp markets)."""
    return bool(token) and token.startswith(_RWA_PREFIX)
