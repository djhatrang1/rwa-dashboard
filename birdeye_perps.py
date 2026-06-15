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
# Hyperliquid's HIP-3 perps carry a builder-dex prefix. We scope this
# module to the `xyz:` dex (the largest RWA dex by OI; ~77 markets).
#
# Categories come from Hyperliquid's `perpConciseAnnotations` info
# endpoint — they tag every listed perp with one of {stocks,
# commodities, indices, fx, preipo, crypto}. The annotation seed lives
# at `hyperliquid_seeds/perp_annotations.json`, refreshed weekly by
# the GitHub Actions cron (see
# `.github/workflows/hyperliquid_categories.yml` +
# `scripts/refresh_hyperliquid_seed.py`).
#
# The hand-curated `_FALLBACK_RWA_CATEGORIES` dict below is kept as a
# defensive backstop — if the seed file is missing or unreadable
# (broken deploy, fresh clone before cron has run), `categorize()`
# falls back to the manual classification so the chart still renders
# something coherent. Once the seed is in place the seed wins for any
# overlapping symbol.

_RWA_PREFIX = "xyz:"

# Hyperliquid's category labels → our display labels. Keep the chart's
# legend wording stable even as Hyperliquid renames buckets upstream.
_HL_CAT_TO_DISPLAY: dict[str, str] = {
    "stocks":      "US Equities",
    "commodities": "Commodities",
    "indices":     "Indices",
    "fx":          "FX",
    "preipo":      "Other RWA",
    # `crypto` shouldn't appear on xyz: but defensively bucket it
    # somewhere visible rather than swallowing it.
    "crypto":      "Other RWA",
}


def _normalize_hl_category(raw: str | None) -> str | None:
    """Map a Hyperliquid `perpConciseAnnotations` category string to
    our display label. Case-insensitive (Hyperliquid has shipped both
    `fx` and `FX`). None / unknown → None so the caller can fall back
    to the hand-curated dict."""
    if not raw:
        return None
    return _HL_CAT_TO_DISPLAY.get(raw.lower())


# Hand-curated backstop — used only when the seed file is missing or
# returns no match. Order doesn't matter; lookup is by token name.
_FALLBACK_RWA_CATEGORIES: dict[str, set[str]] = {
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
    "US Equities": {
        "NVDA", "MSFT", "AAPL", "GOOGL", "AMZN", "META", "TSLA",
        "AMD", "AVGO", "MU", "INTC", "ORCL", "LLY", "NFLX", "NOW",
        "ARM", "COST", "BABA", "RIVN", "COIN", "MSTR", "HOOD",
        "PLTR", "GME", "HIMS", "RKLB", "DKNG", "ZM", "DELL", "IBM",
        "USAR", "URNM", "BB", "CRWV", "CRCL", "NBIS",
        "DRAM", "LITE", "ASML", "TSM", "SMSN", "HYUNDAI", "MRVL",
        "SNDK",
    },
    "Other RWA": {
        "SPCX",   # SpaceX pre-IPO
        "SKHX",   # opaque ticker
        "CBRS",   # opaque ticker
        "QNT",    # likely Quant Network or similar
        "PURRDAT", "BIRD",
    },
}


def _load_seed_categories() -> dict[str, str]:
    """Parse the on-disk Hyperliquid annotation seed into
    {bare_ticker: display_category} for the xyz: dex only.

    LRU-cached on the loader (see decorator) so repeated calls within
    one process pay the JSON read + parse cost exactly once. The seed
    file is replaced atomically by the weekly cron — process restart
    picks up the new mapping; live processes serve the cached old map
    until restart (acceptable: categories drift slowly)."""
    # Local import — circular-safe and keeps hyperliquid.py optional
    # for the test path (categorize() still works on the fallback dict
    # even if hyperliquid.py is somehow missing).
    try:
        import hyperliquid  # noqa: WPS433
    except Exception:
        return {}
    seed = hyperliquid.load_seed()
    if not seed:
        return {}
    out: dict[str, str] = {}
    for entry in seed.get("annotations", []):
        try:
            token, meta = entry
        except (TypeError, ValueError):
            continue
        if not isinstance(token, str) or not token.startswith(_RWA_PREFIX):
            continue
        bare = token[len(_RWA_PREFIX):]
        display = _normalize_hl_category((meta or {}).get("category"))
        if display:
            out[bare] = display
    return out


# Module-level cache populated lazily on first categorize() call. We
# don't use functools.lru_cache because (a) the loader takes no args
# and (b) we want a single explicit invalidator for tests.
_seed_categories_cache: dict[str, str] | None = None


def _categories() -> dict[str, str]:
    """Return the seed-derived ticker→category map (cached). Call this
    instead of `_load_seed_categories()` directly so repeat lookups
    don't re-read the JSON every time."""
    global _seed_categories_cache
    if _seed_categories_cache is None:
        _seed_categories_cache = _load_seed_categories()
    return _seed_categories_cache


def categorize(token: str) -> str:
    """Return the RWA category for an xyz:* token; None for non-RWA.

    Resolution order:
      1. Hyperliquid `perpConciseAnnotations` seed (authoritative,
         refreshed weekly by cron).
      2. Hand-curated fallback dict (defensive — used when the seed
         file is missing or the ticker isn't in it).
      3. "Other RWA" (terminal fallback for unknown xyz: tickers).
    """
    if not token.startswith(_RWA_PREFIX):
        return None
    bare = token[len(_RWA_PREFIX):]
    # 1. Seed (authoritative).
    seed_cat = _categories().get(bare)
    if seed_cat:
        return seed_cat
    # 2. Hand-curated fallback.
    for cat, members in _FALLBACK_RWA_CATEGORIES.items():
        if bare in members:
            return cat
    # 3. Terminal.
    return "Other RWA"


def is_rwa(token: str) -> bool:
    """True if the token symbol carries the `xyz:` prefix (= one of
    Hyperliquid's RWA / tokenized-asset perp markets)."""
    return bool(token) and token.startswith(_RWA_PREFIX)
