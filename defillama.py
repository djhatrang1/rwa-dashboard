"""DefiLlama free-API fetchers with on-disk snapshot fallback.

Centralises every DefiLlama HTTP call used by either dashboard so we
have one place to add caching, error handling, and the disk-snapshot
fallback for outages. Each fetcher:

  1. Tries the live API.
  2. On success, writes the raw payload to `defillama_seeds/` so the
     next outage has a recent snapshot to fall back on.
  3. On failure (network error, 5xx, rate-limit), reads the most
     recent disk snapshot and returns it — the dashboard keeps
     rendering historical data instead of showing an empty chart.

Same disk-seed pattern as `allium_seeds/`, `rwa_seeds/`,
`blockworks_seeds/`, `solana_seeds/dex/`, `mc_seed_*.json` — disk
snapshots are immutable historical exports refreshed when the live
fetcher succeeds.

Why JSON not CSV for these payloads: DefiLlama's per-protocol payload
is a deeply-nested dict (`chainTvls` → per-chain → per-token series).
Flattening it to CSV would lose structure the downstream parsers
need. Tabular endpoints (no nesting) like `/protocols` could go
either way; we stick with JSON to keep one storage convention per
source.

Endpoints wrapped:
  GET https://api.llama.fi/protocols
      Full protocols catalog. Single ~10 MB blob; ~hourly refresh
      cadence on DefiLlama's side, so 1h Streamlit TTL is correct.
  GET https://api.llama.fi/protocol/{slug}
      Per-protocol historical tvl + per-chain + per-token. The
      workhorse — read by lending, RWA, treasury renderers.
  GET https://stablecoins.llama.fi/stablecoin/{id}
      Per-stablecoin issuance time series split by chain.
  GET https://stablecoins.llama.fi/stablecoincharts/{chain}
      All-chain stablecoin total over time, one chain at a time.

Cache layer: Streamlit's `@st.cache_data(ttl=3600)` wraps every
public fetcher so concurrent reruns inside the cache window don't
re-hit the API (or the disk). The seed write happens INSIDE the
cached function — first call of each 1h window writes the seed,
subsequent cache hits don't touch disk.
"""
from __future__ import annotations

import logging

import pandas as pd
import requests
import streamlit as st

import seed_cache


log = logging.getLogger(__name__)


_SEEDS_DIR = "defillama_seeds"


def _slugify(s: str) -> str:
    """Make a string safe for use as a filename component."""
    return (s or "").lower().replace("/", "_").replace(" ", "_")


# ── Payload trimming ───────────────────────────────────────────────────
#
# Raw DefiLlama protocol/stablecoin payloads carry many fields the
# dashboards don't use (raw token-count series, audit metadata,
# methodology blobs, etc.). A full /protocol/save snapshot pretty-
# printed is 33 MB; trimming to the fields we actually read drops it
# below 1 MB. Across all ~70 cached protocol+stablecoin payloads, the
# seed dir shrinks from ~280 MB to ~5–15 MB — a footprint that fits
# in the repo and refreshes via `seeds-refresh` without bloating
# every commit.
#
# What we keep:
#   protocol:    chainTvls[chain].tvl          → lending stack + RWA MC
#                chainTvls[chain].tokensInUsd  → per-asset breakdown
#   stablecoin:  chainBalances[chain].tokens[].date
#                                              → per-chain MC over time
#                chainBalances[chain].tokens[].circulating.peggedUSD
#
# What we drop:
#   protocol:    raw `tokens` count series (we use the USD-priced cut)
#                tvl/tokensInUsd/tokens at TOP level (we use chainTvls.*)
#                metadata: name, audits, mcap, methodology, etc.
#   stablecoin:  bridgedAmount, breakdown by issuer, etc.
#
# The trim is per-write; reads of trimmed seeds still satisfy every
# existing call site because the call sites only read kept fields.


# Only these slugs are read for per-asset (tokensInUsd) breakdown by
# the dashboards. For everyone else, dropping tokensInUsd from the
# seed reduces the trimmed-payload size by another ~80% (saves ~30 MB
# across the seed dir, brings the total to ~10 MB on disk). The
# in-memory live payload always carries it; only the on-disk
# fallback is slimmer.
_PER_ASSET_SLUGS = {"kamino-lend", "jupiter-lend"}


def _trim_protocol_for_seed(d: dict, *, keep_per_asset: bool = False) -> dict:
    """Strip everything except chainTvls.{chain}.tvl (and
    chainTvls.{chain}.tokensInUsd if keep_per_asset). Returns a NEW
    dict — does not mutate the input (the in-memory dict is still
    used by the live-fetcher caller for charting)."""
    if not isinstance(d, dict):
        return {}
    ct_in = d.get("chainTvls") or {}
    ct_out: dict = {}
    for chain, payload in ct_in.items():
        if not isinstance(payload, dict):
            continue
        cell: dict = {}
        if "tvl" in payload:
            cell["tvl"] = payload["tvl"]
        if keep_per_asset and "tokensInUsd" in payload:
            cell["tokensInUsd"] = payload["tokensInUsd"]
        if cell:
            ct_out[chain] = cell
    out = {"chainTvls": ct_out}
    # Keep the identifying name so seeds are inspectable without
    # decoding the filename mapping.
    for k in ("name", "slug"):
        if k in d:
            out[k] = d[k]
    return out


def _trim_stablecoin_for_seed(d: dict) -> dict:
    """Strip everything except chainBalances.{chain}.tokens (the
    per-day per-chain `minted` peggedUSD series). Drops bridgedAmount,
    raw token counts, etc.

    NOTE: switched from `circulating` to `minted` (June 2026) to align
    with Artemis's per-chain stablecoin supply numbers. The two
    differ by `unreleased` (issuer treasury inventory not yet in
    market) — typically 10-15% of issued supply. Cross-checked over
    40 weeks of Solana USDC: mean abs error vs Artemis dropped from
    12.1% (circulating) to 3.2% (minted). Seeds now carry the
    `minted` field name to make the semantic explicit; the live-fetch
    consumer prefers `minted` and falls back to `circulating` so
    existing seeds don't go to zero during the rollout window."""
    if not isinstance(d, dict):
        return {}
    cb_in = d.get("chainBalances") or {}
    cb_out: dict = {}
    for chain, payload in cb_in.items():
        if not isinstance(payload, dict):
            continue
        tokens = payload.get("tokens") or []
        # Each entry is {date, circulating: {peggedUSD: float},
        # minted: ..., unreleased: ..., bridgedTo: ...}. We read
        # `minted` (matches Artemis).
        trimmed = []
        for pt in tokens:
            if not isinstance(pt, dict):
                continue
            d_v = pt.get("date")
            mc = (pt.get("minted") or {}).get("peggedUSD")
            if d_v is None or mc is None:
                continue
            trimmed.append({"date": d_v,
                             "minted": {"peggedUSD": mc}})
        if trimmed:
            cb_out[chain] = {"tokens": trimmed}
    out = {"chainBalances": cb_out}
    for k in ("name", "symbol", "id"):
        if k in d:
            out[k] = d[k]
    return out


def _trim_protocols_catalog_for_seed(catalog: list) -> list:
    """The full /protocols catalog is ~10 MB and 90% of it is
    metadata (audits, descriptions, methodology, logos, twitter
    handles). The call sites only read `slug`, `name`, `category`,
    `chains`, and `chainTvls` (the current-snapshot dict, NOT the
    daily series — that lives under /protocol/{slug})."""
    if not isinstance(catalog, list):
        return []
    out = []
    for p in catalog:
        if not isinstance(p, dict):
            continue
        out.append({
            k: p.get(k) for k in (
                "slug", "name", "category", "chains", "chainTvls")
            if p.get(k) is not None
        })
    return out


# ── /protocols (full catalog) ───────────────────────────────────────────

@st.cache_data(ttl=3600, show_spinner=False)
def fetch_protocols() -> list:
    """Full DefiLlama protocols catalog. Returns a list-of-dicts (the
    raw JSON response). On API failure, falls back to the most recent
    on-disk snapshot — returns [] if no snapshot exists either."""
    try:
        r = requests.get("https://api.llama.fi/protocols", timeout=30)
        r.raise_for_status()
        data = r.json()
    except Exception as exc:
        log.warning("DefiLlama /protocols failed: %s — using disk seed",
                     exc)
        seed = seed_cache.read_seed_json(_SEEDS_DIR, "protocols.json")
        return seed or []
    if isinstance(data, list) and data:
        seed_cache.write_seed_json(
            _trim_protocols_catalog_for_seed(data),
            _SEEDS_DIR, "protocols.json")
    return data or []


# ── /protocol/{slug} (per-protocol history) ─────────────────────────────

@st.cache_data(ttl=3600, show_spinner=False)
def fetch_protocol(slug: str) -> dict:
    """Per-protocol historical payload. Returns the raw JSON dict.
    Empty dict if neither live nor seed is available — caller must
    handle the empty case (every existing call site already does).

    Single-slug files (rather than one mega-file with every slug)
    keep git diffs small: refreshing one slug touches one file."""
    if not slug:
        return {}
    fn = f"protocol_{_slugify(slug)}.json"
    try:
        r = requests.get(f"https://api.llama.fi/protocol/{slug}", timeout=30)
        r.raise_for_status()
        data = r.json()
    except Exception as exc:
        log.warning("DefiLlama /protocol/%s failed: %s — using disk seed",
                     slug, exc)
        seed = seed_cache.read_seed_json(_SEEDS_DIR, fn)
        return seed or {}
    if isinstance(data, dict) and data:
        seed_cache.write_seed_json(
            _trim_protocol_for_seed(
                data,
                keep_per_asset=(slug in _PER_ASSET_SLUGS)),
            _SEEDS_DIR, fn)
    return data or {}


# ── /stablecoin/{id} (per-stablecoin chain breakdown) ───────────────────

@st.cache_data(ttl=3600, show_spinner=False)
def fetch_stablecoin(stable_id: int) -> dict:
    """Per-stablecoin payload from stablecoins.llama.fi. Same shape as
    fetch_protocol — raw JSON dict with on-disk fallback."""
    if not stable_id:
        return {}
    fn = f"stablecoin_{int(stable_id)}.json"
    try:
        r = requests.get(
            f"https://stablecoins.llama.fi/stablecoin/{stable_id}",
            timeout=30)
        r.raise_for_status()
        data = r.json()
    except Exception as exc:
        log.warning("DefiLlama /stablecoin/%s failed: %s — using disk seed",
                     stable_id, exc)
        seed = seed_cache.read_seed_json(_SEEDS_DIR, fn)
        return seed or {}
    if isinstance(data, dict) and data:
        seed_cache.write_seed_json(
            _trim_stablecoin_for_seed(data), _SEEDS_DIR, fn)
    return data or {}


# ── /stablecoincharts/{chain} (all-chain breakdown, one chain at a time) ─

@st.cache_data(ttl=3600, show_spinner=False)
def fetch_stablecoin_chain_chart(chain: str) -> pd.DataFrame:
    """Per-chain stablecoin MC time series. Returns DataFrame[date,
    mc_usd]. Tabular shape, so CSV here (one file per chain — same
    granularity as fetch_protocol's per-slug files)."""
    if not chain:
        return pd.DataFrame(columns=["date", "mc_usd"])
    fn = f"stablecoin_chain_{_slugify(chain)}.csv"
    try:
        r = requests.get(
            f"https://stablecoins.llama.fi/stablecoincharts/{chain}",
            timeout=30)
        r.raise_for_status()
        pts = r.json() or []
    except Exception as exc:
        log.warning(
            "DefiLlama stablecoincharts/%s failed: %s — using disk seed",
            chain, exc)
        seed = seed_cache.read_seed_df(_SEEDS_DIR, fn,
                                        parse_dates=["date"])
        if seed.empty:
            return pd.DataFrame(columns=["date", "mc_usd"])
        # Normalize the date column back to ISO strings to match the
        # live path's return type (mc renderers parse it themselves).
        seed["date"] = seed["date"].dt.strftime("%Y-%m-%d")
        return seed[["date", "mc_usd"]]
    rows = []
    for p in pts:
        try:
            d = pd.to_datetime(int(p["date"]), unit="s").strftime("%Y-%m-%d")
            # Read `totalMintedUSD` (matches Artemis's chain-level
            # totals — at the aggregate endpoint, `totalCirculatingUSD`
            # = `totalMintedUSD` + `totalBridgedToUSD`, so picking
            # minted avoids double-counting native-mint + bridged-in
            # supply that some downstream consumers treat as
            # equivalent to native issuance). Switched from
            # `totalCirculatingUSD` to `totalMintedUSD` in June 2026.
            mc = float((p.get("totalMintedUSD")
                          or {}).get("peggedUSD") or 0)
            if mc > 0:
                rows.append({"date": d, "mc_usd": mc})
        except (KeyError, TypeError, ValueError):
            continue
    df = pd.DataFrame(rows)
    seed_cache.write_seed_df(df, _SEEDS_DIR, fn)
    return df
