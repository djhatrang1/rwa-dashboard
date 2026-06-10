"""Solana Dashboard — single-chain breakout app.

Lives in the same repo as `stocks_dashboard.py` and shares all its
infrastructure (Settings, CacheDB, pullers, helpers) via library import.
`stocks_dashboard.py` wraps its UI rendering in `if __name__ == "__main__":`
so importing it here has no UI side effects — just exposes the classes
and helpers. Deploy as a second Streamlit Cloud app pointing to this
file. Reuses the same secrets and writes to the same Postgres cache.

Verticals wired today:
  • SOL token — price + daily volume from Birdeye OHLCV V3 (close,
    v_usd, full daily history). Current MC / supply / holders from
    /defi/token_overview snapshot. Birdeye doesn't expose historical
    holder count, MC, or supply — drop these per-token seed files at
    the repo root to backfill:
        mc_seed_sol_mc.json       — historical market cap
        mc_seed_sol_supply.json   — historical circulating supply
        mc_seed_sol_holders.json  — historical holder count
    Same {"payload": {"mc": [...], "t": [unix_seconds]}} shape as the
    existing commodity / stablecoin seeds. Until each seed lands, the
    corresponding chart shows a placeholder noting the current snapshot.
  • RWA — Solana-only view of the 4 RWA groups (tokenized stocks /
    commodities / stablecoins / treasuries) from the main dashboard.

Other verticals from spec (DEX / Stablecoins / Payments / Foreign L1 /
Lending / Perps / Prediction) — pending data-source research + new
puller implementations. Added to the sidebar incrementally as each
one comes online.
"""
from __future__ import annotations

import streamlit as st

# ── Page setup (must run before any other st.* call) ───────────────────────────
st.set_page_config(
    page_title="Solana Dashboard · Birdeye Peak",
    page_icon="assets/logos/Birdeye_Peak_Logomark_White.svg",
    layout="wide",
)

# ── Library import — pulls in Settings/CacheDB/pullers without rendering UI ───
# stocks_dashboard.py wraps its UI in `if __name__ == "__main__"` so this
# import only executes the module-level helper/class definitions.
import stocks_dashboard as sd

# ── Puller initialization (cached in session_state — avoids 30s+ reinit) ──────
# Mirrors the pattern from the parent dashboard. Version-gated so a bumped
# _PULLERS_VERSION on the lib side automatically forces a fresh init here too.
# Two-part validity check: version mismatch OR a known group is missing.
# The group-presence check catches a Streamlit Cloud edge case where
# session_state survives across a partial code reload — version key
# tracks the lib commit, but if the cached pullers list was built
# before a new GROUP existed (e.g. 'solana_tokens' added later) the
# version-only check fires only on the version bump and the next user
# session can latch onto stale state. Re-init whenever any expected
# group is absent so the cache self-heals.
_EXPECTED_GROUPS = ("solana_tokens", "tokenized_stocks", "tokenized_commodities",
                    "stablecoins", "treasuries")
def _pullers_stale() -> bool:
    cached = st.session_state.get("solana_dash_pullers")
    if cached is None: return True
    if st.session_state.get("solana_dash_pullers_version") != sd._PULLERS_VERSION:
        return True
    present = {getattr(p, "GROUP", "") for p in cached}
    return any(g not in present for g in _EXPECTED_GROUPS)

if _pullers_stale():
    st.session_state["solana_dash_pullers"] = sd.init_pullers(sd.settings, sd.cache_db)
    st.session_state["solana_dash_pullers_version"] = sd._PULLERS_VERSION

pullers = st.session_state["solana_dash_pullers"]
solana_native_pullers = [p for p in pullers if getattr(p, "GROUP", "") == "solana_tokens"]
stocks_pullers        = [p for p in pullers if getattr(p, "GROUP", "") == "tokenized_stocks"]
commodity_pullers     = [p for p in pullers if getattr(p, "GROUP", "") == "tokenized_commodities"]
stablecoin_pullers    = [p for p in pullers if getattr(p, "GROUP", "") == "stablecoins"]
treasury_pullers      = [p for p in pullers if getattr(p, "GROUP", "") == "treasuries"]

# ── Sidebar — vertical navigation ─────────────────────────────────────────────
# Only RWA is wired up today. Other verticals from the user's spec are
# documented in the module docstring; they'll appear here as each gets
# its data source nailed down.
_VERTICALS = ["SOL token", "Stablecoins", "Lending", "RWA",
              "Foreign L1 tokens", "Prediction Markets", "Perp DEXs"]

with st.sidebar:
    st.markdown(
        '<p style="font-size:22px;font-weight:700;margin-bottom:8px;">'
        'Solana Dashboard</p>',
        unsafe_allow_html=True,
    )
    st.caption("Birdeye Peak · single-chain breakout")
    st.divider()
    vertical = st.radio(
        "Vertical", _VERTICALS, index=0, label_visibility="collapsed",
        key="solana_vertical_nav",
    )
    st.divider()
    st.caption(
        "Other verticals coming soon:  \n"
        "DEX · Payments · Perps"
    )

# ── Cached Birdeye fetchers (used by SOL token vertical) ──────────────────────
# TTL=1h: OHLCV daily candles don't roll over more than once per UTC day, so
# refetching more than once per hour is wasted. /defi/token_overview snapshot
# (price/MC/holders) is even more cache-tolerant since it's only a headline
# read. Both gracefully degrade to empty when Birdeye 5xxs.

import requests as _requests
import pandas as _pd
import json as _json
import os as _os
import datetime as _dt
import plotly.graph_objects as _go

_SOL_ADDRESS = "So11111111111111111111111111111111111111112"


@st.cache_data(ttl=3600, show_spinner=False)
def _fetch_sol_ohlcv() -> _pd.DataFrame:
    """Daily SOL OHLCV from Birdeye V3 — returns DataFrame[date, close, v_usd].
    Reads BIRDEYE_API_KEY from sd.settings. Empty frame on error."""
    key = sd.settings.birdeye_api_key
    if not key:
        return _pd.DataFrame(columns=["date", "close", "v_usd"])
    # Birdeye OHLCV V3 requires a unix-second window. Use 2020-01-01 → now+1d
    # to grab all available history in one call.
    t_from = int(_dt.datetime(2020, 1, 1, tzinfo=_dt.timezone.utc).timestamp())
    t_to   = int(_dt.datetime.utcnow().replace(tzinfo=_dt.timezone.utc).timestamp()) + 86400
    try:
        r = _requests.get(
            "https://public-api.birdeye.so/defi/v3/ohlcv",
            params={"address": _SOL_ADDRESS, "type": "1D",
                    "time_from": t_from, "time_to": t_to, "currency": "usd"},
            headers={"X-API-KEY": key, "x-chain": "solana"}, timeout=30)
        r.raise_for_status()
        items = (r.json().get("data") or {}).get("items") or []
    except Exception:
        return _pd.DataFrame(columns=["date", "close", "v_usd"])
    rows = [{
        "date":   _pd.to_datetime(int(p["unix_time"]), unit="s"),
        "close":  float(p.get("c") or 0),
        "v_usd":  float(p.get("v_usd") or 0),
    } for p in items if p.get("unix_time")]
    df = _pd.DataFrame(rows)
    # Filter to days with positive close to skip zero-init rows
    return df[df["close"] > 0].sort_values("date").reset_index(drop=True)


@st.cache_data(ttl=21600, show_spinner=False)
def _fetch_sol_holders_history() -> _pd.DataFrame:
    """Daily holder count from Birdeye /token/v1/holder/chart.

    The endpoint caps `count` at 100 rows per call (per Birdeye docs), so
    we paginate backwards from today by sliding the `to` parameter to one
    second before the oldest timestamp returned. Loop terminates when a
    response comes back empty or hits the same first row twice. TTL 6h —
    holder count is daily-granular and the underlying numbers don't move
    hour-to-hour, so a long TTL saves ~20 paginated calls per cache miss.

    Returns DataFrame[date, holder] sorted ascending."""
    key = sd.settings.birdeye_api_key
    if not key:
        return _pd.DataFrame(columns=["date", "holder"])
    H = {"X-API-KEY": key, "x-chain": "solana"}
    URL = "https://public-api.birdeye.so/token/v1/holder/chart"
    out: list[dict] = []
    seen_oldest: int | None = None
    cursor_to = int(_dt.datetime.utcnow().replace(tzinfo=_dt.timezone.utc)
                     .timestamp())
    # Hard cap: 30 paginated calls × 100/call = 3000 days of history (~8y),
    # more than enough for any Solana token and a backstop against runaway
    # loops if Birdeye starts returning weird overlapping windows.
    for _ in range(30):
        try:
            r = _requests.get(URL, params={
                "token_address": _SOL_ADDRESS, "chart_type": "1d",
                "from": 1, "to": cursor_to, "count": 100,
            }, headers=H, timeout=30)
            r.raise_for_status()
            items = r.json().get("data") or []
        except Exception:
            break
        if not items:
            break
        # API returns newest → oldest; the oldest is at the end.
        oldest = min(int(p["timestamp"]) for p in items)
        if seen_oldest is not None and oldest >= seen_oldest:
            break    # same window as previous call — Birdeye gave us nothing new
        out.extend(items)
        seen_oldest = oldest
        cursor_to = oldest - 1   # step back one second past the oldest row
    if not out:
        return _pd.DataFrame(columns=["date", "holder"])
    df = _pd.DataFrame([{
        "date":   _pd.to_datetime(int(p["timestamp"]), unit="s"),
        "holder": int(p.get("holder") or 0),
    } for p in out])
    return (df.drop_duplicates(subset=["date"])
              .sort_values("date").reset_index(drop=True))


@st.cache_data(ttl=900, show_spinner=False)
def _fetch_sol_overview() -> dict:
    """Current SOL snapshot from /defi/token_overview. TTL 15min — short
    enough to keep the headline metric fresh, long enough to avoid hammering
    Birdeye on every page rerun."""
    key = sd.settings.birdeye_api_key
    if not key:
        return {}
    try:
        r = _requests.get(
            "https://public-api.birdeye.so/defi/token_overview",
            params={"address": _SOL_ADDRESS},
            headers={"X-API-KEY": key, "x-chain": "solana"}, timeout=15)
        r.raise_for_status()
        return r.json().get("data") or {}
    except Exception:
        return {}


def _load_sol_seed(filename: str) -> _pd.DataFrame:
    """Load a {payload: {mc: [...], t: [unix_seconds]}} seed file (same
    shape the existing mc_seed_*.json files use) into a 2-col DataFrame.
    Returns empty if the file doesn't exist. `mc` key is reused for any
    metric (market cap, supply, holder count) since the seed loader is
    schema-agnostic."""
    path = _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), filename)
    if not _os.path.exists(path):
        return _pd.DataFrame(columns=["date", "value"])
    try:
        with open(path) as f:
            raw = _json.load(f)
    except Exception:
        return _pd.DataFrame(columns=["date", "value"])
    payload = raw.get("payload", raw) if isinstance(raw, dict) else {}
    vals = payload.get("mc") or payload.get("value") or []
    ts   = payload.get("t") or []
    rows = [{"date": _pd.to_datetime(int(t), unit="s"), "value": float(v)}
            for t, v in zip(ts, vals) if v is not None]
    return _pd.DataFrame(rows).sort_values("date").reset_index(drop=True)


# ── SOL token vertical ────────────────────────────────────────────────────────
def _render_sol_token() -> None:
    """SOL price + volume from Birdeye OHLCV V3, current MC/holders/supply
    from /defi/token_overview snapshot, and (if seed JSONs are present)
    historical MC, supply, and holder count series."""
    st.markdown("## SOL token")
    st.caption(
        "Native Solana token (wrapped SOL mint "
        f"`{_SOL_ADDRESS[:6]}…{_SOL_ADDRESS[-4:]}`). Price + volume from "
        "Birdeye OHLCV V3 daily (close, v_usd); current MC / supply / "
        "holders from /defi/token_overview. Historical MC / supply / "
        "holders backfill from seed JSONs at the repo root when present."
    )

    # ── Headline metrics row ───────────────────────────────────────────────
    snap = _fetch_sol_overview()
    if not snap:
        st.warning("Birdeye token_overview unavailable — try again in a moment "
                   "(or verify BIRDEYE_API_KEY is set in Streamlit secrets).")
        return

    price   = float(snap.get("price") or 0)
    mc      = float(snap.get("marketCap") or 0)
    vol_24h = float(snap.get("v24hUSD") or 0)
    holders = int(snap.get("holder") or 0)
    supply  = float(snap.get("circulatingSupply") or snap.get("totalSupply") or 0)
    chg_24h = float(snap.get("priceChange24hPercent") or 0)

    m1, m2, m3, m4, m5 = st.columns(5)
    m1.metric("Price",       f"${price:,.2f}",
              delta=f"{chg_24h:+.2f}% (24h)" if chg_24h else None)
    m2.metric("Market Cap",  f"${mc/1e9:.2f}B" if mc >= 1e9 else f"${mc/1e6:.2f}M")
    m3.metric("24h Volume",  f"${vol_24h/1e9:.2f}B" if vol_24h >= 1e9 else f"${vol_24h/1e6:.0f}M")
    m4.metric("Holders",     f"{holders:,}")
    m5.metric("Circ. Supply",f"{supply/1e6:,.2f}M SOL")

    st.divider()

    # ── OHLCV-backed price + volume charts ─────────────────────────────────
    with st.spinner("Loading SOL OHLCV history…"):
        ohlcv = _fetch_sol_ohlcv()
    if ohlcv.empty:
        st.warning("Birdeye OHLCV returned no data — try refreshing.")
        return

    # ── Price (D/W/M) ──────────────────────────────────────────────────────
    def _build_sol_price_fig(df_view):
        fig = _go.Figure()
        fig.add_trace(_go.Scatter(
            x=df_view["date"], y=df_view["close"], name="SOL",
            mode="lines", line=dict(color="#9945FF", width=1.5),
            hovertemplate="%{y:$,.2f}<extra>SOL</extra>",
        ))
        fig.update_layout(
            height=380, hovermode="x unified",
            margin=dict(t=10, b=10, l=10, r=10), showlegend=False,
            yaxis=dict(tickprefix="$", tickformat=".2f", showgrid=True,
                       rangemode="tozero"),
        )
        return fig

    sd._chart_dwm_simple(
        "Daily Price (USD)",
        source_df=ohlcv[["date", "close"]].copy(),
        build_fig=_build_sol_price_fig,
        raw_df=ohlcv[["date", "close", "v_usd"]],
        raw_key="sol_price",
        raw_filename="sol_daily_ohlcv",
        raw_fmt={"close": "${:,.2f}", "v_usd": "${:,.0f}"},
        caption=(f"Source: Birdeye OHLCV V3, daily close · "
                 f"{len(ohlcv)} days from "
                 f"{ohlcv['date'].min().date()} → {ohlcv['date'].max().date()}"
                 f" · Weekly/Monthly = period-close price."),
        col_aggs={"close": "last"},
    )

    # ── Volume (D/W/M) ─────────────────────────────────────────────────────
    # Outlier clip — factor=50, min_retained=0 (SOL has one 2023-01-03
    # $41T glitch that's so massive it accounts for >97% of cumulative
    # v_usd; the default 0.5 retained-guard would preserve it).
    v_clipped = sd.TokenGroupMetricsPuller._clip_outliers(
        ohlcv["v_usd"], factor=50.0, min_retained=0.0)
    _sol_vol_df = ohlcv[["date"]].copy()
    _sol_vol_df["v_usd"] = v_clipped.values

    def _build_sol_vol_fig(df_view):
        fig = _go.Figure()
        fig.add_trace(_go.Bar(
            x=df_view["date"], y=df_view["v_usd"], name="Volume",
            marker_color="#14F195", opacity=0.85,
            hovertemplate="%{y:$,.0f}<extra>v_usd</extra>",
        ))
        fig.update_layout(
            height=320, hovermode="x unified",
            margin=dict(t=10, b=10, l=10, r=10), showlegend=False,
            yaxis=dict(tickprefix="$", tickformat="~s", showgrid=True,
                       rangemode="tozero"),
        )
        return fig

    _vol_raw = ohlcv[["date", "v_usd"]].copy()
    _vol_raw["v_usd_clipped"] = v_clipped.values
    sd._chart_dwm_simple(
        "Daily Trading Volume (USD)",
        source_df=_sol_vol_df,
        build_fig=_build_sol_vol_fig,
        raw_df=_vol_raw, raw_key="sol_vol",
        raw_filename="sol_daily_volume",
        raw_fmt={"v_usd": "${:,.0f}", "v_usd_clipped": "${:,.0f}"},
        caption=(
            "Source: Birdeye OHLCV V3 v_usd · all venues aggregated · "
            "outlier days suppressed (>50× median) — kills the "
            "2023-01-03 $41T glitch + Apr-2026 cluster; preserves the "
            "real Jan 18-20 2025 TRUMP-launch burst. Weekly/Monthly = "
            "summed volume across the period."
        ),
        col_aggs={"v_usd": "sum"},
    )

    # ── Holders (D/W/M) ────────────────────────────────────────────────────
    with st.spinner("Loading SOL holder history…"):
        holders_df = _fetch_sol_holders_history()
    if holders_df.empty:
        st.info(
            f"Birdeye holder-chart endpoint returned nothing — current "
            f"snapshot from /defi/token_overview: **{holders:,}**."
        )
    else:
        def _build_sol_holders_fig(df_view):
            fig = _go.Figure()
            fig.add_trace(_go.Scatter(
                x=df_view["date"], y=df_view["holder"], name="Holders",
                mode="lines", line=dict(color="#7DCE82", width=1.5),
                hovertemplate="Holders: %{y:,.0f}<extra></extra>",
            ))
            fig.update_layout(
                height=340, hovermode="x unified",
                margin=dict(t=10, b=10, l=10, r=10), showlegend=False,
                yaxis=dict(showgrid=True, rangemode="tozero",
                           tickformat=","),
            )
            return fig

        sd._chart_dwm_simple(
            "Holder Count",
            source_df=holders_df,
            build_fig=_build_sol_holders_fig,
            raw_df=holders_df, raw_key="sol_holders",
            raw_filename="sol_holder_history",
            raw_fmt={"holder": "{:,}"},
            caption=(
                "Source: Birdeye `/token/v1/holder/chart` daily, "
                "paginated. Weekly/Monthly = period-close holder count."
            ),
            col_aggs={"holder": "last"},
            fmt_mode="count",
        )

    # ── Optional seed-backed charts (MC, supply) ──────────────────────────
    # `fmt_mode` controls y-axis prefix: 'currency' adds '$', 'count' doesn't.
    # `snapshot_str` is the headline-metric formatted current value, used in
    # the "drop a seed file" placeholder message when the JSON is missing.
    for label, filename, color, fmt_mode, snapshot_str in [
        ("Market Cap",         "mc_seed_sol_mc.json",     "#FF8C42",
                                "currency", f"${mc/1e9:.2f}B"),
        ("Circulating Supply", "mc_seed_sol_supply.json", "#5BC0EB",
                                "count",    f"{supply:,.0f} SOL"),
    ]:
        seed = _load_sol_seed(filename)
        if seed.empty:
            st.subheader(label)
            st.info(
                f"No historical {label.lower()} series yet. Drop a seed file "
                f"named `{filename}` at the repo root with the same shape as "
                "the existing `mc_seed_*.json` files — "
                "`{\"payload\": {\"mc\": [values], \"t\": [unix_seconds]}}` — "
                f"and it'll render here. Current snapshot from Birdeye: "
                f"**{snapshot_str}**."
            )
            continue

        # Bind loop vars to closure args so each iteration's builder
        # captures the right label/color/seed.
        def _build_sol_seed_fig(df_view, _label=label, _color=color, _seed_full=seed):
            fig = _go.Figure()
            fig.add_trace(_go.Scatter(
                x=df_view["date"], y=df_view["value"], name=_label,
                mode="lines", line=dict(color=_color, width=1.5),
                hovertemplate=f"{_label}: %{{y:,.0f}}<extra></extra>",
            ))
            # Tight y-axis range so a near-flat trace doesn't bunch
            # against the chart top — derived from the full daily seed
            # so the W/M tabs share the same vertical scale.
            y_min = float(_seed_full["value"].min())
            y_max = float(_seed_full["value"].max())
            y_range = ([y_min * 0.95, y_max * 1.05]
                       if y_max > 0 else None)
            fig.update_layout(
                height=340, hovermode="x unified",
                margin=dict(t=10, b=10, l=10, r=10), showlegend=False,
                yaxis=dict(showgrid=True, range=y_range),
            )
            return fig

        _safe_label = label.lower().replace(" ", "_")
        _raw = seed.rename(columns={"value": _safe_label})
        sd._chart_dwm_simple(
            label,
            source_df=seed,
            build_fig=_build_sol_seed_fig,
            raw_df=_raw,
            raw_key=f"sol_{_safe_label}",
            raw_filename=f"sol_{_safe_label}",
            raw_fmt={_safe_label: "${:,.0f}" if fmt_mode == "currency"
                                            else "{:,.2f}"},
            col_aggs={"value": "last"},  # MC/supply are stocks, not flows
            fmt_mode=fmt_mode,
        )


# ── Lending vertical — DefiLlama-backed supply + borrow per protocol ──────────
# DefiLlama free API exposes per-protocol historical supply (chainTvls.Solana.tvl)
# and borrow (chainTvls.Solana-borrowed.tvl) on a daily basis going back to each
# protocol's launch. 36 lending+CDP protocols deployed on Solana today, ~$2.35B
# combined supply. We render the top-N as a stacked area and lump the rest into
# "Others" so the chart stays readable.
_LENDING_TOP_N = 10  # tokens in the stack; rest aggregated into "Others"


@st.cache_data(ttl=3600, show_spinner=False)
def _fetch_solana_lending_catalog() -> _pd.DataFrame:
    """List of all Solana lending+CDP protocols from DefiLlama /protocols,
    sorted by current Solana supply desc. Cached 1h (catalog moves slowly,
    new protocols don't appear daily)."""
    try:
        r = _requests.get("https://api.llama.fi/protocols", timeout=30)
        r.raise_for_status()
        data = r.json()
    except Exception as exc:
        log_fn = getattr(sd, "log", None)
        if log_fn: log_fn.warning("DefiLlama /protocols failed: %s", exc)
        return _pd.DataFrame()
    rows = []
    LENDING_CATS = {"Lending", "CDP", "RWA Lending", "Cross Chain Lending",
                    "Uncollateralized Lending"}
    for p in data:
        if (p.get("category") not in LENDING_CATS
                or "Solana" not in (p.get("chains") or [])):
            continue
        rows.append({
            "slug":     p.get("slug"),
            "name":     p.get("name"),
            "category": p.get("category"),
            "supply":   float((p.get("chainTvls") or {}).get("Solana", 0) or 0),
        })
    return _pd.DataFrame(rows).sort_values("supply", ascending=False).reset_index(drop=True)


@st.cache_data(ttl=3600, show_spinner=False)
def _fetch_solana_lending_history(slug: str) -> _pd.DataFrame:
    """Per-protocol historical supply + borrow on Solana from
    DefiLlama /protocol/{slug}. Returns DataFrame[date, supply, borrow]."""
    try:
        r = _requests.get(f"https://api.llama.fi/protocol/{slug}", timeout=30)
        r.raise_for_status()
        d = r.json()
    except Exception:
        return _pd.DataFrame(columns=["date", "supply", "borrow"])
    ct = d.get("chainTvls", {})
    sup_pts = (ct.get("Solana") or {}).get("tvl", []) or []
    bor_pts = (ct.get("Solana-borrowed") or {}).get("tvl", []) or []
    sup_map = {int(p["date"]): float(p.get("totalLiquidityUSD") or 0) for p in sup_pts}
    bor_map = {int(p["date"]): float(p.get("totalLiquidityUSD") or 0) for p in bor_pts}
    all_ts = sorted(set(sup_map) | set(bor_map))
    rows = [{"date": _pd.to_datetime(t, unit="s"),
             "supply": sup_map.get(t, 0.0),
             "borrow": bor_map.get(t, 0.0)} for t in all_ts]
    return _pd.DataFrame(rows)


@st.cache_data(ttl=3600, show_spinner=False)
def _fetch_lending_per_asset_history(slug: str, top_n: int = 20) -> tuple:
    """Per-asset historical supply + borrow within one Solana lending
    protocol. DefiLlama doesn't split Kamino into Main/JLP/Altcoin sub-
    markets, but `chainTvls.Solana.tokensInUsd` gives a per-token daily
    USD breakdown which is more granular than per-market anyway.

    Returns (wide_df, top_assets) where:
      • wide_df has columns [date, supply_<TOKEN>, borrow_<TOKEN>, ...,
        supply_others, borrow_others]
      • top_assets is the ordered list of top-N asset symbols (chosen
        by PEAK combined supply+borrow across the entire history — so
        assets that were huge in the past but small today still get
        their own ribbon, instead of being lumped into Others and
        making Others bigger than every named asset)
    """
    try:
        r = _requests.get(f"https://api.llama.fi/protocol/{slug}", timeout=30)
        r.raise_for_status()
        d = r.json()
    except Exception:
        return _pd.DataFrame(), []
    ct = d.get("chainTvls", {})
    sup_pts = (ct.get("Solana") or {}).get("tokensInUsd", []) or []
    bor_pts = (ct.get("Solana-borrowed") or {}).get("tokensInUsd", []) or []
    if not sup_pts:
        return _pd.DataFrame(), []

    # Top-N selection: hybrid of INTEGRAL (cumulative supply+borrow $-days)
    # + LATEST snapshot.
    #   • Integral catches assets that mattered historically — both brief
    #     spikes (e.g. WIF in 2024) and consistently-medium assets (BSOL
    #     at \$80M for 200 days contributes more area than a 1-week spike
    #     to \$200M).
    #   • Latest catches NEW assets that didn't accumulate integral yet
    #     (e.g. USDE only existed a few months on Kamino → integral rank
    #     ~25, but it's #1 on today's snapshot at \$237M).
    # Take 15 from integral + 5 from latest (deduped), cap at top_n.
    # Pure integral leaves today's Others at ~40% because new entrants
    # are missed; pure latest leaves past Others at ~60%+ because
    # historical heavyweights have shrunk. Hybrid keeps Others under
    # ~15% across the entire timeline.
    sup_by_ts = {int(p["date"]): (p.get("tokens") or {}) for p in sup_pts}
    bor_by_ts = {int(p["date"]): (p.get("tokens") or {}) for p in bor_pts}
    asset_integral: dict[str, float] = {}
    for ts in set(sup_by_ts) | set(bor_by_ts):
        s_tokens = sup_by_ts.get(ts, {})
        b_tokens = bor_by_ts.get(ts, {})
        for a in set(s_tokens) | set(b_tokens):
            v = float(s_tokens.get(a, 0) or 0) + float(b_tokens.get(a, 0) or 0)
            asset_integral[a] = asset_integral.get(a, 0.0) + v

    latest_ts_sup = max(sup_by_ts) if sup_by_ts else None
    latest_ts_bor = max(bor_by_ts) if bor_by_ts else None
    latest_sup = sup_by_ts.get(latest_ts_sup, {}) if latest_ts_sup else {}
    latest_bor = bor_by_ts.get(latest_ts_bor, {}) if latest_ts_bor else {}
    asset_latest = {a: float(latest_sup.get(a, 0) or 0)
                       + float(latest_bor.get(a, 0) or 0)
                    for a in set(latest_sup) | set(latest_bor)}

    integral_ranked = [a for a, _ in sorted(asset_integral.items(),
                                            key=lambda kv: -kv[1])]
    latest_ranked   = [a for a, _ in sorted(asset_latest.items(),
                                            key=lambda kv: -kv[1])]
    INT_QUOTA = max(1, top_n - 5)   # baseline allocation to integral
    top_assets: list[str] = []
    seen: set[str] = set()
    # First: integral top INT_QUOTA (historical heavyweights + consistent)
    for a in integral_ranked[:INT_QUOTA]:
        if a and a not in seen:
            top_assets.append(a); seen.add(a)
    # Then: fill remaining slots from latest (catches new entrants).
    # No LAT_QUOTA cap — keep adding until we hit top_n OR run out, so
    # dedup overlap doesn't cost us slots (without this we'd land at
    # 15-16 named ribbons instead of the requested top_n=20).
    for a in latest_ranked:
        if len(top_assets) >= top_n:
            break
        if a and a not in seen:
            top_assets.append(a); seen.add(a)
    # Finally: if still short (e.g. very small protocol with <20 unique
    # assets ever), keep going through integral overflow.
    for a in integral_ranked[INT_QUOTA:]:
        if len(top_assets) >= top_n:
            break
        if a and a not in seen:
            top_assets.append(a); seen.add(a)

    # Build wide frame
    rows: dict[int, dict] = {}
    for p in sup_pts:
        ts = int(p["date"]); tokens = p.get("tokens") or {}
        rows.setdefault(ts, {})
        for a in top_assets:
            rows[ts][f"supply_{a}"] = float(tokens.get(a, 0) or 0)
        rows[ts]["supply_others"] = float(sum(
            v for k, v in tokens.items() if k not in top_assets and v))
    for p in bor_pts:
        ts = int(p["date"]); tokens = p.get("tokens") or {}
        rows.setdefault(ts, {})
        for a in top_assets:
            rows[ts][f"borrow_{a}"] = float(tokens.get(a, 0) or 0)
        rows[ts]["borrow_others"] = float(sum(
            v for k, v in tokens.items() if k not in top_assets and v))

    wide_rows = []
    for ts in sorted(rows):
        r0 = {"date": _pd.to_datetime(ts, unit="s")}
        r0.update(rows[ts])
        wide_rows.append(r0)
    wide = _pd.DataFrame(wide_rows)
    # Ensure every expected column exists, but LEAVE NaN values intact so
    # the downstream ffill in _build_lending_stack can bridge data gaps
    # (DefiLlama occasionally has a day where supply data is present but
    # borrow is missing — e.g. Kamino Jan 23, 2025 — and zero-filling here
    # would defeat the carry-forward, producing a visible $0 cliff in
    # the stacked area chart).
    for a in top_assets:
        for prefix in ("supply_", "borrow_"):
            col = f"{prefix}{a}"
            if col not in wide.columns:
                wide[col] = float("nan")
    if "supply_others" not in wide.columns:
        wide["supply_others"] = float("nan")
    if "borrow_others" not in wide.columns:
        wide["borrow_others"] = float("nan")
    return wide, top_assets


def _render_protocol_asset_breakdown(slug: str, display_name: str) -> None:
    """Render a per-asset supply + borrow stack pair for one Solana
    lending protocol. Used for Kamino + Jupiter sections (the two
    largest by far, individually deserving their own breakdown)."""
    wide, top_assets = _fetch_lending_per_asset_history(slug, top_n=20)
    if wide.empty or not top_assets:
        st.info(f"No per-asset history available for {display_name}.")
        return
    # Build the (slug, label) tuples expected by _build_lending_stack —
    # here the 'slug' coordinate IS the asset symbol since columns are
    # supply_<ASSET> / borrow_<ASSET>.
    protocols = [(a, a) for a in top_assets]
    palette = ["#FF8C42", "#5BC0EB", "#7DCE82", "#9B5DE5", "#F15BB5",
               "#FEE440", "#00BBF9", "#00F5D4", "#FB8B24", "#A4036F",
               "#E84142", "#F3BA2F", "#0052FF", "#F7931A", "#9945FF",
               "#FFD500", "#00C08B", "#627EEA", "#FF6B6B", "#4ECDC4",
               "#888888"]   # last = Others
    _slug_safe = slug.replace("-", "_")
    c_left, c_right = st.columns(2, gap="medium")
    with c_left:
        _build_lending_stack("supply", protocols, wide, palette,
                             raw_key_prefix=f"lending_{_slug_safe}_by_asset",
                             chart_title=f"{display_name} — Supply by Asset")
    with c_right:
        _build_lending_stack("borrow", protocols, wide, palette,
                             raw_key_prefix=f"lending_{_slug_safe}_by_asset",
                             chart_title=f"{display_name} — Borrow by Asset")


def _build_lending_stack(metric: str, protocols: list[tuple[str, str]],
                        wide: _pd.DataFrame, palette: list[str],
                        raw_key_prefix: str | None = None,
                        chart_title: str | None = None) -> None:
    """metric = 'supply' or 'borrow'. protocols = [(slug, display_name)].
    wide = DataFrame with columns 'date' + '<metric>_<slug>' per protocol +
    '<metric>_others' for the catch-all bucket.

    `raw_key_prefix` (kwarg, optional) wires the 📋 raw-data button — pass
    a unique string per call so the Streamlit widget keys don't collide
    across the 6 stacks on the lending page (protocol-level supply/borrow
    + Kamino-by-asset supply/borrow + Jupiter-by-asset supply/borrow).

    `chart_title` (kwarg, optional) — bold title rendered on the SAME row
    as the 📋 button (forwarded into sd._chart's chart_title kwarg), so
    the icon and title share one row instead of the button getting its
    own empty row above the rangeselector."""
    cols = [f"{metric}_{s}" for s, _ in protocols] + [f"{metric}_others"]
    labels = [n for _, n in protocols] + ["Others"]

    def _build_lending_fig(df_view):
        fig = _go.Figure()
        present_cols = [c for c in cols if c in df_view.columns]
        present_labels = [labels[cols.index(c)] for c in present_cols]
        totals_v = df_view[present_cols].ffill().fillna(0).sum(axis=1)
        for i, (col, label) in enumerate(zip(present_cols, present_labels)):
            y = df_view[col].ffill().fillna(0.0)
            fig.add_trace(_go.Scatter(
                x=df_view["date"], y=y, name=label,
                mode="lines",
                line=dict(width=0.8, color=palette[i % len(palette)]),
                stackgroup=metric,
                customdata=y.map(sd._fmt_usd),
                hovertemplate=f"{label}: %{{customdata}}<extra></extra>",
            ))
        fig.add_trace(_go.Scatter(
            x=df_view["date"], y=totals_v, name="Total",
            mode="lines", line=dict(width=0, color="rgba(0,0,0,0)"),
            showlegend=False, stackgroup=None,
            customdata=totals_v.map(sd._fmt_usd),
            hovertemplate="<b>Total: %{customdata}</b><extra></extra>",
        ))
        y_max = float(totals_v.max() or 0)
        fig.update_layout(
            height=460, hovermode="x unified",
            margin=dict(t=20, b=90, l=10, r=10),
            legend=dict(orientation="h", yanchor="top", y=-0.22,
                        xanchor="center", x=0.5),
            yaxis=dict(showgrid=True, rangemode="tozero",
                       range=[0, y_max * 1.10] if y_max > 0 else None),
        )
        return fig

    # supply_/borrow_ are TVL stocks (deposited / outstanding) so 'last'
    # is the right resample rule (period-close value, not period sum).
    _aggs = {c: "last" for c in cols}

    if chart_title and raw_key_prefix:
        _raw = wide[["date"] + cols].copy()
        _raw["total"] = (wide[cols].ffill().fillna(0).sum(axis=1).values)
        sd._chart_dwm_simple(
            chart_title,
            source_df=wide[["date"] + cols].copy(),
            build_fig=_build_lending_fig,
            raw_df=_raw,
            raw_key=f"{raw_key_prefix}_{metric}",
            raw_filename=f"{raw_key_prefix}_{metric}",
            col_aggs=_aggs,
        )
    else:
        # Backward compat for callers without chart_title.
        raw_kwargs = {}
        if raw_key_prefix:
            _raw = wide[["date"] + cols].copy()
            _raw["total"] = (wide[cols].ffill().fillna(0).sum(axis=1).values)
            raw_kwargs = {
                "raw_df": _raw,
                "raw_key": f"{raw_key_prefix}_{metric}",
                "raw_filename": f"{raw_key_prefix}_{metric}",
            }
        sd._chart(_build_lending_fig(wide), use_container_width=True,
                  **raw_kwargs)


def _render_lending() -> None:
    """Solana lending market — protocol-level supply + borrow over time.
    Top-N protocols stacked individually, the rest into 'Others'. Headline
    metric row + 2 stacked-area charts + catalog table.

    Data: DefiLlama free /protocol/{slug} endpoint. Supply =
    chainTvls.Solana.tvl, Borrow = chainTvls.Solana-borrowed.tvl. Both
    daily, both back to each protocol's Solana launch.
    """
    st.markdown("## Lending")
    st.caption(
        "Solana lending markets — per-protocol supply (assets deposited) and "
        "borrow (loans outstanding). Top "
        f"{_LENDING_TOP_N} protocols rendered individually; the rest are "
        "aggregated into 'Others'. Source: DefiLlama free `/protocol/{slug}` "
        "endpoint, daily history back to each protocol's launch."
    )

    with st.spinner("Loading Solana lending catalog…"):
        catalog = _fetch_solana_lending_catalog()
    if catalog.empty:
        st.warning("DefiLlama /protocols returned no Solana lending data.")
        return

    # Top-N by current supply; remainder rolled into 'Others'.
    top = catalog.head(_LENDING_TOP_N).reset_index(drop=True)
    rest = catalog.iloc[_LENDING_TOP_N:].reset_index(drop=True)

    # ── Per-protocol historical fetches (paralleliz-able via @cache_data) ──
    with st.spinner(f"Loading per-protocol history (top {_LENDING_TOP_N} + others)…"):
        top_frames: dict[str, _pd.DataFrame] = {}
        for _, p in top.iterrows():
            df = _fetch_solana_lending_history(p["slug"])
            if not df.empty:
                top_frames[p["slug"]] = df
        # Aggregate the long-tail into one 'others' frame
        others_dfs = [_fetch_solana_lending_history(p["slug"]) for _, p in rest.iterrows()]
        others_dfs = [df for df in others_dfs if not df.empty]

    if not top_frames:
        st.warning("Per-protocol history fetch failed for every top protocol.")
        return

    # ── Build wide frame: one supply_<slug> + borrow_<slug> per protocol ───
    wide = None
    for slug, df in top_frames.items():
        renamed = df.rename(columns={
            "supply": f"supply_{slug}", "borrow": f"borrow_{slug}",
        })
        wide = renamed if wide is None else wide.merge(renamed, on="date", how="outer")

    # Aggregate the 'others' bucket into supply_others / borrow_others.
    if others_dfs:
        others_wide = None
        for i, df in enumerate(others_dfs):
            r = df.rename(columns={"supply": f"_s{i}", "borrow": f"_b{i}"})
            others_wide = r if others_wide is None else others_wide.merge(r, on="date", how="outer")
        s_cols = [c for c in others_wide.columns if c.startswith("_s")]
        b_cols = [c for c in others_wide.columns if c.startswith("_b")]
        others_agg = _pd.DataFrame({
            "date":           others_wide["date"],
            "supply_others":  others_wide[s_cols].fillna(0).sum(axis=1),
            "borrow_others":  others_wide[b_cols].fillna(0).sum(axis=1),
        })
        wide = wide.merge(others_agg, on="date", how="outer")
    else:
        wide["supply_others"] = 0.0
        wide["borrow_others"] = 0.0
    wide = wide.sort_values("date").reset_index(drop=True)

    # ── Headline metrics: total supply, total borrow, avg utilization ──────
    sup_cols = [c for c in wide.columns if c.startswith("supply_")]
    bor_cols = [c for c in wide.columns if c.startswith("borrow_")]
    latest = wide.iloc[-1] if len(wide) else None
    tot_sup = float(wide[sup_cols].ffill().fillna(0).sum(axis=1).iloc[-1])
    tot_bor = float(wide[bor_cols].ffill().fillna(0).sum(axis=1).iloc[-1])
    util = (tot_bor / tot_sup * 100) if tot_sup else 0
    asof = _pd.to_datetime(latest["date"]).strftime("%Y-%m-%d") if latest is not None else "?"
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Total Supply",       f"${tot_sup/1e9:.2f}B")
    m2.metric("Total Borrow",       f"${tot_bor/1e9:.2f}B")
    m3.metric("Utilization",        f"{util:.1f}%")
    m4.metric("Protocols tracked",  f"{len(catalog)}")
    st.caption(f"As of {asof} · top {len(top)} of {len(catalog)} shown "
               f"individually, remaining {len(others_dfs)} as 'Others'.")
    st.divider()

    # ── Two stacked-area charts (supply + borrow), two columns ─────────────
    protocols = [(row["slug"], row["name"]) for _, row in top.iterrows()
                 if row["slug"] in top_frames]
    palette = ["#FF8C42", "#5BC0EB", "#7DCE82", "#9B5DE5", "#F15BB5",
               "#FEE440", "#00BBF9", "#00F5D4", "#FB8B24", "#A4036F",
               "#888888"]  # last entry = Others
    c_left, c_right = st.columns(2, gap="medium")
    with c_left:
        _build_lending_stack("supply", protocols, wide, palette,
                             raw_key_prefix="lending_by_protocol",
                             chart_title="Total Supply by Protocol")
    with c_right:
        _build_lending_stack("borrow", protocols, wide, palette,
                             raw_key_prefix="lending_by_protocol",
                             chart_title="Total Borrow by Protocol")

    # ── Kamino per-asset breakdown ──────────────────────────────────────────
    st.divider()
    st.subheader("Kamino Lend — by asset")
    st.caption(
        "Per-asset supply and borrow within Kamino's Solana markets. "
        "DefiLlama doesn't split Kamino into Main / JLP / Altcoin "
        "sub-markets, but its `tokensInUsd` field gives a per-token "
        "daily breakdown — top 8 assets by combined supply+borrow are "
        "shown, the rest aggregated as 'Others'."
    )
    _render_protocol_asset_breakdown("kamino-lend", "Kamino Lend")

    # ── Jupiter Lend per-asset breakdown ────────────────────────────────────
    st.divider()
    st.subheader("Jupiter Lend — by asset")
    st.caption(
        "Per-asset supply and borrow within Jupiter Lend on Solana. "
        "Jupiter is a single-market protocol on DefiLlama; the breakdown "
        "below is by deposited / borrowed asset (top 8 + Others)."
    )
    _render_protocol_asset_breakdown("jupiter-lend", "Jupiter Lend")

    # ── Catalog table (collapsed by default — drilldown for power users) ───
    st.divider()
    with st.expander(f"All Solana lending protocols ({len(catalog)})",
                     expanded=False):
        st.caption("Sortable. Click column headers to re-sort.")
        cat_disp = catalog.copy()
        cat_disp["Supply"] = cat_disp["supply"].map(lambda v: f"${v/1e6:.2f}M")
        cat_disp = cat_disp.rename(columns={
            "name": "Name", "category": "Category", "slug": "DefiLlama slug",
        })
        st.dataframe(
            cat_disp[["Name", "Category", "Supply", "DefiLlama slug"]],
            use_container_width=True, hide_index=True, height=520,
        )


# ── Stablecoins vertical — Solana-only stablecoin MC + volume ─────────────────
def _render_stablecoins() -> None:
    """Top-level Stablecoins page. Previously lived as a sub-tab of RWA;
    promoted to its own sidebar entry to match the user's spec where
    Stablecoins is a peer of RWA / DEX / Payments / etc.

    Per puller (today there's one: the 9 hand-tracked stables — USDC,
    USDT, PYUSD, USDe, USD1, USDG, CASH, JupUSD, USDS), renders:
      • Per-token stacked-area MC chart (DefiLlama + seed + Birdeye)
      • USDC + USDT daily volume stack (Birdeye OHLCV V3, outlier-clipped)
      • Other-stables daily volume stack (same source, USDC/USDT excluded)
    USDC vol dwarfs every other stable 10-100× on most days; the split
    keeps the 'others' stack readable instead of squished against zero."""
    st.markdown("## Stablecoins")
    st.caption(
        "Solana-only stablecoin market cap + trading volume. Market cap "
        "via DefiLlama (free API, daily history) plus Solscan-derived "
        "seed JSONs and same-day Birdeye Token Overview snapshots; "
        "trading volume via Birdeye OHLCV V3 (v_usd, daily)."
    )

    if not stablecoin_pullers:
        st.info("No stablecoin pullers registered.")
        return

    for p in stablecoin_pullers:
        st.caption(
            "Solana-only market cap per token, stacked. Sourced from "
            "DefiLlama (free API, daily history) plus the Solscan-"
            "derived seed JSONs and same-day Birdeye Token Overview "
            "snapshots."
        )
        # Title rendered next to the 📋 raw-data button via _chart's
        # chart_title kwarg — same single-row layout used on Foreign L1.
        p.render_market_cap_chain(
            chain="Solana", stacked=True,
            raw_key=f"sd_stables_mc_{p.GROUP_LABEL.lower().replace(' ', '_')}",
            chart_title=f"{p.GROUP_LABEL} — Market Cap (Solana)",
        )

        st.subheader(
            f"{p.GROUP_LABEL} — USDC + USDT Daily Trading Volume (Solana)")
        st.caption(
            "USDC + USDT stacked · Birdeye OHLCV V3, v_usd, daily · "
            "outlier days (>25× median) suppressed for readability — "
            "keeps the legit Jan 18-20 2025 TRUMP-launch burst (~20×)."
        )
        p.render_volume_chain(chain="solana",
                              include_tokens={"USDC", "USDT"},
                              key_suffix="sd_usdc_usdt",
                              clip_outliers=True)

        st.subheader(
            f"{p.GROUP_LABEL} — Other Stables Daily Trading Volume (Solana)")
        st.caption(
            "Everything except USDC + USDT, stacked · Birdeye OHLCV V3, "
            "v_usd, daily · outlier days (>25× per-token median) suppressed."
        )
        p.render_volume_chain(chain="solana",
                              exclude_tokens={"USDC", "USDT"},
                              key_suffix="sd_others",
                              clip_outliers=True)

    # ── Solana stablecoin PAYMENTS (Allium) ──────────────────────────────
    # Two daily-cadence Allium queries scoped to Solana stablecoin
    # payment flows (different table from the MC + DEX-volume above —
    # these are merchant/peer transfers, not exchange / DEX trades).
    # Source dashboard: same Allium stablecoin-payments collection used
    # in the RWA dashboard's Stablecoin payments asset vertical, but
    # filtered server-side to Solana-only here.
    import allium as _allium
    st.divider()
    st.markdown("### Stablecoin Payments — Solana")

    # ── Daily volume + transfer count overlay ────────────────────────────
    # revision bumped v1 → v2 after the user re-edited the Allium query;
    # invalidates the 4h @st.cache_data so the next page-load fetches
    # the updated query result instead of returning the stale cached one.
    _spv_df, _spv_err = _allium.fetch_allium_query_results(
        "mE86r6b8d6RYWwvTfq2p", revision="v2")
    if _spv_df.empty:
        st.subheader("Daily Volume + Transfer Count")
        st.caption(
            "Source: Allium query "
            "[`mE86r6b8d6RYWwvTfq2p`]"
            "(https://app.allium.so/analyze/queries/mE86r6b8d6RYWwvTfq2p)."
        )
        st.info(f"No data. Reason: `{_spv_err or 'empty'}`")
    else:
        _spv_df = _spv_df.copy()
        _spv_df["date"] = _pd.to_datetime(_spv_df["date"],
                                          errors="coerce")
        _spv_df = _spv_df.sort_values("date").reset_index(drop=True)

        # Closure builds the figure for each D/W/M view. Passed
        # skip_yaxis_format=True through _chart_dwm_simple so
        # _chart() preserves the per-axis tickprefix instead of
        # rewriting yaxis2's ticktext to '$N' (transfer count is a
        # raw integer, not USD).
        def _build_spv_fig(df_view):
            fig = _go.Figure()
            fig.add_trace(_go.Bar(
                x=df_view["date"], y=df_view["total_volume_usd"],
                name="Volume (USD)",
                marker_color="#9945FF", opacity=0.85,
                customdata=df_view["total_volume_usd"].map(sd._fmt_usd),
                hovertemplate="Volume: %{customdata}<extra></extra>",
            ))
            fig.add_trace(_go.Scatter(
                x=df_view["date"], y=df_view["transfer_count"],
                name="Transfer Count",
                mode="lines+markers",
                line=dict(color="#14F195", width=1.5),
                marker=dict(color="#14F195", size=4),
                yaxis="y2",
                customdata=df_view["transfer_count"].map(
                    lambda v: f"{int(v):,}"),
                hovertemplate="Transfers: %{customdata}<extra></extra>",
            ))
            fig.update_layout(
                height=400, hovermode="x unified",
                margin=dict(t=10, b=10, l=10, r=10),
                legend=dict(orientation="h", yanchor="bottom",
                            y=1.02, xanchor="right", x=1),
                yaxis=dict(tickprefix="$", tickformat="~s",
                           showgrid=True, rangemode="tozero"),
                yaxis2=dict(overlaying="y", side="right",
                            showgrid=False, tickformat="~s",
                            rangemode="tozero"),
            )
            return fig

        sd._chart_dwm_simple(
            "Daily Volume + Transfer Count",
            source_df=_spv_df,
            build_fig=_build_spv_fig,
            raw_df=_spv_df,
            raw_key="sd_stable_pay_vol_xfer",
            raw_filename="sol_stable_payments_vol_transfers",
            raw_fmt={"total_volume_usd": "${:,.0f}",
                     "transfer_count": "{:,}"},
            caption=(
                "Daily Solana stablecoin payment volume (USD, left "
                "axis) and transfer count (right axis). Source: "
                "Allium query [`mE86r6b8d6RYWwvTfq2p`]"
                "(https://app.allium.so/analyze/queries/mE86r6b8d6RYWwvTfq2p)."
            ),
            col_aggs={"total_volume_usd": "sum",
                       "transfer_count":   "sum"},
            skip_yaxis_format=True,
        )

    st.divider()
    # ── Daily volume by flow category (stacked area) ─────────────────────
    # revision bumped v1 → v2 — see vol+xfer chart above for rationale.
    _spc_df, _spc_err = _allium.fetch_allium_query_results(
        "mR8Xtm7pKCv1C0VVvb6E", revision="v2")
    if _spc_df.empty:
        st.subheader("Daily Volume by Flow Category")
        st.caption(
            "Source: Allium query [`mR8Xtm7pKCv1C0VVvb6E`]"
            "(https://app.allium.so/analyze/queries/mR8Xtm7pKCv1C0VVvb6E)."
        )
        st.info(f"No data. Reason: `{_spc_err or 'empty'}`")
    else:
        _spc_df = _spc_df.copy()
        _spc_df["date"] = _pd.to_datetime(_spc_df["date"],
                                          errors="coerce")
        _spc_df = _spc_df.sort_values("date").reset_index(drop=True)
        _CAT_LABEL = {
            "c2c_volume":     "C2C",
            "c2b_volume":     "C2B",
            "b2c_volume":     "B2C",
            "b2b_i2c_volume": "B2B / I2C",
        }
        _CAT_COLORS = {
            "C2C":       "#10B981",  # emerald
            "C2B":       "#4285F4",  # blue
            "B2C":       "#A78BFA",  # lavender
            "B2B / I2C": "#F97316",  # orange
        }
        _spc_df = _spc_df.rename(columns=_CAT_LABEL)
        _cats = list(_CAT_LABEL.values())
        # Stack order computed off the FULL (daily) df so the D/W/M
        # tabs don't reshuffle band order between granularities.
        _latest = _spc_df.iloc[-1].fillna(0)
        _ordered = sorted(_cats,
                          key=lambda c: float(_latest.get(c, 0) or 0),
                          reverse=True)

        def _build_spc_fig(df_view):
            fig = _go.Figure()
            for cat in reversed(_ordered):
                if cat not in df_view.columns:
                    continue
                color = _CAT_COLORS.get(cat, "#888888")
                y = df_view[cat].fillna(0)
                fig.add_trace(_go.Scatter(
                    x=df_view["date"], y=y, name=cat,
                    mode="lines",
                    line=dict(color=color, width=0.9),
                    stackgroup="spc",
                    customdata=y.map(sd._fmt_usd),
                    hovertemplate=f"{cat}: %{{customdata}}<extra></extra>",
                ))
            present = [c for c in _ordered if c in df_view.columns]
            tot = df_view[present].fillna(0).sum(axis=1)
            fig.add_trace(_go.Scatter(
                x=df_view["date"], y=tot, name="Total",
                mode="lines",
                line=dict(width=0, color="rgba(0,0,0,0)"),
                showlegend=False, stackgroup=None,
                customdata=tot.map(sd._fmt_usd),
                hovertemplate="<b>Total: %{customdata}</b><extra></extra>",
            ))
            y_max = float(tot.max() or 0)
            fig.update_layout(
                height=400, hovermode="x unified",
                margin=dict(t=10, b=10, l=10, r=10),
                legend=dict(orientation="h", yanchor="bottom",
                            y=1.02, xanchor="right", x=1),
                yaxis=dict(tickprefix="$", tickformat="~s",
                           showgrid=True, rangemode="tozero",
                           range=[0, y_max * 1.10] if y_max > 0 else None),
            )
            return fig

        _spc_raw = _spc_df.copy()
        _spc_raw["Total"] = (_spc_df[_ordered].fillna(0).sum(axis=1).values)
        sd._chart_dwm_simple(
            "Daily Volume by Flow Category",
            source_df=_spc_df,
            build_fig=_build_spc_fig,
            raw_df=_spc_raw,
            raw_key="sd_stable_pay_by_cat",
            raw_filename="sol_stable_payments_by_category",
            raw_fmt={c: "${:,.0f}" for c in _cats + ["Total"]},
            caption=(
                "Daily Solana stablecoin payment volume split into "
                "four flow categories: **C2C** (consumer-to-consumer), "
                "**C2B** (consumer-to-business), **B2C** (business-to-"
                "consumer), **B2B/I2C** (business-to-business / "
                "institutional-to-consumer). Source: Allium query "
                "[`mR8Xtm7pKCv1C0VVvb6E`]"
                "(https://app.allium.so/analyze/queries/mR8Xtm7pKCv1C0VVvb6E)."
            ),
            col_aggs={c: "sum" for c in _cats},
        )


# ── Foreign L1 tokens vertical — grouped by underlying asset class ────────────
# Group by what the underlying asset IS, not the bridge tech. Same Bitcoin
# represented two ways (cbBTC + WBTC) lives in BTC; WETH alone in ETH; every
# other foreign L1 native (HYPE/ZEC/MON/AVAX/STRK/WBNB/ZORA/NEAR/TRX) goes
# into Others. Each group renders as MC stack + volume stack, 2 cols.
_FOREIGN_L1_GROUPS: list[tuple[str, list[str]]] = [
    # BTC group: 7 variants ranked by latest MC (largest at top of stack so
    # they read first in the legend; ordering is rendering-only, the
    # aggregate sums are independent of list order).
    ("BTC",    ["cbBTC", "WBTC", "xBTC", "LBTC", "zBTC", "tBTC", "wfragBTC"]),
    ("ETH",    ["WETH"]),
    ("Others", ["HYPE", "ZEC", "MON", "AVAX", "STRK",
                "WBNB", "ZORA", "NEAR", "TRX"]),
]
# Per-token color so the same token reads the same in both charts within a
# section. Tunable; defaults to '#888888' for unknown symbols.
_FOREIGN_L1_COLORS: dict[str, str] = {
    # BTC group — 7 distinct hues so each ribbon reads apart in the stack.
    # cbBTC anchored to Coinbase brand blue; the rest pick complementary
    # high-contrast colors (no two BTC variants share a similar palette
    # so the stacked area + stacked bars are visually decomposable).
    "cbBTC":   "#0052FF",   # Coinbase blue (cbBTC = Coinbase wrap)
    "WBTC":    "#F7931A",   # Bitcoin orange (canonical wrapped BTC)
    "xBTC":    "#00C08B",   # OKX green (xBTC = OKX wrap)
    "LBTC":    "#9945FF",   # Lombard purple
    "zBTC":    "#FFD500",   # Zeus yellow
    "tBTC":    "#E84142",   # Threshold red
    "wfragBTC":"#FF8C42",   # Fragmetric orange variant

    "WETH":    "#627EEA",   # Ethereum blue

    # Others — diverse hues across 9 tokens
    "HYPE":    "#7DCE82", "ZEC":     "#FEE440",
    "MON":     "#5BC0EB", "AVAX":    "#A4036F", "STRK":  "#FB8B24",
    "WBNB":    "#F3BA2F", "ZORA":    "#9B5DE5", "NEAR":  "#00BBF9",
    "TRX":     "#FF060A",
}


def _build_foreign_l1_group_charts(group_label: str, pullers: list) -> None:
    """For one group of foreign-L1 pullers, render a stacked-area MC chart
    in the left column and a stacked-bar daily-volume chart in the right
    column. Both share x-axis (date) and aggregate across group members.
    Empty pullers are skipped silently — a group with zero ready pullers
    emits an info message instead of two blank charts."""
    # Pull each member's daily series and align on date via outer-join merge.
    frames: dict[str, _pd.DataFrame] = {}
    for p in pullers:
        df = p.get_latest()
        if df is None or df.empty:
            continue
        df = df[["date", "market_cap_usd", "volume_usd"]].copy()
        df["date"] = _pd.to_datetime(df["date"])
        frames[p.TOKEN_NAME] = df
    if not frames:
        st.info(
            f"No {group_label} group data cached yet. "
            "Trigger a pull via `PULL_GROUP=solana_tokens "
            "python scripts/run_pull.py`."
        )
        return

    # Build a wide frame: one mc_<sym> + one vol_<sym> column per token.
    wide = None
    for sym, df in frames.items():
        renamed = df.rename(columns={
            "market_cap_usd": f"mc_{sym}", "volume_usd": f"vol_{sym}",
        })
        wide = renamed if wide is None else wide.merge(renamed, on="date", how="outer")
    wide = wide.sort_values("date").reset_index(drop=True)

    mc_cols  = [f"mc_{s}"  for s in frames]
    vol_cols = [f"vol_{s}" for s in frames]
    # Drop leading rows where every token is NaN (before any group member
    # had data). Trims the x-axis to the first day at least one token was live.
    keep = wide[mc_cols].notna().any(axis=1)
    wide = wide.loc[keep].reset_index(drop=True)

    _safe_group = group_label.lower().replace(" ", "_")
    col_left, col_right = st.columns(2, gap="medium")

    # ── Left: stacked-area MC (D/W/M) ───────────────────────────────────
    def _build_fl1_mc_fig(df_view):
        fig = _go.Figure()
        present_mc = [c for c in mc_cols if c in df_view.columns]
        totals_mc = df_view[present_mc].ffill().fillna(0).sum(axis=1)
        for sym in frames:
            col = f"mc_{sym}"
            if col not in df_view.columns:
                continue
            color = _FOREIGN_L1_COLORS.get(sym, "#888888")
            y = df_view[col].ffill().fillna(0.0)
            fig.add_trace(_go.Scatter(
                x=df_view["date"], y=y, name=sym,
                mode="lines", line=dict(width=0.8, color=color),
                stackgroup="mc",
                customdata=y.map(sd._fmt_usd),
                hovertemplate=f"{sym}: %{{customdata}}<extra></extra>",
            ))
        fig.add_trace(_go.Scatter(
            x=df_view["date"], y=totals_mc, name="Total",
            mode="lines", line=dict(width=0, color="rgba(0,0,0,0)"),
            showlegend=False, stackgroup=None,
            customdata=totals_mc.map(sd._fmt_usd),
            hovertemplate="<b>Total: %{customdata}</b><extra></extra>",
        ))
        y_max_mc = float(totals_mc.max() or 0)
        fig.update_layout(
            height=360, hovermode="x unified",
            margin=dict(t=10, b=10, l=10, r=10),
            legend=dict(orientation="h", yanchor="bottom", y=1.02,
                        xanchor="right", x=1),
            yaxis=dict(showgrid=True, rangemode="tozero",
                       range=[0, y_max_mc * 1.10] if y_max_mc > 0 else None),
        )
        return fig

    _mc_source = wide[["date"] + mc_cols].copy()
    _mc_raw = _mc_source.copy()
    _mc_raw["total"] = (wide[mc_cols].ffill().fillna(0).sum(axis=1).values)
    with col_left:
        sd._chart_dwm_simple(
            f"{group_label} — Aggregated Market Cap",
            source_df=_mc_source,
            build_fig=_build_fl1_mc_fig,
            raw_df=_mc_raw, raw_key=f"fl1_mc_{_safe_group}",
            raw_filename=f"foreign_l1_{_safe_group}_market_cap",
            col_aggs={f"mc_{s}": "last" for s in frames},
        )

    # ── Right: stacked-bar daily volume (D/W/M) ─────────────────────────
    def _build_fl1_vol_fig(df_view):
        fig = _go.Figure()
        present_vol = [c for c in vol_cols if c in df_view.columns]
        for sym in frames:
            col = f"vol_{sym}"
            if col not in df_view.columns:
                continue
            color = _FOREIGN_L1_COLORS.get(sym, "#888888")
            # 0 → NaN so Plotly doesn't draw a 0-height tick mark.
            y = df_view[col].replace(0, float("nan"))
            fig.add_trace(_go.Bar(
                x=df_view["date"], y=y, name=sym,
                marker_color=color, opacity=0.8,
                customdata=y.map(sd._fmt_usd),
                hovertemplate=f"{sym}: %{{customdata}}<extra></extra>",
            ))
        totals_v = (df_view[present_vol].fillna(0).sum(axis=1)
                                        .replace(0, float("nan")))
        fig.add_trace(_go.Scatter(
            x=df_view["date"], y=totals_v, name="Total",
            mode="lines", line=dict(width=0, color="rgba(0,0,0,0)"),
            showlegend=False,
            customdata=totals_v.map(sd._fmt_usd),
            hovertemplate="<b>Total: %{customdata}</b><extra></extra>",
        ))
        y_max_v = float(totals_v.max() or 0)
        fig.update_layout(
            height=360, hovermode="x unified", barmode="stack",
            margin=dict(t=10, b=10, l=10, r=10),
            legend=dict(orientation="h", yanchor="bottom", y=1.02,
                        xanchor="right", x=1),
            yaxis=dict(showgrid=True, rangemode="tozero",
                       range=[0, y_max_v * 1.10] if y_max_v > 0 else None),
        )
        return fig

    _vol_source = wide[["date"] + vol_cols].copy()
    _vol_raw = _vol_source.copy()
    _vol_raw["total"] = wide[vol_cols].fillna(0).sum(axis=1).values
    with col_right:
        sd._chart_dwm_simple(
            f"{group_label} — Aggregated Daily Volume",
            source_df=_vol_source,
            build_fig=_build_fl1_vol_fig,
            raw_df=_vol_raw, raw_key=f"fl1_vol_{_safe_group}",
            raw_filename=f"foreign_l1_{_safe_group}_volume",
            col_aggs={f"vol_{s}": "sum" for s in frames},
        )


def _render_foreign_l1() -> None:
    """Foreign L1 / L2 tokens deployed on Solana, grouped by underlying
    asset class: BTC (cbBTC + WBTC), ETH (WETH), Others (everything else).
    Each section renders aggregated MC (stacked area, left) + aggregated
    daily volume (stacked bar, right). The 'Total' line in each hover
    tooltip sums the stack at the hovered date."""
    st.markdown("## Foreign L1 tokens")
    st.caption(
        "Native tokens from other chains bridged or wrapped onto Solana, "
        "grouped by underlying asset. Each group shows aggregated MC + "
        "daily volume from the Solana-side mints (price × on-chain "
        "supply). Sourced from Birdeye OHLCV V3 — token endpoint primary, "
        "pair-aggregation fallback for bridged tokens with empty token "
        "OHLCV like HYPE."
    )

    if not solana_native_pullers:
        st.info(
            "No foreign-L1 pullers active yet. The registry is wired up "
            "but the data cache is empty — wait for the next cron pull "
            "(every 4h) or run `PULL_GROUP=solana_tokens python scripts/"
            "run_pull.py` locally."
        )
        return

    # Build a name → puller lookup so we can grab group members by symbol.
    by_name = {getattr(p, "TOKEN_NAME", ""): p for p in solana_native_pullers}

    for group_label, members in _FOREIGN_L1_GROUPS:
        st.subheader(group_label)
        group_pullers = [by_name[m] for m in members if m in by_name]
        if not group_pullers:
            st.info(f"No active pullers in the {group_label} group.")
            continue
        _build_foreign_l1_group_charts(group_label, group_pullers)
        st.divider()


# ── RWA vertical — Solana-only RWA view (3 sub-tabs) ──────────────────────────
def _render_rwa() -> None:
    """Solana RWA view: 4 sub-tabs mirroring the parent dashboard's Solana
    page layout (stocks → commodities → stablecoins → treasuries). Reuses
    the chain-aware renderers from stocks_dashboard so the chart styling
    (rangeslider, B/M/K ticks, Total hover, etc.) stays in lock-step with
    the main dashboard automatically."""
    st.markdown("## RWA")
    st.caption(
        "Real-world asset tokens issued on Solana: tokenized stocks, "
        "commodities, stablecoins, treasuries & money-market funds. "
        "Same data sources as the main rwa-dashboard."
    )

    # Stablecoins promoted to its own top-level sidebar vertical (per the
    # user's spec), so the RWA sub-tabs no longer include it.
    tab_stocks, tab_commodities, tab_treasuries = st.tabs(
        ["Tokenized stocks", "Tokenized commodities", "Treasuries & MMFs"])

    # ── Stocks ──────────────────────────────────────────────────────────────
    with tab_stocks:
        if not stocks_pullers:
            st.info("No tokenized stock group pullers registered.")
        else:
            # Combined overview — all projects, one stacked vol chart.
            # Chain-filtered to Solana so we don't (a) double-count
            # Solana volume via the legacy chain-agnostic vol_*_usd
            # cols that mirror the chain-suffixed ones, and (b) bleed
            # Ethereum / BSC volume from ondo_group_evm + xStocks EVM
            # entries into a Solana-titled chart.
            st.subheader("All Tokenized Stocks — Volume by Project")
            combined_df = sd._combined_stocks_df(
                stocks_pullers, chain="Solana")
            if combined_df is None:
                st.info("Waiting for first pull…")
            else:
                # dict.fromkeys preserves order + dedupes "Ondo" so the
                # two Ondo sub-pullers (sol + evm) contribute to a single
                # legend entry rather than two.
                labels = list(dict.fromkeys(
                    p.GROUP_LABEL for p in stocks_pullers))
                _raw = combined_df.copy()
                _present = [l for l in labels if l in _raw.columns]
                _raw["Total"] = _raw[_present].fillna(0).sum(axis=1)
                _fmt = {col: "${:,.0f}" for col in _present + ["Total"]}
                with st.container(key="sd_combined_chart"):
                    if st.button("📋", key="sd_raw_combined_stocks",
                                 help="View raw data"):
                        sd._raw_data_modal(
                            _raw.sort_values("date", ascending=False), _fmt)
                    ctab_d, ctab_w, ctab_m = st.tabs(["Daily", "Weekly", "Monthly"])
                    with ctab_d:
                        sd._chart(sd._build_combined_stocks_fig(
                            combined_df, labels, "D", 380),
                            use_container_width=True)
                    with ctab_w:
                        sd._chart(sd._build_combined_stocks_fig(
                            combined_df, labels, "W", 380),
                            use_container_width=True)
                    with ctab_m:
                        sd._chart(sd._build_combined_stocks_fig(
                            combined_df, labels, "M", 380),
                            use_container_width=True)
            st.divider()

            # Combined market cap — total tokenized-stock MC on Solana,
            # stacked by project. Same per-project label set + colors as
            # the Volume chart above so the two read as a coherent pair
            # (band stack on the right shows how much MC each project is
            # carrying; Total in the unified-hover tooltip = aggregate
            # Solana tokenized-stock MC at that date).
            mc_combined_df = sd._combined_stocks_mc_chain_df(
                stocks_pullers, chain="Solana")
            if mc_combined_df is None or mc_combined_df.empty:
                st.info(
                    "No Solana market-cap data for tokenized stocks yet. "
                    "Series will populate on the next pull (every 4h)."
                )
            else:
                # dedupe project labels — see note on the volume chart above.
                mc_labels  = list(dict.fromkeys(
                    p.GROUP_LABEL for p in stocks_pullers))
                _mc_present = [l for l in mc_labels
                               if l in mc_combined_df.columns]
                _mc_raw = mc_combined_df.copy()
                _mc_raw["Total"] = (_mc_raw[_mc_present].ffill()
                                                       .fillna(0)
                                                       .sum(axis=1))
                sd._chart_dwm_simple(
                    "All Tokenized Stocks — Market Cap by Project (Solana)",
                    source_df=mc_combined_df,
                    build_fig=lambda df_view: sd._build_combined_stocks_mc_fig(
                        df_view, mc_labels, height=400),
                    raw_df=_mc_raw.sort_values("date", ascending=False),
                    raw_key="sd_combined_stocks_mc",
                    raw_filename="solana_tokenized_stocks_total_mc",
                    col_aggs={l: "last" for l in mc_labels},
                )
            st.divider()

            # Per-group volume — 2 per row. Dedupe by GROUP_LABEL +
            # pick the Solana-active puller per project (after the Ondo
            # split there are 2 "Ondo" pullers; only ondo_group_sol has
            # any Solana tokens to render here).
            _per_proj_pullers = sd._dedupe_pullers_for_chain(
                stocks_pullers, "solana")
            for row_start in range(0, len(_per_proj_pullers), 2):
                col_a, col_b = st.columns(2, gap="medium")
                for col, p in zip(
                    (col_a, col_b),
                    _per_proj_pullers[row_start: row_start + 2],
                ):
                    with col:
                        st.subheader(p.GROUP_LABEL)
                        p.render_volume_chain(chain="solana",
                                              clip_outliers=True)
                st.divider()

    # ── Commodities ─────────────────────────────────────────────────────────
    with tab_commodities:
        if not commodity_pullers:
            st.info("No tokenized commodity pullers registered.")
        else:
            for p in commodity_pullers:
                st.subheader(f"{p.GROUP_LABEL} — Trading Volume (Solana)")
                p.render_volume_chain(chain="solana", clip_outliers=True)

                st.caption(
                    "Solana-only market cap per token, stacked. Sourced from "
                    "DefiLlama (XAUM) plus Solscan-seeded history for GOLD / "
                    "VNXAU / PAXG-bridge / XAUt0 and same-day Birdeye Token "
                    "Overview snapshots — total band height = total tokenized "
                    "gold MC on Solana."
                )
                _safe_p = (getattr(p, "name", p.GROUP_LABEL).lower()
                                                          .replace("-", "_")
                                                          .replace(" ", "_"))
                p.render_market_cap_chain(
                    chain="Solana", stacked=True,
                    raw_key=f"sd_commod_mc_{_safe_p}",
                    chart_title=f"{p.GROUP_LABEL} — Market Cap by Token (Solana)",
                )

    # ── Treasuries & MMFs ───────────────────────────────────────────────────
    with tab_treasuries:
        if not treasury_pullers:
            st.info("No treasury pullers registered.")
        else:
            for p in treasury_pullers:
                st.caption(
                    "Per-token market cap on Solana, from DefiLlama's free "
                    "API (daily history). These tokens have no on-chain "
                    "trading activity tracked; only market cap is shown."
                )
                _safe_p = (getattr(p, "name", p.GROUP_LABEL).lower()
                                                          .replace("-", "_")
                                                          .replace(" ", "_"))
                p.render_market_cap_chain(
                    chain="Solana", stacked=True,
                    raw_key=f"sd_treas_mc_{_safe_p}",
                    chart_title=f"{p.GROUP_LABEL} — Market Cap (Solana)",
                )


# ── Prediction Markets vertical (Dune Analytics) ──────────────────────────────
# Each platform's data comes from a Dune query; results are cached 4h to
# match the cron cadence and keep monthly Dune credit burn near-zero (one
# /results read per cache miss — we consume the existing cached snapshot
# rather than /execute, which costs credits and takes minutes).
#
# Jupiter query 6287629:  Jupiter Prediction Market Notional Volume.
# Phantom query 6386183:  Phantom Prediction Markets TVL (cumulative delta).
# Phantom queries 6386520 (Volume/Fees/Tx) and 6453064 (Users) are private
# at source — Dune API returns "Query not found" so we can't render them
# unless the dashboard owner makes them public, or we fork them on Dune
# (which creates a new public ID under our account).
# Jupiter prediction-market data — 6 queries from the
# /datadashboards/jupiter-prediction-markets dashboard:
_DUNE_QUERY_JUPITER_NOTIONAL = 6287629   # day, Notional Volume, Cumulative Notional Volume
_DUNE_QUERY_JUPITER_VOLUME   = 6287873   # day, Volume, Cumulative Volume
_DUNE_QUERY_JUPITER_FEES     = 6294302   # day, Fees, Cumulative Fees
_DUNE_QUERY_JUPITER_TX       = 6287720   # date, Transctions(sic), Cumulative Transactions
_DUNE_QUERY_JUPITER_TVL      = 6298659   # hour (hourly!), TVLDelta, TVL_CumulativeDelta
_DUNE_QUERY_JUPITER_USERS    = 6294160   # Date, New, Old, Cumulative Unique Users

# DFlow prediction-market data — 2 public queries from
# /stepanalytics_team/prediction-markets-on-solana:
_DUNE_QUERY_DFLOW_ACTIVITY   = 6510861   # unified daily: Notional/Volume/Fees/Tx/Users
_DUNE_QUERY_DFLOW_TOKBAL     = 6512170   # long-format day, symbol, token_balance

# Phantom prediction-market data — only TVL is publicly queryable.
_DUNE_QUERY_PHANTOM_TVL      = 6386183

# Allium query for the Jupiter vs DFlow head-to-head comparison section.
# Pre-joined server-side (one row per day, both platforms in adjacent
# columns) and live to today — much fresher than the client-side merge
# of separate Dune queries we used before. Returns trade_date +
# {jupiter,dflow}_{volume_usd,trades,traders,markets}.
# Scope: 2026 YTD only (Dune has Oct-Dec 2025 history but is stale).
_ALLIUM_QUERY_PRED_COMPARE   = "fyYRvSnCSHnSXaEsmEm1"


def _allium_request(method: str, url: str, headers: dict,
                    json_body: dict | None = None,
                    timeout: int = 30,
                    max_attempts: int = 6) -> "_requests.Response":
    """HTTP wrapper that retries on 429 (Allium rate-limit) with backoff,
    honoring the `Retry-After` header when present.

    Allium's free / starter tier rate-limits pretty aggressively — we
    saw ~10 req/min ceilings during testing. The previous fetcher used
    a flat r.get/.post and exploded on the first 429. This helper
    retries up to `max_attempts` times with backoff capped at 30s per
    sleep (the longest single retry-after we'd expect), then re-raises
    so the outer try/except in _fetch_allium_query_results surfaces a
    real error message to the user."""
    import time as _time
    backoff = 2
    for attempt in range(max_attempts):
        if json_body is not None:
            r = _requests.post(url, json=json_body, headers=headers,
                               timeout=timeout)
        else:
            r = _requests.get(url, headers=headers, timeout=timeout)
        if r.status_code != 429:
            return r
        # 429 — honor Retry-After (seconds) if present, else exponential.
        wait = r.headers.get("Retry-After")
        try:
            wait_s = int(wait) if wait else backoff
        except ValueError:
            wait_s = backoff
        wait_s = min(max(wait_s, 1), 30)   # clamp [1, 30]
        _time.sleep(wait_s)
        backoff = min(backoff * 2, 30)
    # Out of attempts — return the last 429 response so .raise_for_status
    # surfaces a 429-shaped error.
    return r


@st.cache_data(ttl=14400, show_spinner="Running Allium query (≈45s)…")
def _fetch_allium_query_results_cached(query_id: str,
                                       run_limit: int = 10000,
                                       poll_seconds: int = 10,
                                       initial_wait_seconds: int = 15,
                                       max_wait_seconds: int = 180) -> _pd.DataFrame:
    """Cached inner — RAISES on any failure (missing key, async timeout,
    run error). Raising instead of returning empty matters because
    @st.cache_data caches return values but NOT exceptions — so a
    failed run won't get pinned for 4h.

    All three HTTP calls (run-async POST, run-state GET poll, results
    GET) go through _allium_request which retries 429s with backoff.
    Polling defaults: 15s initial wait before first poll (most runs
    finish in 20-30s so this avoids burning a request on a known-pending
    state), then 10s between subsequent polls (Allium free-tier
    rate-limit is ~10 req/min; this stays well under it).

    Reads ALLIUM_API_KEY from st.secrets first, env fallback."""
    try:
        key = st.secrets.get("ALLIUM_API_KEY", "")
    except Exception:
        key = ""
    if not key:
        key = _os.environ.get("ALLIUM_API_KEY", "")
    if not key:
        raise RuntimeError("ALLIUM_API_KEY missing")

    H = {"X-API-KEY": key}
    import time as _time

    # ── Step 1: kick off async run ─────────────────────────────────────
    r = _allium_request(
        "POST",
        f"https://api.allium.so/api/v1/explorer/queries/{query_id}/run-async",
        headers=H,
        json_body={"parameters": {}, "run_config": {"limit": run_limit}},
    )
    r.raise_for_status()
    run_id = r.json().get("run_id")
    if not run_id:
        raise RuntimeError("Allium /run-async returned no run_id")

    # ── Step 2: poll until success/fail/timeout ────────────────────────
    # Initial wait — Allium runs almost never finish in <15s, so polling
    # earlier just burns rate-limit quota on a known-pending state.
    _time.sleep(initial_wait_seconds)
    elapsed = initial_wait_seconds
    while elapsed < max_wait_seconds:
        try:
            r = _allium_request(
                "GET",
                f"https://api.allium.so/api/v1/explorer/query-runs/{run_id}",
                headers=H,
            )
            r.raise_for_status()
            status = (r.json().get("status") or "").lower()
        except Exception:
            status = "unknown"
        if status == "success":
            break
        if status in ("failed", "cancelled", "canceled", "error"):
            raise RuntimeError(f"Allium run ended with status={status}")
        _time.sleep(poll_seconds)
        elapsed += poll_seconds
    else:
        raise TimeoutError(
            f"Allium run did not complete in {max_wait_seconds}s")

    # ── Step 3: fetch results ──────────────────────────────────────────
    r = _allium_request(
        "GET",
        f"https://api.allium.so/api/v1/explorer/query-runs/{run_id}/results",
        headers=H,
    )
    r.raise_for_status()
    rows = (r.json() or {}).get("data") or []
    if not rows:
        raise RuntimeError("Allium results endpoint returned 0 rows")
    return _pd.DataFrame(rows)


def _fetch_allium_query_results(query_id: str,
                                run_limit: int = 10000
                                ) -> tuple[_pd.DataFrame, str | None]:
    """Outer wrapper — catches any exception from the cached inner and
    returns (empty_df, error_message). Since failures raise (and aren't
    cached), the next page-load automatically retries instead of being
    stuck with a bad cached empty for the full 4h TTL.

    Error message is surfaced in the renderer's placeholder so users can
    debug missing keys vs auth failures vs async timeouts without
    digging through Cloud logs."""
    try:
        df = _fetch_allium_query_results_cached(query_id,
                                                run_limit=run_limit)
        return df, None
    except Exception as e:
        # Trim long stack traces; the type + first line is what matters.
        msg = f"{type(e).__name__}: {str(e)[:250]}"
        return _pd.DataFrame(), msg


def _allium_key_source() -> str | None:
    """Returns 'st.secrets' or 'env' if an ALLIUM_API_KEY is present,
    None if nowhere. Used by the renderer to disambiguate key-missing
    failures from auth-rejected failures in the diagnostic message."""
    try:
        if st.secrets.get("ALLIUM_API_KEY", ""):
            return "st.secrets"
    except Exception:
        pass
    if _os.environ.get("ALLIUM_API_KEY", ""):
        return "env"
    return None


@st.cache_data(ttl=14400, show_spinner=False)
def _fetch_dune_query_results(query_id: int) -> _pd.DataFrame:
    """Pull the latest-cached results for a Dune query id via the public REST
    endpoint. Returns a DataFrame where the timestamp column is normalised
    to `day` (datetime) and every other column is kept verbatim with its
    Dune column name. Caller is responsible for renaming / casting.

    Reads DUNE_API_KEY from st.secrets first then falls back to the env
    var so the same call works on Streamlit Cloud and locally. Empty frame
    on auth/network failure (renderer shows an info placeholder).

    Uses /results (the latest materialised execution), NOT /execute — we
    consume the existing cached snapshot rather than triggering a fresh
    run, which costs credits and takes minutes."""
    try:
        key = st.secrets.get("DUNE_API_KEY", "")
    except Exception:
        key = ""
    if not key:
        key = _os.environ.get("DUNE_API_KEY", "")
    if not key:
        return _pd.DataFrame()
    try:
        r = _requests.get(
            f"https://api.dune.com/api/v1/query/{query_id}/results",
            headers={"X-Dune-API-Key": key},
            params={"limit": 5000},   # generous cap; queries return ~200 rows
            timeout=30,
        )
        r.raise_for_status()
        rows = (r.json().get("result") or {}).get("rows") or []
    except Exception:
        return _pd.DataFrame()
    if not rows:
        return _pd.DataFrame()
    df = _pd.DataFrame(rows)
    # Normalise the timestamp column to 'day' (datetime). Dune queries on
    # this dashboard use a mix of 'day' / 'date' / 'Date' / 'hour' / 'time'
    # depending on the author's convention — coalesce them all to 'day' so
    # the renderer can always reference one column name. First match wins;
    # if none of the candidates exist the frame passes through unchanged
    # and the caller's "missing 'day' column" guard catches it.
    for _cand in ("day", "date", "Date", "hour", "time", "Time", "timestamp"):
        if _cand in df.columns:
            df = df.rename(columns={_cand: "day"})
            # Force tz-naive UTC. Dune queries on the Jupiter dashboard mix
            # `timestamp(3) with time zone` (5 of 6) and bare `timestamp(3)`
            # (TVL query 6298659). Comparing a tz-aware Timestamp with a
            # tz-naive one — e.g. in max(asof_candidates) for the headline
            # KPIs — raises `Cannot compare tz-naive and tz-aware`. Coercing
            # via utc=True then dropping tz gives a single naive-UTC dtype
            # that compares cleanly across queries.
            df["day"] = (_pd.to_datetime(df["day"], utc=True)
                            .dt.tz_localize(None))
            df = df.sort_values("day").reset_index(drop=True)
            break
    return df


def _render_prediction_markets() -> None:
    """On-chain Solana prediction-market activity. Layout:

      1. Jupiter vs DFlow comparison — 4 metrics where the two
         platforms publish in compatible units (Notional Volume,
         Volume, Fees, Transactions). Each metric becomes one daily
         + cumulative chart pair with both platforms overlaid on
         the same axes, so users can eyeball-compare trajectories.
      2. Jupiter (additional) — TVL + Unique Users (Jupiter-only).
      3. DFlow (additional) — Daily Active Users + Token Balance
         by symbol (DFlow-only).
      4. Phantom — TVL daily delta + cumulative.
    """
    st.markdown("## Prediction Markets")
    st.caption(
        "On-chain Solana prediction-market activity sourced from Dune "
        "Analytics. Each query is cached 4h to match the cron cadence."
    )
    st.divider()

    _render_jupiter_dflow_comparison_section()
    st.divider()
    # Per-platform sections collapsed by default — the comparison
    # section above is the headline view; these are platform-specific
    # extras (Jupiter TVL + Users, DFlow Active Users + Token Balance,
    # Phantom TVL) so most users won't expand them every visit.
    with st.expander("Jupiter — Additional Metrics", expanded=False):
        _render_jupiter_prediction_section()
    with st.expander("DFlow — Additional Metrics", expanded=False):
        _render_dflow_prediction_section()
    with st.expander("Phantom Prediction Markets", expanded=False):
        _render_phantom_prediction_section()


def _render_jupiter_dflow_comparison_section() -> None:
    """Jupiter vs DFlow head-to-head sourced from Allium query
    fyYRvSnCSHnSXaEsmEm1 (Solana Prediction Markets: Jupiter vs DFlow
    Historical Comparison - Pivoted). One server-side-joined query
    returns 4 metric pairs in one shot:

      • Volume   — daily USD (num_contracts × settlement_token_price)
      • Trades   — daily trade count
      • Traders  — daily unique addresses (NEW vs the prior Dune source)
      • Markets  — daily unique tickers traded (NEW)

    Cumulative columns are computed client-side via cumsum. For
    Volume + Trades the cumulative is the literal lifetime total. For
    Traders + Markets it's the sum of daily-uniques across time
    (i.e. 'trader-days' / 'market-days' — a measure of engagement
    breadth over time, NOT lifetime distinct users — labelled
    accordingly in the cum chart titles).

    Trade-off vs the previous Dune-merged section: Allium scope is
    2026-YTD only (no Oct-Dec 2025 history) but every reading is live
    to today instead of weeks-stale, and adds 2 metrics Dune doesn't
    expose at all."""
    st.subheader("Jupiter vs DFlow — Comparable Metrics")
    st.caption(
        "Server-side joined head-to-head sourced from Allium query "
        f"[{_ALLIUM_QUERY_PRED_COMPARE}]"
        f"(https://app.allium.so/explorer/queries/{_ALLIUM_QUERY_PRED_COMPARE}). "
        "Jupiter purple ◆ DFlow orange. 2026 YTD."
    )

    df, err = _fetch_allium_query_results(_ALLIUM_QUERY_PRED_COMPARE)
    if df.empty or "trade_date" not in df.columns:
        key_src = _allium_key_source()
        if not key_src:
            st.error(
                "No `ALLIUM_API_KEY` detected — neither in "
                "`st.secrets` nor in the environment. On Streamlit "
                "Cloud, add it via *Manage app → Settings → Secrets*. "
                "Locally, add it to `.env`."
            )
        else:
            msg = f"Key detected via **{key_src}** but the fetch failed."
            if err:
                msg += f"\n\nLast error:\n```\n{err}\n```"
            msg += (
                "\n\nCommon causes:\n"
                "- **401/403** — key is wrong / truncated / has trailing spaces\n"
                "- **TimeoutError** — Allium async run took > 180s\n"
                "- **429** — Allium rate-limit (rare; only after many bursty hits)"
            )
            st.error(msg)
        return

    df = df.copy()
    df["day"] = _pd.to_datetime(df["trade_date"])
    df = df.sort_values("day").reset_index(drop=True)

    def _frame(daily_col: str) -> _pd.DataFrame:
        """Slice one platform's daily column out of the pivoted Allium
        frame and add a client-side cumsum 'cumulative' column."""
        if daily_col not in df.columns:
            return _pd.DataFrame(columns=["day", "daily", "cumulative"])
        out = df[["day", daily_col]].rename(columns={daily_col: "daily"})
        out["daily"] = out["daily"].astype(float)
        out["cumulative"] = out["daily"].cumsum()
        return out

    pairs = {
        "volume":  (_frame("jupiter_volume_usd"),
                    _frame("dflow_volume_usd")),
        "trades":  (_frame("jupiter_trades"),
                    _frame("dflow_trades")),
        "traders": (_frame("jupiter_traders"),
                    _frame("dflow_traders")),
        "markets": (_frame("jupiter_markets"),
                    _frame("dflow_markets")),
    }

    # ── KPI grid: 2 rows × 4 cols (Jupiter top / DFlow bottom) ─────────
    def _latest(_df, col):
        return float(_df[col].iloc[-1]) if not _df.empty else None

    def _fmt_metric(v, mode="currency"):
        if v is None:    return "—"
        if mode == "count": return f"{int(v):,}"
        return sd._fmt_usd(v)

    asof = df["day"].iloc[-1].strftime("%Y-%m-%d")

    # KPI labels: Volume + Trades show CUMULATIVE (literal lifetime
    # totals); Traders + Markets show LATEST DAILY (since cumsum of
    # daily-uniques isn't a lifetime-distinct count — calling it
    # 'cumulative traders' would be misleading).
    st.caption("**Jupiter** — 2026 YTD")
    j1, j2, j3, j4 = st.columns(4)
    j1.metric("Cum. Volume",
              _fmt_metric(_latest(pairs["volume"][0],  "cumulative")))
    j2.metric("Cum. Trades",
              _fmt_metric(_latest(pairs["trades"][0],  "cumulative"), mode="count"))
    j3.metric("Latest Daily Traders",
              _fmt_metric(_latest(pairs["traders"][0], "daily"), mode="count"))
    j4.metric("Latest Daily Markets",
              _fmt_metric(_latest(pairs["markets"][0], "daily"), mode="count"))
    st.caption("**DFlow** — 2026 YTD")
    d1, d2, d3, d4 = st.columns(4)
    d1.metric("Cum. Volume",
              _fmt_metric(_latest(pairs["volume"][1],  "cumulative")))
    d2.metric("Cum. Trades",
              _fmt_metric(_latest(pairs["trades"][1],  "cumulative"), mode="count"))
    d3.metric("Latest Daily Traders",
              _fmt_metric(_latest(pairs["traders"][1], "daily"), mode="count"))
    d4.metric("Latest Daily Markets",
              _fmt_metric(_latest(pairs["markets"][1], "daily"), mode="count"))
    st.caption(f"As of {asof}.")
    st.write("")

    # ── 4 head-to-head chart pairs (8 charts total in 4 rows of 2) ─────
    _compare_specs = [
        (pairs["volume"][0],  pairs["volume"][1],
         "Daily Volume — Jupiter vs DFlow",
         "Cumulative Volume — Jupiter vs DFlow",
         "pm_alm_volume",  "currency"),
        (pairs["trades"][0],  pairs["trades"][1],
         "Daily Trades — Jupiter vs DFlow",
         "Cumulative Trades — Jupiter vs DFlow",
         "pm_alm_trades",  "count"),
        (pairs["traders"][0], pairs["traders"][1],
         "Daily Unique Traders — Jupiter vs DFlow",
         "Cumulative Trader-Days — Jupiter vs DFlow",
         "pm_alm_traders", "count"),
        (pairs["markets"][0], pairs["markets"][1],
         "Daily Unique Markets — Jupiter vs DFlow",
         "Cumulative Market-Days — Jupiter vs DFlow",
         "pm_alm_markets", "count"),
    ]
    for jup_df, dfl_df, dt, ct, key, mode in _compare_specs:
        if jup_df.empty and dfl_df.empty:
            st.info(f"No data for `{key}` yet.")
            continue
        _render_dune_metric_compare_pair(
            series=[
                (jup_df, "Jupiter", _JUP_COLOR, _JUP_FILL),
                (dfl_df, "DFlow",   _DFL_COLOR, _DFL_FILL),
            ],
            daily_col="daily", cum_col="cumulative",
            daily_title=dt, cum_title=ct,
            raw_key_prefix=key, fmt_mode=mode,
        )


_JUP_COLOR = "#9945FF"                        # Jupiter brand purple
_JUP_FILL  = "rgba(153, 69, 255, 0.25)"
_DFL_COLOR = "#F97316"                        # DFlow — orange (complement
_DFL_FILL  = "rgba(249, 115, 22, 0.25)"       # to Jupiter purple; distinct
                                              # from Phantom lavender + the
                                              # CASH teal / USDC blue used
                                              # inside the DFlow token-
                                              # balance stack)


def _render_dune_metric_pair(
    df: _pd.DataFrame,
    daily_col: str,
    cum_col: str,
    daily_title: str,
    cum_title: str,
    raw_key_prefix: str,
    color: str = _JUP_COLOR,
    fill: str = _JUP_FILL,
    fmt_mode: str = "currency",
) -> None:
    """Render a daily-bar + cumulative-area chart pair side-by-side in a
    2-col layout. Both charts share the standard time-controls + 📋
    raw-data button via sd._chart's chart_title kwarg.

    Used to keep the 6 Jupiter metrics (Notional Volume, Volume, Fees,
    Transactions, TVL, Users) DRY — each metric is one call to this
    helper. fmt_mode='count' drops the $ prefix for non-USD metrics
    (Transactions, Users)."""
    # Hover formatter: $ for currency, comma-separated for counts.
    # raw_fmt_str carries the same convention into the 📋 raw-data modal
    # — defaults to '${:,.0f}' for USD via _raw_data_modal's auto-fmt;
    # explicitly '{:,.0f}' (no $) for count metrics so Transactions /
    # Users render as 175,474 not $175,474.
    if fmt_mode == "count":
        _hover_fmt   = lambda v: f"{int(v):,}"
        _raw_fmt_str = "{:,.0f}"
    else:
        _hover_fmt   = sd._fmt_usd
        _raw_fmt_str = "${:,.0f}"

    col_left, col_right = st.columns(2, gap="medium")

    # ── Left: daily values (D/W/M) ─────────────────────────────────────
    def _build_daily_fig(df_view):
        fig = _go.Figure()
        fig.add_trace(_go.Bar(
            x=df_view["day"], y=df_view[daily_col], name=daily_title,
            marker=dict(color=color),
            customdata=df_view[daily_col].map(_hover_fmt),
            hovertemplate="%{x|%Y-%m-%d}: %{customdata}<extra></extra>",
        ))
        y_max_d = float(df_view[daily_col].max() or 0)
        fig.update_layout(
            height=400, hovermode="x unified",
            margin=dict(t=10, b=10, l=10, r=10), showlegend=False,
            yaxis=dict(showgrid=True, rangemode="tozero",
                       range=[0, y_max_d * 1.10] if y_max_d > 0 else None),
        )
        return fig

    with col_left:
        sd._chart_dwm_simple(
            daily_title,
            source_df=df[["day", daily_col]].copy(),
            build_fig=_build_daily_fig,
            raw_df=df[["day", daily_col]].copy(),
            raw_key=f"{raw_key_prefix}_daily",
            raw_fmt={daily_col: _raw_fmt_str},
            raw_filename=f"{raw_key_prefix}_daily",
            col_aggs={daily_col: "sum"},
            fmt_mode=fmt_mode,
        )

    # ── Right: cumulative (D/W/M; 'last' = period-end running total) ───
    def _build_cum_fig(df_view):
        fig = _go.Figure()
        fig.add_trace(_go.Scatter(
            x=df_view["day"], y=df_view[cum_col], name=cum_title,
            mode="lines", line=dict(color=color, width=1.5),
            fill="tozeroy", fillcolor=fill,
            customdata=df_view[cum_col].map(_hover_fmt),
            hovertemplate="%{x|%Y-%m-%d}: %{customdata}<extra></extra>",
        ))
        y_max_c = float(df_view[cum_col].max() or 0)
        fig.update_layout(
            height=400, hovermode="x unified",
            margin=dict(t=10, b=10, l=10, r=10), showlegend=False,
            yaxis=dict(showgrid=True, rangemode="tozero",
                       range=[0, y_max_c * 1.10] if y_max_c > 0 else None),
        )
        return fig

    with col_right:
        sd._chart_dwm_simple(
            cum_title,
            source_df=df[["day", cum_col]].copy(),
            build_fig=_build_cum_fig,
            raw_df=df[["day", cum_col]].copy(),
            raw_key=f"{raw_key_prefix}_cum",
            raw_fmt={cum_col: _raw_fmt_str},
            raw_filename=f"{raw_key_prefix}_cum",
            col_aggs={cum_col: "last"},
            fmt_mode=fmt_mode,
        )


def _render_dune_metric_compare_pair(
    series: list[tuple[_pd.DataFrame, str, str, str]],
    daily_col: str,
    cum_col: str,
    daily_title: str,
    cum_title: str,
    raw_key_prefix: str,
    fmt_mode: str = "currency",
) -> None:
    """Render a side-by-side daily + cumulative chart pair where each
    chart overlays one trace per platform from `series`. Use this when
    two or more platforms publish the SAME metric in compatible units
    (e.g. Jupiter vs DFlow Notional Volume / Volume / Fees / Tx) so the
    viewer can eyeball-compare trajectories on one axis.

    `series` items are tuples (df, label, color, fill_rgba). Each df
    must have a `day` column plus the requested daily_col + cum_col.

    Daily chart  → lines+markers per platform (bars don't compose for
                   N overlapping series).
    Cumulative   → solid line per platform, no fill — when one platform
                   is much larger than another, a filled-area overlay
                   visually hides the smaller series. Lines work for any
                   magnitude gap."""
    if fmt_mode == "count":
        _hover_fmt   = lambda v: f"{int(v):,}"
        _raw_fmt_str = "{:,.0f}"
    else:
        _hover_fmt   = sd._fmt_usd
        _raw_fmt_str = "${:,.0f}"

    # Build merged raw frames: outer-join on day → one col per platform
    # so the 📋 modal lets analysts see all platforms side-by-side.
    raw_daily = None
    raw_cum   = None
    for df, label, _c, _f in series:
        if df.empty:
            continue
        sub_d = df[["day", daily_col]].rename(columns={daily_col: label})
        sub_c = df[["day", cum_col]].rename(columns={cum_col: label})
        raw_daily = sub_d if raw_daily is None else raw_daily.merge(
            sub_d, on="day", how="outer")
        raw_cum   = sub_c if raw_cum   is None else raw_cum.merge(
            sub_c, on="day", how="outer")
    if raw_daily is not None:
        raw_daily = raw_daily.sort_values("day").reset_index(drop=True)
    if raw_cum is not None:
        raw_cum = raw_cum.sort_values("day").reset_index(drop=True)

    col_left, col_right = st.columns(2, gap="medium")
    _present = [(d, lbl, c, f) for d, lbl, c, f in series if not d.empty]

    # ── Left: daily lines+markers per platform (D/W/M) ──────────────────
    def _build_compare_daily_fig(df_view):
        fig = _go.Figure()
        for _df_p, label, color, _ in _present:
            if label not in df_view.columns:
                continue
            y = df_view[label]
            fig.add_trace(_go.Scatter(
                x=df_view["day"], y=y, name=label,
                mode="lines+markers",
                line=dict(color=color, width=1.5),
                marker=dict(color=color, size=4),
                customdata=y.map(_hover_fmt),
                hovertemplate=f"{label}: %{{customdata}}<extra></extra>",
            ))
        y_max_d = (raw_daily.drop(columns="day").max(numeric_only=True).max()
                   if raw_daily is not None else 0)
        try:
            y_max_d = float(y_max_d or 0)
        except (TypeError, ValueError):
            y_max_d = 0
        fig.update_layout(
            height=420, hovermode="x unified",
            margin=dict(t=10, b=10, l=10, r=10),
            legend=dict(orientation="h", yanchor="bottom", y=1.02,
                        xanchor="right", x=1),
            yaxis=dict(showgrid=True, rangemode="tozero",
                       range=[0, y_max_d * 1.10] if y_max_d > 0 else None),
        )
        return fig

    _raw_fmt_d = {label: _raw_fmt_str for _, label, _, _ in _present}
    with col_left:
        if raw_daily is None or raw_daily.empty:
            st.info(f"No data for {daily_title} yet.")
        else:
            sd._chart_dwm_simple(
                daily_title,
                source_df=raw_daily,
                build_fig=_build_compare_daily_fig,
                raw_df=raw_daily,
                raw_key=f"{raw_key_prefix}_daily",
                raw_fmt=_raw_fmt_d,
                raw_filename=f"{raw_key_prefix}_daily",
                col_aggs={lbl: "sum" for _, lbl, _, _ in _present},
                fmt_mode=fmt_mode,
            )

    # ── Right: cumulative lines per platform (D/W/M; 'last') ────────────
    def _build_compare_cum_fig(df_view):
        fig = _go.Figure()
        for _df_p, label, color, _ in _present:
            if label not in df_view.columns:
                continue
            y = df_view[label]
            fig.add_trace(_go.Scatter(
                x=df_view["day"], y=y, name=label,
                mode="lines",
                line=dict(color=color, width=2),
                customdata=y.map(_hover_fmt),
                hovertemplate=f"{label}: %{{customdata}}<extra></extra>",
            ))
        y_max_c = (raw_cum.drop(columns="day").max(numeric_only=True).max()
                   if raw_cum is not None else 0)
        try:
            y_max_c = float(y_max_c or 0)
        except (TypeError, ValueError):
            y_max_c = 0
        fig.update_layout(
            height=420, hovermode="x unified",
            margin=dict(t=10, b=10, l=10, r=10),
            legend=dict(orientation="h", yanchor="bottom", y=1.02,
                        xanchor="right", x=1),
            yaxis=dict(showgrid=True, rangemode="tozero",
                       range=[0, y_max_c * 1.10] if y_max_c > 0 else None),
        )
        return fig

    _raw_fmt_c = {label: _raw_fmt_str for _, label, _, _ in _present}
    with col_right:
        if raw_cum is None or raw_cum.empty:
            st.info(f"No data for {cum_title} yet.")
        else:
            sd._chart_dwm_simple(
                cum_title,
                source_df=raw_cum,
                build_fig=_build_compare_cum_fig,
                raw_df=raw_cum,
                raw_key=f"{raw_key_prefix}_cum",
                raw_fmt=_raw_fmt_c,
                raw_filename=f"{raw_key_prefix}_cum",
                col_aggs={lbl: "last" for _, lbl, _, _ in _present},
                fmt_mode=fmt_mode,
            )


def _fetch_jupiter_metric(query_id: int, daily_src: str, cum_src: str,
                          resample_to_daily: bool = False) -> _pd.DataFrame:
    """Wrapper around _fetch_dune_query_results that normalises a Jupiter
    metric query's columns to ['day', 'daily', 'cumulative']. Set
    resample_to_daily=True for the TVL query (which returns hourly rows)
    to collapse to end-of-day. Empty frame on fetch failure."""
    df = _fetch_dune_query_results(query_id)
    if df.empty or "day" not in df.columns or daily_src not in df.columns:
        return _pd.DataFrame(columns=["day", "daily", "cumulative"])
    out = df[["day", daily_src, cum_src]].rename(
        columns={daily_src: "daily", cum_src: "cumulative"})
    out["daily"] = out["daily"].astype(float)
    out["cumulative"] = out["cumulative"].astype(float)
    if resample_to_daily:
        # End-of-day TVL = last cumulative reading per UTC day. The TVL
        # query is hourly (4305 rows ≈ 180 days × 24); collapsing avoids
        # drawing 4k+ bars while preserving the daily-close trajectory.
        # Also sum daily deltas within each day to get a real daily-Δ bar.
        out["_d"] = out["day"].dt.floor("D")
        agg = out.groupby("_d", as_index=False).agg(
            daily=("daily", "sum"),
            cumulative=("cumulative", "last"),
        ).rename(columns={"_d": "day"})
        out = agg.sort_values("day").reset_index(drop=True)
    return out


def _render_jupiter_prediction_section() -> None:
    """Jupiter-only metrics (TVL + Unique Users). The 4 metrics Jupiter
    shares with DFlow (Notional Volume, Volume, Fees, Transactions) are
    rendered in the comparison section above to avoid duplication.

    Section title is supplied by the st.expander wrapper in
    _render_prediction_markets — no internal subheader needed."""
    st.caption(
        "Jupiter-only metrics not published in compatible units by DFlow. "
        "Source: [datadashboards/jupiter-prediction-markets]"
        "(https://dune.com/datadashboards/jupiter-prediction-markets)."
    )

    df_tvl   = _fetch_jupiter_metric(_DUNE_QUERY_JUPITER_TVL,
                                     "TVLDelta", "TVL_CumulativeDelta",
                                     resample_to_daily=True)
    df_users = _fetch_jupiter_metric(_DUNE_QUERY_JUPITER_USERS,
                                     "New", "Cumulative Unique Users")

    if df_tvl.empty and df_users.empty:
        st.info("No Dune data available for Jupiter's TVL or Users queries.")
        return

    def _latest(_df, col):
        return float(_df[col].iloc[-1]) if not _df.empty else None

    def _fmt_metric(v, mode="currency"):
        if v is None:    return "—"
        if mode == "count": return f"{int(v):,}"
        return sd._fmt_usd(v)

    asof_candidates = [d["day"].iloc[-1] for d in [df_tvl, df_users] if not d.empty]
    asof = max(asof_candidates).strftime("%Y-%m-%d") if asof_candidates else "?"

    k1, k2 = st.columns(2)
    k1.metric("Current TVL",
              _fmt_metric(_latest(df_tvl, "cumulative")))
    k2.metric("Cumulative Unique Users",
              _fmt_metric(_latest(df_users, "cumulative"), mode="count"))
    st.caption(f"As of {asof}.")
    st.write("")

    _metric_specs = [
        (df_tvl,
         "Jupiter — Daily TVL Delta",
         "Jupiter — Cumulative TVL",
         "pm_jup_tvl",       "currency"),
        (df_users,
         "Jupiter — Daily New Users",
         "Jupiter — Cumulative Unique Users",
         "pm_jup_users",     "count"),
    ]
    for spec_df, dt, ct, key, mode in _metric_specs:
        if spec_df.empty:
            st.info(f"No data for `{key}` yet.")
            continue
        _render_dune_metric_pair(
            spec_df, daily_col="daily", cum_col="cumulative",
            daily_title=dt, cum_title=ct,
            raw_key_prefix=key, fmt_mode=mode,
        )


def _render_dflow_prediction_section() -> None:
    """DFlow-only metrics: Daily Active Users + Token Balance by symbol.
    The 4 metrics DFlow shares with Jupiter (Notional Volume, Volume,
    Fees, Transactions) are rendered in the comparison section above to
    avoid duplication. Sources:
      • 6510861 — unified daily activity (we only consume N_Users here;
        the other 4 cols go to the comparison section above)
      • 6512170 — long-format token balance (day, symbol, token_balance)

    Section title is supplied by the st.expander wrapper in
    _render_prediction_markets — no internal subheader needed.
    """
    st.caption(
        "DFlow-only metrics not published by Jupiter. Source: "
        "[stepanalytics_team/prediction-markets-on-solana]"
        "(https://dune.com/stepanalytics_team/prediction-markets-on-solana)."
    )

    raw = _fetch_dune_query_results(_DUNE_QUERY_DFLOW_ACTIVITY)
    have_users = (not raw.empty and "day" in raw.columns
                  and "N_Users" in raw.columns)
    df_users = _pd.DataFrame(columns=["day", "daily"])
    if have_users:
        df_users = raw[["day", "N_Users"]].rename(
            columns={"N_Users": "daily"}).copy()
        df_users["daily"] = df_users["daily"].astype(float)

    tokbal = _fetch_dune_query_results(_DUNE_QUERY_DFLOW_TOKBAL)
    have_tokbal = (not tokbal.empty and "day" in tokbal.columns
                   and "symbol" in tokbal.columns
                   and "token_balance" in tokbal.columns)
    if have_tokbal:
        tokbal = tokbal.copy()
        tokbal["token_balance"] = tokbal["token_balance"].astype(float)

    if df_users.empty and not have_tokbal:
        st.info("No Dune data available for DFlow's Active Users or "
                "Token Balance queries.")
        return

    def _fmt_metric(v, mode="currency"):
        if v is None:    return "—"
        if mode == "count": return f"{int(v):,}"
        return sd._fmt_usd(v)

    latest_tokbal_total = None
    if have_tokbal:
        latest_per_sym = (tokbal.sort_values("day")
                                .groupby("symbol")
                                .tail(1)["token_balance"].sum())
        latest_tokbal_total = float(latest_per_sym)

    asof_candidates = []
    if not df_users.empty: asof_candidates.append(df_users["day"].iloc[-1])
    if have_tokbal:        asof_candidates.append(tokbal["day"].max())
    asof = max(asof_candidates).strftime("%Y-%m-%d") if asof_candidates else "?"

    k1, k2 = st.columns(2)
    k1.metric("7d Avg Daily Active Users",
              _fmt_metric(df_users["daily"].tail(7).mean() if not df_users.empty else None,
                          mode="count"))
    k2.metric("Total Token Balance",
              _fmt_metric(latest_tokbal_total))
    st.caption(f"As of {asof}.")
    st.write("")

    # ── Daily Active Users (D/W/M; sum across period) ──────────────────
    if not df_users.empty:
        def _build_dfl_users_fig(df_view):
            fig = _go.Figure()
            fig.add_trace(_go.Bar(
                x=df_view["day"], y=df_view["daily"],
                name="Daily Active Users",
                marker=dict(color=_DFL_COLOR),
                customdata=df_view["daily"].map(lambda v: f"{int(v):,}"),
                hovertemplate="%{x|%Y-%m-%d}: %{customdata}<extra></extra>",
            ))
            y_max_u = float(df_view["daily"].max() or 0)
            fig.update_layout(
                height=400, hovermode="x unified",
                margin=dict(t=10, b=10, l=10, r=10), showlegend=False,
                yaxis=dict(showgrid=True, rangemode="tozero",
                           range=[0, y_max_u * 1.10] if y_max_u > 0 else None),
            )
            return fig

        sd._chart_dwm_simple(
            "DFlow — Daily Active Users",
            source_df=df_users[["day", "daily"]].copy(),
            build_fig=_build_dfl_users_fig,
            raw_df=df_users[["day", "daily"]].copy(),
            raw_key="pm_dfl_users_daily",
            raw_fmt={"daily": "{:,.0f}"},
            raw_filename="pm_dfl_users_daily",
            col_aggs={"daily": "sum"},
            fmt_mode="count",
        )

    # ── Token balance stacked area (CASH + USDC) — D/W/M; 'last' ───────
    if have_tokbal:
        # Pivot long → wide so plotly gets one column per symbol.
        wide = (tokbal.pivot_table(index="day", columns="symbol",
                                   values="token_balance", aggfunc="last")
                       .sort_index()
                       .reset_index())
        for sym in wide.columns[1:]:
            wide[sym] = wide[sym].ffill()
        _palette = {"CASH": "#4ECDC4", "USDC": "#2775CA"}
        symbols = [c for c in wide.columns if c != "day"]
        totals = wide[symbols].fillna(0).sum(axis=1)

        def _build_dfl_bal_fig(df_view):
            fig = _go.Figure()
            present_syms = [s for s in symbols if s in df_view.columns]
            tot_v = df_view[present_syms].fillna(0).sum(axis=1)
            for sym in present_syms:
                y = df_view[sym].fillna(0)
                fig.add_trace(_go.Scatter(
                    x=df_view["day"], y=y, name=sym,
                    mode="lines",
                    line=dict(width=0.8,
                              color=_palette.get(sym, _DFL_COLOR)),
                    stackgroup="bal",
                    customdata=y.map(sd._fmt_usd),
                    hovertemplate=f"{sym}: %{{customdata}}<extra></extra>",
                ))
            fig.add_trace(_go.Scatter(
                x=df_view["day"], y=tot_v, name="Total",
                mode="lines", line=dict(width=0, color="rgba(0,0,0,0)"),
                showlegend=False, stackgroup=None,
                customdata=tot_v.map(sd._fmt_usd),
                hovertemplate="<b>Total: %{customdata}</b><extra></extra>",
            ))
            y_max_b = float(tot_v.max() or 0)
            fig.update_layout(
                height=400, hovermode="x unified",
                margin=dict(t=10, b=10, l=10, r=10),
                legend=dict(orientation="h", yanchor="bottom",
                            y=1.02, xanchor="right", x=1),
                yaxis=dict(showgrid=True, rangemode="tozero",
                           range=[0, y_max_b * 1.10] if y_max_b > 0 else None),
            )
            return fig

        _raw_bal = wide.copy()
        _raw_bal["total"] = totals.values
        sd._chart_dwm_simple(
            "DFlow — Token Balance by Symbol",
            source_df=wide[["day"] + symbols].copy(),
            build_fig=_build_dfl_bal_fig,
            raw_df=_raw_bal,
            raw_key="pm_dfl_token_balance",
            raw_filename="pm_dfl_token_balance",
            col_aggs={s: "last" for s in symbols},
        )


def _render_phantom_prediction_section() -> None:
    """Phantom prediction-market TVL (Dune query 6386183 — TVL cumulative
    delta). The companion queries on the source dashboard (Volume/Fees/Tx
    via 6386520, Users via 6453064) are private at source and return
    'Query not found' via API — surfaced as a footer note.

    Section title is supplied by the st.expander wrapper in
    _render_prediction_markets — no internal subheader needed."""
    st.caption(
        f"Source: Dune query [{_DUNE_QUERY_PHANTOM_TVL}]"
        f"(https://dune.com/queries/{_DUNE_QUERY_PHANTOM_TVL}) "
        "(powered by DFlow & Kalshi)."
    )

    df = _fetch_dune_query_results(_DUNE_QUERY_PHANTOM_TVL)
    if df.empty or "day" not in df.columns:
        st.info("No Dune data available for the Phantom TVL query.")
        return

    df = df.assign(
        tvl_delta = df["TVLDelta"].astype(float),
        tvl_cum   = df["TVL_CumulativeDelta"].astype(float),
    )

    # Headline metrics: latest cumulative TVL + peak TVL + 7d avg daily delta.
    latest_tvl = float(df["tvl_cum"].iloc[-1])
    peak_tvl   = float(df["tvl_cum"].max())
    delta_7d   = df["tvl_delta"].tail(7).mean()
    asof       = df["day"].iloc[-1].strftime("%Y-%m-%d")
    m1, m2, m3 = st.columns(3)
    m1.metric("Current TVL",           sd._fmt_usd(latest_tvl))
    m2.metric("Peak TVL",              sd._fmt_usd(peak_tvl))
    m3.metric("7d Avg Daily Δ",        sd._fmt_usd(delta_7d))
    st.caption(f"As of {asof} · {len(df)} daily observations.")

    _PHANTOM_COLOR = "#AB9FF2"   # Phantom signature lavender
    _PHANTOM_FILL  = "rgba(171, 159, 242, 0.30)"
    # Diverging bar colors for the daily-delta chart so withdrawals (the
    # 2026-03-07 -$499K outflow event) read instantly without needing the
    # tooltip — green inflows / red outflows.
    bar_colors = ["#4ECDC4" if v >= 0 else "#FF6B6B"
                  for v in df["tvl_delta"]]

    col_left, col_right = st.columns(2, gap="medium")

    # ── Left: daily TVL delta (D/W/M; sum, may go negative) ────────────
    def _build_phantom_tvl_daily_fig(df_view):
        fig = _go.Figure()
        _colors = ["#4ECDC4" if v >= 0 else "#FF6B6B"
                   for v in df_view["tvl_delta"]]
        fig.add_trace(_go.Bar(
            x=df_view["day"], y=df_view["tvl_delta"], name="Daily Δ",
            marker=dict(color=_colors),
            customdata=df_view["tvl_delta"].map(sd._fmt_usd),
            hovertemplate="%{x|%Y-%m-%d}: %{customdata}<extra></extra>",
        ))
        _y_lo = float(df_view["tvl_delta"].min())
        _y_hi = float(df_view["tvl_delta"].max())
        _pad  = max(abs(_y_lo), abs(_y_hi)) * 0.10
        fig.update_layout(
            height=420, hovermode="x unified",
            margin=dict(t=10, b=10, l=10, r=10), showlegend=False,
            yaxis=dict(showgrid=True, zeroline=True,
                       zerolinecolor="rgba(255,255,255,0.3)",
                       range=[_y_lo - _pad, _y_hi + _pad]),
        )
        return fig

    with col_left:
        sd._chart_dwm_simple(
            "Phantom — Daily TVL Delta",
            source_df=df[["day", "tvl_delta"]].copy(),
            build_fig=_build_phantom_tvl_daily_fig,
            raw_df=df[["day", "tvl_delta"]].copy(),
            raw_key="pm_phantom_tvl_daily",
            raw_filename="phantom_prediction_market_tvl_daily_delta",
            col_aggs={"tvl_delta": "sum"},  # delta = flow → sum periods
        )

    # ── Right: cumulative TVL (D/W/M; 'last' = period-end TVL) ─────────
    def _build_phantom_tvl_cum_fig(df_view):
        fig = _go.Figure()
        fig.add_trace(_go.Scatter(
            x=df_view["day"], y=df_view["tvl_cum"], name="Cumulative TVL",
            mode="lines", line=dict(color=_PHANTOM_COLOR, width=1.5),
            fill="tozeroy", fillcolor=_PHANTOM_FILL,
            customdata=df_view["tvl_cum"].map(sd._fmt_usd),
            hovertemplate="%{x|%Y-%m-%d}: %{customdata}<extra></extra>",
        ))
        y_max_c = float(df_view["tvl_cum"].max() or 0)
        fig.update_layout(
            height=420, hovermode="x unified",
            margin=dict(t=10, b=10, l=10, r=10), showlegend=False,
            yaxis=dict(showgrid=True, rangemode="tozero",
                       range=[0, y_max_c * 1.10] if y_max_c > 0 else None),
        )
        return fig

    with col_right:
        sd._chart_dwm_simple(
            "Phantom — Cumulative TVL",
            source_df=df[["day", "tvl_cum"]].copy(),
            build_fig=_build_phantom_tvl_cum_fig,
            raw_df=df[["day", "tvl_cum"]].copy(),
            raw_key="pm_phantom_tvl_cumulative",
            raw_filename="phantom_prediction_market_tvl_cumulative",
            col_aggs={"tvl_cum": "last"},
        )

    st.caption(
        ":information_source: The source dashboard also tracks Volume / "
        "Fees / Transactions (query 6386520) and Unique Users (query "
        "6453064) but those queries are private — Dune's API returns "
        "`Query not found`. To surface them here either ask the dashboard "
        "owner to make them public, or fork them on Dune to get a public "
        "query ID under your account."
    )


# ── Perp DEXs vertical (Blockworks Research scraped) ─────────────────────────
# Data: blockworks.fetch_perp_dex_data() — scrapes the Blockworks
# Analytics page for current execution IDs, then pulls 11 query
# results from rest.blockworksresearch.com (no auth required, ~MBs
# of JSON, all cached for 4h via @st.cache_data).
import blockworks as _blockworks

# The chartwrap-button-pinning CSS lives inside stocks_dashboard.main()
# which solana_dashboard doesn't invoke — without this call the 📋
# raw-data button on every D/W/M chart renders as a default Streamlit
# square button on its own row above the tabs instead of pinning to
# the tab row's right edge.
sd.inject_chartwrap_css()


# Brand colors for the major Solana perp DEXs — picked from each
# project's primary brand color where possible, otherwise distinct
# from the Solana / SOL palette already in use elsewhere.
_PERP_DEX_COLORS = {
    "Drift":       "#9945FF",   # Solana purple — Drift is the OG
    "Jupiter":     "#FBA43A",   # Jupiter orange
    "Flash Trade": "#3DD7B0",   # Flash teal
    "GMTrade":     "#F45BBE",   # GMX-ish pink
    "Pacifica":    "#5BC0EB",   # cyan
    "Phoenix":     "#FF6B6B",   # red
    "Bullet":      "#FEE440",   # yellow
}

# Per-asset fallback palette for the Crypto / Commodity / Equity /
# Index / FX charts where each stacked band is a TICKER (BTC, XAU,
# TSLA, EUR…) not a DEX. 18 maximally-distinct hues picked from
# opposite quadrants of the color wheel so adjacent stacked bands
# always read as different colors. Cycled by index into the
# sorted-by-latest-value column order, so band-to-color mapping is
# stable per render — and consistent across the chart traces +
# legend expander swatches.
_PERP_ASSET_PALETTE = [
    "#4285F4",  # google blue
    "#EF4444",  # red
    "#10B981",  # emerald
    "#F97316",  # orange
    "#A78BFA",  # lavender
    "#06B6D4",  # cyan
    "#EC4899",  # pink
    "#FBBF24",  # yellow
    "#14B8A6",  # teal
    "#1E40AF",  # navy
    "#84CC16",  # lime
    "#FB7185",  # coral
    "#9333EA",  # violet
    "#0EA5E9",  # sky
    "#F59E0B",  # amber
    "#22C55E",  # green
    "#E11D48",  # rose
    "#7C3AED",  # purple
]


def _resolve_perp_color(name: str, idx: int) -> str:
    """Color for a perp-chart series. Resolves DEX brand colors first
    (Drift/Jupiter/Flash Trade/etc.) so the per-DEX charts stay
    branded; falls back to the per-asset palette cycled by index for
    everything else (BTC/XAU/TSLA/EUR/…), so the asset-class charts
    differentiate bands instead of stacking 10 greys."""
    if name in _PERP_DEX_COLORS:
        return _PERP_DEX_COLORS[name]
    return _PERP_ASSET_PALETTE[idx % len(_PERP_ASSET_PALETTE)]


def _render_perp_dexs() -> None:
    """Solana perp DEXs analytics — sourced from the Blockworks
    Research dashboard (blockworks.com/analytics/solana/perp-dexs-
    solana) via their public read-side REST endpoint.

    Layout:
      Headline metrics (24h volume / OI / fees from the snapshot
      query) → per-DEX stacked area for Volume, OI, Fees+Rev, Markets
      → per-asset-class daily volume stacks (Commodities / Equities /
      Indices / FX) in 2-col rows.
    """
    st.markdown("## Solana Perp DEXs")
    st.caption(
        "On-chain perpetual-futures DEX activity on Solana. Source: "
        "[Blockworks Research analytics dashboard]("
        f"{_blockworks.DASHBOARD_URL}) — execution IDs scraped from "
        "the SSR'd dashboard page, raw rows pulled from "
        "`rest.blockworksresearch.com`'s public execution endpoint. "
        "Cached 4h. Tracks Drift, Jupiter, Flash Trade, GMTrade, "
        "Pacifica, Phoenix, Bullet."
    )

    data = _blockworks.fetch_perp_dex_data()
    if not data:
        st.warning(
            "Blockworks data fetch failed — page scrape returned no "
            "execution IDs. Either the dashboard moved or the page "
            "structure changed. Check the network manually."
        )
        return

    # ── Helpers ────────────────────────────────────────────────────────────
    def _pivot_metric(df: _pd.DataFrame, metric_col: str,
                       dim_filter=None) -> _pd.DataFrame:
        """Pivot the qid=4594-style wide table to date×symbol where
        `metric_col` is populated. `dim_filter(sym)` returns True for
        symbols to keep — defaults to keeping non-prefixed names
        (DEX-level, not the _Drift / __BTC sub-rows)."""
        if dim_filter is None:
            dim_filter = lambda s: (
                isinstance(s, str) and not s.startswith("_")
                and s not in ("Total",)
            )
        if metric_col not in df.columns or "symbol" not in df.columns:
            return _pd.DataFrame()
        sub = df[df[metric_col].notna() & df["symbol"].apply(dim_filter)]
        if sub.empty:
            return _pd.DataFrame()
        wide = (sub.pivot_table(index="date", columns="symbol",
                                values=metric_col, aggfunc="sum")
                   .sort_index().reset_index())
        return wide

    def _sort_cols_by_latest(wide: _pd.DataFrame) -> list[str]:
        """Order non-date columns by latest value desc — largest DEX
        sits at the bottom of the stack as the anchor."""
        if wide.empty or len(wide.columns) <= 1:
            return []
        cols = [c for c in wide.columns if c != "date"]
        latest = wide.iloc[-1].fillna(0)
        return sorted(cols, key=lambda c: float(latest.get(c, 0) or 0),
                      reverse=True)

    def _build_perp_stack(wide: _pd.DataFrame, fmt_kind: str = "currency",
                          height: int = 380):
        """Stacked area figure from a date×<DEX> wide df. fmt_kind:
        'currency' → '$X.YB' y-axis; 'count' → bare numbers.

        Plotly's inline legend is hidden — the collapsible HTML legend
        rendered by `_perp_legend_expander` below each chart shows the
        swatches without stealing chart real-estate (some asset-class
        charts have 10+ symbols which would dominate the plot if
        rendered inline)."""
        ordered = _sort_cols_by_latest(wide)
        # Color-by-rank: position in `ordered` (sorted by latest value
        # desc) drives the palette index. Same `(name, idx)` pair is
        # used by `_perp_legend_expander` below so chart band colors
        # match the expander swatches exactly.
        color_for = {name: _resolve_perp_color(name, i)
                     for i, name in enumerate(ordered)}
        fig = _go.Figure()
        totals = wide[ordered].ffill().fillna(0).sum(axis=1) if ordered else _pd.Series()
        # Add smallest-last so largest band lands at the bottom of the
        # visual stack (anchor + most readable).
        for col in reversed(ordered):
            color = color_for[col]
            y = wide[col].ffill().fillna(0)
            fig.add_trace(_go.Scatter(
                x=wide["date"], y=y, name=col,
                mode="lines",
                line=dict(color=color, width=0.9),
                stackgroup="perps",
                customdata=y.map(sd._fmt_usd if fmt_kind == "currency"
                                  else (lambda v: f"{int(v):,}")),
                hovertemplate=f"{col}: %{{customdata}}<extra></extra>",
            ))
        if not totals.empty:
            fig.add_trace(_go.Scatter(
                x=wide["date"], y=totals, name="Total",
                mode="lines",
                line=dict(width=0, color="rgba(0,0,0,0)"),
                showlegend=False, stackgroup=None,
                customdata=totals.map(sd._fmt_usd if fmt_kind == "currency"
                                       else (lambda v: f"{int(v):,}")),
                hovertemplate="<b>Total: %{customdata}</b><extra></extra>",
            ))
        y_max = float(totals.max() or 0) if not totals.empty else 0
        fig.update_layout(
            height=height, hovermode="x unified",
            margin=dict(t=10, b=10, l=10, r=10),
            showlegend=False,  # collapsible HTML legend handles it
            yaxis=dict(
                tickprefix="$" if fmt_kind == "currency" else "",
                tickformat="~s",
                showgrid=True, rangemode="tozero",
                range=[0, y_max * 1.10] if y_max > 0 else None,
            ),
        )
        return fig

    def _perp_legend_expander(wide: _pd.DataFrame) -> None:
        """Render a collapsible HTML-grid legend below a perp chart.
        Mirrors the pattern from render_market_cap_chain's large-N
        chart legend (Solana-tab volume chart, xStocks/Ondo per-token
        MC). Color = _PERP_DEX_COLORS lookup; falls back to grey for
        anything outside the known DEX set (per-symbol breakdowns
        like __BTC / __ETH that don't have brand colors)."""
        ordered = _sort_cols_by_latest(wide)
        if not ordered:
            return
        # Use the SAME (name, idx) → color mapping the chart traces
        # use, so swatch colors match the chart bands exactly. DEX
        # name → brand color; ticker → fallback palette cycled by
        # rank index (largest by latest value gets palette[0]).
        with st.expander(f"Legend ({len(ordered)} series)",
                         expanded=False):
            items_html = "".join(
                f'<div style="display:flex;align-items:center;gap:5px;'
                f'white-space:nowrap">'
                f'<span style="display:inline-block;width:12px;height:12px;'
                f'border-radius:2px;background:'
                f'{_resolve_perp_color(col, i)};'
                f'flex-shrink:0"></span>'
                f'<span style="font-size:0.8rem">{col}</span></div>'
                for i, col in enumerate(ordered)
            )
            st.markdown(
                f'<div style="display:grid;'
                f'grid-template-columns:repeat(8,1fr);'
                f'gap:6px 16px;padding:4px 0">{items_html}</div>',
                unsafe_allow_html=True,
            )

    # ── Headline metrics (24h, from snapshot queries) ─────────────────────
    snap = data.get(4600)
    snap_sym = data.get(4601)
    if snap is not None and not snap.empty:
        latest_vol = float(snap["delta_vol"].fillna(0).sum()) if "delta_vol" in snap.columns else None
        latest_oi  = float(snap["delta_oi"].fillna(0).sum())  if "delta_oi" in snap.columns else None
        latest_tc  = int(snap["delta_tc"].fillna(0).sum())    if "delta_tc" in snap.columns else None
        c1, c2, c3, c4 = st.columns(4)
        if latest_vol: c1.metric("24h Volume",   sd._fmt_usd(latest_vol))
        if latest_oi:  c2.metric("24h Δ OI",     sd._fmt_usd(latest_oi))
        if latest_tc:  c3.metric("24h Trades",   f"{latest_tc:,}")
        if 4631 in data and not data[4631].empty:
            d31 = data[4631]
            if "delta_fee_1d" in d31.columns:
                c4.metric("24h Fees",
                          sd._fmt_usd(float(d31["delta_fee_1d"].fillna(0).sum())))

    main = data.get(4594)
    fees = data.get(4630)
    markets = data.get(4617)

    if main is None or main.empty:
        st.warning("Main metrics query (4594) missing — check "
                   "Blockworks page scrape.")
        return

    # Each section renders its own trailing st.divider() *inside* the
    # data-present guard. That way an empty section (e.g. a query that
    # returned no rows for one metric) doesn't leave an orphan divider
    # stacked against the next section's divider, which read as two
    # parallel lines with empty space between them.
    st.divider()
    # ── Daily Volume by DEX ────────────────────────────────────────────────
    vol_wide = _pivot_metric(main, "vol_exch")
    if not vol_wide.empty:
        _raw = vol_wide.copy()
        _ordered = _sort_cols_by_latest(vol_wide)
        _raw["Total"] = vol_wide[_ordered].fillna(0).sum(axis=1)
        sd._chart_dwm_simple(
            "Daily Volume by DEX",
            source_df=vol_wide,
            build_fig=lambda df_view: _build_perp_stack(df_view),
            raw_df=_raw.sort_values("date", ascending=False),
            raw_key="perp_dex_vol_by_dex",
            raw_filename="solana_perp_dex_volume_by_dex",
            caption=(
                "Daily on-chain perpetual-futures notional volume per "
                "DEX. Stacked area; hover shows per-DEX + Total."
            ),
            col_aggs={c: "sum" for c in _ordered},
        )
        _perp_legend_expander(vol_wide)
        st.divider()

    # ── Open Interest by DEX ──────────────────────────────────────────────
    oi_wide = _pivot_metric(main, "oi_exch")
    if not oi_wide.empty:
        _raw_oi = oi_wide.copy()
        _ord_oi = _sort_cols_by_latest(oi_wide)
        _raw_oi["Total"] = oi_wide[_ord_oi].fillna(0).sum(axis=1)
        sd._chart_dwm_simple(
            "Open Interest by DEX",
            source_df=oi_wide,
            build_fig=lambda df_view: _build_perp_stack(df_view),
            raw_df=_raw_oi.sort_values("date", ascending=False),
            raw_key="perp_dex_oi_by_dex",
            raw_filename="solana_perp_dex_oi_by_dex",
            caption=(
                "Daily open interest per DEX. OI is a stock not a "
                "flow → Weekly/Monthly = period-end (last) value."
            ),
            col_aggs={c: "last" for c in _ord_oi},
        )
        _perp_legend_expander(oi_wide)
        st.divider()

    # ── Fees + Revenue ─────────────────────────────────────────────────────
    if fees is not None and not fees.empty:
        fees_wide = _pivot_metric(fees, "fee_usd_totals",
                                  dim_filter=lambda s: (
                                      isinstance(s, str)
                                      and s != "Total"
                                      and not s.startswith("_")
                                  ))
        rev_wide = _pivot_metric(fees, "rev_usd_totals",
                                 dim_filter=lambda s: (
                                     isinstance(s, str)
                                     and s != "Total"
                                     and not s.startswith("_")
                                 ))
        if not fees_wide.empty or not rev_wide.empty:
            col_fees, col_rev = st.columns(2, gap="medium")
            with col_fees:
                if not fees_wide.empty:
                    _raw_f = fees_wide.copy()
                    _ord_f = _sort_cols_by_latest(fees_wide)
                    _raw_f["Total"] = fees_wide[_ord_f].fillna(0).sum(axis=1)
                    sd._chart_dwm_simple(
                        "Daily Fees by DEX",
                        source_df=fees_wide,
                        build_fig=lambda df_view: _build_perp_stack(df_view),
                        raw_df=_raw_f.sort_values("date", ascending=False),
                        raw_key="perp_dex_fees",
                        raw_filename="solana_perp_dex_fees",
                        caption="Daily protocol fees paid per DEX.",
                        col_aggs={c: "sum" for c in _ord_f},
                    )
                    _perp_legend_expander(fees_wide)
            with col_rev:
                if not rev_wide.empty:
                    _raw_r = rev_wide.copy()
                    _ord_r = _sort_cols_by_latest(rev_wide)
                    _raw_r["Total"] = rev_wide[_ord_r].fillna(0).sum(axis=1)
                    sd._chart_dwm_simple(
                        "Daily Revenue by DEX",
                        source_df=rev_wide,
                        build_fig=lambda df_view: _build_perp_stack(df_view),
                        raw_df=_raw_r.sort_values("date", ascending=False),
                        raw_key="perp_dex_rev",
                        raw_filename="solana_perp_dex_rev",
                        caption=(
                            "Daily protocol revenue per DEX (post-LP / "
                            "post-rebate, what flows to the protocol)."
                        ),
                        col_aggs={c: "sum" for c in _ord_r},
                    )
                    _perp_legend_expander(rev_wide)
            st.divider()

    # ── Number of markets per DEX ─────────────────────────────────────────
    if markets is not None and not markets.empty:
        mk_wide = _pivot_metric(markets, "num_markets_totals")
        if not mk_wide.empty:
            _raw_m = mk_wide.copy()
            _ord_m = _sort_cols_by_latest(mk_wide)
            _raw_m["Total"] = mk_wide[_ord_m].fillna(0).sum(axis=1)
            sd._chart_dwm_simple(
                "Number of Markets per DEX",
                source_df=mk_wide,
                build_fig=lambda df_view: _build_perp_stack(
                    df_view, fmt_kind="count"),
                raw_df=_raw_m.sort_values("date", ascending=False),
                raw_key="perp_dex_market_count",
                raw_filename="solana_perp_dex_market_count",
                caption=(
                    "Number of distinct trading pairs listed per DEX. "
                    "Weekly/Monthly = period-end (last) count."
                ),
                col_aggs={c: "last" for c in _ord_m},
                fmt_mode="count",
            )
            _perp_legend_expander(mk_wide)
            st.divider()

    # ── Asset-class breakdowns ────────────────────────────────────────────
    # Filter for the per-asset-class charts: their `symbol` column
    # uses a SINGLE underscore prefix (_XAU / _WTI / _PAXG / _TSLA /
    # _SP500 / _EUR / etc.) to namespace the asset-class symbols away
    # from grand totals ("Total") and DEX names. The default
    # _pivot_metric filter rejects every `_*` string, so we have to
    # supply one that keeps single-underscore names + drops double-
    # underscore (sub-categories) + "Total".
    def _asset_class_filter(s):
        return (isinstance(s, str)
                and s.startswith("_")
                and not s.startswith("__")
                and s != "Total")

    def _render_asset_class_section(
        metric_prefix: str,
        section_title: str,
        caption: str,
        agg_rule: str,
        key_suffix: str,
    ) -> None:
        """Render one 2-col grid of 5 asset-class charts for a given
        metric. `metric_prefix` is 'vol' or 'oi' — appended to '_market_symbol'
        to find the right column on the source df. `agg_rule` controls
        the D/W/M resample: 'sum' for flows (volume), 'last' for stocks
        (open interest). `key_suffix` namespaces the raw-data button
        + CSV filename so the two sections don't collide."""
        st.subheader(section_title)
        st.caption(caption)

        # The per-symbol rows on qid=4594 use a DOUBLE underscore
        # prefix (__BTC / __ETH / …) while qid=4625-4628 use SINGLE
        # underscore (_XAU / _TSLA / …). Filter accordingly.
        crypto_filter = lambda s: (
            isinstance(s, str) and s.startswith("__") and s[2:] in
            ("BTC","ETH","SOL","BNB","XRP","HYPE","OTHER","ZEC"))
        metric_col = f"{metric_prefix}_market_symbol"
        asset_charts = [
            ("Crypto perps",    4594, metric_col, crypto_filter),
            ("Commodity perps", 4625, metric_col, _asset_class_filter),
            ("Equity perps",    4626, metric_col, _asset_class_filter),
            ("Index perps",     4627, metric_col, _asset_class_filter),
            ("FX perps",        4628, metric_col, _asset_class_filter),
        ]
        for row_start in range(0, len(asset_charts), 2):
            cols = st.columns(2, gap="medium")
            for col, spec in zip(cols, asset_charts[row_start: row_start + 2]):
                title, qid, mcol, dim_filter = spec
                df_q = data.get(qid)
                if df_q is None or df_q.empty:
                    with col:
                        st.info(f"{title}: no data for query {qid}.")
                    continue
                wide = _pivot_metric(df_q, mcol, dim_filter=dim_filter)
                if wide.empty:
                    with col:
                        st.info(f"{title}: pivoted result is empty.")
                    continue
                # Strip leading "_" from per-symbol col names so legend
                # reads "XAU / WTI / TSLA / EUR" not "_XAU / _WTI / ...".
                wide = wide.rename(columns={
                    c: c.lstrip("_") for c in wide.columns if c != "date"
                })
                _raw_a = wide.copy()
                _ord_a = _sort_cols_by_latest(wide)
                _raw_a["Total"] = wide[_ord_a].fillna(0).sum(axis=1)
                with col:
                    sd._chart_dwm_simple(
                        title,
                        source_df=wide,
                        build_fig=lambda df_view: _build_perp_stack(
                            df_view, height=360),
                        raw_df=_raw_a.sort_values("date", ascending=False),
                        raw_key=f"perp_assetclass_{qid}_{key_suffix}",
                        raw_filename=f"solana_perp_{title.lower().replace(' ','_')}_{key_suffix}",
                        col_aggs={c: agg_rule for c in _ord_a},
                    )
                    _perp_legend_expander(wide)

    # Volume by asset class — flow metric, sum across periods.
    _render_asset_class_section(
        metric_prefix="vol",
        section_title="Volume by asset class",
        caption=(
            "Each chart breaks down perp volume on one asset class "
            "across the symbols traded on Solana DEXs."
        ),
        agg_rule="sum",
        key_suffix="vol",
    )
    st.divider()
    # Open Interest by asset class — stock metric, last across periods.
    _render_asset_class_section(
        metric_prefix="oi",
        section_title="Open Interest by asset class",
        caption=(
            "Per-symbol open interest on each asset class. OI is a "
            "stock not a flow → Weekly/Monthly = period-end (last) "
            "value."
        ),
        agg_rule="last",
        key_suffix="oi",
    )


# ── Dispatch ─────────────────────────────────────────────────────────────────
if vertical == "SOL token":
    _render_sol_token()
elif vertical == "Stablecoins":
    _render_stablecoins()
elif vertical == "Lending":
    _render_lending()
elif vertical == "RWA":
    _render_rwa()
elif vertical == "Foreign L1 tokens":
    _render_foreign_l1()
elif vertical == "Prediction Markets":
    _render_prediction_markets()
elif vertical == "Perp DEXs":
    _render_perp_dexs()
