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
              "Foreign L1 tokens", "Prediction Markets"]

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

    st.subheader("Daily Price (USD)")
    st.caption(f"Source: Birdeye OHLCV V3, daily close · "
               f"{len(ohlcv)} days from "
               f"{ohlcv['date'].min().date()} → {ohlcv['date'].max().date()}")
    fig_p = _go.Figure()
    fig_p.add_trace(_go.Scatter(
        x=ohlcv["date"], y=ohlcv["close"], name="SOL",
        mode="lines", line=dict(color="#9945FF", width=1.5),
        hovertemplate="%{y:$,.2f}<extra>SOL</extra>",
    ))
    fig_p.update_layout(
        height=380, hovermode="x unified",
        margin=dict(t=10, b=10, l=10, r=10), showlegend=False,
        yaxis=dict(tickprefix="$", tickformat=".2f", showgrid=True,
                   rangemode="tozero"),
    )
    sd._chart(fig_p, use_container_width=True,
              raw_df=ohlcv[["date", "close", "v_usd"]],
              raw_key="sol_price", raw_filename="sol_daily_ohlcv",
              raw_fmt={"close": "${:,.2f}", "v_usd": "${:,.0f}"})

    st.subheader("Daily Trading Volume (USD)")
    st.caption(
        "Source: Birdeye OHLCV V3 v_usd · all venues aggregated · outlier "
        "days suppressed (>50× global median) — catches Birdeye v_usd "
        "glitches like 2023-01-03 reporting \\$41T (50,466× median) and "
        "the Apr-2026 \\$89-440B cluster; preserves the Jan 18-20 2025 "
        "TRUMP-launch burst (~32× median, real)."
    )
    # Reuse the puller's static outlier clipper but disable the
    # min_retained guard for SOL. The default (0.5) protects sparse-but-
    # real distributions (e.g. USDe stablecoin with low median + occasional
    # legit burst days). SOL has the OPPOSITE problem: one absurd Birdeye
    # glitch day (2023-01-03 reported \$41T, 50,466× median) is so massive
    # it singlehandedly accounts for >97% of total cumulative v_usd, so
    # clipping it would leave <50% retained → guard would trip and
    # preserve the glitch. Forcing min_retained=0 lets the clip do its job.
    # factor=50 (vs the 25 we use on stablecoin/commodity charts) is the
    # right cutoff for SOL: catches the 2023-01-03 glitch (\$41T = 50,466×
    # median) + the suspect April-2026 cluster (\$89B-\$440B, 100-536×),
    # but preserves the legit Jan 18-20 2025 TRUMP-launch burst (\$24B-
    # \$33B = 29-40× median). On a token with this much real daily volume
    # variance, the tighter 25× threshold would over-clip.
    v_clipped = sd.TokenGroupMetricsPuller._clip_outliers(
        ohlcv["v_usd"], factor=50.0, min_retained=0.0)
    fig_v = _go.Figure()
    fig_v.add_trace(_go.Bar(
        x=ohlcv["date"], y=v_clipped, name="Volume",
        marker_color="#14F195", opacity=0.85,
        hovertemplate="%{y:$,.0f}<extra>v_usd</extra>",
    ))
    fig_v.update_layout(
        height=320, hovermode="x unified",
        margin=dict(t=10, b=10, l=10, r=10), showlegend=False,
        yaxis=dict(tickprefix="$", tickformat="~s", showgrid=True,
                   rangemode="tozero"),
    )
    # Raw data shows BOTH the clipped (visible) and original v_usd so the
    # downloader can see which days got suppressed by the outlier filter.
    _vol_raw = ohlcv[["date", "v_usd"]].copy()
    _vol_raw["v_usd_clipped"] = v_clipped
    sd._chart(fig_v, use_container_width=True,
              raw_df=_vol_raw, raw_key="sol_vol",
              raw_filename="sol_daily_volume",
              raw_fmt={"v_usd": "${:,.0f}", "v_usd_clipped": "${:,.0f}"})

    # ── Holder Count chart — Birdeye /token/v1/holder/chart ───────────────
    st.subheader("Holder Count")
    st.caption(
        "Source: Birdeye `/token/v1/holder/chart` daily, paginated. "
        "Net-change percentages reflect day-over-day deltas in unique "
        "holder addresses."
    )
    with st.spinner("Loading SOL holder history…"):
        holders_df = _fetch_sol_holders_history()
    if holders_df.empty:
        st.info(
            f"Birdeye holder-chart endpoint returned nothing — current "
            f"snapshot from /defi/token_overview: **{holders:,}**."
        )
    else:
        fig_h = _go.Figure()
        fig_h.add_trace(_go.Scatter(
            x=holders_df["date"], y=holders_df["holder"], name="Holders",
            mode="lines", line=dict(color="#7DCE82", width=1.5),
            hovertemplate="Holders: %{y:,.0f}<extra></extra>",
        ))
        fig_h.update_layout(
            height=340, hovermode="x unified",
            margin=dict(t=10, b=10, l=10, r=10), showlegend=False,
            yaxis=dict(showgrid=True, rangemode="tozero", tickformat=","),
        )
        # fmt_mode="count" — holder count is an integer, not a USD value,
        # so the y-axis ticks should read '6.8M' not '$6.8M'.
        sd._chart(fig_h, use_container_width=True, fmt_mode="count",
                  raw_df=holders_df, raw_key="sol_holders",
                  raw_filename="sol_holder_history",
                  raw_fmt={"holder": "{:,}"})

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
        st.subheader(label)
        seed = _load_sol_seed(filename)
        if seed.empty:
            st.info(
                f"No historical {label.lower()} series yet. Drop a seed file "
                f"named `{filename}` at the repo root with the same shape as "
                "the existing `mc_seed_*.json` files — "
                "`{\"payload\": {\"mc\": [values], \"t\": [unix_seconds]}}` — "
                f"and it'll render here. Current snapshot from Birdeye: "
                f"**{snapshot_str}**."
            )
            continue
        fig = _go.Figure()
        fig.add_trace(_go.Scatter(
            x=seed["date"], y=seed["value"], name=label,
            mode="lines", line=dict(color=color, width=1.5),
            hovertemplate=f"{label}: %{{y:,.0f}}<extra></extra>",
        ))
        # Tight y-axis range so the line uses the full vertical space instead
        # of being a near-flat trace bunched up against the chart top.
        # Floor: 95% of min — for SOL supply that's 537M × 0.95 ≈ 510M, so
        # the chart starts around 500M instead of 0 and the supply growth
        # actually reads. Ceiling: 105% of max for a bit of headroom above
        # the latest value.
        y_min, y_max = float(seed["value"].min()), float(seed["value"].max())
        y_range = [y_min * 0.95, y_max * 1.05] if y_max > 0 else None
        fig.update_layout(
            height=340, hovermode="x unified",
            margin=dict(t=10, b=10, l=10, r=10), showlegend=False,
            yaxis=dict(showgrid=True, range=y_range),
        )
        # Raw data uses the per-chart label (Market Cap / Supply) to
        # disambiguate the two loop iterations' button keys + CSV filenames.
        _safe_label = label.lower().replace(" ", "_")
        _raw = seed.rename(columns={"value": _safe_label})
        sd._chart(fig, use_container_width=True, fmt_mode=fmt_mode,
                  raw_df=_raw, raw_key=f"sol_{_safe_label}",
                  raw_filename=f"sol_{_safe_label}",
                  raw_fmt={_safe_label: "${:,.0f}" if fmt_mode == "currency" else "{:,.2f}"})


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
    fig = _go.Figure()
    cols = [f"{metric}_{s}" for s, _ in protocols] + [f"{metric}_others"]
    labels = [n for _, n in protocols] + ["Others"]
    totals = wide[cols].ffill().fillna(0).sum(axis=1)
    for i, (col, label) in enumerate(zip(cols, labels)):
        y = wide[col].ffill().fillna(0.0)
        fig.add_trace(_go.Scatter(
            x=wide["date"], y=y, name=label,
            mode="lines", line=dict(width=0.8, color=palette[i % len(palette)]),
            stackgroup=metric, hoverinfo="x+y+name",
            customdata=y.map(sd._fmt_usd),
            hovertemplate=f"{label}: %{{customdata}}<extra></extra>",
        ))
    fig.add_trace(_go.Scatter(
        x=wide["date"], y=totals, name="Total",
        mode="lines", line=dict(width=0, color="rgba(0,0,0,0)"),
        showlegend=False, stackgroup=None,
        customdata=totals.map(sd._fmt_usd),
        hovertemplate="<b>Total: %{customdata}</b><extra></extra>",
    ))
    y_max = float(totals.max() or 0)
    # Legend below the chart, NOT above — top placement collides with the
    # rangeselector buttons (1M/3M/6M/YTD/1Y/All) when the legend wraps
    # to a second row, which happens reliably in the narrow 2-col layout
    # with 11 entries (10 protocols + Others). Bottom placement scales to
    # any number of items without colliding. Bottom margin bumped to fit
    # the legend rows above the rangeslider strip.
    fig.update_layout(
        height=460, hovermode="x unified",
        margin=dict(t=20, b=90, l=10, r=10),
        legend=dict(orientation="h", yanchor="top", y=-0.22,
                    xanchor="center", x=0.5),
        yaxis=dict(showgrid=True, rangemode="tozero",
                   range=[0, y_max * 1.10] if y_max > 0 else None),
    )
    # Raw data: per-protocol (or per-asset) metric values for this stack +
    # the computed Total. Caller supplies a unique prefix so the button
    # key + downloaded CSV name don't collide with sibling stacks.
    raw_kwargs = {}
    if raw_key_prefix:
        _raw = wide[["date"] + cols].copy()
        _raw["total"] = totals.values
        raw_kwargs = {
            "raw_df": _raw,
            "raw_key": f"{raw_key_prefix}_{metric}",
            "raw_filename": f"{raw_key_prefix}_{metric}",
        }
    if chart_title:
        raw_kwargs["chart_title"] = chart_title
    sd._chart(fig, use_container_width=True, **raw_kwargs)


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

    col_left, col_right = st.columns(2, gap="medium")

    # ── Left: stacked-area MC ───────────────────────────────────────────
    with col_left:
        fig_mc = _go.Figure()
        totals_mc = wide[mc_cols].ffill().fillna(0).sum(axis=1)
        for sym in frames:
            color = _FOREIGN_L1_COLORS.get(sym, "#888888")
            y = wide[f"mc_{sym}"].ffill().fillna(0.0)
            fig_mc.add_trace(_go.Scatter(
                x=wide["date"], y=y, name=sym,
                mode="lines", line=dict(width=0.8, color=color),
                stackgroup="mc", hoverinfo="x+y+name",
                customdata=y.map(sd._fmt_usd),
                hovertemplate=f"{sym}: %{{customdata}}<extra></extra>",
            ))
        # Invisible Total trace → 'Total: $X.XB' line in unified hover.
        fig_mc.add_trace(_go.Scatter(
            x=wide["date"], y=totals_mc, name="Total",
            mode="lines", line=dict(width=0, color="rgba(0,0,0,0)"),
            showlegend=False, stackgroup=None,
            customdata=totals_mc.map(sd._fmt_usd),
            hovertemplate="<b>Total: %{customdata}</b><extra></extra>",
        ))
        y_max_mc = float(totals_mc.max() or 0)
        fig_mc.update_layout(
            height=360, hovermode="x unified",
            margin=dict(t=10, b=10, l=10, r=10),
            legend=dict(orientation="h", yanchor="bottom", y=1.02,
                        xanchor="right", x=1),
            yaxis=dict(showgrid=True, rangemode="tozero",
                       range=[0, y_max_mc * 1.10] if y_max_mc > 0 else None),
        )
        # Raw data: per-token MC values + computed Total.
        _mc_raw = wide[["date"] + mc_cols].copy()
        _mc_raw["total"] = totals_mc.values
        _safe_group = group_label.lower().replace(" ", "_")
        sd._chart(fig_mc, use_container_width=True,
                  chart_title=f"{group_label} — Aggregated Market Cap",
                  raw_df=_mc_raw, raw_key=f"fl1_mc_{_safe_group}",
                  raw_filename=f"foreign_l1_{_safe_group}_market_cap")

    # ── Right: stacked-bar daily volume ─────────────────────────────────
    with col_right:
        fig_v = _go.Figure()
        totals_v = wide[vol_cols].fillna(0).sum(axis=1).replace(0, float("nan"))
        for sym in frames:
            color = _FOREIGN_L1_COLORS.get(sym, "#888888")
            # Replace 0s with NaN so the bar doesn't render — Plotly draws
            # a 0-height tick mark otherwise that visually fills the day.
            y = wide[f"vol_{sym}"].replace(0, float("nan"))
            fig_v.add_trace(_go.Bar(
                x=wide["date"], y=y, name=sym,
                marker_color=color, opacity=0.8,
                customdata=y.map(sd._fmt_usd),
                hovertemplate=f"{sym}: %{{customdata}}<extra></extra>",
            ))
        fig_v.add_trace(_go.Scatter(
            x=wide["date"], y=totals_v, name="Total",
            mode="lines", line=dict(width=0, color="rgba(0,0,0,0)"),
            showlegend=False,
            customdata=totals_v.map(sd._fmt_usd),
            hovertemplate="<b>Total: %{customdata}</b><extra></extra>",
        ))
        y_max_v = float(totals_v.max() or 0)
        fig_v.update_layout(
            height=360, hovermode="x unified", barmode="stack",
            margin=dict(t=10, b=10, l=10, r=10),
            legend=dict(orientation="h", yanchor="bottom", y=1.02,
                        xanchor="right", x=1),
            yaxis=dict(showgrid=True, rangemode="tozero",
                       range=[0, y_max_v * 1.10] if y_max_v > 0 else None),
        )
        _vol_raw = wide[["date"] + vol_cols].copy()
        _vol_raw["total"] = wide[vol_cols].fillna(0).sum(axis=1).values
        sd._chart(fig_v, use_container_width=True,
                  chart_title=f"{group_label} — Aggregated Daily Volume",
                  raw_df=_vol_raw, raw_key=f"fl1_vol_{_safe_group}",
                  raw_filename=f"foreign_l1_{_safe_group}_volume")


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
            st.subheader("All Tokenized Stocks — Volume by Project")
            combined_df = sd._combined_stocks_df(stocks_pullers)
            if combined_df is None:
                st.info("Waiting for first pull…")
            else:
                labels = [p.GROUP_LABEL for p in stocks_pullers]
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

            # Per-group volume — 2 per row.
            for row_start in range(0, len(stocks_pullers), 2):
                col_a, col_b = st.columns(2, gap="medium")
                for col, p in zip(
                    (col_a, col_b),
                    stocks_pullers[row_start: row_start + 2],
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

                st.subheader(f"{p.GROUP_LABEL} — Market Cap by Token")
                st.caption(
                    "Solana-only market cap per token, stacked. Sourced from "
                    "DefiLlama (XAUM) plus Solscan-seeded history for GOLD / "
                    "VNXAU / PAXG-bridge / XAUt0 and same-day Birdeye Token "
                    "Overview snapshots — total band height = total tokenized "
                    "gold MC on Solana."
                )
                p.render_market_cap_chain(chain="Solana", stacked=True)

    # ── Treasuries & MMFs ───────────────────────────────────────────────────
    with tab_treasuries:
        if not treasury_pullers:
            st.info("No treasury pullers registered.")
        else:
            for p in treasury_pullers:
                st.subheader(f"{p.GROUP_LABEL} — Market Cap (Solana)")
                st.caption(
                    "Per-token market cap on Solana, from DefiLlama's free "
                    "API (daily history). These tokens have no on-chain "
                    "trading activity tracked; only market cap is shown."
                )
                p.render_market_cap_chain(chain="Solana", stacked=True)


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

# Phantom prediction-market data — only TVL is publicly queryable.
_DUNE_QUERY_PHANTOM_TVL      = 6386183


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
    """On-chain Solana prediction-market activity. Per-platform sections
    fed by Dune queries; each section renders a daily + cumulative chart
    pair side-by-side using sd._chart's standard time-controls + 📋
    raw-data button + B/M/K y-axis format.

    Current platforms:
      • Jupiter — Notional volume (daily + cumulative)
      • Phantom — TVL (daily delta + cumulative delta)
    """
    st.markdown("## Prediction Markets")
    st.caption(
        "On-chain Solana prediction-market activity sourced from Dune "
        "Analytics. Each query is cached 4h to match the cron cadence."
    )
    st.divider()

    _render_jupiter_prediction_section()
    st.divider()
    _render_phantom_prediction_section()


_JUP_COLOR = "#9945FF"                        # Jupiter brand purple
_JUP_FILL  = "rgba(153, 69, 255, 0.25)"


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
    with col_left:
        fig_d = _go.Figure()
        fig_d.add_trace(_go.Bar(
            x=df["day"], y=df[daily_col], name=daily_title,
            marker=dict(color=color),
            customdata=df[daily_col].map(_hover_fmt),
            hovertemplate="%{x|%Y-%m-%d}: %{customdata}<extra></extra>",
        ))
        y_max_d = float(df[daily_col].max() or 0)
        fig_d.update_layout(
            height=400, hovermode="x unified",
            margin=dict(t=10, b=10, l=10, r=10), showlegend=False,
            yaxis=dict(showgrid=True, rangemode="tozero",
                       range=[0, y_max_d * 1.10] if y_max_d > 0 else None),
        )
        sd._chart(fig_d, use_container_width=True, fmt_mode=fmt_mode,
                  chart_title=daily_title,
                  raw_df=df[["day", daily_col]].copy(),
                  raw_key=f"{raw_key_prefix}_daily",
                  raw_fmt={daily_col: _raw_fmt_str},
                  raw_filename=f"{raw_key_prefix}_daily")

    with col_right:
        fig_c = _go.Figure()
        fig_c.add_trace(_go.Scatter(
            x=df["day"], y=df[cum_col], name=cum_title,
            mode="lines", line=dict(color=color, width=1.5),
            fill="tozeroy", fillcolor=fill,
            customdata=df[cum_col].map(_hover_fmt),
            hovertemplate="%{x|%Y-%m-%d}: %{customdata}<extra></extra>",
        ))
        y_max_c = float(df[cum_col].max() or 0)
        fig_c.update_layout(
            height=400, hovermode="x unified",
            margin=dict(t=10, b=10, l=10, r=10), showlegend=False,
            yaxis=dict(showgrid=True, rangemode="tozero",
                       range=[0, y_max_c * 1.10] if y_max_c > 0 else None),
        )
        sd._chart(fig_c, use_container_width=True, fmt_mode=fmt_mode,
                  chart_title=cum_title,
                  raw_df=df[["day", cum_col]].copy(),
                  raw_key=f"{raw_key_prefix}_cum",
                  raw_fmt={cum_col: _raw_fmt_str},
                  raw_filename=f"{raw_key_prefix}_cum")


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
    """Jupiter prediction-market activity — 6 metric chart pairs sourced
    from the public /datadashboards/jupiter-prediction-markets dashboard.

    Order: Notional Volume → Volume → Fees → Transactions → TVL → Users
    (money first, activity next, liquidity last). Each metric renders as
    a daily-bar + cumulative-area pair via _render_dune_metric_pair so
    the 6 sections stay structurally identical and easy to scan."""
    st.subheader("Jupiter Prediction Markets")
    st.caption(
        "Source: [datadashboards/jupiter-prediction-markets]"
        "(https://dune.com/datadashboards/jupiter-prediction-markets) — "
        "6 Dune queries (Notional Volume · Volume · Fees · Transactions · "
        "TVL · Unique Users)."
    )

    # Fetch all 6 metrics up-front. Each call hits the 4h cache so this is
    # cheap on warm-cache reruns. Empty frames fall through to per-section
    # placeholders below so a single broken query doesn't black-hole the
    # whole page.
    df_notional = _fetch_jupiter_metric(_DUNE_QUERY_JUPITER_NOTIONAL,
                                        "Notional Volume",
                                        "Cumulative Notional Volume")
    df_volume   = _fetch_jupiter_metric(_DUNE_QUERY_JUPITER_VOLUME,
                                        "Volume", "Cumulative Volume")
    df_fees     = _fetch_jupiter_metric(_DUNE_QUERY_JUPITER_FEES,
                                        "Fees", "Cumulative Fees")
    df_tx       = _fetch_jupiter_metric(_DUNE_QUERY_JUPITER_TX,
                                        "Transctions",   # sic — Dune typo
                                        "Cumulative Transactions")
    df_tvl      = _fetch_jupiter_metric(_DUNE_QUERY_JUPITER_TVL,
                                        "TVLDelta", "TVL_CumulativeDelta",
                                        resample_to_daily=True)
    df_users    = _fetch_jupiter_metric(_DUNE_QUERY_JUPITER_USERS,
                                        "New", "Cumulative Unique Users")

    # No data at all? Show one placeholder and bail.
    if all(d.empty for d in [df_notional, df_volume, df_fees, df_tx,
                             df_tvl, df_users]):
        st.info(
            "No Dune data available. Either the `DUNE_API_KEY` is missing "
            "(set it in `.env` locally or Streamlit Cloud secrets) or the "
            "queries have no cached results yet."
        )
        return

    # ── Headline KPIs: 6 latest cumulative counters in 2 rows of 3 ──────
    def _latest(_df, col):
        return float(_df[col].iloc[-1]) if not _df.empty else None

    def _fmt_metric(v, mode="currency"):
        if v is None:    return "—"
        if mode == "count": return f"{int(v):,}"
        return sd._fmt_usd(v)

    asof_candidates = [d["day"].iloc[-1] for d in
                       [df_notional, df_volume, df_fees, df_tx, df_tvl, df_users]
                       if not d.empty]
    asof = max(asof_candidates).strftime("%Y-%m-%d") if asof_candidates else "?"

    r1c1, r1c2, r1c3 = st.columns(3)
    r1c1.metric("Cumulative Notional",
                _fmt_metric(_latest(df_notional, "cumulative")))
    r1c2.metric("Cumulative Volume",
                _fmt_metric(_latest(df_volume, "cumulative")))
    r1c3.metric("Cumulative Fees",
                _fmt_metric(_latest(df_fees, "cumulative")))
    r2c1, r2c2, r2c3 = st.columns(3)
    r2c1.metric("Cumulative Transactions",
                _fmt_metric(_latest(df_tx, "cumulative"), mode="count"))
    r2c2.metric("Current TVL",
                _fmt_metric(_latest(df_tvl, "cumulative")))
    r2c3.metric("Cumulative Unique Users",
                _fmt_metric(_latest(df_users, "cumulative"), mode="count"))
    st.caption(f"As of {asof}.")
    st.write("")    # vertical breathing room before the chart grid

    # ── 6 metric chart pairs (12 charts total in 6 rows of 2) ───────────
    _metric_specs = [
        # (df, daily_title, cum_title, raw_key_prefix, fmt_mode)
        (df_notional,
         "Jupiter — Daily Notional Volume",
         "Jupiter — Cumulative Notional Volume",
         "pm_jup_notional",  "currency"),
        (df_volume,
         "Jupiter — Daily Volume",
         "Jupiter — Cumulative Volume",
         "pm_jup_volume",    "currency"),
        (df_fees,
         "Jupiter — Daily Fees",
         "Jupiter — Cumulative Fees",
         "pm_jup_fees",      "currency"),
        (df_tx,
         "Jupiter — Daily Transactions",
         "Jupiter — Cumulative Transactions",
         "pm_jup_tx",        "count"),
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


def _render_phantom_prediction_section() -> None:
    """Phantom prediction-market TVL (Dune query 6386183 — TVL cumulative
    delta). The companion queries on the source dashboard (Volume/Fees/Tx
    via 6386520, Users via 6453064) are private at source and return
    'Query not found' via API — surfaced as a footer note."""
    st.subheader("Phantom Prediction Markets")
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

    # ── Left: daily TVL delta (bar; can be negative) ───────────────────
    with col_left:
        fig_d = _go.Figure()
        fig_d.add_trace(_go.Bar(
            x=df["day"], y=df["tvl_delta"], name="Daily Δ",
            marker=dict(color=bar_colors),
            customdata=df["tvl_delta"].map(sd._fmt_usd),
            hovertemplate="%{x|%Y-%m-%d}: %{customdata}<extra></extra>",
        ))
        # Symmetric or zero-anchored range — y can be negative so don't
        # force rangemode='tozero'. Pad 10% past whichever extreme is
        # bigger in absolute terms so the bars don't hit the chart edge.
        _y_lo = float(df["tvl_delta"].min())
        _y_hi = float(df["tvl_delta"].max())
        _pad  = max(abs(_y_lo), abs(_y_hi)) * 0.10
        fig_d.update_layout(
            height=420, hovermode="x unified",
            margin=dict(t=10, b=10, l=10, r=10), showlegend=False,
            yaxis=dict(showgrid=True, zeroline=True,
                       zerolinecolor="rgba(255,255,255,0.3)",
                       range=[_y_lo - _pad, _y_hi + _pad]),
        )
        sd._chart(fig_d, use_container_width=True,
                  chart_title="Phantom — Daily TVL Delta",
                  raw_df=df[["day", "tvl_delta"]].copy(),
                  raw_key="pm_phantom_tvl_daily",
                  raw_filename="phantom_prediction_market_tvl_daily_delta")

    # ── Right: cumulative TVL (area) ───────────────────────────────────
    with col_right:
        fig_c = _go.Figure()
        fig_c.add_trace(_go.Scatter(
            x=df["day"], y=df["tvl_cum"], name="Cumulative TVL",
            mode="lines", line=dict(color=_PHANTOM_COLOR, width=1.5),
            fill="tozeroy", fillcolor=_PHANTOM_FILL,
            customdata=df["tvl_cum"].map(sd._fmt_usd),
            hovertemplate="%{x|%Y-%m-%d}: %{customdata}<extra></extra>",
        ))
        y_max_c = float(df["tvl_cum"].max() or 0)
        fig_c.update_layout(
            height=420, hovermode="x unified",
            margin=dict(t=10, b=10, l=10, r=10), showlegend=False,
            yaxis=dict(showgrid=True, rangemode="tozero",
                       range=[0, y_max_c * 1.10] if y_max_c > 0 else None),
        )
        sd._chart(fig_c, use_container_width=True,
                  chart_title="Phantom — Cumulative TVL",
                  raw_df=df[["day", "tvl_cum"]].copy(),
                  raw_key="pm_phantom_tvl_cumulative",
                  raw_filename="phantom_prediction_market_tvl_cumulative")

    st.caption(
        ":information_source: The source dashboard also tracks Volume / "
        "Fees / Transactions (query 6386520) and Unique Users (query "
        "6453064) but those queries are private — Dune's API returns "
        "`Query not found`. To surface them here either ask the dashboard "
        "owner to make them public, or fork them on Dune to get a public "
        "query ID under your account."
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
