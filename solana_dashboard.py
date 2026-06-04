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
if (st.session_state.get("solana_dash_pullers_version") != sd._PULLERS_VERSION
        or "solana_dash_pullers" not in st.session_state):
    st.session_state["solana_dash_pullers"] = sd.init_pullers(sd.settings, sd.cache_db)
    st.session_state["solana_dash_pullers_version"] = sd._PULLERS_VERSION

pullers = st.session_state["solana_dash_pullers"]
stocks_pullers     = [p for p in pullers if getattr(p, "GROUP", "") == "tokenized_stocks"]
commodity_pullers  = [p for p in pullers if getattr(p, "GROUP", "") == "tokenized_commodities"]
stablecoin_pullers = [p for p in pullers if getattr(p, "GROUP", "") == "stablecoins"]
treasury_pullers   = [p for p in pullers if getattr(p, "GROUP", "") == "treasuries"]

# ── Sidebar — vertical navigation ─────────────────────────────────────────────
# Only RWA is wired up today. Other verticals from the user's spec are
# documented in the module docstring; they'll appear here as each gets
# its data source nailed down.
_VERTICALS = ["SOL token", "RWA"]

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
        "DEX · Stablecoins · Payments · Foreign L1 ·  \n"
        "Lending · Perps · Prediction"
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
    sd._chart(fig_p, use_container_width=True)

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
    sd._chart(fig_v, use_container_width=True)

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
        sd._chart(fig_h, use_container_width=True)

    # ── Optional seed-backed charts (MC, supply) ──────────────────────────
    for label, filename, color, fmt in [
        ("Market Cap",   "mc_seed_sol_mc.json",      "#FF8C42", "${:.2f}B"),
        ("Circulating Supply",
                         "mc_seed_sol_supply.json",  "#5BC0EB", "{:,.0f} SOL"),
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
                f"**{'$' + str(round(mc/1e9, 2)) + 'B' if 'Market' in label else ('{:,.0f}'.format(supply) + ' SOL' if 'Supply' in label else '{:,}'.format(holders))}**."
            )
            continue
        fig = _go.Figure()
        fig.add_trace(_go.Scatter(
            x=seed["date"], y=seed["value"], name=label,
            mode="lines", line=dict(color=color, width=1.5),
            hovertemplate=f"{label}: %{{y:,.0f}}<extra></extra>",
        ))
        fig.update_layout(
            height=340, hovermode="x unified",
            margin=dict(t=10, b=10, l=10, r=10), showlegend=False,
            yaxis=dict(showgrid=True, rangemode="tozero",
                       tickprefix=("$" if "Market" in label else ""),
                       tickformat="~s"),
        )
        sd._chart(fig, use_container_width=True)


# ── RWA vertical — Solana-only RWA view (4 sub-tabs) ──────────────────────────
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

    tab_stocks, tab_commodities, tab_stablecoins, tab_treasuries = st.tabs(
        ["Tokenized stocks", "Tokenized commodities",
         "Stablecoins", "Treasuries & MMFs"])

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

    # ── Stablecoins ─────────────────────────────────────────────────────────
    with tab_stablecoins:
        if not stablecoin_pullers:
            st.info("No stablecoin pullers registered.")
        else:
            for p in stablecoin_pullers:
                st.subheader(f"{p.GROUP_LABEL} — Market Cap (Solana)")
                st.caption(
                    "Solana-only market cap per token, stacked. Sourced from "
                    "DefiLlama (free API, daily history) plus the Solscan-"
                    "derived seed JSONs and same-day Birdeye Token Overview "
                    "snapshots."
                )
                p.render_market_cap_chain(chain="Solana", stacked=True)

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


# ── Dispatch ─────────────────────────────────────────────────────────────────
if vertical == "SOL token":
    _render_sol_token()
elif vertical == "RWA":
    _render_rwa()
