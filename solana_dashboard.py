"""Solana Dashboard — single-chain breakout app.

Lives in the same repo as `stocks_dashboard.py` and shares all its
infrastructure (Settings, CacheDB, pullers, helpers) via library import.
The original `stocks_dashboard.py` wraps its UI rendering in a
`if __name__ == "__main__":` guard so importing it here has no UI
side effects — just exposes the classes and helpers.

Deploy as a second Streamlit Cloud app pointing to this file. Reuses
the same secrets (BIRDEYE_API_KEY / COINGECKO_API_KEY / DATABASE_URL)
and writes to the same Postgres cache as the main dashboard.

Verticals (sidebar):
  • RWA — Solana-only view of the four RWA groups we already track:
          tokenized stocks, tokenized commodities, stablecoins,
          treasuries & MMFs. Implemented today.
  • Other verticals (SOL token / DEX / Stablecoins / Payments / Foreign
    L1 / Lending / Perps / Prediction) — listed in the user's spec,
    pending data-source research + new puller implementations.
    Added to the sidebar incrementally as each one comes online.
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
_VERTICALS = ["RWA"]

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
        "SOL token · DEX · Stablecoins · Payments ·  \n"
        "Foreign L1 · Lending · Perps · Prediction"
    )

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
if vertical == "RWA":
    _render_rwa()
