"""rwa.xyz historical seed loader.

The multi-year stacked-area chart on each rwa.xyz category page is
rendered client-side from rwa.xyz's enterprise REST API — the daily
time series isn't exposed through any public endpoint. Users export
the chart's underlying data via the "Download CSV" button on
app.rwa.xyz and drop the CSV into `rwa_seeds/`. This module reads
those CSVs into a wide DataFrame ready for stacked-area plotting.

Same seed model as `allium_seeds/*.csv` and `mc_seed_*.json` — disk
snapshot for immutable past data, refreshed manually when the user
wants to extend the time axis.

A live-scrape companion (snapshot of current aggregates, top-50 asset
list, league tables, etc.) lives in a separate uncommitted change —
this module ships seed-loading only.

Source CSV shape (verified on rwa-xyz-credit-market-caps.csv):
  Timestamp,Date,Measure,<asset_1>,<asset_2>,...,All Others (N items)
- Timestamp: ms-since-epoch (redundant with Date)
- Date:      ISO YYYY-MM-DD
- Measure:   typically 'Bridged Token Value (Dollar)'
- One column per named asset, NaN where the asset hadn't launched
- "All Others (N items)" rolls up the long-tail (beyond top 50)
"""
from __future__ import annotations

import logging
import os
from typing import Optional

import pandas as pd
import streamlit as st


log = logging.getLogger(__name__)

# Directory housing rwa.xyz historical seed CSVs (exported manually
# from the chart UI on app.rwa.xyz/<slug>). Resolved relative to this
# module so the path works both from `streamlit run` (CWD = repo
# root) and any cron script that imports `rwa_xyz` from a different
# CWD.
_SEED_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "rwa_seeds")


@st.cache_data(ttl=14_400, show_spinner=False)
def load_history_seed(slug: str = "credit") -> pd.DataFrame:
    """Load a category-level historical time series from an exported
    rwa.xyz CSV in `rwa_seeds/<slug>_market_caps.csv`.

    Returns a wide DataFrame with a `date` column (datetime) and one
    column per asset (display name verbatim from the CSV header) —
    the shape a stacked-area chart wants. The "All Others (N items)"
    rollup column is preserved as-is. NaN-fill is intact so each
    asset only plots from its inception (rwa.xyz's chart behaves
    the same).

    Returns an empty DataFrame if the seed file is missing or
    unreadable — caller surfaces that with an st.info.

    `slug` ∈ {"credit", "commodities", "treasuries", ...} — drives
    the filename lookup. New slugs auto-resolve once the user drops
    the matching CSV in.
    """
    filename = f"{slug}_market_caps.csv"
    path = os.path.join(_SEED_DIR, filename)
    if not os.path.exists(path):
        log.warning("rwa.xyz seed missing: %s", path)
        return pd.DataFrame()
    try:
        df = pd.read_csv(path)
    except Exception as exc:
        log.warning("rwa.xyz seed read failed for %s: %s", path, exc)
        return pd.DataFrame()
    if "Date" not in df.columns:
        log.warning("rwa.xyz seed %s lacks a Date column", path)
        return pd.DataFrame()
    df = df.copy()
    df["date"] = pd.to_datetime(df["Date"], errors="coerce")
    drop_cols = [c for c in ("Timestamp", "Date", "Measure")
                  if c in df.columns]
    df = df.drop(columns=drop_cols)
    # `date` first so downstream chart helpers can `df["date"]` without
    # worrying about column order.
    cols = ["date"] + [c for c in df.columns if c != "date"]
    df = df[cols].sort_values("date").reset_index(drop=True)
    log.info(
        "rwa.xyz seed loaded: %s — %d rows, %d series, %s → %s",
        filename, len(df), len(df.columns) - 1,
        df["date"].min(), df["date"].max())
    return df


def load_credit_history_seed() -> pd.DataFrame:
    """Convenience wrapper for the Tokenized Credit historical CSV."""
    return load_history_seed("credit")


# ── Per-asset distributed-vs-represented classification ────────────────
# rwa.xyz tags every credit asset with `tokenization_types`:
#   ['Onchain Distributed'] — RWA tokens using the blockchain as a
#      distribution layer. Onchain investors can subscribe, hold, and
#      manage the asset directly through their wallets / custodians.
#   ['Onchain Represented'] — RWA assets that exist primarily off-chain
#      and are merely represented onchain (e.g. Figure HELOC tokens are
#      a wrapper around the off-chain HELOC pool — investors don't
#      transact in the wrapper).
#
# We extracted the mapping for the top-50 credit assets (= the 50
# named columns in `rwa_seeds/credit_market_caps.csv`) directly from
# `app.rwa.xyz/_next/data/<buildId>/credit.json`. The classification
# changes only when rwa.xyz reclassifies an asset OR when a new asset
# enters the top 50 — both are rare. Refresh by re-running the probe
# inside the rwa.xyz scraper module (on the WIP branch) or by hand-
# checking the credit.json output.
#
# Snapshot: 2026-06-11 — 14 distributed, 36 represented, 0 unmatched.
_CREDIT_CLASSIFICATION: dict[str, str] = {
    "ASENA FIF - Single Tranche": "represented",
    "Apollo Diversified Credit Securitize Fund": "distributed",
    "Berkeley Square Emerging Markets NPA Portfolio 1": "represented",
    "Berkeley Square Emerging Markets NPA Portfolio 2": "represented",
    "Berkeley Square Emerging Markets Portfolio 1": "represented",
    "Berkeley Square Emerging Markets Portfolio 2": "represented",
    "Blockstream Mining Note 2": "distributed",
    "EU / LatAm PoS Financing Senior Secured Term Loan": "represented",
    "FIS-FLB1-Aviation2025-1": "represented",
    "FIS-FLB1-CRE2025-1": "represented",
    "FalconX Credit Vault": "distributed",
    "Figure HELOC Token": "represented",
    "First Lien HoldCo PIK Note": "represented",
    "Galaxy CLO 2025-1 - Class B": "distributed",
    "Gov't Contractor Financier Senior Secured Term Notes": "represented",
    "Janus Henderson Anemoy AAA CLO Fund": "distributed",
    "LatAm Fintech Senior Secured Delayed Draw Term Loan": "represented",
    "LatAm Middle-Market Lender Senior Secured Term Loan": "represented",
    "LitFi Firm Post-Settlement Note ": "represented",
    "Midas mF-ONE": "distributed",
    "North America Fintech Senior Secured Term Notes": "represented",
    "North America Legal Receivables Senior Secured Loan": "represented",
    "North America Neobank Senior Secured Term Loan": "represented",
    "North America PoS Lender Senior Secured Term Notes": "represented",
    "North America Real Estate Development Senior Secured Term Loan":
        "represented",
    "North America Rent Financing Platform Senior Secured Term Notes":
        "represented",
    "North America Third Party Online Merchant Senior Secured Term Notes":
        "represented",
    "North American Fintech Senior Secured Delay Draw Term Loan "
    "Corporate Charge Card for SMBs": "represented",
    "OnRe Tokenized Reinsurance": "distributed",
    "PKH Mining Note 2": "distributed",
    "PRIME": "distributed",
    "Project Robinson": "represented",
    "Project Robinson Tranche II": "represented",
    "Project Robinson Tranche III": "represented",
    "REACH TOTAL RETURN FIC FIA - Single Tranche": "represented",
    "REACH TOTAL RETURN FUNDO DE INVESTIMENTO EM AÇÕES - Single Tranche":
        "represented",
    "Reach Referenciado DI FI RF - Single Tranche": "represented",
    "Securitize AAA CLO Tokenized Fund, Ltd": "distributed",
    "Singapore Fintech Convertible Notes": "represented",
    "South American Consumer PoS Lender Senior Secured Term Loan":
        "represented",
    "Stable Return SP": "distributed",
    "Syrup USDC": "distributed",
    "Syrup USDT": "distributed",
    "US Buy Now, Pay Later Finance Provider Senior Secured Term Notes":
        "represented",
    "VERT's 12th Debenture - Single Tranche": "represented",
    "VERT's 94th CRA – 1st tranche": "represented",
    "VERT's 94th CRA – 2nd tranche": "represented",
    "VERT's FIDC Byx Mozart - Senior": "represented",
    "VuMe Bond 2030": "represented",
    "sUSDat": "distributed",
}


def classify_credit_asset(name: str) -> Optional[str]:
    """Return 'distributed' / 'represented' / None for an asset name
    from the credit seed CSV. None means the name isn't in our cached
    classification — either it's the 'All Others' rollup column, or
    it's a new top-50 entrant we haven't refreshed for yet."""
    if not isinstance(name, str):
        return None
    return _CREDIT_CLASSIFICATION.get(name)


# Default "All Others" apportionment between the two buckets, used when
# the long-tail rollup is split across the two charts. Derived from
# rwa.xyz's snapshot aggregates (Distributed $5.36B / Represented
# $23.28B = ~18.7% / 81.3%) — same proportion applied historically as
# a constant since we lack per-day platform-level splits in the seed.
# Refresh together with _CREDIT_CLASSIFICATION when reclassification
# occurs.
ALL_OTHERS_DIST_SHARE = 0.187
ALL_OTHERS_REPR_SHARE = 0.813
