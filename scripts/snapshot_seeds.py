"""One-shot seed snapshot script.

Hits every live API behind the disk-cache fallback and writes the
result to the corresponding seed dir. Run when you want to prime or
refresh the on-disk snapshots:

  PYTHONPATH=. python3 scripts/snapshot_seeds.py

This is also what the GHA cron should invoke after a successful
puller run if/when we wire seed auto-refresh into CI. Until then,
the seeds get refreshed any time the live dashboard fetches succeed
(write happens inside the @st.cache_data function on the first hit
of every 1h window).

Idempotent — re-runs overwrite existing seed files with whatever the
API currently returns. Skips writes on empty payloads so a transient
API failure during the snapshot run can't blank a good prior seed.
"""
from __future__ import annotations

import logging
import os
import sys

# Make the repo root importable when this script is run from anywhere.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s — %(message)s")

# Force-load .env so PAYMENTSCAN_API_KEY (only needed for Paymentscan
# private endpoints) is available even outside the streamlit context.
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


# In a headless CLI run, Streamlit logs a "No runtime found" warning
# on every @st.cache_data call but otherwise works fine — the cache
# is per-session and an empty session has nothing to clear. Don't
# bother trying to clear caches here; just call the fetchers and let
# them write the seeds.


def snapshot_paymentscan():
    """Cover the public Paymentscan endpoints. `currencies/networks/
    infra` need full.read scope; skip silently if 401."""
    import paymentscan as ps
    targets = [
        # Endpoint, period — these match every chart's current fetch.
        ("chains",     "daily"),
        ("projects",   "daily"),
        ("currencies", "daily"),
        ("networks",   "daily"),
        ("infra",      "daily"),
    ]
    for endpoint, period in targets:
        df, err = ps.fetch(endpoint, period)
        flag = "ok" if err is None else f"err({err[:60]})"
        print(f"  paymentscan/{endpoint}/{period}: {len(df)} rows [{flag}]")


def snapshot_defillama():
    """Cover the DefiLlama endpoints that have live fetchers in either
    dashboard. The `/protocol/{slug}` set is curated from the slug
    constants used by stocks_dashboard.py + the Solana lending
    protocols on solana_dashboard.py.

    /protocols (the full catalog) is hit once. After that, the slugs
    we want individual snapshots for are derived from the catalog
    itself (top-N lending protocols on Solana) so this script keeps
    working as the catalog evolves."""
    import defillama as dl

    # Full catalog first — also primes the slug list used below.
    protocols = dl.fetch_protocols()
    print(f"  defillama/protocols: {len(protocols)} entries")

    # Lending + CDP slugs on Solana — same filter the solana dashboard
    # uses to build its lending catalog.
    LENDING_CATS = {"Lending", "CDP", "RWA Lending",
                    "Cross Chain Lending", "Uncollateralized Lending"}
    lending_slugs = []
    for p in protocols or []:
        if (p.get("category") in LENDING_CATS
                and "Solana" in (p.get("chains") or [])):
            slug = p.get("slug")
            if slug:
                lending_slugs.append(slug)

    # Top-50 by gross supply — keeps the snapshot scope reasonable.
    def _gross(p):
        ct = p.get("chainTvls") or {}
        return float(ct.get("Solana", 0) or 0) + \
               float(ct.get("Solana-borrowed", 0) or 0)
    by_slug = {p["slug"]: p for p in (protocols or []) if p.get("slug")}
    lending_slugs = sorted(
        lending_slugs, key=lambda s: -_gross(by_slug[s]))[:50]

    # Known RWA slugs from stocks_dashboard.py — extracted by grepping
    # the call sites. Refresh manually if new ones get added.
    rwa_slugs = [
        "ondo-finance", "securitize", "blackrock-buidl",
        "franklin-templeton", "wisdomtree", "hashnote", "matrixdock",
        "midas", "ondo-usdy", "ondo-ousg", "openeden",
        "circle", "kyc-fund", "maple",
    ]

    for slug in sorted(set(lending_slugs + rwa_slugs)):
        d = dl.fetch_protocol(slug)
        ch_count = len((d or {}).get("chainTvls") or {})
        print(f"  defillama/protocol/{slug}: "
              f"{ch_count} chains in chainTvls")

    # Stablecoin per-chain charts — list mirrors _ALL_CHAIN_STABLE_TOP.
    chains = ["Ethereum", "Solana", "Hyperliquid L1", "BSC", "Base",
              "Arbitrum", "Polygon", "Tron", "Avalanche", "Aptos"]
    for chain in chains:
        df = dl.fetch_stablecoin_chain_chart(chain)
        print(f"  defillama/stablecoincharts/{chain}: {len(df)} rows")

    # Stablecoin per-coin payloads — known IDs from
    # stocks_dashboard.py's _STABLECOIN_DEFILLAMA config. The set is
    # small (~15 coins); harvest by id rather than slug here because
    # /stablecoin/{id} is keyed by integer.
    KNOWN_STABLE_IDS = [
        1, 2, 3, 5, 6, 7, 22, 36, 95, 121, 146, 159,
    ]
    for sid in KNOWN_STABLE_IDS:
        d = dl.fetch_stablecoin(sid)
        balances = (d or {}).get("chainBalances") or {}
        print(f"  defillama/stablecoin/{sid}: "
              f"{len(balances)} chains in chainBalances")


def main():
    print("Snapshotting Paymentscan…")
    snapshot_paymentscan()
    print()
    print("Snapshotting DefiLlama…")
    snapshot_defillama()
    print()
    print("Done. Inspect paymentscan_seeds/ and defillama_seeds/.")


if __name__ == "__main__":
    main()
