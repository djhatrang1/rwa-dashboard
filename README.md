# RWA Dashboard

Multi-chain real-world-asset dashboard built on Streamlit. Covers tokenized
stocks, commodities, stablecoins, and treasuries across Solana, Ethereum,
BNB Chain, Base, and an aggregated "All chain" view.

Data sources:
- **Birdeye OHLCV + Token Overview** (Solana-native pricing, volume, market cap)
- **DefiLlama free API** (`/protocol/<slug>`, `/stablecoin/<id>`) — multi-chain
  market cap history for tokens Birdeye doesn't cover
- **Per-token seed files** (`mc_seed_<symbol>.json`) — backfilled history for
  permissioned tokens before our scheduler started recording

## Quick start (local)

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .streamlit/secrets.toml.example .env   # then fill in BIRDEYE_API_KEY
streamlit run stocks_dashboard.py
```

Local runs use SQLite (`crypto_data_stocks.db`) for the pull cache.

## Deployed (Streamlit Community Cloud)

1. **Supabase Postgres** stores pull snapshots. Connection string lives in
   Streamlit Cloud secrets as `DATABASE_URL`.
2. **GitHub Actions cron** (`.github/workflows/pull.yml`) fires every 6 hours,
   runs the puller in headless mode, and writes to Postgres.
3. The Streamlit app reads from the same Postgres on every page render.

To deploy a new instance:
1. Create a Supabase project; grab the **pooler** connection string
   (Settings → Database → Connection pooler → Transaction mode, port 6543).
2. Add `BIRDEYE_API_KEY` + `DATABASE_URL` to:
   - GitHub: Settings → Secrets and variables → Actions
   - Streamlit Cloud: App settings → Secrets
3. Push to `main` and connect the repo at <https://share.streamlit.io>.

## Adding a new token group

1. Append to one of `_TOKENIZED_STOCK_GROUPS`, `_TOKENIZED_COMMODITY_GROUPS`,
   `_STABLECOIN_GROUPS`, or `_TREASURY_GROUPS` in `stocks_dashboard.py`.
2. If the group needs multi-chain history, add the DefiLlama slug/id to the
   matching `_*_DEFILLAMA` config.
3. Bump `_PULLERS_VERSION` so the next render re-instantiates pullers.

## Repo layout

```
stocks_dashboard.py            # the entire app (~3.2k lines)
mc_seed_<symbol>.json          # backfill seeds for individual stablecoins
.streamlit/config.toml         # Birdeye Peak theme
.streamlit/secrets.toml.example  # template for cloud secrets
scripts/run_pull.py            # entry point for GitHub Actions cron
scripts/migrate_sqlite_to_postgres.py  # one-time data backfill helper
.github/workflows/pull.yml     # 6-hour cron + manual dispatch
runtime.txt                    # pins Python 3.11 on Streamlit Cloud
requirements.txt               # pip deps (incl. psycopg for Postgres)
```
