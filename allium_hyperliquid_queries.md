# Hyperliquid RWA-perps queries for Allium

Three queries to power the historical RWA perps view. Create them in
your Allium account (web UI), then share the query IDs back and I'll
wire them through `allium.py` the same way the Paymentscan + Solana
stablecoin payments queries are plumbed.

Column names below are best-guesses based on Allium's docs
(https://docs.allium.so/historical-data/supported-blockchains/hyperliquid).
If any column doesn't exist, adjust it — the query structure stays
the same.

## Query 1 — Daily volume + OI for xyz DEX (RWA umbrella)

Time series of total RWA perp activity. One row per day. Powers a
2-axis chart: bars = daily volume, line = end-of-day OI.

```sql
SELECT
    DATE(date)               AS date,
    SUM(volume_usd)          AS daily_volume_usd,
    SUM(fees_usd)            AS daily_fees_usd,
    MAX(open_interest_usd)   AS open_interest_usd,
    SUM(trade_count)         AS trade_count,
    COUNT(DISTINCT trader)   AS unique_traders
FROM hyperliquid.metrics.dex_overview
WHERE dex_name = 'xyz'           -- the HIP-3 DEX that hosts xyz:* RWAs
  AND date >= '2025-01-01'       -- adjust if you want a tighter window
GROUP BY DATE(date)
ORDER BY date
```

If `dex_overview` is already pre-aggregated to one row per day per
DEX, the GROUP BY collapses to a no-op and you can simplify to a
plain `SELECT ... FROM ... WHERE`. Keep the GROUP BY for safety in
case multiple rows share a date.

## Query 2 — Per-market daily OI (all 70 xyz:\* perps)

Long-format time series: one row per (date, market). Stacked-area
chart of OI by market over time — the OI history Birdeye doesn't
expose.

```sql
WITH daily AS (
    SELECT
        DATE(timestamp)              AS date,
        coin                         AS market,
        AVG(open_interest_usd)       AS daily_avg_oi_usd,
        MAX(open_interest_usd)       AS daily_max_oi_usd,
        AVG(mid_price)               AS daily_avg_price_usd
    FROM hyperliquid.raw.perpetual_market_asset_contexts
    WHERE coin LIKE 'xyz:%'
      AND timestamp >= TIMESTAMP '2025-01-01'
    GROUP BY DATE(timestamp), coin
)
SELECT
    date,
    market,
    daily_avg_oi_usd  AS open_interest_usd,
    daily_avg_price_usd AS price_usd
FROM daily
ORDER BY date, market
```

The `perpetual_market_asset_contexts` table is a snapshot table
(many rows per day per coin as the market state evolves). The
GROUP BY collapses to one row per (date, market). I'm using AVG for
OI because it smooths intra-day swings; if you want the daily close
instead, swap to a window function picking the LAST row per
(date, market). Let me know which you'd prefer.

## Query 3 — Per-market daily volume (all 70 xyz:\* perps)

Same long format. Stacked-area chart of volume by market over time,
parallel to the OI chart.

```sql
SELECT
    DATE(trade_timestamp)    AS date,
    coin                     AS market,
    SUM(volume_usd)          AS daily_volume_usd,
    SUM(fees_usd)            AS daily_fees_usd,
    COUNT(*)                 AS trade_count
FROM hyperliquid.dex.trades
WHERE coin LIKE 'xyz:%'
  AND trade_timestamp >= TIMESTAMP '2025-01-01'
GROUP BY DATE(trade_timestamp), coin
ORDER BY date, market
```

If `hyperliquid.dex.trades` doesn't carry a `coin` column (some
Allium tables expose `symbol` or `market_id` instead), swap the
column name. The HIP-3 perp coins are stored verbatim as
`'xyz:SP500'`, `'xyz:GOLD'`, etc. per the Birdeye data we already
have.

## After you create them in Allium

Share the query IDs back (e.g. `aBcDeFgHiJkLmNoPqRsT`) and I'll:

1. Add wiring in `stocks_dashboard.py` → `selected_asset == "RWA perps"` block
2. Each query → its own chart on the page (3 new charts below the
   current snapshot ones):
   - Query 1 → "Total Volume + OI over time" (dual-axis time series)
   - Query 2 → "OI by Market over time" (stacked area, top 12 + Others)
   - Query 3 → "Volume by Market over time" (stacked area, top 12 + Others)
3. All 3 charts route through `_chart_dwm_simple` so they inherit
   D/W/M tabs + 📋 raw-data button + slider + 3-tier legend per the
   cardinal chart rule.

The 4h `@st.cache_data` TTL in `allium.py` means each query runs at
most once per 4h on Cloud, which is well under Allium starter-tier
limits.
