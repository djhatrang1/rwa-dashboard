# Repo conventions

These rules apply to every change in this repo. Newer entries at the bottom.

## Data sources — closed list
Only pull data from these six sources unless the user explicitly authorizes a new one:

- Birdeye
- Dune
- DefiLlama
- CoinGecko
- Allium
- Blockworks Research
- Paymentscan

Do NOT scrape, hit undocumented endpoints, or use third-party aggregators outside this list. When you find yourself wanting another source, ask first.

## Secrets handling
- Never commit `.env`, `credentials.json`, or anything that looks like a key. The `.gitignore` already covers them — don't override.
- New API keys go in `.env` (local) and Streamlit Cloud Secrets (production, both apps). Reader pattern: `st.secrets.get("KEY", "")` first, env fallback. See `allium.py` and `paymentscan.py` for the canonical shape.
- Never skip git hooks (`--no-verify`, `--no-gpg-sign`) unless the user explicitly asks.

## Git
- Never run destructive ops (`reset --hard`, `push --force`, `branch -D`, `checkout .`) without explicit user request.
- Prefer NEW commits over `--amend`.
- Commit messages: short imperative subject; body explains the "why", not the "what". Co-author line at the bottom.

## Chart conventions

### Time controls
Every time-series chart uses `_chart()` or `_chart_dwm_simple()` so it inherits `_apply_time_controls`. That helper:
- Sets `rangeslider=dict(visible=True, ...)` — the rangeslider is the only time-range UI
- Explicitly clears `rangeselector` (no 1M/3M/6M/YTD/1Y/All buttons)
- Sets `type="date"` on the x-axis

**Do NOT** add `rangeselector=dict(visible=True, buttons=[...])` anywhere. The user removed the buttons across the entire codebase in `v59`. The rangeslider is sufficient.

### D/W/M selector
Time-series charts get a Daily / Weekly / Monthly tab via `_chart_dwm_simple` (single-axis) or `_chart_dwm_frame` (multi-axis). Daily is the default tab. Don't hand-roll period switches.

### Raw-data button
Every chart gets a 📋 button in its top-right corner, pinned by `inject_chartwrap_css()`. Pattern: pass `raw_df` + `raw_key` + `raw_filename` into the chart helper; CSV download is generated automatically.

### Legends — **MUST use `_legend`** (3-tier rule)
Do NOT use Plotly's inline legend on new charts. Instead:

1. Set `showlegend=False` on `fig.update_layout(...)`
2. Below the chart call site, call `_legend(entries, label="...")`

The helper auto-dispatches on series count — callers don't pick the tier:

| Series count | Rendering |
|---|---|
| **0–1** | Nothing — header / chart title already carries the meaning |
| **2–5** | Always-visible swatch row below the chart (no click required) |
| **6+** | Collapsed `st.expander` titled "Legend (N \<label\>)" |

```python
fig.update_layout(showlegend=False, ...)
_chart_dwm_simple("My Chart", source_df=..., build_fig=...)
_legend(
    [(series_name, hex_color) for series_name, hex_color in ordered],
    label="tokens")  # plural noun appears in "Legend (N tokens)" — only shown for 6+ tier
```

The helper lives at module level in `stocks_dashboard.py` and is reachable from `solana_dashboard.py` as `sd._legend(...)`. It renders an 8-column CSS grid of swatches; the tier picks whether that grid is bare (2–5) or wrapped in an `st.expander` (6+). `_legend_expander(...)` is a deprecated alias that routes to the same dispatcher — prefer `_legend()` in new code.

**Why each tier:**
- 1 series — a "legend" of one label is pure noise; the chart title already names it.
- 2–5 series — short enough that the legend fits below the chart without crowding; hiding it behind a click would cost more than it saves.
- 6+ series — inline Plotly legends with this many series stretch horizontally, shrink the plot area, and crowd the page. Collapsing reclaims that vertical space.

### Captions
Standard order: subheader → caption → chart. Caption explains the data source (with link) and any non-obvious aggregation choice. Don't put data-source links in the chart title.

## Layout
- Standard chart helpers (`_chart`, `_chart_dwm_simple`, `_chart_dwm_frame`) handle the chartwrap container + raw button positioning. Don't rebuild this manually.
- Two-column rows use `st.columns(2, gap="medium")`.

## Versioning
- `_PULLERS_VERSION` in `stocks_dashboard.py` is the cache-bust knob for puller registry + DB payload caches. Bump it when:
  - Adding/removing pullers
  - Changing a puller's output schema (column names, chain-suffix conventions)
  - Changing the structure of `_TOKENIZED_STOCK_GROUPS`
- The `_need_init` block already calls `_cached_latest_payload.clear()` on version bump, so the 4h DB-payload cache is busted too.

## Streamlit Cloud
- Two apps share one Supabase Postgres (set via `DATABASE_URL`): `stocks_dashboard.py` (RWA) and `solana_dashboard.py` (Solana).
- Both apps need every API key in their respective Secrets — paste the same TOML into each.
- The GHA cron at `.github/workflows/` runs pullers headlessly every N hours; Force Pull button on the UI triggers an immediate sync.
