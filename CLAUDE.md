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

## Chart conventions — cardinal rule

**Every new time-series chart goes through `_chart_dwm_simple()` (single-axis) or `_chart_dwm_frame()` (multi-axis / custom resampling).** Doing so gives the chart all four required behaviors automatically:

1. **Time slider** (rangeslider visible on the x-axis)
2. **D/W/M tabs** (Daily default, Weekly / Monthly resampled)
3. **📋 raw-data button** (pinned top-right, opens a CSV-download dialog)
4. **3-tier legend** (auto-extracted from the daily figure's traces)

Bypassing these helpers with a direct `st.plotly_chart(...)` skips all four — don't do it for time-series. Bar/categorical charts (no time axis) can use `st.plotly_chart` since the time-controls don't apply.

```python
def _build_my_fig(df_view):
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df_view["date"], y=df_view["foo"], name="Foo",
                              line=dict(color="#4285F4")))
    fig.add_trace(go.Scatter(x=df_view["date"], y=df_view["bar"], name="Bar",
                              line=dict(color="#10B981")))
    fig.update_layout(showlegend=False, ...)   # <-- always suppress Plotly's
    return fig

_chart_dwm_simple(
    "My Chart Title",
    source_df=df,
    build_fig=_build_my_fig,
    raw_df=df.sort_values("date", ascending=False),
    raw_key="unique_key",
    raw_filename="my_chart_data",
    caption="Data source + any non-obvious aggregation choices.",
    col_aggs={"foo": "sum", "bar": "sum"},   # how W/M tabs aggregate cols
    # legend_entries is OPTIONAL — auto-extracted from the daily fig if omitted
)
```

### Time controls
`_apply_time_controls` (called inside `_chart()` / `_chart_dwm_*`):
- Sets `rangeslider=dict(visible=True, ...)` — the slider is the only time-range UI
- Explicitly clears `rangeselector` (no 1M/3M/6M/YTD/1Y/All buttons)
- Sets `type="date"` on the x-axis

**Do NOT** add `rangeselector=dict(visible=True, buttons=[...])` anywhere. The buttons were removed in `v59`; the slider is sufficient.

### D/W/M selector
Always via `_chart_dwm_simple` or `_chart_dwm_frame`. Daily is the default tab. Don't hand-roll period switches with `st.radio`.

### Raw-data 📋 button
Auto-pinned to the chart's top-right by `inject_chartwrap_css()`. Pattern: pass `raw_df` + `raw_key` (unique) + `raw_filename` into the chart helper.

### Legends — 3-tier `_legend()` (auto-dispatched)
Do NOT use Plotly's inline legend. Set `showlegend=False` on `fig.update_layout(...)`. The chart helpers auto-extract entries from your daily fig's traces (trace name + `line.color` / `marker.color` / `fillcolor`) and render via `_legend()` after the tabs.

Auto-dispatched by series count:

| Series count | Rendering |
|---|---|
| **0–1** | Nothing — header / chart title already carries the meaning |
| **2–5** | Always-visible swatch row below the chart (no click required) |
| **6+** | Collapsed `st.expander` titled "Legend (N \<label\>)" |

Pass `legend_label="tokens"` (or `"chains"`, `"issuers"`, etc.) to `_chart_dwm_simple` for the expander header noun (only used at the 6+ tier).

Override the auto-extracted entries by passing `legend_entries=[(name, color), ...]` explicitly — needed when:
- Bar trace has per-bar `marker.color` (a list, not one color)
- You want a different order / label than the trace order

For charts that DON'T route through the helpers (e.g. you have a one-off bar chart with `st.plotly_chart`): hide Plotly's legend with `showlegend=False` and call `_legend(...)` manually below. The helper lives at module level in `stocks_dashboard.py`; from `solana_dashboard.py` use `sd._legend(...)`. `_legend_expander(...)` is a deprecated alias that routes to the same dispatcher — prefer `_legend()` in new code.

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
