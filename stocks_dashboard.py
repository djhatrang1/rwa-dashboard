"""
Crypto Data Dashboard — SOP Framework
======================================
Run:   streamlit run sop_base.py
Extend: subclass DataPuller → implement fetch() + render() → add to init_pullers()

Environment variables (or .env file):
  BIRDEYE_API_KEY   – Birdeye API key
  DUNE_API_KEY      – Dune Analytics API key
  ALLIUM_API_KEY    – Allium API key
  PULL_INTERVAL     – seconds between pulls (default 14400 = 4 h)
  DB_PATH           – SQLite file path (default crypto_data.db)
"""

from __future__ import annotations

import abc
import json as _json
import logging
import math
import os
import sqlite3
import threading
import time
from datetime import datetime
from typing import List, Optional
from urllib.parse import urlparse

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import requests
import streamlit as st
from apscheduler.events import EVENT_JOB_ERROR
from apscheduler.schedulers.background import BackgroundScheduler
from pydantic_settings import BaseSettings
from streamlit_autorefresh import st_autorefresh
from tenacity import retry, stop_after_attempt, wait_exponential

# Optional Postgres driver — only required when DATABASE_URL is set.
try:
    import psycopg
    from psycopg.types.json import Json as _PgJson
except ImportError:  # pragma: no cover — fine for local SQLite-only runs
    psycopg = None
    _PgJson = None


# ── Secrets bridge: st.secrets (Streamlit Cloud) → os.environ ────────────────
# pydantic-settings only reads env vars / .env, not st.secrets. Copy known keys
# from st.secrets into os.environ at import time so cloud deploys pick them up
# without touching any pydantic-settings code paths. No-op locally.
for _k in ("BIRDEYE_API_KEY", "DATABASE_URL"):
    if _k not in os.environ:
        try:
            _v = st.secrets.get(_k)  # type: ignore[attr-defined]
        except Exception:
            _v = None
        if _v:
            os.environ[_k] = str(_v)

# ══════════════════════════════════════════════════════════════════════════════
# 1. CONFIGURATION
# ══════════════════════════════════════════════════════════════════════════════

class Settings(BaseSettings):
    # API keys — populated from environment / .env automatically (uppercased)
    birdeye_api_key: str = ""
    dune_api_key: str = ""
    allium_api_key: str = ""
    # CoinGecko Pro key (CG-...). When set, _fetch_coingecko_mc routes to
    # pro-api.coingecko.com with the x-cg-pro-api-key header and unlocks
    # days=max history. Empty string → falls back to the rate-limited free
    # public API.
    coingecko_api_key: str = ""

    # Base URLs
    birdeye_base_url: str = "https://public-api.birdeye.so"
    defillama_base_url: str = "https://api.llama.fi"

    # Pull cadence: 4 hours = 14 400 s
    pull_interval_seconds: int = 14_400

    # SQLite cache (used when DATABASE_URL is not set).
    db_path: str = "crypto_data_stocks.db"

    # Optional Postgres DSN. When present (Streamlit Cloud / CI cron),
    # CacheDB writes to Postgres instead of the local SQLite file.
    database_url: str = ""

    # Streamlit UI auto-refresh. The data-pull cron runs every 6h on
    # GitHub Actions, so a fast UI refresh cadence buys nothing — it just
    # re-runs the whole script (chart rebuilds, dropdown resets, scroll
    # position jumps) while viewers are reading. 1800s = 30 min keeps
    # the page reasonably fresh without interrupting the viewing
    # experience. Override via env / st.secrets `ui_refresh_seconds`.
    ui_refresh_seconds: int = 1800

    # `extra = "ignore"` so unused .env / st.secrets keys (e.g. an older
    # ANTHROPIC_API_KEY left over from prior features) don't crash startup.
    model_config = {
        "env_file": ".env",
        "env_file_encoding": "utf-8",
        "extra": "ignore",
    }


settings = Settings()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s  %(message)s",
)
log = logging.getLogger("sop")


# ══════════════════════════════════════════════════════════════════════════════
# 2. SQLITE CACHE
# ══════════════════════════════════════════════════════════════════════════════

def _payload_to_df(payload) -> pd.DataFrame:
    """Normalise a stored payload (JSON text from SQLite, or list-of-dicts from
    Postgres JSONB) back into a DataFrame."""
    if isinstance(payload, str):
        return pd.read_json(payload, orient="records")
    return pd.DataFrame(payload or [])


class CacheDB:
    """Thread-safe store for pull snapshots. Uses Postgres when DATABASE_URL is
    set (Streamlit Cloud / CI), otherwise falls back to a local SQLite file."""

    def __init__(self, db_path: str, database_url: str = "") -> None:
        self.db_path = db_path
        self.database_url = (database_url or "").strip()
        self.backend = "postgres" if self.database_url else "sqlite"
        if self.backend == "postgres" and psycopg is None:
            raise RuntimeError(
                "DATABASE_URL is set but the `psycopg` package is not installed. "
                "Add `psycopg[binary]>=3.1` to requirements.txt."
            )
        self._lock = threading.Lock()
        self._init()

    # ── internal ──────────────────────────────────────────────────────────────

    def _connect(self):
        if self.backend == "postgres":
            return psycopg.connect(self.database_url, autocommit=True)
        return sqlite3.connect(self.db_path, check_same_thread=False)

    @retry(stop=stop_after_attempt(4),
           wait=wait_exponential(multiplier=1, min=1, max=8),
           reraise=True)
    def _init(self) -> None:
        """Create the schema. Wrapped in tenacity retry so transient Supabase
        pgbouncer drops (EDBHANDLEREXITED) on startup don't crash the app —
        the next attempt grabs a fresh pooler connection cleanly."""
        with self._connect() as c:
            if self.backend == "postgres":
                with c.cursor() as cur:
                    cur.execute("""
                        CREATE TABLE IF NOT EXISTS pulls (
                            id        BIGSERIAL PRIMARY KEY,
                            puller    TEXT NOT NULL,
                            pulled_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                            payload   JSONB NOT NULL,
                            status    TEXT NOT NULL DEFAULT 'ok'
                        )
                    """)
                    cur.execute(
                        "CREATE INDEX IF NOT EXISTS idx_pulls_puller_time "
                        "ON pulls(puller, pulled_at DESC)"
                    )
            else:
                c.execute("""
                    CREATE TABLE IF NOT EXISTS pulls (
                        id        INTEGER PRIMARY KEY AUTOINCREMENT,
                        puller    TEXT NOT NULL,
                        pulled_at TEXT NOT NULL,
                        payload   TEXT NOT NULL,
                        status    TEXT NOT NULL DEFAULT 'ok'
                    )
                """)
                c.execute(
                    "CREATE INDEX IF NOT EXISTS idx_puller_time "
                    "ON pulls(puller, pulled_at)"
                )

    # ── public API ────────────────────────────────────────────────────────────

    def save(self, puller: str, df: pd.DataFrame) -> None:
        with self._lock, self._connect() as c:
            if self.backend == "postgres":
                payload = _json.loads(df.to_json(orient="records"))
                with c.cursor() as cur:
                    cur.execute(
                        "INSERT INTO pulls (puller, pulled_at, payload) "
                        "VALUES (%s, %s, %s)",
                        (puller, datetime.utcnow(), _PgJson(payload)),
                    )
            else:
                c.execute(
                    "INSERT INTO pulls (puller, pulled_at, payload) VALUES (?,?,?)",
                    (puller, datetime.utcnow().isoformat(),
                     df.to_json(orient="records")),
                )

    def latest(self, puller: str) -> Optional[pd.DataFrame]:
        """Return the most recent snapshot, or None if no data yet."""
        with self._connect() as c:
            if self.backend == "postgres":
                with c.cursor() as cur:
                    cur.execute(
                        "SELECT payload, pulled_at FROM pulls "
                        "WHERE puller=%s ORDER BY pulled_at DESC LIMIT 1",
                        (puller,),
                    )
                    row = cur.fetchone()
            else:
                row = c.execute(
                    "SELECT payload, pulled_at FROM pulls "
                    "WHERE puller=? ORDER BY pulled_at DESC LIMIT 1",
                    (puller,),
                ).fetchone()
        if not row:
            return None
        df = _payload_to_df(row[0])
        df.attrs["pulled_at"] = row[1].isoformat() if hasattr(row[1], "isoformat") else row[1]
        return df

    def history(self, puller: str, n: int = 200) -> pd.DataFrame:
        """Return the last *n* snapshots stacked into one DataFrame."""
        with self._connect() as c:
            if self.backend == "postgres":
                with c.cursor() as cur:
                    cur.execute(
                        "SELECT pulled_at, payload FROM pulls "
                        "WHERE puller=%s ORDER BY pulled_at DESC LIMIT %s",
                        (puller, n),
                    )
                    rows = cur.fetchall()
            else:
                rows = c.execute(
                    "SELECT pulled_at, payload FROM pulls "
                    "WHERE puller=? ORDER BY pulled_at DESC LIMIT ?",
                    (puller, n),
                ).fetchall()
        if not rows:
            return pd.DataFrame()
        frames = []
        for pulled_at, payload in rows:
            df = _payload_to_df(payload)
            df["pulled_at"] = pd.to_datetime(pulled_at)
            frames.append(df)
        return pd.concat(frames, ignore_index=True)


def _build_cache_db() -> CacheDB:
    return CacheDB(settings.db_path, settings.database_url)


# Streamlit re-executes this script on every interaction (autorefresh every
# 30 s here), so a plain `CacheDB(...)` at module level would open a fresh
# Postgres connection per rerun and saturate Supabase's pgbouncer pooler,
# triggering EDBHANDLEREXITED drops. `st.cache_resource` caches the instance
# for the lifetime of the Streamlit process so connection setup happens once.
# PULL_ONLY mode runs the script as a plain interpreter (no Streamlit ctx);
# the try/except falls through to a normal construction in that case.
try:
    cache_db = st.cache_resource(_build_cache_db)()
except Exception:
    cache_db = _build_cache_db()
log.info(
    "CacheDB backend = %s (%s)",
    cache_db.backend,
    "Postgres" if cache_db.backend == "postgres" else cache_db.db_path,
)


# ══════════════════════════════════════════════════════════════════════════════
# 3. BASE DATAPULLER (ABC)
# ══════════════════════════════════════════════════════════════════════════════

class DataPuller(abc.ABC):
    """
    Abstract base for every data source.

    How to add a new source
    -----------------------
    1. Subclass DataPuller.
    2. Set a unique class-level ``name`` string.
    3. Implement ``fetch()`` — hit your API, return a clean pd.DataFrame.
    4. Implement ``render()`` — draw your Streamlit tab.
    5. Instantiate in ``init_pullers()`` and append to the returned list.

    The framework handles scheduling, caching, retry, and UI refresh.
    """

    name: str = "unnamed"

    def __init__(self, settings: Settings, db: CacheDB) -> None:
        self.settings = settings
        self.db = db
        self.logger = logging.getLogger(f"sop.{self.name}")
        self.last_pulled_at: Optional[datetime] = None

    # ── core API ──────────────────────────────────────────────────────────────

    @abc.abstractmethod
    def fetch(self) -> pd.DataFrame:
        """Call the upstream API; return a clean, flat DataFrame."""

    def pull(self) -> pd.DataFrame:
        """fetch() → stamp timestamp → persist to SQLite. Called by scheduler."""
        df = self.fetch()
        self.last_pulled_at = datetime.utcnow()
        self.db.save(self.name, df)
        self.logger.info("Pulled %d rows at %s", len(df), self.last_pulled_at)
        return df

    def get_latest(self) -> Optional[pd.DataFrame]:
        """Load the most recent cached snapshot. Wrapped in a 5-minute
        st.cache_data so multiple chart renders inside one autorefresh tick
        (and follow-up reruns within the TTL) reuse a single DataFrame in
        memory instead of re-pulling thousands of rows from Postgres."""
        result = _cached_latest_payload(self.name)
        if result is None:
            return None
        df, pulled_at = result
        # cache_data's serialisation strips DataFrame.attrs, so reattach it
        # on every call (cheap, no copy of underlying data).
        df.attrs["pulled_at"] = pulled_at
        return df


@st.cache_data(ttl=300, show_spinner=False)
def _cached_latest_payload(puller_name: str):
    """Module-level cache wrapper for the latest-pull DataFrame. Keyed by
    puller name; TTL 5 min — well under the 6-hour cron cadence, so users
    see fresh data within a few minutes of each pull but every chart on a
    page hits the same materialised DataFrame instead of pulling its own.
    Returns (df, pulled_at_iso) — splitting attrs out because st.cache_data
    doesn't preserve DataFrame.attrs across cache hits."""
    df = cache_db.latest(puller_name)
    if df is None:
        return None
    return df, df.attrs.get("pulled_at", "")

    def get_history(self, n: int = 200) -> pd.DataFrame:
        """Load the last *n* cached snapshots stacked into one DataFrame."""
        return self.db.history(self.name, n)

    # ── UI ────────────────────────────────────────────────────────────────────

    @abc.abstractmethod
    def render(self) -> None:
        """Draw this puller's section inside its Streamlit tab."""


# ══════════════════════════════════════════════════════════════════════════════
# 4. SCHEDULER  (APScheduler + tenacity retry)
# ══════════════════════════════════════════════════════════════════════════════

class PullScheduler:
    """Wraps APScheduler; runs each DataPuller on a fixed interval with retry."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._scheduler = BackgroundScheduler(timezone="UTC")
        self._pullers: List[DataPuller] = []

    def register(self, puller: DataPuller) -> "PullScheduler":
        self._pullers.append(puller)
        return self  # fluent

    @staticmethod
    def _make_job(puller: DataPuller):
        @retry(
            stop=stop_after_attempt(3),
            wait=wait_exponential(multiplier=2, min=5, max=120),
            reraise=True,
        )
        def _job():
            puller.pull()

        return _job

    def start(self) -> None:
        self._scheduler.add_listener(
            lambda evt: log.error("Job %s failed: %s", evt.job_id, evt.exception),
            EVENT_JOB_ERROR,
        )
        for puller in self._pullers:
            self._scheduler.add_job(
                self._make_job(puller),
                trigger="interval",
                seconds=self.settings.pull_interval_seconds,
                id=puller.name,
                next_run_time=datetime.utcnow(),  # run immediately on startup
                misfire_grace_time=300,
            )
        self._scheduler.start()
        log.info("Scheduler started — %d puller(s) registered", len(self._pullers))

    def stop(self) -> None:
        self._scheduler.shutdown(wait=False)

    @property
    def pullers(self) -> List[DataPuller]:
        return list(self._pullers)


# ══════════════════════════════════════════════════════════════════════════════
# 5. EXAMPLE PULLERS
# ══════════════════════════════════════════════════════════════════════════════

# ── 5a. Birdeye — USDC daily trading volume across chains ─────────────────────

class USDCVolumePuller(DataPuller):
    """
    Daily USDC trading volume (USD) on Solana, Ethereum, Arbitrum, Base, and BSC
    via the Birdeye OHLCV endpoint. History starts Jan 1 2024.

    Requires: BIRDEYE_API_KEY
    Customise: override CHAINS or START_DATE.
    """

    name = "usdc_volume"

    START_TS: int = int(datetime(2024, 1, 1, tzinfo=None).timestamp())

    CHAINS: dict[str, dict] = {
        "Solana":   {"chain": "solana",   "address": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"},
        "Arbitrum": {"chain": "arbitrum", "address": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"},
        "Base":     {"chain": "base",     "address": "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"},
        "BSC":      {"chain": "bsc",      "address": "0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d"},
        "Ethereum": {"chain": "ethereum", "address": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"},
    }

    # Brand colours for the stacked bars
    COLORS: dict[str, str] = {
        "Solana":   "#9945FF",
        "Ethereum": "#627EEA",
        "Arbitrum": "#28A0F0",
        "Base":     "#0052FF",
        "BSC":      "#F0B90B",
    }

    def fetch(self) -> pd.DataFrame:
        time_to = int(datetime.utcnow().timestamp())
        frames: list[pd.DataFrame] = []

        for chain_name, cfg in self.CHAINS.items():
            chain_rows: list[dict] = []
            time_from = self.START_TS

            # Paginate in 1 000-day windows (API hard limit)
            while time_from < time_to:
                try:
                    resp = requests.get(
                        f"{self.settings.birdeye_base_url}/defi/ohlcv",
                        headers={
                            "X-API-KEY": self.settings.birdeye_api_key,
                            "x-chain": cfg["chain"],
                        },
                        params={
                            "address": cfg["address"],
                            "type": "1D",
                            "time_from": time_from,
                            "time_to": time_to,
                            "currency": "usd",
                        },
                        timeout=20,
                    )
                    resp.raise_for_status()
                    items = resp.json().get("data", {}).get("items", [])
                except Exception as exc:
                    self.logger.warning("fetch failed for %s: %s", chain_name, exc)
                    break

                if not items:
                    break

                chain_rows.extend(items)

                if len(items) < 1000:
                    break  # exhausted available data
                # Advance window past the last returned candle
                time_from = int(items[-1]["unixTime"]) + 86_400

            if chain_rows:
                df_chain = pd.DataFrame(chain_rows)
                df_chain["chain"] = chain_name
                # Derive human-readable date string (round-trips cleanly through JSON)
                df_chain["date"] = (
                    pd.to_datetime(df_chain["unixTime"], unit="s")
                    .dt.strftime("%Y-%m-%d")
                )
                df_chain = df_chain.rename(columns={"v": "volume_usd"})
                frames.append(df_chain[["date", "chain", "volume_usd"]])

        if not frames:
            return pd.DataFrame(columns=["date", "chain", "volume_usd"])

        df = pd.concat(frames, ignore_index=True)

        # ── IQR outlier removal per chain ─────────────────────────────────────
        # Drop days where volume is beyond 3 × IQR from Q1/Q3 for that chain.
        clean_frames: list[pd.DataFrame] = []
        for chain_name, grp in df.groupby("chain"):
            q1 = grp["volume_usd"].quantile(0.25)
            q3 = grp["volume_usd"].quantile(0.75)
            iqr = q3 - q1
            lo, hi = q1 - 3 * iqr, q3 + 3 * iqr
            removed = (~grp["volume_usd"].between(lo, hi)).sum()
            if removed:
                self.logger.info("Removed %d outlier(s) for %s", removed, chain_name)
            clean_frames.append(grp[grp["volume_usd"].between(lo, hi)])

        return pd.concat(clean_frames, ignore_index=True)

    def _stacked_bar(self, df: pd.DataFrame, x: str, title: str, chain_order: list, period: str = "d") -> None:
        """Render a stacked bar chart + per-chain totals + raw table."""
        fig = px.bar(
            df,
            x=x,
            y="volume_usd",
            color="chain",
            title=title,
            labels={x: x.replace("_", " ").title(), "volume_usd": "Volume (USD)", "chain": "Chain"},
            barmode="stack",
            category_orders={"chain": chain_order},
            color_discrete_map=self.COLORS,
        )
        fig.update_layout(
            hovermode="x unified",
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            yaxis_tickformat="$~s",
            # x-axis controls (type=date, rangeslider, range-selector buttons)
            # are applied uniformly by _chart() — no need to set them here.
        )
        _chart(fig, use_container_width=True)

        chain_totals = df.groupby("chain")["volume_usd"].sum().reindex(chain_order)
        cols = st.columns(len(chain_totals))
        for col, (chain, vol) in zip(cols, chain_totals.items()):
            col.metric(chain, f"${vol / 1e9:.1f}B")

        _, _raw_col = st.columns([0.95, 0.05])
        with _raw_col:
            if st.button("📋", key=f"raw_usdc_{period}", help="View raw data"):
                _raw_data_modal(
                    df.sort_values([x, "chain"], ascending=[False, True]),
                    {"volume_usd": "${:,.0f}"},
                )

    def render(self) -> None:
        st.subheader("USDC Trading Volume by Chain")

        if not self.settings.birdeye_api_key:
            st.warning("Set BIRDEYE_API_KEY in your .env to enable live data.")

        df = self.get_latest()
        if df is None or df.empty:
            st.info("Waiting for first pull…")
            return

        st.caption(f"Last pull: {df.attrs.get('pulled_at', '?')} UTC · Source: Birdeye")

        df["date"] = pd.to_datetime(df["date"])
        chain_order = [c for c in self.CHAINS if c in df["chain"].unique()]

        # ── Summary metrics (always across full daily data) ───────────────────
        total_vol = df["volume_usd"].sum()
        latest_day = df["date"].max()
        latest_df = df[df["date"] == latest_day]
        latest_total = latest_df["volume_usd"].sum()
        top_chain = latest_df.loc[latest_df["volume_usd"].idxmax(), "chain"]

        c1, c2, c3 = st.columns(3)
        c1.metric("Total Volume (since Jan 2024)", f"${total_vol / 1e9:.1f}B")
        c2.metric(f"Volume ({latest_day.strftime('%b %d, %Y')})", f"${latest_total / 1e9:.2f}B")
        c3.metric("Top Chain (latest day)", top_chain)

        # ── Daily / Weekly / Monthly tabs ────────────────────────────────────
        tab_daily, tab_weekly, tab_monthly = st.tabs(["Daily", "Weekly", "Monthly"])

        with tab_daily:
            self._stacked_bar(
                df, "date",
                "USDC Daily Volume by Chain (Jan 2024 → today)",
                chain_order, period="d",
            )

        with tab_weekly:
            weekly = (
                df.assign(week=df["date"].dt.to_period("W").dt.start_time)
                .groupby(["week", "chain"], as_index=False)["volume_usd"].sum()
                .rename(columns={"week": "week_start"})
            )
            self._stacked_bar(
                weekly, "week_start",
                "USDC Weekly Volume by Chain (Jan 2024 → today)",
                chain_order, period="w",
            )

        with tab_monthly:
            monthly = (
                df.assign(month=df["date"].dt.to_period("M").dt.start_time)
                .groupby(["month", "chain"], as_index=False)["volume_usd"].sum()
                .rename(columns={"month": "month_start"})
            )
            self._stacked_bar(
                monthly, "month_start",
                "USDC Monthly Volume by Chain (Jan 2024 → today)",
                chain_order, period="m",
            )


# ── 5b. Generic Solana Token Metrics Puller ───────────────────────────────────

class SolanaTokenMetricsPuller(DataPuller):
    """
    Generic daily price/volume/market-cap puller for any Solana token via Birdeye OHLCV.
    Subclass (or use the factory below) to set TOKEN_NAME, ADDRESS, and START_TS.

    Primary path  : token-level /defi/ohlcv (fast, single call).
    Fallback path : pair-level OHLCV aggregated across top pairs — used for bridged
                    tokens (e.g. HYPE) where the token endpoint returns empty.
                    · USDC/USDT pairs → volume_usd = v_hype × close_usd
                    · SOL pairs       → volume_usd = v_hype × close_sol × sol_usd_price
                      (SOL/USD price is fetched per-day from SOL's own OHLCV history)

    Market cap = close_price_usd × circulating_supply (fetched fresh each pull).
    Requires: BIRDEYE_API_KEY
    """

    GROUP               : str  = "solana_tokens"
    TOKEN_NAME          : str  = ""
    ADDRESS             : str  = ""
    START_TS            : int  = int(datetime(2024, 1, 1).timestamp())
    # Set False for tokens whose circulating supply changes rapidly (e.g. stablecoins)
    # *and* for which no DeFiLlama historical supply exists.
    # When DEFILLAMA_STABLE_ID is set the supply series is fetched and used instead,
    # so SHOW_MC_CHART will be forced True at fetch time.
    SHOW_MC_CHART       : bool = True
    # Left-axis label in the chart. Overridden to "Circulating Supply (USD)" for
    # stablecoins where we plot DeFiLlama supply instead of price × current_circ_supply.
    MC_CHART_LABEL      : str  = "Market Cap (USD)"
    # DeFiLlama stablecoins endpoint ID (0 = not a tracked stablecoin).
    # When set, fetch() pulls historical circulating supply from DeFiLlama and
    # stores it in the market_cap_usd column so the chart line shows real supply history.
    DEFILLAMA_STABLE_ID : int  = 0
    DEFILLAMA_CHAIN     : str  = "Solana"   # chain name as used by DeFiLlama

    # Well-known Solana mints
    _SOL_MINT   = "So11111111111111111111111111111111111111112"
    _USDC_MINT  = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
    _USDT_MINT  = "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"

    # ── Private helpers ────────────────────────────────────────────────────────
    def _paginated_ohlcv(self, headers: dict, address: str, time_from: int,
                         time_to: int, endpoint: str = "token") -> list[dict]:
        """Fetch daily OHLCV with pagination.  endpoint='token' or 'pair'."""
        path  = "/defi/ohlcv" if endpoint == "token" else "/defi/ohlcv/pair"
        key   = "address"
        rows: list[dict] = []
        t_from = time_from
        while t_from < time_to:
            try:
                params: dict = {"type": "1D", "time_from": t_from, "time_to": time_to,
                                key: address}
                if endpoint == "token":
                    params["currency"] = "usd"
                resp = requests.get(
                    f"{self.settings.birdeye_base_url}{path}",
                    headers=headers, params=params, timeout=20,
                )
                resp.raise_for_status()
                items = resp.json().get("data", {}).get("items", [])
            except Exception as exc:
                self.logger.warning("%s OHLCV fetch failed (%s): %s",
                                    self.TOKEN_NAME, address[:8], exc)
                break
            if not items:
                break
            rows.extend(items)
            if len(items) < 1000:
                break
            t_from = int(items[-1]["unixTime"]) + 86_400
        return rows

    def _fetch_circ_supply(self, headers: dict) -> float | None:
        try:
            r = requests.get(
                f"{self.settings.birdeye_base_url}/defi/v3/token/market-data",
                headers=headers, params={"address": self.ADDRESS}, timeout=15,
            )
            r.raise_for_status()
            return r.json()["data"]["circulating_supply"]
        except Exception as exc:
            self.logger.warning("%s circ-supply fetch failed: %s", self.TOKEN_NAME, exc)
            return None

    def _fetch_defillama_supply(self) -> dict[str, float]:
        """
        Return {date_str: circulating_supply_usd} from DeFiLlama's stablecoins API.
        Only called when DEFILLAMA_STABLE_ID is set.  Filters to DEFILLAMA_CHAIN.
        """
        if not self.DEFILLAMA_STABLE_ID:
            return {}
        try:
            url  = f"https://stablecoins.llama.fi/stablecoin/{self.DEFILLAMA_STABLE_ID}"
            resp = requests.get(url, timeout=30)
            resp.raise_for_status()
            chain_data = (
                resp.json()
                .get("chainBalances", {})
                .get(self.DEFILLAMA_CHAIN, {})
            )
            return {
                pd.to_datetime(item["date"], unit="s").strftime("%Y-%m-%d"):
                float(item["circulating"]["peggedUSD"])
                for item in chain_data.get("tokens", [])
                if item.get("circulating", {}).get("peggedUSD") is not None
            }
        except Exception as exc:
            self.logger.warning("%s DeFiLlama supply fetch failed: %s",
                                self.TOKEN_NAME, exc)
            return {}

    def _fetch_sol_price_by_day(self, headers: dict, time_to: int) -> dict[str, float]:
        """Return {date_str: sol_usd_price} for every day we have SOL OHLCV data."""
        rows = self._paginated_ohlcv(headers, self._SOL_MINT, self.START_TS, time_to,
                                     endpoint="token")
        out: dict[str, float] = {}
        for r in rows:
            date = pd.to_datetime(r["unixTime"], unit="s").strftime("%Y-%m-%d")
            out[date] = r["c"]   # close price in USD (currency=usd for token endpoint)
        return out

    # ── Primary fetch: token-level OHLCV ──────────────────────────────────────
    def _try_token_ohlcv(self, headers: dict, time_to: int) -> pd.DataFrame | None:
        # MC is no longer computed here. It used to be price × today's supply
        # applied across all historical days, which over/under-states past
        # MC whenever supply changes. fetch() now snapshots today's MC from
        # Birdeye Token Overview + carries forward prior snapshots + applies
        # any mc_seed_<symbol>.json on disk — matching the commodity / stable
        # / treasury puller pattern.
        rows = self._paginated_ohlcv(headers, self.ADDRESS, self.START_TS, time_to,
                                     endpoint="token")
        if not rows:
            return None
        df = pd.DataFrame(rows)
        df["date"]           = pd.to_datetime(df["unixTime"], unit="s").dt.strftime("%Y-%m-%d")
        df["price_usd"]      = df["c"]
        df["volume_usd"]     = df["v"] * df["c"]   # v = native token units; ×price → USD
        df["market_cap_usd"] = None                # filled by fetch() (snapshot + carry-forward + seed)
        return df[["date", "price_usd", "volume_usd", "market_cap_usd"]]

    # ── Fallback fetch: aggregate top pairs' OHLCV ────────────────────────────
    def _fetch_all_pairs(self, headers: dict) -> list[dict]:
        """
        Fetch up to 40 unique pairs by issuing two parallel-style requests:
        - top 20 by current 24h volume  (catches today's most active pools)
        - top 20 by liquidity           (catches historically stable pools)
        Deduplicates by pair address so no pool is counted twice.
        """
        seen: set[str] = set()
        all_pairs: list[dict] = []
        for sort_by in ("volume24h", "liquidity"):
            try:
                r = requests.get(
                    f"{self.settings.birdeye_base_url}/defi/v2/markets",
                    headers=headers,
                    params={"address": self.ADDRESS, "sort_by": sort_by,
                            "sort_type": "desc", "limit": 20},
                    timeout=15,
                )
                r.raise_for_status()
                items = r.json().get("data", {}).get("items", [])
            except Exception as exc:
                self.logger.warning("%s markets fetch (sort=%s) failed: %s",
                                    self.TOKEN_NAME, sort_by, exc)
                continue
            for p in items:
                addr = p.get("address", "")
                if addr and addr not in seen:
                    seen.add(addr)
                    all_pairs.append(p)
        return all_pairs

    def _try_pair_ohlcv(self, headers: dict, time_to: int) -> pd.DataFrame:
        pairs = self._fetch_all_pairs(headers)

        STABLE = {self._USDC_MINT, self._USDT_MINT}
        relevant: list[dict] = []
        need_sol = False
        for p in pairs:
            base  = p.get("base",  {}).get("address", "")
            quote = p.get("quote", {}).get("address", "")
            # Accept pairs where token is base with a USD/SOL quote,
            # OR where token is the quote with SOL as base (inverted pair).
            if base == self.ADDRESS:
                if quote in STABLE:
                    relevant.append({"address": p["address"], "quote": "usd", "inverted": False})
                elif quote == self._SOL_MINT:
                    relevant.append({"address": p["address"], "quote": "sol", "inverted": False})
                    need_sol = True
            elif quote == self.ADDRESS and base == self._SOL_MINT:
                # SOL-TOKEN pair: v = SOL volume, close = SOL price in TOKEN
                relevant.append({"address": p["address"], "quote": "sol_inv", "inverted": True})
                need_sol = True

        if not relevant:
            return pd.DataFrame(columns=["date", "price_usd", "volume_usd", "market_cap_usd"])

        # Fetch SOL/USD price history if any SOL-quoted pair is included
        sol_by_day: dict[str, float] = {}
        if need_sol:
            self.logger.info("%s: fetching SOL price history for pair-volume conversion",
                             self.TOKEN_NAME)
            sol_by_day = self._fetch_sol_price_by_day(headers, time_to)

        # Aggregate volumes and prices per day across all relevant pairs
        vol_by_day:   dict[str, float]       = {}
        price_by_day: dict[str, list[float]] = {}

        for p_info in relevant:
            items = self._paginated_ohlcv(headers, p_info["address"],
                                          self.START_TS, time_to, endpoint="pair")
            for item in items:
                date  = pd.to_datetime(item["unixTime"], unit="s").strftime("%Y-%m-%d")
                close = item["c"]
                vol   = item["v"]

                if p_info["quote"] == "usd":
                    # HYPE-USDC: v = HYPE amount, close = HYPE/USDC price
                    vol_usd   = vol * close
                    price_usd = close
                elif p_info["quote"] == "sol":
                    # HYPE-SOL: v = HYPE amount, close = HYPE/SOL price
                    sol_px = sol_by_day.get(date)
                    if sol_px is None:
                        continue
                    vol_usd   = vol * close * sol_px
                    price_usd = close * sol_px
                else:
                    # SOL-HYPE (inverted): v = SOL amount, close = SOL/HYPE price
                    # token_price_usd = sol_usd / close
                    sol_px = sol_by_day.get(date)
                    if sol_px is None:
                        continue
                    vol_usd   = vol * sol_px          # SOL volume in USD
                    price_usd = sol_px / close if close else None
                    if price_usd is None:
                        continue

                vol_by_day[date]   = vol_by_day.get(date, 0.0) + vol_usd
                price_by_day.setdefault(date, []).append(price_usd)

        if not vol_by_day:
            return pd.DataFrame(columns=["date", "price_usd", "volume_usd", "market_cap_usd"])

        out_rows = []
        for date in sorted(vol_by_day):
            prices = price_by_day.get(date, [])
            price  = sum(prices) / len(prices) if prices else None
            out_rows.append({
                "date"          : date,
                "price_usd"     : price,
                "volume_usd"    : vol_by_day[date],
                "market_cap_usd": None,    # filled by fetch() via snapshot + carry-forward + seed
            })
        df = pd.DataFrame(out_rows)
        df.attrs["volume_source"] = "pair-aggregation (top pairs)"
        return df

    # ── Public fetch ──────────────────────────────────────────────────────────
    def fetch(self) -> pd.DataFrame:
        headers = {"X-API-KEY": self.settings.birdeye_api_key, "x-chain": "solana"}
        time_to = int(datetime.utcnow().timestamp())

        # Price + volume from OHLCV (no MC computed there anymore — see below).
        df = self._try_token_ohlcv(headers, time_to)
        if df is None or df.empty:
            self.logger.info("%s: token OHLCV empty — falling back to pair aggregation",
                             self.TOKEN_NAME)
            df = self._try_pair_ohlcv(headers, time_to)

        if df.empty:
            return df

        # ── Market cap: snapshot + carry-forward + seed ──────────────────
        # Replaces the old "price × today's supply" approximation. Matches
        # the commodity / stable / treasury puller pattern:
        #   1. Carry forward whatever MC values were already cached from
        #      prior pulls (so each pull only ADDS today's snapshot rather
        #      than replacing the whole series).
        #   2. Apply mc_seed_<symbol>.json or mc_seed_<address>.json if
        #      present on disk — same shape as the commodity/stable seeds
        #      ({"payload": {"mc": [...], "t": [unix_seconds]}}). Used to
        #      backfill historical MC before per-pull snapshotting started.
        #   3. Overwrite today's row with a fresh Birdeye Token Overview
        #      snapshot — `marketCap` field, which equals price × on-chain
        #      circulating supply at the pull moment (the correct MC, not
        #      a wrong-supply approximation).
        # The series builds up over time — first pull only has today; after
        # N days the chart shows N points (plus whatever the seed provides).
        df["market_cap_usd"] = None
        today_str = datetime.utcnow().strftime("%Y-%m-%d")

        # (1) Carry-forward prior cached values.
        prior = self.get_latest()
        if prior is not None and not prior.empty and "market_cap_usd" in prior.columns:
            prior_mc = dict(zip(prior["date"], prior["market_cap_usd"]))
            df["market_cap_usd"] = df["date"].map(prior_mc)

        # (2) Seed JSON — keyed by lowercase symbol OR lowercase address so
        # users can name the file whichever way they prefer. Seeds OVERWRITE
        # carry-forward values (they're user-supplied authoritative history,
        # whereas carry-forward might be stale snapshots — or, during the
        # transition from the old price×supply MC source, outright wrong).
        # combine_first: keep seed where present, fall back to existing
        # market_cap_usd (carry-forward) where seed has no entry for the day.
        seed_all = _load_mc_seed()
        seed = (seed_all.get(self.TOKEN_NAME.lower())
                or seed_all.get(str(self.ADDRESS).lower()) or {})
        if seed:
            seeded = df["date"].map(seed)
            df["market_cap_usd"] = seeded.combine_first(df["market_cap_usd"])

        # (3) Today's Birdeye Token Overview snapshot — always wins.
        today_mc = self._fetch_overview_mc(headers)
        if today_mc is not None:
            df.loc[df["date"] == today_str, "market_cap_usd"] = today_mc

        # For stablecoins with a DefiLlama ID, prefer real historical circ
        # supply over the snapshot path — overwrites everything above with
        # DefiLlama's daily series (used to be the only path).
        if self.DEFILLAMA_STABLE_ID:
            supply_by_day = self._fetch_defillama_supply()
            if supply_by_day:
                df["market_cap_usd"] = df["date"].map(supply_by_day)

        return df

    def _fetch_overview_mc(self, headers: dict) -> float | None:
        """Today's market cap from Birdeye /defi/token_overview (the
        `marketCap` field). Returns None on any error so fetch() can fall
        through to the carry-forward / seed path without crashing."""
        try:
            r = requests.get(
                f"{self.settings.birdeye_base_url}/defi/token_overview",
                params={"address": self.ADDRESS},
                headers=headers, timeout=15,
            )
            r.raise_for_status()
            mc = (r.json().get("data") or {}).get("marketCap")
            return float(mc) if mc else None
        except Exception as exc:
            self.logger.warning(
                "%s: Birdeye Token Overview MC fetch failed (%s)",
                self.TOKEN_NAME, exc)
            return None

    # ── Resampling helper ─────────────────────────────────────────────────────
    @staticmethod
    def _resample(df: pd.DataFrame, period: str) -> pd.DataFrame:
        """Aggregate daily df to weekly ('W') or monthly ('M').
        Volume is summed; price and market_cap take the period's last value."""
        col = "week" if period == "W" else "month"
        return (
            df.assign(**{col: df["date"].dt.to_period(period).dt.start_time})
            .groupby(col, as_index=False)
            .agg({"volume_usd": "sum", "price_usd": "last",
                  "market_cap_usd": "last"})
            .rename(columns={col: "date"})
        )

    # ── Shared chart builder ───────────────────────────────────────────────────
    def _build_fig(self, df: pd.DataFrame, height: int) -> go.Figure:
        """
        Return the Market Cap / Volume figure.
        When SHOW_MC_CHART is False (e.g. stablecoins whose circulating supply grows
        rapidly, making price × current_supply meaningless historically) the MC line
        and left axis are omitted — only the volume bars are rendered.
        """
        def _aligned_ticks(vmax: float, n: int = 6) -> list[float]:
            if vmax <= 0 or not pd.notna(vmax):
                return [0.0] * n
            raw  = vmax / (n - 1)
            mag  = 10 ** math.floor(math.log10(raw))
            step = math.ceil(raw / mag) * mag
            return [i * step for i in range(n)]

        N         = 6
        vol_ticks = _aligned_ticks(df["volume_usd"].max(), N)
        show_mc   = (
            self.SHOW_MC_CHART
            and df["market_cap_usd"].notna().any()
        )

        fig = make_subplots(specs=[[{"secondary_y": show_mc}]])

        if show_mc:
            mc_ticks = _aligned_ticks(df["market_cap_usd"].dropna().max(), N)
            fig.add_trace(
                go.Scatter(x=df["date"], y=df["market_cap_usd"],
                           name=self.MC_CHART_LABEL, line=dict(color="#28A0F0", width=2)),
                secondary_y=False,
            )

        fig.add_trace(
            go.Bar(x=df["date"], y=df["volume_usd"],
                   name="Volume", marker_color="#9945FF", opacity=0.6),
            secondary_y=show_mc,
        )

        layout: dict = dict(
            title="",
            hovermode="x unified", height=height,
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            margin=dict(t=10, b=10, l=10, r=10),
        )
        if show_mc:
            layout["yaxis"] = dict(
                tickprefix="$", tickformat="~s",
                tickmode="array", tickvals=mc_ticks, range=[0, mc_ticks[-1]],
                showgrid=True,
            )
            layout["yaxis2"] = dict(
                tickprefix="$", tickformat="~s",
                tickmode="array", tickvals=vol_ticks, range=[0, vol_ticks[-1]],
                showgrid=False, overlaying="y", side="right",
            )
        else:
            layout["yaxis"] = dict(
                tickprefix="$", tickformat="~s",
                tickmode="array", tickvals=vol_ticks, range=[0, vol_ticks[-1]],
                showgrid=True,
            )
        fig.update_layout(**layout)
        return fig

    # ── Full render (used inside full-screen dialog) ───────────────────────────
    def render(self) -> None:
        if not self.settings.birdeye_api_key:
            st.warning("Set BIRDEYE_API_KEY in your .env to enable live data.")

        df = self.get_latest()
        if df is None or df.empty:
            st.info("Waiting for first pull…")
            return

        vol_src  = df.attrs.get("volume_source", "token OHLCV")
        src_note = f" · Volume via {vol_src}" if vol_src != "token OHLCV" else ""
        st.caption(f"Last pull: {df.attrs.get('pulled_at', '?')} UTC · Source: Birdeye{src_note}")

        df["date"] = pd.to_datetime(df["date"])
        latest = df.sort_values("date").iloc[-1]

        c1, c2, c3 = st.columns(3)
        c1.metric("Latest Price",      f"${latest['price_usd']:,.4f}")
        c2.metric("Latest 24h Volume", f"${latest['volume_usd']:,.0f}")
        if self.SHOW_MC_CHART:
            # Non-stablecoin: market cap from the historical series (price × circ supply)
            mktcap = latest["market_cap_usd"]
            c3.metric("Market Cap", f"${mktcap/1e6:.2f}M" if pd.notna(mktcap) else "N/A")
        else:
            # Stablecoin: show circulating supply (tokens minted on-chain) — more
            # meaningful than market cap for a $1-pegged token.
            headers = {"X-API-KEY": self.settings.birdeye_api_key, "x-chain": "solana"}
            try:
                r = requests.get(
                    f"{self.settings.birdeye_base_url}/defi/v3/token/market-data",
                    headers=headers, params={"address": self.ADDRESS}, timeout=10,
                )
                r.raise_for_status()
                supply = r.json()["data"]["circulating_supply"]
                c3.metric("Circulating Supply",
                          f"{supply/1e6:.2f}M" if supply else "N/A")
            except Exception:
                # Fallback: derive from stored market_cap_usd ÷ price (≈ supply when price ≈ $1)
                mktcap = latest["market_cap_usd"]
                price  = latest["price_usd"] or 1.0
                supply = (mktcap / price) if pd.notna(mktcap) else None
                c3.metric("Circulating Supply",
                          f"{supply/1e6:.2f}M" if supply else "N/A")

        tab_d, tab_w, tab_m = st.tabs(["Daily", "Weekly", "Monthly"])
        with tab_d:
            _chart(self._build_fig(df, height=520), use_container_width=True)
        with tab_w:
            _chart(self._build_fig(self._resample(df, "W"), height=520),
                            use_container_width=True)
        with tab_m:
            _chart(self._build_fig(self._resample(df, "M"), height=520),
                            use_container_width=True)

    def raw_data_fmt(self) -> dict:
        """Column format dict used for the raw-data modal."""
        return {
            "price_usd": "${:,.4f}",
            "volume_usd": "${:,.0f}",
            "market_cap_usd": "${:,.0f}",
        }

    # ── Latest price label (for grid card headers) ────────────────────────────
    def latest_price_label(self) -> str:
        df = self.get_latest()
        if df is None or df.empty:
            return ""
        price = df.sort_values("date").iloc[-1]["price_usd"]
        try:
            return f"${float(price):,.2f}"
        except (TypeError, ValueError):
            return ""

    # ── Compact render (used in 2-per-row grid) ────────────────────────────────
    def render_compact(self) -> None:
        df = self.get_latest()
        if df is None or df.empty:
            st.caption("Waiting for first pull…")
            return

        df["date"] = pd.to_datetime(df["date"])
        _chart(self._build_fig(df, height=300), use_container_width=True)


def _make_solana_puller(token_name: str, address: str,
                        start: datetime = datetime(2024, 1, 1),
                        group: str = "solana_tokens",
                        defillama_stable_id: int = 0) -> type:
    """Factory: return a SolanaTokenMetricsPuller subclass for one token."""
    safe = token_name.lower().replace("-", "_").replace(" ", "_")
    # For stablecoins without a DeFiLlama supply series, hide the MC chart line
    # because price × current_supply applied to all history is a flat, misleading line.
    # When a DeFiLlama ID is provided the real historical supply series will be fetched
    # and stored in market_cap_usd, so we re-enable the chart and relabel the axis.
    has_defillama  = defillama_stable_id > 0
    show_mc        = (group != "stablecoins") or has_defillama
    mc_label       = "Circulating Supply (USD)" if has_defillama else "Market Cap (USD)"
    return type(
        f"{token_name.replace('-','').replace(' ','')}SolanaMetricsPuller",
        (SolanaTokenMetricsPuller,),
        {
            "name"               : f"{safe}_solana_metrics",
            "TOKEN_NAME"         : token_name,
            "ADDRESS"            : address,
            "START_TS"           : int(start.timestamp()),
            "GROUP"              : group,
            "SHOW_MC_CHART"      : show_mc,
            "MC_CHART_LABEL"     : mc_label,
            "DEFILLAMA_STABLE_ID": defillama_stable_id,
        },
    )


# ── Solana token registry (name, address, history_start) ──────────────────────
_SOLANA_TOKENS: list[tuple[str, str, datetime]] = [
    ("WETH",  "7vfCXTUXx5WJV5JADk17DUJ4ksgau7utNKj4b963voxs", datetime(2024, 1, 1)),
    ("HYPE",  "98sMhvDwXj1RQi5c5Mndm3vPe9cBqPrbLaufMXFNMh5g",  datetime(2024, 1, 1)),
    ("ZEC",   "A7bdiYdS5GjqGFtxf17ppRHtDKPkkRqbKtR27dxvQXaS",  datetime(2024, 1, 1)),
    ("MON",   "CrAr4RRJMBVwRsZtT62pEhfA9H5utymC2mVx8e7FreP2",  datetime(2024, 1, 1)),
    ("cbBTC", "cbbtcf3aa214zXHbiAZQwf4122FBYbraNdFqgw4iMij",   datetime(2024, 1, 1)),
    ("WBTC",  "3NZ9JMVBmGAqocybic2c7LQCJScmgsAZ6vQqTDzcqmJh",  datetime(2024, 1, 1)),
    ("AVAX",  "avaxGHCq3T7hoxd73oY2KY9hJSTaeMibXvHy5KNzh5D",   datetime(2024, 1, 1)),
    ("STRK",  "HsRpHQn6VbyMs5b5j5SV6xQ2VvpvvCCzu19GjytVSCoz",  datetime(2024, 1, 1)),
    ("WBNB",  "9gP2kCy3wA1ctvYWQk75guqXuHfrEomqydHLtcTCqiLa",  datetime(2024, 1, 1)),
    ("ZORA",  "soKqZS9pASwBNS46G388nhK7XVtPaTyReffXEd3zora",   datetime(2024, 1, 1)),
    ("NEAR",  "3ZLekZYq2qkZiSpnSvabjit34tUkjSwD1JFuW9as9wBG",  datetime(2024, 1, 1)),
    ("TRX",   "GbbesPbaYh5uiAZSYNXTc7w9jty1rpg3P9L4JeN4LkKc",  datetime(2024, 1, 1)),
    # ── More BTC variants on Solana — Birdeye-verified MCs below ─────────
    ("wfragBTC", "WFRGB49tP8CdKubqCdt5Spo2BdGS4BpgoinNER5TYUm", datetime(2024, 1, 1)),   # Fragmetric staked-BTC ~$0.56M
    ("tBTC",     "6DNSN2BJsaPFdFFc1zP37kkeNe4Usc1Sqkzr9C9vPWcU", datetime(2024, 1, 1)),   # Threshold Network tBTC v2 ~$1.4M
    ("LBTC",     "LBTCgU4b3wsFKsPwBn1rRZDx5DoFutM6RPiEt1TPDsY", datetime(2024, 1, 1)),    # Lombard Staked BTC ~$6.8M
    ("zBTC",     "zBTCug3er3tLyffELcvDNrKkCymbPWysGcWihESYfLg", datetime(2024, 1, 1)),    # Zeus Network zBTC ~$6.1M
    ("xBTC",     "CtzPWv73Sn1dMGVU3ZtLv9yWSyUAanBni19YWDaznnkn", datetime(2024, 1, 1)),   # OKX Wrapped BTC ~$22.4M
]

# ── Stablecoin token registry (name, address, history_start, defillama_stable_id) ─
# defillama_stable_id: ID from https://stablecoins.llama.fi/stablecoins (0 = not tracked).
# When set, historical circulating supply is fetched from DeFiLlama and shown on the chart.
_STABLECOIN_TOKENS: list[tuple[str, str, datetime, int]] = [
    ("USD1", "USD1ttGY1N17NEEHLmELoaybftRBUSErhqYiQzvEmuB", datetime(2024, 1, 1), 262),
]


# ── 5c. Token Group Metrics Puller (multi-token aggregated groups) ────────────

# Process-wide cache for CoinGecko market-cap history. Shared across all puller
# threads/sessions so concurrent schedulers don't trip CoinGecko's free-tier
# rate limit; the lock serializes the (rare) actual HTTP calls.
_CG_MC_CACHE: dict[str, tuple[float, dict]] = {}
_CG_MC_TTL   = 3600.0   # seconds
_CG_MC_LOCK  = threading.Lock()

# Process-wide cache for DefiLlama per-chain market-cap history (free API).
# Cached so concurrent scheduler threads don't duplicate work or hammer the API.
_DL_CACHE: dict[str, tuple[float, dict]] = {}
_DL_TTL   = 3600.0
_DL_LOCK  = threading.Lock()


def _chain_safe(chain: str) -> str:
    """Normalize a DefiLlama chain name for use in column suffixes."""
    return (chain or "").lower().replace(" ", "_").replace("-", "_")


# DefiLlama returns the BNB Chain under different labels on different endpoints:
# the stablecoin endpoint emits "BSC" while the protocol endpoint emits "Binance".
# We normalise to "Binance" so both data sources stack in one column.
_DL_CHAIN_ALIASES = {
    "BSC":                 "Binance",
    "BNB Chain":           "Binance",
    "Binance Smart Chain": "Binance",
    "BinanceSmartChain":   "Binance",  # spelling used by Backed.fi xStocks CSV
    "bsc":                 "Binance",  # lowercase fallback
    "binance":             "Binance",
}


def _norm_chain(chain: str) -> str:
    return _DL_CHAIN_ALIASES.get(chain, chain)


def _fetch_dl_protocol(slug: str) -> dict:
    """Return {chain_name: {date_str: market_cap_usd_float}} from DefiLlama
    `/protocol/{slug}` (chainTvls.{chain}.tvl). For RWA funds TVL ≈ AUM ≈ MC."""
    now = time.time()
    key = f"protocol:{slug}"
    with _DL_LOCK:
        hit = _DL_CACHE.get(key)
        if hit and now - hit[0] < _DL_TTL:
            return hit[1]
        try:
            r = requests.get(f"https://api.llama.fi/protocol/{slug}", timeout=30)
            r.raise_for_status()
            data = r.json()
        except Exception as exc:
            log.warning("DefiLlama /protocol/%s fetch failed: %s", slug, exc)
            return hit[1] if hit else {}
        out: dict = {}
        for chain, payload in (data.get("chainTvls") or {}).items():
            # Skip aggregate / pool / bridge buckets like "Solana-staking".
            if "-" in chain:
                continue
            series: dict = {}
            for pt in (payload.get("tvl") or []):
                try:
                    ts = int(pt["date"]); v = float(pt["totalLiquidityUSD"])
                    series[pd.to_datetime(ts, unit="s").strftime("%Y-%m-%d")] = v
                except (KeyError, TypeError, ValueError):
                    continue
            if series:
                ch = _norm_chain(chain)
                # Merge if alias already populated by another label.
                if ch in out:
                    out[ch].update(series)
                else:
                    out[ch] = series
        _DL_CACHE[key] = (now, out)
        return out


def _fetch_dl_stablecoin(stable_id: int) -> dict:
    """Return {chain_name: {date_str: market_cap_usd}} from DefiLlama
    `/stablecoin/{id}` (chainBalances.{chain}.tokens[].circulating.peggedUSD)."""
    now = time.time()
    key = f"stablecoin:{stable_id}"
    with _DL_LOCK:
        hit = _DL_CACHE.get(key)
        if hit and now - hit[0] < _DL_TTL:
            return hit[1]
        try:
            r = requests.get(
                f"https://stablecoins.llama.fi/stablecoin/{stable_id}", timeout=30)
            r.raise_for_status()
            data = r.json()
        except Exception as exc:
            log.warning("DefiLlama /stablecoin/%s fetch failed: %s", stable_id, exc)
            return hit[1] if hit else {}
        out: dict = {}
        for chain, payload in (data.get("chainBalances") or {}).items():
            series: dict = {}
            for pt in (payload.get("tokens") or []):
                try:
                    ts = int(pt["date"])
                    mc = (pt.get("circulating") or {}).get("peggedUSD")
                    if mc is None:
                        continue
                    series[pd.to_datetime(ts, unit="s").strftime("%Y-%m-%d")] = float(mc)
                except (KeyError, TypeError, ValueError):
                    continue
            if series:
                ch = _norm_chain(chain)
                if ch in out:
                    out[ch].update(series)
                else:
                    out[ch] = series
        _DL_CACHE[key] = (now, out)
        return out


# ── All-chain stablecoins aggregate (DefiLlama per-chain + CoinGecko catalog) ──
# These power the All-chain → Stablecoins tab. Both sources are cached server-
# side (TTL=1h via @st.cache_data) — stablecoin chain totals only move a few
# percent intra-day, and the existing 4h GitHub Actions pull cron doesn't touch
# these endpoints. CoinGecko Pro is required for the catalog (free tier rate-
# limits would knock /coins/markets calls out of any meaningful TTL window).

# Top chains we render in the stacked-area breakdown. Picked by DefiLlama
# /stablecoincharts/{chain} current totals — these 10 cover ~95% of stablecoin
# MC on chains DefiLlama tracks. Order is intentional: largest at the bottom
# of the stack so smaller chains read on top. Tron's chart endpoint reports
# only ~$1.4B vs the $90B in its /stablecoins catalog because DefiLlama's per-
# chain chart counts USDT-Tron under a different chain alias (a known platform
# quirk — see DefiLlama's stablecoin bridging tracker for the breakdown).
_ALL_CHAIN_STABLE_TOP = [
    "Ethereum", "Solana", "Hyperliquid L1", "BSC", "Base",
    "Arbitrum", "Polygon", "Tron", "Avalanche", "Aptos",
]

@st.cache_data(ttl=3600, show_spinner=False)
def _fetch_stablecoin_chain_chart(chain: str) -> pd.DataFrame:
    """DefiLlama /stablecoincharts/{chain} → DataFrame[date, mc_usd]. Returns
    an empty DataFrame on any error so callers can skip the chain without
    blowing up the whole stack."""
    try:
        r = requests.get(
            f"https://stablecoins.llama.fi/stablecoincharts/{chain}", timeout=30)
        r.raise_for_status()
        pts = r.json() or []
    except Exception as exc:
        log.warning("DefiLlama stablecoincharts/%s failed: %s", chain, exc)
        return pd.DataFrame(columns=["date", "mc_usd"])
    rows = []
    for p in pts:
        try:
            d  = pd.to_datetime(int(p["date"]), unit="s").strftime("%Y-%m-%d")
            mc = float((p.get("totalCirculatingUSD") or {}).get("peggedUSD") or 0)
            if mc > 0:
                rows.append({"date": d, "mc_usd": mc})
        except (KeyError, TypeError, ValueError):
            continue
    return pd.DataFrame(rows)


@st.cache_data(ttl=3600, show_spinner=False)
def _fetch_cg_stablecoins_catalog(per_page: int = 50) -> pd.DataFrame:
    """CoinGecko Pro /coins/markets?category=stablecoins → top-N catalog.
    Empty DataFrame if no API key is configured or the call fails."""
    key = settings.coingecko_api_key
    if not key:
        return pd.DataFrame()
    try:
        r = requests.get(
            "https://pro-api.coingecko.com/api/v3/coins/markets",
            params={"vs_currency": "usd", "category": "stablecoins",
                    "order": "market_cap_desc", "per_page": per_page, "page": 1,
                    "sparkline": "false"},
            headers={"x-cg-pro-api-key": key}, timeout=30)
        r.raise_for_status()
        rows = r.json() or []
    except Exception as exc:
        log.warning("CoinGecko stablecoins catalog fetch failed: %s", exc)
        return pd.DataFrame()
    df = pd.DataFrame([{
        "rank":             c.get("market_cap_rank"),
        "symbol":           (c.get("symbol") or "").upper(),
        "name":             c.get("name"),
        "market_cap_usd":   c.get("market_cap") or 0,
        "vol_24h_usd":      c.get("total_volume") or 0,
        "price_usd":        c.get("current_price") or 0,
        "mc_change_24h_pct": c.get("market_cap_change_percentage_24h") or 0,
    } for c in rows])
    return df


def _render_all_chain_stablecoins() -> None:
    """The new All-chain → Stablecoins composite view. Three blocks:
      1. Headline metric — total stablecoin MC on the top tracked chains
      2. Per-chain stacked area chart — historical MC split by chain
      3. CoinGecko top-50 catalog table — global MC + 24h vol per stablecoin
    Data sources: DefiLlama per-chain historical, CoinGecko Pro for catalog."""
    # ── Pull per-chain frames (concurrent-safe via @st.cache_data) ─────────
    chain_frames: dict[str, pd.DataFrame] = {}
    with st.spinner("Loading stablecoin chain history…"):
        for ch in _ALL_CHAIN_STABLE_TOP:
            df = _fetch_stablecoin_chain_chart(ch)
            if not df.empty:
                chain_frames[ch] = df

    if not chain_frames:
        st.warning("DefiLlama stablecoin endpoints returned no data — try again "
                   "in a moment (their stablecoins.llama.fi host sometimes 5xxs).")
        return

    # ── Wide frame: one column per chain ───────────────────────────────────
    wide = None
    for ch, df in chain_frames.items():
        renamed = df.rename(columns={"mc_usd": f"mc_{ch}_usd"})
        renamed["date"] = pd.to_datetime(renamed["date"])
        wide = renamed if wide is None else wide.merge(renamed, on="date", how="outer")
    wide = wide.sort_values("date").reset_index(drop=True)
    mc_cols = [c for c in wide.columns if c.startswith("mc_") and c.endswith("_usd")]

    # Drop trailing partial day(s): DefiLlama's per-chain stablecoin endpoint
    # serves incomplete data for the current UTC day — chains report values
    # 30-50% lower than yesterday until their daily aggregation completes
    # overnight. Without this trim the headline metric shows e.g. \$120B on
    # the partial day vs \$305B on the prior complete day. Walk backwards
    # dropping any trailing row whose total < 90% of the prior row.
    while len(wide) >= 2:
        last_total = float(wide.iloc[-1][mc_cols].fillna(0).sum())
        prev_total = float(wide.iloc[-2][mc_cols].fillna(0).sum())
        if prev_total > 0 and last_total < 0.9 * prev_total:
            wide = wide.iloc[:-1].reset_index(drop=True)
        else:
            break

    # ── Headline metric: latest complete day across tracked chains ─────────
    # Use ffilled values so chains that didn't update on the latest complete
    # day still contribute their last-known MC (matches what the chart shows
    # — keeps the metric and the stack visually consistent).
    wide_filled = wide.copy()
    for c in mc_cols:
        wide_filled[c] = wide_filled[c].ffill().fillna(0.0)
    latest_row    = wide_filled.iloc[-1]
    total_latest  = float(latest_row[mc_cols].sum())
    prev_row      = wide_filled.iloc[-2] if len(wide_filled) > 1 else latest_row
    prev_total    = float(prev_row[mc_cols].sum())
    delta         = total_latest - prev_total
    pct           = (delta / prev_total * 100) if prev_total else 0
    sign_dollar   = f"+${delta/1e9:.2f}B" if delta >= 0 else f"-${abs(delta)/1e9:.2f}B"
    asof          = latest_row.get("date")
    asof_str      = pd.to_datetime(asof).strftime("%Y-%m-%d") if pd.notna(asof) else "?"
    st.metric(
        f"Total stablecoin MC on top {len(chain_frames)} chains  (as of {asof_str})",
        f"${total_latest/1e9:.2f}B",
        delta=f"{sign_dollar}  ({pct:+.2f}%)",
    )

    # ── Stacked area: per-chain breakdown over time ────────────────────────
    st.subheader("Stablecoin Market Cap by Chain — Stacked Historical")
    st.caption(
        "Per-chain stablecoin circulating supply, sourced from "
        "DefiLlama `/stablecoincharts/{chain}`. Top 10 chains by latest MC "
        "shown stacked; bottom layer is the largest chain. CoinGecko doesn't "
        "expose a per-chain split, so this view uses DefiLlama exclusively. "
        "Tron under-reports here vs its catalog total (~$90B) — DefiLlama's "
        "per-chain endpoint counts USDT-Tron under a different chain bucket."
    )
    fig = go.Figure()
    # Sort chains by latest MC so the stack reads largest-at-bottom
    sorted_chains = sorted(chain_frames.keys(),
                           key=lambda c: -float(latest_row.get(f"mc_{c}_usd", 0) or 0))
    palette = ["#FF8C42", "#5BC0EB", "#7DCE82", "#9B5DE5", "#F15BB5",
               "#FEE440", "#00BBF9", "#00F5D4", "#FB8B24", "#A4036F"]
    for i, ch in enumerate(sorted_chains):
        col = f"mc_{ch}_usd"
        y = wide[col].ffill().fillna(0.0)
        fig.add_trace(go.Scatter(
            x=wide["date"], y=y, name=ch,
            mode="lines", line=dict(width=0.8, color=palette[i % len(palette)]),
            stackgroup="stables", hoverinfo="x+y+name",
        ))
    # Pad y-axis 10% above the stacked peak so the topmost ribbon isn't
    # flush with the top tick (see render_market_cap_chain for context).
    totals = wide[mc_cols].ffill().fillna(0).sum(axis=1)
    stacked_max = float(totals.max() or 0)
    # Bonus "Total" line in the unified hover tooltip — see
    # render_market_cap_chain for the rationale.
    fig.add_trace(go.Scatter(
        x=wide["date"], y=totals, name="Total",
        mode="lines", line=dict(width=0, color="rgba(0,0,0,0)"),
        showlegend=False, stackgroup=None,
        customdata=totals.map(_fmt_usd),
        hovertemplate="<b>Total: %{customdata}</b><extra></extra>",
    ))
    fig.update_layout(
        height=460, hovermode="x unified",
        margin=dict(t=10, b=10, l=10, r=10),
        legend=dict(orientation="h", yanchor="bottom", y=1.02,
                    xanchor="right", x=1),
        yaxis=dict(tickprefix="$",
                   tickformat="~s", showgrid=True,
                   range=[0, stacked_max * 1.10] if stacked_max > 0 else None,
                   rangemode="tozero"),
    )
    _chart(fig, use_container_width=True)

    # ── CoinGecko top-N table ──────────────────────────────────────────────
    st.subheader("Top Stablecoins by Global Market Cap")
    st.caption(
        "Source: CoinGecko Pro `/coins/markets?category=stablecoins`. "
        "MC is **global** (summed across every chain the token is deployed on); "
        "for the per-chain split see the stacked chart above."
    )
    cat = _fetch_cg_stablecoins_catalog(per_page=50)
    if cat.empty:
        st.info("CoinGecko catalog unavailable — verify COINGECKO_API_KEY is set "
                "and the Pro key is valid.")
        return
    # Render as a sortable dataframe with formatted columns
    cat_disp = cat.copy()
    cat_disp["MC"]       = cat_disp["market_cap_usd"].map(lambda v: f"${v/1e9:.2f}B")
    cat_disp["24h Vol"]  = cat_disp["vol_24h_usd"].map(lambda v: f"${v/1e9:.2f}B")
    cat_disp["Price"]    = cat_disp["price_usd"].map(lambda v: f"${v:,.4f}")
    cat_disp["24h Δ %"]  = cat_disp["mc_change_24h_pct"].map(lambda v: f"{v:+.2f}%")
    st.dataframe(
        cat_disp[["symbol","name","MC","24h Vol","24h Δ %"]]
            .rename(columns={"symbol":"Symbol","name":"Name"}),
        use_container_width=True, hide_index=True, height=520,
    )


def _apply_time_controls(fig: go.Figure) -> go.Figure:
    """Attach a date rangeslider + quick-range buttons (1M/3M/6M/YTD/1Y/All)
    to a time-series Plotly figure so viewers can scope the visible window
    without us baking a fixed range into the layout.

    Applied via a tiny wrapper around every _chart() call in this
    module — keeps every chart consistent without each builder having to
    reimplement the same control config. Idempotent: re-calling on an
    already-decorated fig just overwrites with the same dict.

    Style notes:
      • thickness=0.05 keeps the slider strip slim (~5% of chart height)
        so it doesn't eat into the data area on the small 300px charts.
      • Buttons sit above the plot area (y=1.12) with a dark translucent
        background that reads on both light and dark themes.
      • activecolor matches the Birdeye Peak orange used elsewhere.
    """
    fig.update_xaxes(
        type="date",
        rangeslider=dict(
            visible=True,
            thickness=0.06,
            # Visible outline so the slider strip reads as its own UI band
            # rather than blending into the chart background.
            bordercolor="#888888",
            borderwidth=1,
            # Translucent dark fill — lets the auto-rendered mini-chart
            # traces inside read as desaturated grey on the dark theme
            # instead of the full-saturation colors of the main chart.
            bgcolor="rgba(30,30,30,0.6)",
        ),
        rangeselector=dict(
            buttons=[
                dict(count=1,  label="1M",  step="month", stepmode="backward"),
                dict(count=3,  label="3M",  step="month", stepmode="backward"),
                dict(count=6,  label="6M",  step="month", stepmode="backward"),
                dict(count=1,  label="YTD", step="year",  stepmode="todate"),
                dict(count=1,  label="1Y",  step="year",  stepmode="backward"),
                dict(step="all", label="All"),
            ],
            bgcolor="rgba(40,40,40,0.7)",
            activecolor="#FFA500",
            font=dict(color="white", size=11),
            x=0, y=1.12, xanchor="left", yanchor="top",
        ),
    )
    return fig


def _apply_b_format_to_yaxes(fig: go.Figure, fmt_mode: str = "currency") -> go.Figure:
    """Replace Plotly's SI tick labels (G for giga) with the more readable
    B / M / K suffixes that finance dashboards use. Plotly's `tickformat='~s'`
    has no `B` option in its D3 vocabulary, so the only way to get a `B`
    label is to override `tickvals` + `ticktext` explicitly.

    Walks every y-axis on the figure (yaxis, yaxis2, …). If the axis already
    has explicit tickvals (e.g. _build_fig precomputes them), reformat those.
    Otherwise generate ~6 nice-step ticks from the axis's explicit range, or
    fall back to scanning trace y values when no range was set.

    `fmt_mode` controls the prefix on each label:
      • "currency" (default) — '$1.5B', '$45.0M' etc. for USD values
      • "count"              — '1.5B', '45.0M' etc. for integer counts
                                (holder counts, # of transactions, etc.) —
                                no '$' prefix and no decimal places under 10

    Safe to no-op when there's no data and idempotent — re-calling on an
    already-decorated fig overwrites with the same tickvals/ticktext."""
    import math

    def fmt(v) -> str:
        try:
            v = float(v)
        except (TypeError, ValueError):
            return ""
        prefix = "$" if fmt_mode == "currency" else ""
        if v == 0:
            return f"{prefix}0"
        a = abs(v); sign = "-" if v < 0 else ""
        if a >= 1e12: return f"{sign}{prefix}{a/1e12:.1f}T"
        if a >= 1e9:  return f"{sign}{prefix}{a/1e9:.1f}B"
        if a >= 1e6:  return f"{sign}{prefix}{a/1e6:.1f}M"
        if a >= 1e3:  return f"{sign}{prefix}{a/1e3:.1f}K"
        if fmt_mode == "count":
            return f"{sign}{prefix}{a:,.0f}"
        return f"{sign}{prefix}{a:.2f}" if a < 10 else f"{sign}{prefix}{a:.0f}"

    def nice_ticks(lo: float, hi: float, target: int = 6) -> list[float]:
        if hi <= lo:
            return []
        rough = (hi - lo) / target
        mag = 10 ** math.floor(math.log10(rough)) if rough > 0 else 1
        step = 10 * mag
        for nice in (1, 2, 2.5, 5, 10):
            if rough / mag <= nice:
                step = nice * mag
                break
        ticks: list[float] = []
        v = math.ceil(lo / step) * step
        while v <= hi * 1.001:
            ticks.append(v)
            v += step
        return ticks

    for ax_name in ("yaxis", "yaxis2", "yaxis3", "yaxis4"):
        try:
            ax = fig.layout[ax_name]
        except (KeyError, AttributeError):
            continue
        if ax is None:
            continue
        # Use existing tickvals when the chart pre-computed them
        # (e.g. _build_fig does); otherwise derive from range or trace data.
        vals = list(ax.tickvals) if ax.tickvals else None
        if vals is None:
            rng = ax.range
            if rng is not None and len(rng) == 2:
                vals = nice_ticks(float(rng[0]), float(rng[1]))
            else:
                ax_id = "y" if ax_name == "yaxis" else "y" + ax_name[len("yaxis"):]
                ys: list[float] = []
                for tr in fig.data:
                    t_yaxis = getattr(tr, "yaxis", None) or "y"
                    if t_yaxis != ax_id:
                        continue
                    y = getattr(tr, "y", None)
                    if y is None:
                        continue
                    for v in y:
                        try:
                            f = float(v)
                            if f == f:  # exclude NaN
                                ys.append(f)
                        except (TypeError, ValueError):
                            continue
                if ys:
                    vals = nice_ticks(0.0, max(ys) * 1.05)
        if not vals:
            continue
        ax.tickmode = "array"
        ax.tickvals = vals
        ax.ticktext = [fmt(v) for v in vals]
        # Strip any prior formatting that would conflict with our explicit
        # labels (Plotly otherwise re-applies tickprefix/tickformat on top
        # of ticktext, producing weird "$$1.5B" style labels).
        if ax.tickformat:
            ax.tickformat = ""
        if ax.tickprefix:
            ax.tickprefix = ""
    return fig


def _chart(fig: go.Figure, fmt_mode: str = "currency",
           raw_df: pd.DataFrame | None = None,
           raw_key: str | None = None,
           raw_fmt: dict | None = None,
           raw_filename: str | None = None,
           **kwargs) -> None:
    """Render a time-series Plotly fig with the standard time controls
    (rangeslider + 1M/3M/6M/YTD/1Y/All buttons) and B/M/K-formatted y-axis
    labels (vs Plotly's default SI which uses 'G' for billions).

    `fmt_mode` controls y-axis prefix:
      • "currency" (default) — '$' prefix on every tick (USD charts)
      • "count"              — bare integer labels (holder count etc.)

    When `raw_df` + `raw_key` are passed, prepends a 📋 button above the
    chart that pops open a dialog with the source DataFrame + a Download
    CSV button. `raw_key` must be globally unique across the page (used
    as Streamlit's widget key). `raw_fmt` is an optional Pandas Styler
    format dict; defaults to USD with thousands separator for every
    non-date column. `raw_filename` controls the downloaded CSV's name
    (defaults to raw_key)."""
    if raw_df is not None and raw_key is not None:
        if st.button("📋", key=f"raw_btn_{raw_key}", help="View raw data"):
            _raw_data_modal(raw_df, raw_fmt, raw_filename or raw_key)
    return st.plotly_chart(
        _apply_b_format_to_yaxes(_apply_time_controls(fig), fmt_mode=fmt_mode),
        **kwargs)


def _fmt_usd(v) -> str:
    """USD with K/M/B suffixes (never SI 'milli'); sub-$1 shown in cents."""
    try:
        v = float(v)
    except (TypeError, ValueError):
        return ""
    if math.isnan(v) or v == 0:
        return ""
    a = abs(v)
    if a >= 1e9:
        return f"${v / 1e9:,.2f}B"
    if a >= 1e6:
        return f"${v / 1e6:,.2f}M"
    if a >= 1e3:
        return f"${v / 1e3:,.2f}K"
    if a >= 1:
        return f"${v:,.2f}"
    return f"${v:.2f}"


# ── Optional historical market-cap seed ───────────────────────────────────────
# Backfill market-cap history (Birdeye Token Overview only gives a current value).
# Two ways to supply data, matched to a token by SYMBOL (case-insensitive) or MINT:
#   • Per token: a file  mc_seed_<symbol>.json  (e.g. mc_seed_usdc.json) holding
#     that token's raw API response.
#   • Combined:  mc_history_seed.json  →  { "<symbol-or-mint>": <token-data>, ... }
# Each token-data may be any of:
#   {"payload": {"mc": [...], "t": [...]}}   (t = unix seconds or ms)
#   {"mc": [...], "t": [...]}
#   {"YYYY-MM-DD": <market_cap_usd>, ...}
# Seeded values merge into each pull's per-token MC series and then persist via
# carry-forward (so they survive even if the file is later removed).
_MC_SEED_CACHE: dict = {"sig": None, "data": None}


def _normalize_mc_series(obj) -> dict:
    """Normalize one token's seed data to {YYYY-MM-DD: market_cap_float}."""
    if not isinstance(obj, dict):
        return {}
    payload = obj.get("payload") if isinstance(obj.get("payload"), dict) else obj
    mc, t = payload.get("mc"), payload.get("t")
    out: dict = {}
    if isinstance(mc, list) and isinstance(t, list):
        unit = "ms" if (t and t[0] and float(t[0]) > 1e12) else "s"
        for ts, v in zip(t, mc):
            try:
                if v is None:
                    continue
                out[pd.to_datetime(int(ts), unit=unit).strftime("%Y-%m-%d")] = float(v)
            except (TypeError, ValueError):
                continue
        return out
    for d, v in obj.items():                     # flat {date: mc}
        try:
            out[pd.to_datetime(d).strftime("%Y-%m-%d")] = float(v)
        except (TypeError, ValueError):
            continue
    return out


def _load_mc_seed() -> dict:
    import json, os, glob
    base = os.path.dirname(os.path.abspath(__file__))
    files = sorted(glob.glob(os.path.join(base, "mc_seed_*.json")))
    combined = os.path.join(base, "mc_history_seed.json")
    if os.path.exists(combined):
        files.append(combined)
    if not files:
        return {}
    sig = tuple((f, os.path.getmtime(f)) for f in files)
    if _MC_SEED_CACHE["data"] is not None and _MC_SEED_CACHE["sig"] == sig:
        return _MC_SEED_CACHE["data"]
    out: dict = {}
    for f in files:
        try:
            with open(f, "r") as fh:
                raw = json.load(fh)
        except Exception as exc:
            log.warning("seed file %s could not be read: %s", os.path.basename(f), exc)
            continue
        name = os.path.basename(f)
        if name.startswith("mc_seed_"):
            key = name[len("mc_seed_"):-len(".json")].lower()
            series = _normalize_mc_series(raw)
            if series:
                out.setdefault(key, {}).update(series)
        elif isinstance(raw, dict):              # combined file
            for k, v in raw.items():
                series = _normalize_mc_series(v)
                if series:
                    out.setdefault(str(k).lower(), {}).update(series)
    _MC_SEED_CACHE["sig"] = sig
    _MC_SEED_CACHE["data"] = out
    return out


class TokenGroupMetricsPuller(DataPuller):
    """
    Aggregates multiple Solana tokens into one group chart.
    · Left axis  (line)          : total daily market cap (sum across all tokens)
    · Right axis (stacked bars)  : per-token daily volume in USD

    Subclass (or use _make_stock_group_puller) to set GROUP_LABEL and TOKENS.

    Requires: BIRDEYE_API_KEY
    """

    GROUP       : str  = "tokenized_stocks"
    GROUP_LABEL : str  = ""
    TOKENS      : list = []     # list of (display_name, address)
    START_TS    : int  = int(datetime(2024, 1, 1).timestamp())

    # Optional historical market-cap series. "" = no MC line (Birdeye has no
    # historical MC/supply); "coingecko" = pull daily MC from CoinGecko using
    # COINGECKO_IDS; "birdeye_overview" = snapshot Birdeye Token Overview;
    # "defillama" = pull per-chain history from DefiLlama (DEFILLAMA_TOKENS).
    MARKET_CAP_SOURCE : str  = ""
    COINGECKO_IDS     : dict = {}
    # Per-token DefiLlama lookup. Each entry is either
    #   {"type": "protocol",   "slug": "<slug>"}     → /protocol/{slug}    chainTvls.{chain}.tvl
    #   {"type": "stablecoin", "id":   <int>}        → /stablecoin/{id}    chainBalances.{chain}.tokens
    DEFILLAMA_TOKENS  : dict = {}
    # Tokens still pulled/cached but hidden from the charts (display only).
    HIDDEN_TOKENS     : frozenset = frozenset()
    # Per-chain hidden overrides: {chain_lower: {sym1, sym2}}. Used to hide
    # a token on ONE chain while keeping it visible on others (e.g. USDC
    # hidden on the Solana stablecoins chart but visible on Ethereum).
    HIDDEN_TOKENS_BY_CHAIN: dict = {}
    # If True, skip OHLCV/volume fetching entirely — only MC is pulled & cached.
    # Use for groups with no trading activity (e.g. tokenized treasuries / MMFs).
    SKIP_VOLUME       : bool = False

    _SOL_MINT  = "So11111111111111111111111111111111111111112"
    _USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
    _USDT_MINT = "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"

    # Birdeye Peak chart series — text-capable shades, legible on dark canvas
    _COLORS = [
        "#d2b58f",  # tan/7
        "#6F97D5",  # navy/6
        "#6FD58F",  # green/6
        "#D56F7C",  # crimson/6
        "#9590A0",  # purple/6
        "#cc8943",  # tan/5 (amber)
        "#56B276",  # green/5
        "#B25667",  # crimson/5
        "#567AB2",  # navy/5
        "#BBA383",  # earth/6
        "#2E9F59",  # green/4
        "#6C6678",  # purple/5
    ]

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _paginated_ohlcv(self, headers: dict, address: str, time_from: int,
                         time_to: int, endpoint: str = "token") -> list[dict]:
        # v3 token endpoint returns data for tokens (like Ondo) where v1 returns empty;
        # pair endpoint stays on the original /defi/ohlcv/pair path.
        if endpoint == "token":
            path      = "/defi/v3/ohlcv"
            time_key  = "unix_time"   # v3 uses unix_time, not unixTime
        else:
            path      = "/defi/ohlcv/pair"
            time_key  = "unixTime"
        rows: list[dict] = []
        t_from = time_from
        while t_from < time_to:
            try:
                params: dict = {"type": "1D", "time_from": t_from,
                                "time_to": time_to, "address": address}
                if endpoint == "token":
                    params["currency"] = "usd"
                resp = requests.get(
                    f"{self.settings.birdeye_base_url}{path}",
                    headers=headers, params=params, timeout=20,
                )
                resp.raise_for_status()
                # The v3 endpoint may return data as {"items": [...]} or as a bare
                # list directly under "data".  Handle both shapes defensively.
                raw_data = resp.json().get("data") or {}
                if isinstance(raw_data, list):
                    items = raw_data
                else:
                    items = raw_data.get("items", [])
            except Exception as exc:
                self.logger.warning("%s OHLCV fetch failed (%s): %s",
                                    self.GROUP_LABEL, address[:8], exc)
                break
            if not items:
                break
            rows.extend(items)
            if len(items) < 1000:
                break
            t_from = int(items[-1][time_key]) + 86_400
        return rows

    # ── Chain helpers ───────────────────────────────────────────────────────────
    # Map "logical" chain name (used in TOKENS tuples + column suffixes) to the
    # x-chain value Birdeye expects. Keeps Birdeye-API naming separate from the
    # canonical chain name used for column storage (which matches DefiLlama).
    _BIRDEYE_CHAIN_API = {
        "solana":   "solana",
        "ethereum": "ethereum",
        "binance":  "bsc",    # canonical = "Binance" (DefiLlama-style)
        "bsc":      "bsc",
        "base":     "base",
        "arbitrum": "arbitrum",
        "polygon":  "polygon",
        "optimism": "optimism",
        "avalanche":"avalanche",
        "sui":      "sui",
        "zksync":   "zksync",
    }

    @staticmethod
    def _birdeye_chain_for(address: str) -> str:
        """Infer Birdeye `x-chain` from address format. EVM = ethereum; else
        solana. Only used when a TOKENS entry has no explicit 3rd chain element."""
        a = str(address or "").strip()
        return "ethereum" if a.startswith("0x") else "solana"

    @staticmethod
    def _api_address(address: str) -> str:
        """Normalise an address for Birdeye URL params. Birdeye's Ethereum /
        BSC endpoints validate checksum casing strictly and will return
        ``address is invalid format`` for some valid EIP-55 checksums (e.g.
        USDC's 0xA0b…). Lowercasing every EVM address sidesteps that and is
        a no-op for Solana addresses (which are base58 and case-sensitive)."""
        a = str(address or "").strip()
        return a.lower() if a.startswith("0x") else a

    def _hidden_for_chain(self, chain: str | None) -> frozenset:
        """Effective hidden-tokens set for a chain: global HIDDEN_TOKENS plus
        any per-chain overrides in HIDDEN_TOKENS_BY_CHAIN. `chain=None`
        (all-chain view) uses HIDDEN_TOKENS only."""
        if not chain:
            return self.HIDDEN_TOKENS
        extra = self.HIDDEN_TOKENS_BY_CHAIN.get(str(chain).lower(), frozenset())
        if not extra:
            return self.HIDDEN_TOKENS
        return frozenset(self.HIDDEN_TOKENS | extra)

    @staticmethod
    def _token_chain(token_tuple) -> str:
        """Return the canonical chain (DefiLlama-style name) for a TOKENS row.

        TOKENS rows MUST be 3-tuples ``(symbol, address, chain)``. The chain
        element is required so the same address on multiple EVM chains (e.g.
        Backed.fi's 0x proxies on both Ethereum and BNB Chain) can be tracked
        separately. Common chain spellings ('BSC', 'BinanceSmartChain', etc.)
        are normalised to canonical 'Binance' so column suffixes stay
        consistent with the DefiLlama-written ``mc_<token>_binance_usd`` form.
        """
        if len(token_tuple) < 3 or not token_tuple[2]:
            raise ValueError(
                f"TOKENS row must be a (symbol, address, chain) 3-tuple — "
                f"got {token_tuple!r}. Add the chain explicitly (e.g. "
                f"'Solana', 'Ethereum', 'BinanceSmartChain')."
            )
        return _norm_chain(str(token_tuple[2]))

    def _birdeye_headers(self, address: str, chain: str | None = None) -> dict:
        """API headers with the right `x-chain`. Pass explicit `chain` when a
        token's address alone can't disambiguate (e.g. the same 0x address
        deployed on both Ethereum and BSC)."""
        if chain:
            key = chain.lower()
            api_chain = self._BIRDEYE_CHAIN_API.get(key, key)
        else:
            api_chain = self._birdeye_chain_for(address)
        return {
            "X-API-KEY": self.settings.birdeye_api_key,
            "x-chain":   api_chain,
        }

    def _fetch_circ_supply(self, headers: dict, address: str) -> float | None:
        try:
            r = requests.get(
                f"{self.settings.birdeye_base_url}/defi/v3/token/market-data",
                headers=headers, params={"address": address}, timeout=15,
            )
            r.raise_for_status()
            return r.json()["data"]["circulating_supply"]
        except Exception as exc:
            self.logger.warning("%s circ-supply fetch failed (%s): %s",
                                self.GROUP_LABEL, address[:8], exc)
            return None

    def _fetch_token_overview_mc(self, headers: dict, address: str) -> float | None:
        """Current Solana-only market cap from Birdeye Token Overview."""
        try:
            r = requests.get(
                f"{self.settings.birdeye_base_url}/defi/token_overview",
                headers=headers, params={"address": address}, timeout=20,
            )
            r.raise_for_status()
            d = r.json().get("data") or {}
            mc = d.get("marketCap", d.get("market_cap"))
            return float(mc) if mc is not None else None
        except Exception as exc:
            self.logger.warning("%s Token Overview MC fetch failed (%s): %s",
                                self.GROUP_LABEL, address[:8], exc)
            return None

    def _fetch_coingecko_mc(self, cg_id: str) -> dict[str, float]:
        """Return {date_str: market_cap_usd} from CoinGecko daily history.

        Birdeye has no historical market-cap/supply series, so MC is sourced
        from CoinGecko (asset-level, may be cross-chain — see UI caption).
        Cached process-wide (1 h TTL) and serialized via a lock so multiple
        scheduler threads don't trip CoinGecko's rate limit.

        Reads settings.coingecko_api_key (loaded once at module import). When
        present, routes to pro-api.coingecko.com with x-cg-pro-api-key and
        days=max for full history; otherwise falls back to the free public
        API with days=365.
        """
        now = time.time()
        with _CG_MC_LOCK:
            hit = _CG_MC_CACHE.get(cg_id)
            if hit and now - hit[0] < _CG_MC_TTL:
                return hit[1]
            cg_key = settings.coingecko_api_key
            if cg_key:
                base    = "https://pro-api.coingecko.com/api/v3"
                headers = {"x-cg-pro-api-key": cg_key}
                days    = "max"
            else:
                base    = "https://api.coingecko.com/api/v3"
                headers = {}
                days    = "365"
            caps = None
            for attempt in range(3):
                try:
                    r = requests.get(
                        f"{base}/coins/{cg_id}/market_chart",
                        params={"vs_currency": "usd", "days": days},
                        headers=headers, timeout=30,
                    )
                    r.raise_for_status()
                    caps = r.json().get("market_caps", []) or []
                    break
                except Exception as exc:
                    if attempt < 2:
                        time.sleep(12 * (attempt + 1))   # back off (esp. on HTTP 429)
                        continue
                    self.logger.warning("%s CoinGecko MC fetch failed (%s): %s",
                                        self.GROUP_LABEL, cg_id, exc)
                    return hit[1] if hit else {}   # serve stale cache if we have it
            out: dict[str, float] = {}
            for ts, mc in caps:
                if mc is None:
                    continue
                date = pd.to_datetime(ts, unit="ms").strftime("%Y-%m-%d")
                out[date] = mc   # last reading of the day wins
            if out:
                _CG_MC_CACHE[cg_id] = (now, out)
            return out

    def _fetch_sol_price_by_day(self, headers: dict, time_to: int) -> dict[str, float]:
        rows = self._paginated_ohlcv(headers, self._SOL_MINT,
                                     self.START_TS, time_to, endpoint="token")
        # v3 token endpoint uses unix_time field
        return {
            pd.to_datetime(r["unix_time"], unit="s").strftime("%Y-%m-%d"): r["c"]
            for r in rows
        }

    def _fetch_token_daily(self, headers: dict, time_to: int,
                           token_name: str, address: str,
                           circ_supply: float | None,
                           sol_by_day: dict) -> dict[str, dict]:
        """
        Returns {date: {price_usd, volume_usd, market_cap_usd}} for one token.
        Tries token-level OHLCV first; falls back to pair aggregation.
        sol_by_day dict is updated in-place if SOL prices are needed for the first time.
        """
        # ── Token-level OHLCV via v3 endpoint (fast path) ────────────────────
        rows = self._paginated_ohlcv(headers, address,
                                     self.START_TS, time_to, endpoint="token")
        if rows:
            result: dict[str, dict] = {}
            for r in rows:
                # v3 endpoint: unix_time field; v_usd is volume already in USD
                date   = pd.to_datetime(r["unix_time"], unit="s").strftime("%Y-%m-%d")
                price  = r["c"]
                # Use Birdeye's reported USD volume (v_usd); fall back to v × c only
                # when v_usd is absent/None on a given candle.
                vol    = r.get("v_usd")
                if vol is None:
                    vol = r["v"] * r["c"]
                # Market cap is intentionally omitted: Birdeye's circulating_supply
                # for tokenized stock tokens equals the underlying company's total
                # shares outstanding, making price × supply equal to the full company
                # market cap — not the on-chain token market cap.
                result[date] = {"price_usd": price, "volume_usd": vol,
                                "market_cap_usd": None}
            return result

        # ── Pair-level fallback ────────────────────────────────────────────────
        self.logger.info("%s / %s: token OHLCV empty — using pair fallback",
                         self.GROUP_LABEL, token_name)
        try:
            r = requests.get(
                f"{self.settings.birdeye_base_url}/defi/v2/markets",
                headers=headers,
                params={"address": address, "sort_by": "volume24h",
                        "sort_type": "desc", "limit": 20},
                timeout=15,
            )
            r.raise_for_status()
            pairs = r.json().get("data", {}).get("items", [])
        except Exception as exc:
            self.logger.warning("%s markets fetch failed (%s): %s",
                                self.GROUP_LABEL, address[:8], exc)
            return {}

        STABLE = {self._USDC_MINT, self._USDT_MINT}
        relevant: list[dict] = []
        need_sol = False
        for p in pairs:
            base  = p.get("base",  {}).get("address", "")
            quote = p.get("quote", {}).get("address", "")
            if base != address:
                continue
            if quote in STABLE:
                relevant.append({"address": p["address"], "quote": "usd"})
            elif quote == self._SOL_MINT:
                relevant.append({"address": p["address"], "quote": "sol"})
                need_sol = True

        if not relevant:
            return {}

        if need_sol and not sol_by_day:
            self.logger.info("%s: fetching SOL price history", self.GROUP_LABEL)
            sol_by_day.update(self._fetch_sol_price_by_day(headers, time_to))

        vol_by_day:   dict[str, float]       = {}
        price_by_day: dict[str, list[float]] = {}
        for p_info in relevant:
            items = self._paginated_ohlcv(headers, p_info["address"],
                                          self.START_TS, time_to, endpoint="pair")
            for item in items:
                date  = pd.to_datetime(item["unixTime"], unit="s").strftime("%Y-%m-%d")
                close = item["c"]
                vol   = item["v"]
                if p_info["quote"] == "usd":
                    vol_usd   = vol * close
                    price_usd = close
                else:
                    sol_px = sol_by_day.get(date)
                    if sol_px is None:
                        continue
                    vol_usd   = vol * close * sol_px
                    price_usd = close * sol_px
                vol_by_day[date]   = vol_by_day.get(date, 0.0) + vol_usd
                price_by_day.setdefault(date, []).append(price_usd)

        result = {}
        for date in sorted(vol_by_day):
            prices = price_by_day.get(date, [])
            price  = sum(prices) / len(prices) if prices else None
            mktcap = None  # see note above — circ_supply is underlying company shares
            result[date] = {"price_usd": price, "volume_usd": vol_by_day[date],
                            "market_cap_usd": mktcap}
        return result

    # ── Public fetch ──────────────────────────────────────────────────────────
    def fetch(self) -> pd.DataFrame:
        if not self.TOKENS:
            return pd.DataFrame()

        time_to    = int(datetime.utcnow().timestamp())
        sol_by_day: dict[str, float] = {}   # fetched lazily, shared across tokens

        # Token data is keyed by (name, chain) so the same symbol can carry
        # separate Solana / Ethereum / BSC volume series without overwriting.
        token_data: dict[tuple[str, str], dict] = {}
        if not self.SKIP_VOLUME:
            for idx, tok in enumerate(self.TOKENS):
                token_name = tok[0]
                # Lowercase EVM addresses before any Birdeye call — checksum
                # casing 400s on some endpoints (see _api_address docstring).
                address    = self._api_address(tok[1] if len(tok) > 1 else "")
                chain      = self._token_chain(tok)
                # Small inter-token delay so we don't hit Birdeye's rate limit
                # when the group has many tokens (e.g. 264 Ondo contracts).
                if idx > 0:
                    time.sleep(0.12)
                # Per-token headers — explicit chain wins over address inference
                # so 0x… EVM tokens on BSC hit x-chain=bsc, not ethereum.
                h = self._birdeye_headers(address, chain)
                circ_supply = self._fetch_circ_supply(h, address)
                daily = self._fetch_token_daily(h, time_to, token_name, address,
                                                circ_supply, sol_by_day)
                token_data[(token_name, chain)] = daily

        all_dates: set[str] = set()
        for daily in token_data.values():
            all_dates.update(daily.keys())

        # ── Market cap ─────────────────────────────────────────────────────────
        # Birdeye has no historical MC. For "birdeye_overview" we snapshot the
        # *current* per-token market cap each pull (under today's date) and carry
        # forward the per-token series stored in the previous snapshot, so the MC
        # history accumulates from when tracking began. MC is shown on its own
        # chart (per token), so total_market_cap_usd is left None for the volume
        # chart. The legacy "coingecko" path still builds a summed total line.
        mc_by_date: dict[str, float] = {}        # coingecko: summed total per date
        mc_cols: list[str] = []                  # birdeye: per-token MC columns
        mc_cols_by_date: dict[str, dict] = {}    # birdeye: {date: {col: mc}}
        # ── (0) Historical seed (mc_history_seed.json + per-token files) ────
        # Runs for EVERY puller regardless of MARKET_CAP_SOURCE so treasuries
        # (which fetch MC only from DefiLlama, not Birdeye) still get the
        # local seed merged in. Seed values write to BOTH the legacy chain-
        # agnostic col (so render_market_cap() and other un-migrated charts
        # still see the data) AND the per-chain Solana col (so the per-chain
        # MC chart shows the long history). `setdefault` is used so any value
        # DefiLlama / carry-forward already supplied takes precedence — seed
        # only fills genuine gaps.
        _seed = _load_mc_seed()
        if _seed:
            for tok in self.TOKENS:
                token_name = tok[0]
                address    = tok[1] if len(tok) > 1 else ""
                chain      = self._token_chain(tok)
                ser = (_seed.get(token_name.lower())
                       or _seed.get(str(address).lower()))
                if not ser:
                    continue
                col_legacy = self._mc_col(token_name)
                col_chain  = self._mc_chain_col(token_name, chain)
                for d, mc in ser.items():
                    bucket = mc_cols_by_date.setdefault(d, {})
                    bucket.setdefault(col_legacy, mc)
                    bucket.setdefault(col_chain,  mc)
                for c in (col_legacy, col_chain):
                    if c not in mc_cols:
                        mc_cols.append(c)

        if self.MARKET_CAP_SOURCE == "birdeye_overview":
            # Legacy (chain-agnostic) MC col, one per UNIQUE symbol — kept so
            # the existing Solana per-token MC chart (render_market_cap) keeps
            # working without migration.
            _seen_names: set[str] = set()
            for tok in self.TOKENS:
                t = tok[0]
                if t in _seen_names: continue
                _seen_names.add(t)
                if self._mc_col(t) not in mc_cols:
                    mc_cols.append(self._mc_col(t))
            # (1) carry forward prior snapshot — preserve every mc_* / mcbe_*
            # column we previously stored so chain-suffixed series accumulate
            # over time, not just the legacy chain-agnostic cols.
            prior = self.get_latest()
            if prior is not None and not prior.empty:
                _carry_cols = [c for c in prior.columns
                               if c.startswith("mc_") and c.endswith("_usd")]
                for _, pr in prior.iterrows():
                    d = pd.to_datetime(pr["date"]).strftime("%Y-%m-%d")
                    for col in _carry_cols:
                        if pd.notna(pr.get(col)):
                            mc_cols_by_date.setdefault(d, {})[col] = float(pr[col])
                        if col not in mc_cols:
                            mc_cols.append(col)
            today = datetime.utcnow().strftime("%Y-%m-%d")
            for idx, tok in enumerate(self.TOKENS):
                token_name = tok[0]
                address    = self._api_address(tok[1] if len(tok) > 1 else "")
                chain      = self._token_chain(tok)
                if idx > 0:
                    time.sleep(0.1)   # gentle pacing for large groups
                mc = self._fetch_token_overview_mc(
                    self._birdeye_headers(address, chain), address)
                if mc is not None:
                    # Write to BOTH the legacy chain-agnostic col (Solana-only
                    # historically) AND the per-chain col. The per-chain col is
                    # what render_market_cap_chain reads for non-Solana tabs.
                    col_chain = self._mc_chain_col(token_name, chain)
                    mc_cols_by_date.setdefault(today, {})[col_chain] = mc
                    if col_chain not in mc_cols:
                        mc_cols.append(col_chain)
                    if chain.lower() == "solana":
                        col_legacy = self._mc_col(token_name)
                        mc_cols_by_date.setdefault(today, {})[col_legacy] = mc
            all_dates.update(mc_cols_by_date.keys())
        elif self.MARKET_CAP_SOURCE == "coingecko" and self.COINGECKO_IDS:
            for idx, tok in enumerate(self.TOKENS):
                token_name = tok[0]
                cg_id = self.COINGECKO_IDS.get(token_name)
                if not cg_id:
                    continue
                if idx > 0:
                    # Free tier needs ~2.5s spacing to stay under 30 calls/min;
                    # Pro tier (500+ calls/min) only needs a tiny gentle pace.
                    time.sleep(0.1 if settings.coingecko_api_key else 2.5)
                for date, mc in self._fetch_coingecko_mc(cg_id).items():
                    mc_by_date[date] = mc_by_date.get(date, 0.0) + mc
            all_dates.update(mc_by_date.keys())

        # ── Additive multi-chain DefiLlama (independent of MARKET_CAP_SOURCE) ──
        # Runs alongside birdeye_overview/coingecko if DEFILLAMA_TOKENS is set,
        # so Solana keeps its primary source while every chain DefiLlama covers
        # gets its own mc_<token>_<chain>_usd column.
        if self.DEFILLAMA_TOKENS:
            _seen_dl: set[str] = set()
            for tok in self.TOKENS:
                token_name = tok[0]
                if token_name in _seen_dl: continue
                _seen_dl.add(token_name)
                cfg = self.DEFILLAMA_TOKENS.get(token_name)
                if not cfg:
                    continue
                if cfg.get("type") == "stablecoin":
                    data = _fetch_dl_stablecoin(int(cfg.get("id")))
                elif cfg.get("type") == "protocol":
                    data = _fetch_dl_protocol(str(cfg.get("slug")))
                else:
                    continue
                for chain, series in data.items():
                    col = self._mc_chain_col(token_name, chain)
                    if col not in mc_cols:
                        mc_cols.append(col)
                    for d, mc in series.items():
                        mc_cols_by_date.setdefault(d, {})[col] = mc
            all_dates.update(mc_cols_by_date.keys())

        rows = []
        for date in sorted(all_dates):
            row: dict = {"date": date}
            total_mc = 0.0
            mc_valid = False
            # Track per-(name, chain) to emit chain-suffixed vol cols and the
            # legacy chain-agnostic col (Solana entry wins so pre-refactor
            # readers keep seeing the same value).
            _legacy_vol: dict[str, float] = {}
            for tok in self.TOKENS:
                token_name = tok[0]
                chain      = self._token_chain(tok)
                entry      = token_data.get((token_name, chain), {}).get(date, {})
                vol        = entry.get("volume_usd", 0.0) or 0.0
                safe_name  = token_name.lower().replace("-", "_").replace(" ", "_")
                ch_safe    = _chain_safe(chain)
                row[f"vol_{safe_name}_{ch_safe}_usd"] = vol
                # Legacy column (chain-agnostic): Solana entries always win;
                # otherwise the first non-Solana value seeds it.
                if chain.lower() == "solana" or token_name not in _legacy_vol:
                    _legacy_vol[token_name] = vol
                mc = entry.get("market_cap_usd")
                if mc is not None:
                    total_mc += mc
                    mc_valid  = True
            for token_name, vol in _legacy_vol.items():
                safe_name = token_name.lower().replace("-", "_").replace(" ", "_")
                row[f"vol_{safe_name}_usd"] = vol
            if mc_cols:
                day = mc_cols_by_date.get(date, {})
                for col in mc_cols:
                    row[col] = day.get(col)
                row["total_market_cap_usd"] = None   # MC shown on its own chart
            elif mc_by_date:
                row["total_market_cap_usd"] = mc_by_date.get(date)
            else:
                row["total_market_cap_usd"] = total_mc if mc_valid else None
            rows.append(row)

        return pd.DataFrame(rows) if rows else pd.DataFrame()

    # ── Helpers ───────────────────────────────────────────────────────────────

    @staticmethod
    def _resample(df: pd.DataFrame, period: str) -> pd.DataFrame:
        """Aggregate daily df to weekly ('W') or monthly ('M').
        Every vol_*_usd column (chain-agnostic legacy and chain-suffixed) is
        summed; total_market_cap_usd takes the last value."""
        col      = "week" if period == "W" else "month"
        vol_cols = [c for c in df.columns
                    if c.startswith("vol_") and c.endswith("_usd")]
        agg      = {c: "sum" for c in vol_cols}
        if "total_market_cap_usd" in df.columns:
            agg["total_market_cap_usd"] = "last"
        return (
            df.assign(**{col: df["date"].dt.to_period(period).dt.start_time})
            .groupby(col, as_index=False)
            .agg(agg)
            .rename(columns={col: "date"})
        )

    @staticmethod
    def _safe_col(token_name: str, chain: str | None = None) -> str:
        """Volume column name. When `chain` is given returns the chain-suffixed
        form (e.g. vol_mstrx_ethereum_usd) used by per-chain charts; without
        chain returns the legacy chain-agnostic form (vol_<name>_usd) that the
        original Solana-only chart used."""
        base = token_name.lower().replace('-', '_').replace(' ', '_')
        if chain:
            return f"vol_{base}_{_chain_safe(chain)}_usd"
        return f"vol_{base}_usd"

    @staticmethod
    def _mc_col(token_name: str) -> str:
        return f"mc_{token_name.lower().replace('-', '_').replace(' ', '_')}_usd"

    @staticmethod
    def _mc_chain_col(token_name: str, chain: str) -> str:
        """Per-chain MC column, e.g. mc_buidl_ethereum_usd. Chain matches
        DefiLlama's name normalized via _chain_safe()."""
        safe_t = token_name.lower().replace("-", "_").replace(" ", "_")
        return f"mc_{safe_t}_{_chain_safe(chain)}_usd"

    def _active_sorted_tokens(self, df: pd.DataFrame,
                              chain: str | None = None
                              ) -> list[tuple[str, str, str]]:
        """Return [(token_name, address, color)] for tokens with ≥ $0.01
        historical volume, sorted ascending by most-recent-day volume (bar
        stack order). When `chain` is given, restricts to TOKENS rows whose
        per-row chain matches and uses chain-suffixed volume columns; dedupes
        repeat symbols so a single name doesn't render twice."""
        seen: set[str] = set()
        active: list[tuple[str, str]] = []
        hidden = self._hidden_for_chain(chain)
        for tok in self.TOKENS:
            t = tok[0]
            a = tok[1] if len(tok) > 1 else ""
            if t in hidden or t in seen:
                continue
            if chain and self._token_chain(tok).lower() != chain.lower():
                continue
            col = self._safe_col(t, chain) if chain else self._safe_col(t)
            if col not in df.columns or df[col].sum() < 0.01:
                continue
            seen.add(t)
            active.append((t, a))
        if not active:
            return []
        last_row = df.sort_values("date").iloc[-1]
        col_fn = (lambda name: self._safe_col(name, chain)) if chain \
            else self._safe_col
        ordered = sorted(active, key=lambda ta: last_row.get(col_fn(ta[0]), 0.0))
        return [
            (t, a, self._COLORS[i % len(self._COLORS)])
            for i, (t, a) in enumerate(ordered)
        ]

    # ── Chart builder ─────────────────────────────────────────────────────────
    def _build_fig(self, df: pd.DataFrame,
                   sorted_tokens: list[tuple[str, str, str]],
                   height: int) -> go.Figure:
        def _aligned_ticks(vmax: float, n: int = 6) -> list[float]:
            if vmax <= 0 or not pd.notna(vmax):
                return [0.0] * n
            raw  = vmax / (n - 1)
            mag  = 10 ** math.floor(math.log10(raw))
            step = math.ceil(raw / mag) * mag
            return [i * step for i in range(n)]

        vol_cols = [self._safe_col(t) for t, _, _ in sorted_tokens]

        # ── Per-day top-10 + "Others" ──────────────────────────────────────
        # For each day keep only the 10 highest-volume tokens as named bars;
        # the rest are summed into a single "Others" bar so the hover tooltip
        # stays readable regardless of how many tokens are in the group.
        TOP_N = 10
        df_plot = df.copy()
        others_vals: list[float] = []

        for idx, row in df_plot.iterrows():
            day_vols = {
                t: row[self._safe_col(t)]
                for t, _, _ in sorted_tokens
                if pd.notna(row[self._safe_col(t)]) and row[self._safe_col(t)] > 0
            }
            top_set = set(
                sorted(day_vols, key=lambda k: day_vols[k], reverse=True)[:TOP_N]
            )
            others = 0.0
            for t, _, _ in sorted_tokens:
                col = self._safe_col(t)
                v   = row[col]
                if t not in top_set and pd.notna(v) and v > 0:
                    others += v
                    df_plot.at[idx, col] = float("nan")
            others_vals.append(others if others > 0 else float("nan"))

        df_plot["vol_others_usd"] = others_vals

        N = 6
        # vol_max uses original df so the axis scale covers the full stack height
        vol_max   = df[vol_cols].sum(axis=1).max() if vol_cols else 1.0
        vol_ticks = _aligned_ticks(vol_max, N)

        # Only draw the market cap line when the data actually contains valid values.
        # For tokenized stocks, market_cap_usd is always None because Birdeye's
        # circulating_supply field equals the underlying company's shares outstanding
        # (not the on-chain token supply), so price × supply = company market cap,
        # which is misleading here.
        has_mc = df["total_market_cap_usd"].notna().any()

        if has_mc:
            mc_max   = df["total_market_cap_usd"].dropna().max()
            mc_ticks = _aligned_ticks(mc_max, N)
            fig = make_subplots(specs=[[{"secondary_y": True}]])
        else:
            fig = make_subplots(specs=[[{"secondary_y": False}]])

        # "Others" bar at the bottom of the stack (first trace added).
        fig.add_trace(
            go.Bar(x=df_plot["date"], y=df_plot["vol_others_usd"],
                   name="Others", marker_color="#6C5A4C", opacity=0.5,
                   showlegend=False,
                   customdata=df_plot["vol_others_usd"].map(_fmt_usd),
                   hovertemplate="%{fullData.name}: %{customdata}<extra></extra>"),
            secondary_y=has_mc,
        )

        # Named top-10 bars — drawn ascending so the highest sits on top.
        # Zero values replaced with NaN: hidden from bar stack and hover tooltip.
        for token_name, _, color in sorted_tokens:
            col    = self._safe_col(token_name)
            y_vals = df_plot[col].replace(0, float("nan"))
            fig.add_trace(
                go.Bar(x=df_plot["date"], y=y_vals, name=token_name,
                       marker_color=color, opacity=0.7,
                       showlegend=False,
                       customdata=y_vals.map(_fmt_usd),
                       hovertemplate="%{fullData.name}: %{customdata}<extra></extra>"),
                secondary_y=has_mc,
            )

        # Invisible total-volume trace — appends a summary row at the bottom of the
        # unified hover tooltip so the viewer can read the full-stack total at a glance.
        daily_total = (
            df_plot[[self._safe_col(t) for t, _, _ in sorted_tokens]]
            .fillna(0).sum(axis=1)
            + df_plot["vol_others_usd"].fillna(0)
        ).replace(0, float("nan"))
        fig.add_trace(
            go.Scatter(
                x=df_plot["date"], y=daily_total,
                name="Total",
                mode="lines",
                line=dict(width=0),
                showlegend=False,
                customdata=daily_total.map(_fmt_usd),
                hovertemplate="<b>Total: %{customdata}</b><extra></extra>",
            ),
            secondary_y=has_mc,
        )

        # Total market cap line — only when valid data exists.
        if has_mc:
            fig.add_trace(
                go.Scatter(x=df["date"], y=df["total_market_cap_usd"],
                           name="Total Market Cap",
                           mode="lines+markers",
                           line=dict(color="#FFFFFF", width=2.5),
                           marker=dict(color="#FFFFFF", size=4),
                           connectgaps=True,
                           showlegend=False,
                           customdata=df["total_market_cap_usd"].map(_fmt_usd),
                           hovertemplate="%{fullData.name}: %{customdata}<extra></extra>"),
                secondary_y=False,
            )

        layout_kwargs: dict = dict(
            title="",
            barmode="stack",
            hovermode="x unified",
            height=height,
            showlegend=False,
            margin=dict(t=10, b=10, l=10, r=10),
        )
        if has_mc:
            layout_kwargs["yaxis"]  = dict(
                tickprefix="$", tickformat="~s",
                tickmode="array", tickvals=mc_ticks, range=[0, mc_ticks[-1]],
                showgrid=True,
            )
            layout_kwargs["yaxis2"] = dict(
                tickprefix="$", tickformat="~s",
                tickmode="array", tickvals=vol_ticks, range=[0, vol_ticks[-1]],
                showgrid=False, overlaying="y", side="right",
            )
        else:
            layout_kwargs["yaxis"] = dict(
                tickprefix="$", tickformat="~s",
                tickmode="array", tickvals=vol_ticks, range=[0, vol_ticks[-1]],
                showgrid=True,
            )

        fig.update_layout(**layout_kwargs)
        return fig

    # ── Render ────────────────────────────────────────────────────────────────
    def render(self) -> None:
        if not self.settings.birdeye_api_key:
            st.warning("Set BIRDEYE_API_KEY in your .env to enable live data.")

        df = self.get_latest()
        if df is None or df.empty:
            st.info("Waiting for first pull…")
            return

        df["date"] = pd.to_datetime(df["date"])
        sorted_tokens = self._active_sorted_tokens(df)

        _fmt = {"total_market_cap_usd": "${:,.0f}"}
        for _tok in self.TOKENS:
            _tn   = _tok[0]
            _safe = _tn.lower().replace("-", "_").replace(" ", "_")
            _fmt[f"vol_{_safe}_usd"] = "${:,.0f}"

        st.caption(f"Last pull: {df.attrs.get('pulled_at', '?')} UTC · Source: Birdeye")
        with st.container(key=f"chartwrap_{self.name}"):
            # Raw-data icon — pinned onto the tab row, far right (see CSS)
            if st.button("📋", key=f"raw_{self.name}", help="View raw data"):
                _raw_data_modal(df.sort_values("date", ascending=False), _fmt)
            tab_d, tab_w, tab_m = st.tabs(["Daily", "Weekly", "Monthly"])
            with tab_d:
                _chart(
                    self._build_fig(df, sorted_tokens, height=450),
                    use_container_width=True,
                )
            with tab_w:
                _chart(
                    self._build_fig(self._resample(df, "W"), sorted_tokens, height=450),
                    use_container_width=True,
                )
            with tab_m:
                _chart(
                    self._build_fig(self._resample(df, "M"), sorted_tokens, height=450),
                    use_container_width=True,
                )

        # Collapsible legend — collapsed by default so the chart gets full focus.
        with st.expander(f"Legend ({len(sorted_tokens)} tokens)", expanded=False):
            # "Others" entry first, then named tokens highest-vol first.
            legend_entries = [("Others", "#888888")] + [
                (name, color) for name, _, color in reversed(sorted_tokens)
            ]
            # Render swatches in a tight CSS grid: 8 columns, auto-wrapping rows.
            items_html = "".join(
                f'<div style="display:flex;align-items:center;gap:5px;white-space:nowrap">'
                f'<span style="display:inline-block;width:12px;height:12px;'
                f'border-radius:2px;background:{color};flex-shrink:0"></span>'
                f'<span style="font-size:0.8rem">{name}</span></div>'
                for name, color in legend_entries
            )
            st.markdown(
                f'<div style="display:grid;grid-template-columns:repeat(8,1fr);'
                f'gap:6px 16px;padding:4px 0">{items_html}</div>',
                unsafe_allow_html=True,
            )

    # ── Market-cap chart per chain (DefiLlama multi-chain data) ────────────────
    def render_market_cap_chain(self, chain: str | None = None,
                                stacked: bool = True) -> None:
        """Render MC per token for a specific chain (e.g. 'Solana', 'Ethereum',
        'Binance', 'Base'). When chain is None, sums across all chains per token
        ("all-chain" aggregate view)."""
        df = self.get_latest()
        if df is None or df.empty:
            st.info("Waiting for first pull…")
            return
        df = df.copy()
        df["date"] = pd.to_datetime(df["date"])

        # Map each visible token to a single per-chain series (or sum-across-chains).
        # Same symbol can appear in TOKENS twice (e.g. BUIDL on both Solana and
        # Ethereum addresses) — dedupe so the chart legend / area stack doesn't
        # show duplicates. The MC column lookup is keyed by (name, chain), not
        # address, so the second entry would produce identical data anyway.
        token_series: list[tuple[str, pd.Series]] = []
        _seen: set[str] = set()
        hidden = self._hidden_for_chain(chain)
        for _tok in self.TOKENS:
            token_name = _tok[0]
            if token_name in hidden or token_name in _seen:
                continue
            _seen.add(token_name)
            if chain is None:
                # Sum across every per-chain column for this token (= global MC).
                prefix = f"mc_{token_name.lower().replace('-','_').replace(' ','_')}_"
                cols = [c for c in df.columns
                        if c.startswith(prefix) and c.endswith("_usd")
                        and c != self._mc_col(token_name)]
                if not cols:
                    continue
                s = df[cols].sum(axis=1, min_count=1)
            else:
                col = self._mc_chain_col(token_name, chain)
                if col not in df.columns:
                    continue
                s = df[col]
            if not s.notna().any():
                continue
            token_series.append((token_name, s))

        if not token_series:
            scope = "any chain" if chain is None else chain
            st.info(f"No market-cap data yet for {scope}. Series will populate "
                    "on the next pull.")
            return

        # Restrict rows to those with at least one MC reading.
        keep = pd.concat([s for _, s in token_series], axis=1).notna().any(axis=1)
        mdf = df.loc[keep, ["date"]].copy().sort_values("date")
        for token_name, s in token_series:
            mdf[token_name] = s.loc[mdf.index].values

        # Pre-clip DefiLlama-style isolated 1-day spikes per token before any
        # rendering. Catches glitches like XAUM Solana 2026-03-27 (\$10.24M
        # between Mar 26 \$1.74M and Mar 28 \$5.20M — neighbor-mean replaces
        # it with \$3.47M). Operates on a copy of mdf so the raw cache is
        # never mutated.
        for _tn in [t for t, _ in token_series]:
            mdf[_tn] = self._clip_isolated_spikes(mdf[_tn])

        fig = go.Figure()
        for i, (token_name, _) in enumerate(token_series):
            color = self._COLORS[i % len(self._COLORS)]
            y = mdf[token_name]
            if stacked:
                # Forward-fill THEN fill leading NaNs with 0. ffill carries the
                # token's last good MC across mid-series gaps (e.g. cron-runner
                # outages on May 12-14 2026 where Solscan + Birdeye both
                # reported 0/missing for 3 days). fillna(0) only catches the
                # leading window before the token existed — those genuinely
                # are zero MC, not a data gap. Without ffill, every cron miss
                # collapses the stack to zero for that day, producing a fake
                # cliff in the area chart.
                y = y.ffill().fillna(0.0)
                fig.add_trace(go.Scatter(
                    x=mdf["date"], y=y, name=token_name,
                    mode="lines+markers",
                    line=dict(color=color, width=1.2),
                    marker=dict(color=color, size=4), stackgroup="mc",
                    customdata=y.map(_fmt_usd),
                    hovertemplate="%{fullData.name}: %{customdata}<extra></extra>",
                ))
            else:
                sub = mdf[["date", token_name]].dropna(subset=[token_name])
                fig.add_trace(go.Scatter(
                    x=sub["date"], y=sub[token_name], name=token_name,
                    mode="lines+markers",
                    line=dict(color=color, width=2),
                    marker=dict(color=color, size=5),
                    customdata=sub[token_name].map(_fmt_usd),
                    hovertemplate="%{fullData.name}: %{customdata}<extra></extra>",
                ))
        # Pad y-axis above stacked-peak. Plotly's autorange on stackgroup
        # figures sometimes picks the rounded nice-tick that equals the
        # actual peak (e.g. \$15G when stacked total = \$15.40B), clipping
        # the topmost ribbon. Compute the real stacked max + 10% headroom.
        # Also compute the row-wise total to drive the bonus "Total" line
        # in the unified hover tooltip (added below as an invisible trace).
        y_max = 0.0
        totals: pd.Series | None = None
        if stacked:
            token_names = [t for t, _ in token_series]
            totals = mdf[token_names].ffill().fillna(0).sum(axis=1)
            y_max = float(totals.max() or 0)
        else:
            for _t, _s in token_series:
                y_max = max(y_max, float(_s.max() or 0))
        y_range = [0, y_max * 1.10] if y_max > 0 else None

        # Invisible "Total" trace — adds a bold Total line to the unified
        # hover tooltip without drawing anything on the chart itself
        # (line.width=0 + showlegend=False). Added LAST so it renders at
        # the BOTTOM of the tooltip, summarizing the per-token rows above.
        if stacked and totals is not None:
            fig.add_trace(go.Scatter(
                x=mdf["date"], y=totals, name="Total",
                mode="lines", line=dict(width=0, color="rgba(0,0,0,0)"),
                showlegend=False, stackgroup=None,
                customdata=totals.map(_fmt_usd),
                hovertemplate="<b>Total: %{customdata}</b><extra></extra>",
            ))
        fig.update_layout(
            height=380, hovermode="x unified",
            margin=dict(t=10, b=10, l=10, r=10),
            legend=dict(orientation="h", yanchor="bottom", y=1.02,
                        xanchor="right", x=1),
            yaxis=dict(tickprefix="$",
                       tickformat="~s", showgrid=True,
                       range=y_range, rangemode="tozero"),
        )
        _chart(fig, use_container_width=True)

    @staticmethod
    def _clip_isolated_spikes(series: pd.Series, factor: float = 2.0) -> pd.Series:
        """Replace 1-day-only spikes with the linear interpolation between
        their immediate neighbors. Targeted at DefiLlama protocol-TVL
        glitches where one day's reported chain-MC briefly jumps 2-5× the
        surrounding window then snaps back (e.g. XAUM Solana 2026-03-27
        reported \$10.24M between Mar 26 \$1.74M and Mar 28 \$5.20M —
        Birdeye supply confirms ~400 oz, not the ~2,280 oz that \$10.24M
        would imply).

        A point qualifies as an isolated spike when:
          • value > factor × mean(prev, next), AND
          • value > prev AND value > next (so we only catch upward spikes
            sandwiched between lower neighbors)
        Both neighbors must be non-NaN (otherwise we can't form context).
        Replacement is mean(prev, next) — produces a smooth transition
        through what was likely the day's real mid-point. Returns a copy;
        the underlying cache is untouched.

        Default factor=2.0 is intentionally loose — XAUM's spike was 2.95×
        neighbor mean. Tighter thresholds (1.5×) catch more but risk
        clipping real fast-growth days; looser (3×) miss the glitch."""
        if len(series) < 3:
            return series
        out = series.copy()
        for i in range(1, len(series) - 1):
            v = series.iat[i]
            p = series.iat[i - 1]
            n = series.iat[i + 1]
            if pd.isna(v) or pd.isna(p) or pd.isna(n):
                continue
            neighbor_mean = (p + n) / 2.0
            if neighbor_mean <= 0:
                continue
            if v > factor * neighbor_mean and v > p and v > n:
                out.iat[i] = neighbor_mean
        return out

    @staticmethod
    def _clip_outliers(series: pd.Series, factor: float = 25.0,
                       min_retained: float = 0.5) -> pd.Series:
        """Replace points > factor × global non-zero median with NaN. Used to
        suppress Birdeye's occasional v_usd glitch days (e.g. a single bad
        pair trade or aggregator double-count) where the reported daily
        volume jumps 100-400× a token's normal range. The original cached
        data is untouched — only the chart view is filtered.

        Default factor=25 was tuned against the USDC + USDT Solana series:
        the late-Dec-2024 / early-Jan-2025 Birdeye glitch days hit 31-360×
        per-token median (clipped), while legitimate Jan 18-20 2025 TRUMP
        token-launch activity peaked at 19-20× (preserved). Lower factor
        clips real bursts; higher factor lets glitches through.

        `min_retained` is a safety guard for tokens with extremely skewed
        distributions (e.g. USDe Solana: median \$45K but max \$256M because
        secondary trading is sparse). For those, factor × median caps at a
        tiny number and the clip nukes 90%+ of total volume, hiding the
        token entirely. If the clip would retain less than `min_retained`
        of the total non-zero volume, skip clipping for this column."""
        nz = series[series.fillna(0) > 0]
        if nz.empty:
            return series
        threshold = float(nz.median()) * factor
        if threshold <= 0:
            return series
        total = float(nz.sum())
        kept  = float(nz[nz <= threshold].sum())
        if total > 0 and kept / total < min_retained:
            # Skewed distribution — clip would erase the token. Leave as-is.
            return series
        return series.mask(series > threshold)

    # ── Volume chart filtered to one chain (Birdeye OHLCV V3) ──────────────────
    def render_volume_chain(self, chain: str | None = None,
                            include_tokens: set[str] | None = None,
                            exclude_tokens: set[str] | None = None,
                            key_suffix: str = "",
                            clip_outliers: bool = False,
                            outlier_factor: float = 25.0) -> None:
        """Daily trading-volume chart restricted to tokens whose addresses
        live on `chain` (per `_birdeye_chain_for`). When chain=None every
        token in TOKENS is included regardless of source. Reuses _build_fig
        so the layout matches the Solana volume chart exactly.

        Optional `include_tokens` / `exclude_tokens` (sets of symbol names)
        further filter what's plotted — used to split a dominant token off
        into its own chart (e.g. USDC volume vs all other stablecoins on
        Solana, where USDC's cross-pair total dwarfs everything else).
        `key_suffix` is appended to the Streamlit container/button keys so
        the same puller + chain can be rendered multiple times on one tab
        without key collisions."""
        df = self.get_latest()
        if df is None or df.empty:
            st.info("Waiting for first pull…")
            return
        df = df.copy()
        df["date"] = pd.to_datetime(df["date"])

        # _active_sorted_tokens does the chain filtering + dedup + sort using
        # the chain-suffixed column (`vol_<name>_<chain>_usd`). When chain is
        # None we fall through to the legacy chain-agnostic column.
        sorted_tokens = self._active_sorted_tokens(df, chain=chain)
        if include_tokens:
            sorted_tokens = [t for t in sorted_tokens if t[0] in include_tokens]
        if exclude_tokens:
            sorted_tokens = [t for t in sorted_tokens if t[0] not in exclude_tokens]
        if not sorted_tokens:
            st.info(f"No trading volume recorded on {chain or 'any chain'} yet.")
            return

        st.caption(
            f"Last pull: {df.attrs.get('pulled_at', '?')} UTC · "
            f"Source: Birdeye OHLCV V3 (x-chain: {chain or 'all'})"
        )
        chain_tag = (chain or "all").lower().replace(" ", "_")
        if key_suffix:
            chain_tag = f"{chain_tag}_{key_suffix}"
        with st.container(key=f"chartwrap_{self.name}_vol_{chain_tag}"):
            # Raw-data icon — pinned via existing CSS rules.
            _fmt = {self._safe_col(t, chain): "${:,.0f}"
                    for t, _, _ in sorted_tokens}
            if st.button("📋", key=f"raw_{self.name}_vol_{chain_tag}",
                         help="View raw data"):
                _raw_data_modal(df.sort_values("date", ascending=False), _fmt)
            # Aliased view: rename the chain-suffixed col → legacy col name so
            # _build_fig (which reads _safe_col(name) = vol_<name>_usd) works
            # without modification. The legacy col is also written by fetch()
            # for Solana entries (back-compat) — if we keep both, pandas ends
            # up with two cols of the same name and `row[col]` returns a
            # Series instead of a scalar, blowing up the boolean test inside
            # _build_fig with "truth value of a Series is ambiguous". So we
            # drop the legacy cols *before* the rename to leave exactly one
            # column per token.
            if chain:
                legacy_cols = [self._safe_col(t) for t, _, _ in sorted_tokens]
                _aliases = {self._safe_col(t, chain): self._safe_col(t)
                            for t, _, _ in sorted_tokens}
                df_view = (df.drop(columns=legacy_cols, errors="ignore")
                             .rename(columns=_aliases))
            else:
                df_view = df

            # Optional outlier clip — drop spike days that exceed factor ×
            # global non-zero median for each active token. Targeted at
            # Birdeye v_usd glitches (e.g. USDC Solana Dec 2024 / Jan 2025
            # cluster with $200-450B reported vs ~\$1B normal). Applied
            # BEFORE the x-axis trim so the trim's row-total isn't itself
            # inflated by an outlier in the first non-zero day.
            active_cols = [self._safe_col(t) for t, _, _ in sorted_tokens
                           if self._safe_col(t) in df_view.columns]
            if clip_outliers:
                for col in active_cols:
                    df_view[col] = self._clip_outliers(
                        df_view[col], factor=outlier_factor)

            # Trim the x-axis to start at the first day with any non-zero
            # volume across the active tokens. Without this the chart shows
            # a long empty stretch (e.g. 2018-2024 for Solana stables that
            # were only deployed in 2024) because df carries the union of
            # all dates seen across every cached snapshot.
            if active_cols:
                _row_total = df_view[active_cols].fillna(0).sum(axis=1)
                _first_nz  = _row_total[_row_total > 0].index.min()
                if pd.notna(_first_nz):
                    df_view = df_view.loc[_first_nz:].reset_index(drop=True)

                # Drop cron-runner outage days: rows AFTER the first non-zero
                # day where every active token's volume is NaN-or-zero are
                # almost certainly missed-pull days (it's a near-impossible
                # coincidence for >5 tokens to all have $0 vol on the same
                # day in normal trading). Dropping the row lets Plotly draw
                # the stack continuous across the gap — without this, the
                # stacked-area collapses to zero on outage days and produces
                # a visible cliff (e.g. the May 12-14 2026 drop).
                _row_total2 = df_view[active_cols].fillna(0).sum(axis=1)
                _outage_mask = _row_total2 == 0
                if _outage_mask.any():
                    df_view = df_view.loc[~_outage_mask].reset_index(drop=True)

            tab_d, tab_w, tab_m = st.tabs(["Daily", "Weekly", "Monthly"])
            with tab_d:
                _chart(
                    self._build_fig(df_view, sorted_tokens, height=380),
                    use_container_width=True,
                )
            with tab_w:
                _chart(
                    self._build_fig(self._resample(df_view, "W"), sorted_tokens,
                                    height=380),
                    use_container_width=True,
                )
            with tab_m:
                _chart(
                    self._build_fig(self._resample(df_view, "M"), sorted_tokens,
                                    height=380),
                    use_container_width=True,
                )

    # ── Market-cap chart (per token) ────────────────────────────────────────────
    def render_market_cap(self, stacked: bool = False) -> None:
        """Per-token market cap from the cached MC series.
        stacked=False → one line per token; stacked=True → stacked area."""
        df = self.get_latest()
        if df is None or df.empty:
            st.info("Waiting for first pull…")
            return
        df = df.copy()
        df["date"] = pd.to_datetime(df["date"])
        # Dedupe by symbol — TOKENS can carry the same symbol on multiple
        # chains now (e.g. USDC on both Solana and Ethereum). Without dedup,
        # `present` would list the same mc_<sym>_usd column twice, df[col]
        # would return a 2-column DataFrame instead of a Series, and plotly
        # would crash with `narwhals DuplicateError`.
        present = []
        _seen: set[str] = set()
        for tok in self.TOKENS:
            name = tok[0]
            if name in self.HIDDEN_TOKENS or name in _seen:
                continue
            col = self._mc_col(name)
            if col in df.columns and df[col].notna().any():
                present.append((name, col))
                _seen.add(name)
        if not present:
            st.info("Market-cap history is building — a snapshot is cached each "
                    "pull, so the chart fills in over the coming days.")
            return

        # Rows that carry at least one MC reading (MC accrues from tracking start).
        mc_cols = [c for _, c in present]
        mdf = df.loc[df[mc_cols].notna().any(axis=1),
                     ["date"] + mc_cols].sort_values("date").copy()

        # Pre-clip isolated 1-day spikes (DefiLlama glitches like XAUM Solana
        # 2026-03-27) before rendering. See _clip_isolated_spikes docstring.
        for _col in mc_cols:
            mdf[_col] = self._clip_isolated_spikes(mdf[_col])

        fig = go.Figure()
        for i, (token_name, col) in enumerate(present):
            color = self._COLORS[i % len(self._COLORS)]
            if stacked:
                # See render_market_cap_chain comment: ffill carries each
                # token's MC across cron-runner outage days so the stack
                # doesn't collapse to zero. Leading NaNs (before token
                # existed) are then filled with 0.
                y = mdf[col].ffill().fillna(0.0)
                fig.add_trace(go.Scatter(
                    x=mdf["date"], y=y, name=token_name,
                    mode="lines+markers", line=dict(color=color, width=1.2),
                    marker=dict(color=color, size=5), stackgroup="mc",
                    customdata=y.map(_fmt_usd),
                    hovertemplate="%{fullData.name}: %{customdata}<extra></extra>",
                ))
            else:
                sub = mdf[["date", col]].dropna(subset=[col])
                fig.add_trace(go.Scatter(
                    x=sub["date"], y=sub[col], name=token_name,
                    mode="lines+markers",
                    line=dict(color=color, width=2),
                    marker=dict(color=color, size=5),
                    customdata=sub[col].map(_fmt_usd),
                    hovertemplate="%{fullData.name}: %{customdata}<extra></extra>",
                ))
        # Pad y-axis above stacked-peak — see render_market_cap_chain
        # for the rationale. Same shape: stacked uses per-row sum, line
        # uses per-trace max. Both get 10% headroom.
        y_max = 0.0
        totals: pd.Series | None = None
        if stacked:
            totals = mdf[mc_cols].ffill().fillna(0).sum(axis=1)
            y_max = float(totals.max() or 0)
        else:
            for _c in mc_cols:
                y_max = max(y_max, float(mdf[_c].max() or 0))
        y_range = [0, y_max * 1.10] if y_max > 0 else None

        # Bonus "Total" line in the unified hover tooltip — see
        # render_market_cap_chain for the rationale.
        if stacked and totals is not None:
            fig.add_trace(go.Scatter(
                x=mdf["date"], y=totals, name="Total",
                mode="lines", line=dict(width=0, color="rgba(0,0,0,0)"),
                showlegend=False, stackgroup=None,
                customdata=totals.map(_fmt_usd),
                hovertemplate="<b>Total: %{customdata}</b><extra></extra>",
            ))
        fig.update_layout(
            height=380, hovermode="x unified",
            margin=dict(t=10, b=10, l=10, r=10),
            legend=dict(orientation="h", yanchor="bottom", y=1.02,
                        xanchor="right", x=1),
            yaxis=dict(tickprefix="$",
                       tickformat="~s", showgrid=True,
                       range=y_range, rangemode="tozero"),
        )
        _chart(fig, use_container_width=True)


def _make_stock_group_puller(puller_name: str, label: str,
                              tokens: list[tuple[str, str]],
                              group: str = "tokenized_stocks",
                              market_cap_source: str = "",
                              coingecko_ids: dict | None = None,
                              defillama_tokens: dict | None = None,
                              hidden_tokens: set | None = None,
                              hidden_tokens_by_chain: dict | None = None,
                              skip_volume: bool = False) -> type:
    """Factory: return a TokenGroupMetricsPuller subclass for one group.
    `hidden_tokens_by_chain` is an optional per-chain hide list of the form
    {chain_lower: {sym, ...}} for symbols that should be hidden on some
    chains but visible on others (e.g. USDC on Solana but visible on
    Ethereum). Always-hidden symbols still go in `hidden_tokens`."""
    safe = puller_name.lower().replace("-", "_").replace(" ", "_")
    return type(
        f"{label.replace(' ', '').replace('-', '')}GroupMetricsPuller",
        (TokenGroupMetricsPuller,),
        {
            "name"                 : f"{safe}_metrics",
            "GROUP"                : group,
            "GROUP_LABEL"          : label,
            "TOKENS"               : tokens,
            "MARKET_CAP_SOURCE"    : market_cap_source,
            "COINGECKO_IDS"        : coingecko_ids or {},
            "DEFILLAMA_TOKENS"     : defillama_tokens or {},
            "HIDDEN_TOKENS"        : frozenset(hidden_tokens or ()),
            "HIDDEN_TOKENS_BY_CHAIN": {
                str(k).lower(): frozenset(v or ())
                for k, v in (hidden_tokens_by_chain or {}).items()
            },
            "SKIP_VOLUME"          : bool(skip_volume),
        },
    )


# ── Tokenized stock group registry (puller_name, display_label, [(token, address)]) ──
_TOKENIZED_STOCK_GROUPS: list[tuple[str, str, list]] = [
    (
        "prestocks_group",
        "PreStocks",
        [
            ("ANTHROPIC", "Pren1FvFX6J3E4kXhJuCiAD5aDmGEb7qJRncwA8Lkhw", "Solana"),
            ("ANDURIL", "PresTj4Yc2bAR197Er7wz4UUKSfqt6FryBEdAriBoQB", "Solana"),
            ("OPENAI", "PreweJYECqtQwBtpxHL171nL2K6umo692gTm7Q3rpgF", "Solana"),
            ("XAI", "PreC1KtJ1sBPPqaeeqL6Qb15GTLCYVvyYEwxhdfTwfx", "Solana"),
            ("SPACEX", "PreANxuXjsy2pvisWWMNB6YaJNzr7681wJJr2rHsfTh", "Solana"),
            ("KALSHI", "PreLWGkkeqG1s4HEfFZSy9moCrJ7btsHuUtfcCeoRua", "Solana"),
            ("POLYMARKET", "Pre8AREmFPtoJFT8mQSXQLh56cwJmM7CFDRuoGBZiUP", "Solana"),
        ],
    ),
    (
        "xstocks_group",
        "xStocks",
        [
            ("AAPLx", "XsbEhLAtcf6HdfpFZ5xEMdqW8nfAvcsP5bdudRLJzJp", "Solana"),
            ("ABBVx", "XswbinNKyPmzTa5CskMbCPvMW6G5CMnZXZEeQSSQoie", "Solana"),
            ("ABTx", "XsHtf5RpxsQ7jeJ9ivNewouZKJHbPxhPoEy6yYvULr7", "Solana"),
            ("ACNx", "Xs5UJzmCRQ8DWZjskExdSQDnbE6iLkRu2jjrRAB1JSU", "Solana"),
            ("AMBRx", "XsaQTCgebC2KPbf27KUhdv5JFvHhQ4GDAPURwrEhAzb", "Solana"),
            ("AMDx", "XsXcJ6GZ9kVnjqGsjBnktRcuwMBmvKWh8S93RefZ1rF", "Solana"),
            ("AMZNx", "Xs3eBt7uRfJX8QUs4suhyU8p2M6DoUDrJyWBa8LLZsg", "Solana"),
            ("APPx", "XsPdAVBi8Zc1xvv53k4JcMrQaEDTgkGqKYeh7AYgPHV", "Solana"),
            ("AVGOx", "XsgSaSvNSqLTtFuyWPBhK9196Xb9Bbdyjj4fH3cPJGo", "Solana"),
            ("AZNx", "Xs3ZFkPYT2BN7qBMqf1j1bfTeTm1rFzEFSsQ1z3wAKU", "Solana"),
            ("BACx", "XswsQk4duEQmCbGzfqUUWYmi7pV7xpJ9eEmLHXCaEQP", "Solana"),
            ("BMNRx", "XsrBCwaH8c46xiqXBChzobgufRKxQxAWUWbndgBNzFn", "Solana"),
            ("BRK.Bx", "Xs6B6zawENwAbWVi7w92rjazLuAr5Az59qgWKcNb45x", "Solana"),
            ("BTBTx", "XsPLBFy59Q3hY59KLAJur8QyvziMF4xUxGTxXqXE7cT", "Solana"),
            ("BTGOx", "XsvHMmbDcd14DHHW16PkxPGW7ks77ehxUv1E9Zmxgj4", "Solana"),
            ("CMCSAx", "XsvKCaNsxg2GN8jjUmq71qukMJr7Q1c5R2Mk9P8kcS8", "Solana"),
            ("COINx", "Xs7ZdzSHLU9ftNJsii5fCeJhoRWSC32SQGzGQtePxNu", "Solana"),
            ("COPXx", "XsybfiKkD4UmjkAGT2uR8X2sq9AWFtvGJM2KTffoALZ", "Solana"),
            ("CRCLx", "XsueG8BtpquVJX9LVLLEGuViXUungE6WmK5YZ3p3bd1", "Solana"),
            ("CRMx", "XsczbcQ3zfcgAEt9qHQES8pxKAVG5rujPSHQEXi4kaN", "Solana"),
            ("CRWDx", "Xs7xXqkcK7K8urEqGg52SECi79dRp2cEKKuYjUePYDw", "Solana"),
            ("CSCOx", "Xsr3pdLQyXvDJBFgpR5nexCEZwXvigb8wbPYp4YoNFf", "Solana"),
            ("CVXx", "XsNNMt7WTNA2sV3jrb1NNfNgapxRF5i4i6GcnTRRHts", "Solana"),
            ("DFDVx", "Xs2yquAgsHByNzx68WJC55WHjHBvG9JsMB7CWjTLyPy", "Solana"),
            ("DHRx", "Xseo8tgCZfkHxWS9xbFYeKFyMSbWEvZGFV1Gh53GtCV", "Solana"),
            ("GLDx", "Xsv9hRk1z5ystj9MhnA7Lq4vjSsLwzL2nxrwmwtD3re", "Solana"),
            ("GMEx", "Xsf9mBktVB9BSU5kf4nHxPq5hCBJ2j2ui3ecFGxPRGc", "Solana"),
            ("GOOGLx", "XsCPL9dNWBMvFtTmwcCA5v3xWPSMEBCszbQdiLLq6aN", "Solana"),
            ("GSx", "XsgaUyp4jd1fNBCxgtTKkW64xnnhQcvgaxzsbAq5ZD1", "Solana"),
            ("HDx", "XszjVtyhowGjSC5odCqBpW1CtXXwXjYokymrk7fGKD3", "Solana"),
            ("HONx", "XsRbLZthfABAPAfumWNEJhPyiKDW6TvDVeAeW7oKqA2", "Solana"),
            ("HOODx", "XsvNBAYkrDRNhA7wPHQfX3ZUXZyZLdnCQDfHZ56bzpg", "Solana"),
            ("IBMx", "XspwhyYPdWVM8XBHZnpS9hgyag9MKjLRyE3tVfmCbSr", "Solana"),
            ("IEMGx", "XsFnZawJdLdXfBSEt5Vw29K5vdBiHotdPLjUPafpfHs", "Solana"),
            ("IJRx", "XsyZcb97BzETAqi9BoP2C9D196MiMNBisGMVNje2Thz", "Solana"),
            ("INTCx", "XshPgPdXFRWB8tP1j82rebb2Q9rPgGX37RuqzohmArM", "Solana"),
            ("IWMx", "XsbELVbLGBkn7xfMfyYuUipKGt1iRUc2B7pYRvFTFu3", "Solana"),
            ("JNJx", "XsGVi5eo1Dh2zUpic4qACcjuWGjNv8GCt3dm5XcX6Dn", "Solana"),
            ("JPMx", "XsMAqkcKsUewDrzVkait4e5u4y8REgtyS7jWgCpLV2C", "Solana"),
            ("KOx", "XsaBXg8dU5cPM6ehmVctMkVqoiRG2ZjMo1cyBJ3AykQ", "Solana"),
            ("KRAQx", "XsAiRejKuvLAdq9KtedrMSrabz7SWdzKoVK6Qgac1Ki", "Solana"),
            ("LINx", "XsSr8anD1hkvNMu8XQiVcmiaTP7XGvYu7Q58LdmtE8Z", "Solana"),
            ("LLYx", "Xsnuv4omNoHozR6EEW5mXkw8Nrny5rB3jVfLqi6gKMH", "Solana"),
            ("MAx", "XsApJFV9MAktqnAc6jqzsHVujxkGm9xcSUffaBoYLKC", "Solana"),
            ("MCDx", "XsqE9cRRpzxcGKDXj1BJ7Xmg4GRhZoyY1KpmGSxAWT2", "Solana"),
            ("MDTx", "XsDgw22qRLTv5Uwuzn6T63cW69exG41T6gwQhEK22u2", "Solana"),
            ("METAx", "Xsa62P5mvPszXL1krVUnU5ar38bBSVcWAB6fmPCo5Zu", "Solana"),
            ("MRKx", "XsnQnU7AdbRZYe2akqqpibDdXjkieGFfSkbkjX1Sd1X", "Solana"),
            ("MRVLx", "XsuxRGDzbLjnJ72v74b7p9VY6N66uYgTCyfwwRjVCJA", "Solana"),
            ("MSFTx", "XspzcW1PRtgf6Wj92HCiZdjzKCyFekVD8P5Ueh3dRMX", "Solana"),
            ("MSTRx", "XsP7xzNPvEHS1m6qfanPUGjNmdnmsLKEoNAnHjdxxyZ", "Solana"),
            ("NFLXx", "XsEH7wWfJJu2ZT3UCFeVfALnVA6CP5ur7Ee11KmzVpL", "Solana"),
            ("NVDAx", "Xsc9qvGR1efVDFGLrVsmkzv3qi45LTBjeUKSPmx9qEh", "Solana"),
            ("NVOx", "XsfAzPzYrYjd4Dpa9BU3cusBsvWfVB9gBcyGC87S57n", "Solana"),
            ("OPENx", "XsGtpmjhmC8kyjVSWL4VicGu36ceq9u55PTgF8bhGv6", "Solana"),
            ("ORCLx", "XsjFwUPiLofddX5cWFHW35GCbXcSu1BCUGfxoQAQjeL", "Solana"),
            ("PALLx", "XsTTtPA5V19YwHKDv4xeVXNM6kdsQNJvg3MyWkRUckt", "Solana"),
            ("PEPx", "Xsv99frTRUeornyvCfvhnDesQDWuvns1M852Pez91vF", "Solana"),
            ("PFEx", "XsAtbqkAP1HJxy7hFDeq7ok6yM43DQ9mQ1Rh861X8rw", "Solana"),
            ("PGx", "XsYdjDjNUygZ7yGKfQaB6TxLh2gC6RRjzLtLAGJrhzV", "Solana"),
            ("PLTRx", "XsoBhf2ufR8fTyNSjqfU71DYGaE6Z3SUGAidpzriAA4", "Solana"),
            ("PMx", "Xsba6tUnSjDae2VcopDB6FGGDaxRrewFCDa5hKn5vT3", "Solana"),
            ("PPLTx", "Xst6eFD4YT6sz9RLMysN9SyvaZWtraSdVJQGu5ZkAme", "Solana"),
            ("QQQx", "Xs8S1uUs1zvS2p7iwtsG3b6fkhpvmwz4GYU3gWAmWHZ", "Solana"),
            ("SCHFx", "XsWAnFM77x6YvpdaZoos79R12o4Yj4r7EVkaTWddzhU", "Solana"),
            ("SLVx", "XsxAd6okt8y1RRK6gNg7iJaqiWNiq5Md5EDf3ZrF2dm", "Solana"),
            ("SPYx", "XsoCS1TfEyfFhfvj8EtZ528L3CaKBDBRqRapnBbDF2W", "Solana"),
            ("STRCx", "Xs78JED6PFZxWc2wCEPspZW9kL3Se5J7L5TChKgsidH", "Solana"),
            ("TBLLx", "XsqBC5tcVQLYt8wqGCHRnAUUecbRYXoJCReD6w7QEKp", "Solana"),
            ("TMOx", "Xs8drBWy3Sd5QY3aifG9kt9KFs2K3PGZmx7jWrsrk57", "Solana"),
            ("TONXx", "XscE4GUcsYhcyZu5ATiGUMmhxYa1D5fwbpJw4K6K4dp", "Solana"),
            ("TQQQx", "XsjQP3iMAaQ3kQScQKthQpx9ALRbjKAjQtHg6TFomoc", "Solana"),
            ("TSLAx", "XsDoVfqeBukxuZHWhdvWHBhgEHjGNst4MLodqsJHzoB", "Solana"),
            ("UNHx", "XszvaiXGPwvk2nwb3o9C1CX4K6zH8sez11E6uyup6fe", "Solana"),
            ("VTIx", "XsssYEQjzxBCFgvYFFNuhJFBeHNdLWYeUSP8F45cDr9", "Solana"),
            ("VTx", "XsEdDDTcVGJU6nvdRdVnj53eKTrsCkvtrVfXGmUK68V", "Solana"),
            ("Vx", "XsqgsbXwWogGJsNcVZ3TyVouy2MbTkfCFhCGGGcQZ2p", "Solana"),
            ("WMTx", "Xs151QeqTCiuKtinzfRATnUESM2xTU6V9Wy8Vy538ci", "Solana"),
            ("XOMx", "XsaHND8sHyfMfsWPj6kSdd5VwvCayZvjYgKmmcNL5qh", "Solana"),
            # ── EVM mirror deployments (Backed.fi, identical 0x proxy on both EVMs) ──
("AAPLx", "0x9d275685dc284c8eb1c79f6aba7a63dc75ec890a", "Ethereum"),
            ("ABBVx", "0xfbf2398df672cee4afcc2a4a733222331c742a6a", "Ethereum"),
            ("ABTx", "0x89233399708c18ac6887f90a2b4cd8ba5fedd06e", "Ethereum"),
            ("ACNx", "0x03183ce31b1656b72a55fa6056e287f50c35bbeb", "Ethereum"),
            ("AMBRx", "0x2f9a35ab5ddfbc49927bfdeab98a86c53dc6e763", "Ethereum"),
            ("AMZNx", "0x3557ba345b01efa20a1bddc61f573bfd87195081", "Ethereum"),
            ("APPx", "0x50a1291f69d9d3853def8209cfb1af0b46927be1", "Ethereum"),
            ("AVGOx", "0x38bac69cbbd28156796e4163b2b6dcb81e336565", "Ethereum"),
            ("AZNx", "0x5d642505fe1a28897eb3baba665f454755d8daa2", "Ethereum"),
            ("BACx", "0x314938c596f5ce31c3f75307d2979338c346d7f2", "Ethereum"),
            ("BRK.Bx", "0x12992613fdd35abe95dec5a4964331b1ee23b50d", "Ethereum"),
            ("CMCSAx", "0xbc7170a1280be28513b4e940c681537eb25e39f4", "Ethereum"),
            ("COINx", "0x364f210f430ec2448fc68a49203040f6124096f0", "Ethereum"),
            ("CRCLx", "0xfebded1b0986a8ee107f5ab1a1c5a813491deceb", "Ethereum"),
            ("CRMx", "0x4a4073f2eaf299a1be22254dcd2c41727f6f54a2", "Ethereum"),
            ("CRWDx", "0x214151022c2a5e380ab80cdac31f23ae554a7345", "Ethereum"),
            ("CSCOx", "0x053c784cd87b74f42e0c089f98643e79c1a3ff16", "Ethereum"),
            ("CVXx", "0xad5cdc3340904285b8159089974a99a1a09eb4c0", "Ethereum"),
            ("DFDVx", "0x521860bb5df5468358875266b89bfe90d990c6e7", "Ethereum"),
            ("DHRx", "0xdba228936f4079daf9aa906fd48a87f2300405f4", "Ethereum"),
            ("GLDx", "0x2380f2673c640fb67e2d6b55b44c62f0e0e69da9", "Ethereum"),
            ("GMEx", "0xe5f6d3b2405abdfe6f660e63202b25d23763160d", "Ethereum"),
            ("GOOGLx", "0xe92f673ca36c5e2efd2de7628f815f84807e803f", "Ethereum"),
            ("GSx", "0x3ee7e9b3a992fd23cd1c363b0e296856b04ab149", "Ethereum"),
            ("HDx", "0x766b0cd6ed6d90b5d49d2c36a3761e9728501ba9", "Ethereum"),
            ("HONx", "0x62a48560861b0b451654bfffdb5be6e47aa8ff1b", "Ethereum"),
            ("HOODx", "0xe1385fdd5ffb10081cd52c56584f25efa9084015", "Ethereum"),
            ("IBMx", "0xd9913208647671fe0f48f7f260076b2c6f310aac", "Ethereum"),
            ("INTCx", "0xf8a80d1cb9cfd70d03d655d9df42339846f3b3c8", "Ethereum"),
            ("JNJx", "0xdb0482cfad4789798623e64b15eeba01b16e917c", "Ethereum"),
            ("JPMx", "0xd9fc3e075d45254a1d834fea18af8041207dea0a", "Ethereum"),
            ("KOx", "0xdcc1a2699441079da889b1f49e12b69cc791129b", "Ethereum"),
            ("LINx", "0x15059c599c16fd8f70b633ade165502d6402cd49", "Ethereum"),
            ("LLYx", "0x19c41ea77b34bbdee61c3a87a75d1abda2ed0be4", "Ethereum"),
            ("MAx", "0xb365cd2588065f522d379ad19e903304f6b622c6", "Ethereum"),
            ("MCDx", "0x80a77a372c1e12accda84299492f404902e2da67", "Ethereum"),
            ("MDTx", "0x0588e851ec0418d660bee81230d6c678daf21d46", "Ethereum"),
            ("METAx", "0x96702be57cd9777f835117a809c7124fe4ec989a", "Ethereum"),
            ("MRKx", "0x17d8186ed8f68059124190d147174d0f6697dc40", "Ethereum"),
            ("MRVLx", "0xeaad46f4146ded5a47b55aa7f6c48c191deaec88", "Ethereum"),
            ("MSFTx", "0x5621737f42dae558b81269fcb9e9e70c19aa6b35", "Ethereum"),
            ("MSTRx", "0xae2f842ef90c0d5213259ab82639d5bbf649b08e", "Ethereum"),
            ("NFLXx", "0xa6a65ac27e76cd53cb790473e4345c46e5ebf961", "Ethereum"),
            ("NVDAx", "0xc845b2894dbddd03858fd2d643b4ef725fe0849d", "Ethereum"),
            ("NVOx", "0xf9523e369c5f55ad72dbaa75b0a9b92b3d8b147e", "Ethereum"),
            ("OPENx", "0xbee6b69345f376598fe16abd5592c6f844825e66", "Ethereum"),
            ("ORCLx", "0x548308e91ec9f285c7bff05295badbd56a6e4971", "Ethereum"),
            ("PEPx", "0x36c424a6ec0e264b1616102ad63ed2ad7857413e", "Ethereum"),
            ("PFEx", "0x1ac765b5bea23184802c7d2d497f7c33f1444a9e", "Ethereum"),
            ("PGx", "0xa90424d5d3e770e8644103ab503ed775dd1318fd", "Ethereum"),
            ("PLTRx", "0x6d482cec5f9dd1f05ccee9fd3ff79b246170f8e2", "Ethereum"),
            ("PMx", "0x02a6c1789c3b4fdb1a7a3dfa39f90e5d3c94f4f9", "Ethereum"),
            ("QQQx", "0xa753a7395cae905cd615da0b82a53e0560f250af", "Ethereum"),
            ("SPYx", "0x90a2a4c76b5d8c0bc892a69ea28aa775a8f2dd48", "Ethereum"),
            ("TBLLx", "0x4cbf89ed7bb30b8a860fa86d3c96e9c72931299b", "Ethereum"),
            ("TMOx", "0xaf072f109a2c173d822a4fe9af311a1b18f83d19", "Ethereum"),
            ("TQQQx", "0xfdddb57878ef9d6f681ec4381dcb626b9e69ac86", "Ethereum"),
            ("TSLAx", "0x8ad3c73f833d3f9a523ab01476625f269aeb7cf0", "Ethereum"),
            ("UNHx", "0x167a6375da1efc4a5be0f470e73ecefd66245048", "Ethereum"),
            ("VTIx", "0xbd730e618bcd88c82ddee52e10275cf2f88a4777", "Ethereum"),
            ("Vx", "0x2363fd1235c1b6d3a5088ddf8df3a0b3a30c5293", "Ethereum"),
            ("WMTx", "0x7aefc9965699fbea943e03264d96e50cd4a97b21", "Ethereum"),
            ("XOMx", "0xeedb0273c5af792745180e9ff568cd01550ffa13", "Ethereum"),

            ("AAPLx", "0x9d275685dc284c8eb1c79f6aba7a63dc75ec890a", "BinanceSmartChain"),
            ("ABBVx", "0xfbf2398df672cee4afcc2a4a733222331c742a6a", "BinanceSmartChain"),
            ("ABTx", "0x89233399708c18ac6887f90a2b4cd8ba5fedd06e", "BinanceSmartChain"),
            ("ACNx", "0x03183ce31b1656b72a55fa6056e287f50c35bbeb", "BinanceSmartChain"),
            ("AMBRx", "0x2f9a35ab5ddfbc49927bfdeab98a86c53dc6e763", "BinanceSmartChain"),
            ("AMZNx", "0x3557ba345b01efa20a1bddc61f573bfd87195081", "BinanceSmartChain"),
            ("APPx", "0x50a1291f69d9d3853def8209cfb1af0b46927be1", "BinanceSmartChain"),
            ("AVGOx", "0x38bac69cbbd28156796e4163b2b6dcb81e336565", "BinanceSmartChain"),
            ("AZNx", "0x5d642505fe1a28897eb3baba665f454755d8daa2", "BinanceSmartChain"),
            ("BACx", "0x314938c596f5ce31c3f75307d2979338c346d7f2", "BinanceSmartChain"),
            ("BRK.Bx", "0x12992613fdd35abe95dec5a4964331b1ee23b50d", "BinanceSmartChain"),
            ("CMCSAx", "0xbc7170a1280be28513b4e940c681537eb25e39f4", "BinanceSmartChain"),
            ("COINx", "0x364f210f430ec2448fc68a49203040f6124096f0", "BinanceSmartChain"),
            ("CRCLx", "0xfebded1b0986a8ee107f5ab1a1c5a813491deceb", "BinanceSmartChain"),
            ("CRMx", "0x4a4073f2eaf299a1be22254dcd2c41727f6f54a2", "BinanceSmartChain"),
            ("CRWDx", "0x214151022c2a5e380ab80cdac31f23ae554a7345", "BinanceSmartChain"),
            ("CSCOx", "0x053c784cd87b74f42e0c089f98643e79c1a3ff16", "BinanceSmartChain"),
            ("CVXx", "0xad5cdc3340904285b8159089974a99a1a09eb4c0", "BinanceSmartChain"),
            ("DFDVx", "0x521860bb5df5468358875266b89bfe90d990c6e7", "BinanceSmartChain"),
            ("DHRx", "0xdba228936f4079daf9aa906fd48a87f2300405f4", "BinanceSmartChain"),
            ("GLDx", "0x2380f2673c640fb67e2d6b55b44c62f0e0e69da9", "BinanceSmartChain"),
            ("GMEx", "0xe5f6d3b2405abdfe6f660e63202b25d23763160d", "BinanceSmartChain"),
            ("GOOGLx", "0xe92f673ca36c5e2efd2de7628f815f84807e803f", "BinanceSmartChain"),
            ("GSx", "0x3ee7e9b3a992fd23cd1c363b0e296856b04ab149", "BinanceSmartChain"),
            ("HDx", "0x766b0cd6ed6d90b5d49d2c36a3761e9728501ba9", "BinanceSmartChain"),
            ("HONx", "0x62a48560861b0b451654bfffdb5be6e47aa8ff1b", "BinanceSmartChain"),
            ("HOODx", "0xe1385fdd5ffb10081cd52c56584f25efa9084015", "BinanceSmartChain"),
            ("IBMx", "0xd9913208647671fe0f48f7f260076b2c6f310aac", "BinanceSmartChain"),
            ("INTCx", "0xf8a80d1cb9cfd70d03d655d9df42339846f3b3c8", "BinanceSmartChain"),
            ("JNJx", "0xdb0482cfad4789798623e64b15eeba01b16e917c", "BinanceSmartChain"),
            ("JPMx", "0xd9fc3e075d45254a1d834fea18af8041207dea0a", "BinanceSmartChain"),
            ("KOx", "0xdcc1a2699441079da889b1f49e12b69cc791129b", "BinanceSmartChain"),
            ("LINx", "0x15059c599c16fd8f70b633ade165502d6402cd49", "BinanceSmartChain"),
            ("LLYx", "0x19c41ea77b34bbdee61c3a87a75d1abda2ed0be4", "BinanceSmartChain"),
            ("MAx", "0xb365cd2588065f522d379ad19e903304f6b622c6", "BinanceSmartChain"),
            ("MCDx", "0x80a77a372c1e12accda84299492f404902e2da67", "BinanceSmartChain"),
            ("MDTx", "0x0588e851ec0418d660bee81230d6c678daf21d46", "BinanceSmartChain"),
            ("METAx", "0x96702be57cd9777f835117a809c7124fe4ec989a", "BinanceSmartChain"),
            ("MRKx", "0x17d8186ed8f68059124190d147174d0f6697dc40", "BinanceSmartChain"),
            ("MRVLx", "0xeaad46f4146ded5a47b55aa7f6c48c191deaec88", "BinanceSmartChain"),
            ("MSFTx", "0x5621737f42dae558b81269fcb9e9e70c19aa6b35", "BinanceSmartChain"),
            ("MSTRx", "0xae2f842ef90c0d5213259ab82639d5bbf649b08e", "BinanceSmartChain"),
            ("NFLXx", "0xa6a65ac27e76cd53cb790473e4345c46e5ebf961", "BinanceSmartChain"),
            ("NVDAx", "0xc845b2894dbddd03858fd2d643b4ef725fe0849d", "BinanceSmartChain"),
            ("NVOx", "0xf9523e369c5f55ad72dbaa75b0a9b92b3d8b147e", "BinanceSmartChain"),
            ("OPENx", "0xbee6b69345f376598fe16abd5592c6f844825e66", "BinanceSmartChain"),
            ("ORCLx", "0x548308e91ec9f285c7bff05295badbd56a6e4971", "BinanceSmartChain"),
            ("PEPx", "0x36c424a6ec0e264b1616102ad63ed2ad7857413e", "BinanceSmartChain"),
            ("PFEx", "0x1ac765b5bea23184802c7d2d497f7c33f1444a9e", "BinanceSmartChain"),
            ("PGx", "0xa90424d5d3e770e8644103ab503ed775dd1318fd", "BinanceSmartChain"),
            ("PLTRx", "0x6d482cec5f9dd1f05ccee9fd3ff79b246170f8e2", "BinanceSmartChain"),
            ("PMx", "0x02a6c1789c3b4fdb1a7a3dfa39f90e5d3c94f4f9", "BinanceSmartChain"),
            ("QQQx", "0xa753a7395cae905cd615da0b82a53e0560f250af", "BinanceSmartChain"),
            ("SPYx", "0x90a2a4c76b5d8c0bc892a69ea28aa775a8f2dd48", "BinanceSmartChain"),
            ("TBLLx", "0x4cbf89ed7bb30b8a860fa86d3c96e9c72931299b", "BinanceSmartChain"),
            ("TMOx", "0xaf072f109a2c173d822a4fe9af311a1b18f83d19", "BinanceSmartChain"),
            ("TQQQx", "0xfdddb57878ef9d6f681ec4381dcb626b9e69ac86", "BinanceSmartChain"),
            ("TSLAx", "0x8ad3c73f833d3f9a523ab01476625f269aeb7cf0", "BinanceSmartChain"),
            ("UNHx", "0x167a6375da1efc4a5be0f470e73ecefd66245048", "BinanceSmartChain"),
            ("VTIx", "0xbd730e618bcd88c82ddee52e10275cf2f88a4777", "BinanceSmartChain"),
            ("Vx", "0x2363fd1235c1b6d3a5088ddf8df3a0b3a30c5293", "BinanceSmartChain"),
            ("WMTx", "0x7aefc9965699fbea943e03264d96e50cd4a97b21", "BinanceSmartChain"),
            ("XOMx", "0xeedb0273c5af792745180e9ff568cd01550ffa13", "BinanceSmartChain"),
        ],
    ),
    (
        "ondo_group",
        "Ondo",
        [
            ("AAPLon", "123mYEnRLM2LLYsJW3K6oyYh8uP1fngj732iG638ondo", "Solana"),
            ("ABNBon", "128qNYovdGv2YqayErcJgU7gDwbNVX1VuoxbtWz8ondo", "Solana"),
            ("ABTon", "129gRoHKhVg7CvPMrqVsEB4uYZo6zV4yDZX6NBg9ondo", "Solana"),
            ("ACNon", "12LxMMJYVSf4LoeqjFE47BQQNRciaH9E3nbDfjH4ondo", "Solana"),
            ("ADBEon", "12Rh6JhfW4X5fKP16bbUdb4pcVCKDHFB48x8GG33ondo", "Solana"),
            ("AGGon", "13qTjKx53y6LKGGStiKeieGbnVx3fx1bbwopKFb3ondo", "Solana"),
            ("AMDon", "14diAn5z8kjrKwSC8WLqvBqqe5YmihJhjxRxd8Z6ondo", "Solana"),
            ("AMZNon", "14Tqdo8V1FhzKsE3W2pFsZCzYPQxxupXRcqw9jv6ondo", "Solana"),
            ("APOon", "14VXAhoa1R74vi1ZuiQyGLJrnDMfoFBPJSCpGVz3ondo", "Solana"),
            ("APPon", "14Z8rQQe2Aza33YgEUmj3g3QGNz8DXLiFPuCnsD1ondo", "Solana"),
            ("ARMon", "15SsCZqCsM9fZGhTmP4rdJTPT9WGZKazDSsgeQ8ondo", "Solana"),
            ("ASMLon", "1eLZPRsn8bAKmoxsqDMH9Q2m2k7GMNp6RLSQGm8ondo", "Solana"),
            ("AVGOon", "1FWZtdWN7y38BSXGzbs8D6Shk88oL9atDNgbVz9ondo", "Solana"),
            ("AXPon", "1WxT6NdK7uqpfXuKpALxL2n3f7Rq61XXeHA8UM4ondo", "Solana"),
            ("BABAon", "1zvb9ELBFShBCWKEk5jRTJAaPAwtVt7quEXx1X4ondo", "Solana"),
            ("BAon", "1YVZ4LGpq8CAhpdpm3mgy7GgPb83gJczCpxLUQ3ondo", "Solana"),
            ("BIDUon", "54CoRF2FYMZNJg9tS36xq5BUcLZ7rju1r59jGc2ondo", "Solana"),
            ("BLKon", "5H1VpMzRuoNtRbPTRCz35ETtEUtnkt8hJuQb9v7ondo", "Solana"),
            ("CMGon", "5owVsVFSHACQuippFYdLp3qWRobp2EGcwxMmsr6ondo", "Solana"),
            ("COINon", "5u6KDiNJXxX4rGMfYT4BApZQC5CuDNrG6MHkwp1ondo", "Solana"),
            ("COSTon", "6btaz134wjHkR8sqhAYrtSM6tavftfxnRvnyMd8ondo", "Solana"),
            ("CRCLon", "6xHEyem9hmkGtVq6XGCiQUGpPsHBaoYuYdFNZa5ondo", "Solana"),
            ("CRMon", "7D7ukbcnUNYt7Et5vtsDZhAy28MKu9pkHka1Hp9ondo", "Solana"),
            ("CSCOon", "7DWcZE1uVc8m2mf9pV8KNov28ET7HsvHkhrhgr9ondo", "Solana"),
            ("CVXon", "7tgKziACteG26VjV5xKufojKxwTgCFyTwmWUmz5ondo", "Solana"),
            ("DASHon", "83P1gCFBZfGRCwJuBt9juxJKEsZwejJoG66eTZ6ondo", "Solana"),
            ("DISon", "mJf1xT3suXtkXBCfZcE9oUUuyxkvSgqYBWiX7v1ondo", "Solana"),
            ("EEMon", "916SDKz7y5ZcEZC9CtnQ5Djs1Y8Yv3UAPb6bak8ondo", "Solana"),
            ("EFAon", "AbvryMGnaba9oADMZk8Vp2Av6MtczsncGyfWaC4ondo", "Solana"),
            ("EQIXon", "aheEdmuryJU8ymy8LjYheZH5i2BW1UMsfuWQKD2ondo", "Solana"),
            ("FIGon", "aLDdFsr3VTUQaHFK6yNvQxztvxQ8nxW4AMuSGC7ondo", "Solana"),
            ("FUTUon", "Ao5rKFRQ54W3DKSAtqfhBRPNHewwWRLNLao2JL9ondo", "Solana"),
            ("GEon", "aTBfDuLRqYHBiG82bHA7DzwjSDTFre2dRtGH3S5ondo", "Solana"),
            ("GMEon", "aznKt8v32CwYMEcTcB4bGTv8DXWStCpHrcCtyy7ondo", "Solana"),
            ("GOOGLon", "bbahNA5vT9WJeYft8tALrH1LXWffjwqVoUbqYa1ondo", "Solana"),
            ("GSon", "BchJRy2snmhJZf3rQ9LJ3ePs2BGfYgfvQNo31d2ondo", "Solana"),
            ("HIMSon", "bdh3njeo19d2TBLAKTGvCWdSoArfVw8uZBAJHY4ondo", "Solana"),
            ("HOODon", "BVdXGvmgi6A9oAiwWvBvP76fyTqcCNRJMM7zMN6ondo", "Solana"),
            ("HYGon", "c5ug15fwZRfQhhVa6LHscFY33ebVDHcVCezYpj7ondo", "Solana"),
            ("IAUon", "M77ZvkZ8zW5udRbuJCbuwSwavRa7bGAZYMTwru8ondo", "Solana"),
            ("IBMon", "C8bZkgSxXkyT1RgxByp2teJ24hgimPLoyEYoNa9ondo", "Solana"),
            ("IEFAon", "C9J9vZ8N79GzzxFoRkPWCkGtMKU8akg4FhUk4r9ondo", "Solana"),
            ("IEMGon", "cdVNL7wK8mf1UCDqM6zdrziRv4hmvqWhXeTcck2ondo", "Solana"),
            ("IJHon", "cfPLN9WXD2BTkbZhRZMVXPmVSiRo44hJWRtnaC8ondo", "Solana"),
            ("INTCon", "cJpUMp5R7rZ6fGeLHbHhrRuJzK9mkyKDjZqNpT3ondo", "Solana"),
            ("INTUon", "CozoH5HBTyyeYSQxHcWpGzd4Sq5XBaKzBzvTtN3ondo", "Solana"),
            ("ITOTon", "CPWkMURVvcnX8hGjqCTb8i5LkzV3VSvyk7SeJi8ondo", "Solana"),
            ("IVVon", "CqW2pd6dCPG9xKZfAsTovzDsMmAGKJSDBNcwM96ondo", "Solana"),
            ("IWFon", "dSHPFuMMjZqt7xDYGWrexXTSkdEZAiZngqymQF2ondo", "Solana"),
            ("IWMon", "dvj2kKFSyjpnyYSYppgFdAEVfgjMEoQGi9VaV23ondo", "Solana"),
            ("IWNon", "DX7g7WNjDpVzNK9CG81v7wb6ZbiNzYfkdzH2Xs5ondo", "Solana"),
            ("JDon", "E1aUS5nyv7kaBzdQzPVJW5zfaMgoUJpKYzdnFS2ondo", "Solana"),
            ("JPMon", "E5Gczsavxcomqf6Cw1sGCKLabL1xYD2FzKxVoB4ondo", "Solana"),
            ("KOon", "e6G4pfFcrdKxJuZ4YXixRFfMbpMvgXG2Mjcus71ondo", "Solana"),
            ("LINon", "Edik9MoFp8LAXS9HNu2gRFyihwYqDqv4ZmNmVT9ondo", "Solana"),
            ("LLYon", "eGGxZwNSfuNKRqQLKaz2hc4QkA2mau7skyxPdj7ondo", "Solana"),
            ("LMTon", "EoReHwUnGGekbXFHLj5rbCVKiwWqu32GrETMfw4ondo", "Solana"),
            ("MAon", "EsVHcyRxXFJCLMiuYLWhoDygrNe1BJGpYeZ17X7ondo", "Solana"),
            ("MARAon", "ETCJUmuhs5aY62xgEVWCZ5JR8KPdeXUaJz3LuC5ondo", "Solana"),
            ("MCDon", "EUbJjmDt8JA222M91bVLZs211siZ2jzbFArH9N3ondo", "Solana"),
            ("MELIon", "EWwdgGshGngcMpDV34pWZRSu5bkAuiKuKTTHKQ8ondo", "Solana"),
            ("METAon", "fDxs5y12E7x7jBwCKBXGqt71uJmCWsAQ3Srkte6ondo", "Solana"),
            ("MRVLon", "FovBwhoV5KQjZCdhoM6jgXYwXLX3F8vgAfvmLH7ondo", "Solana"),
            ("MSFTon", "FRmH6iRkMr33DLG6zVLR7EM4LojBFAuq6NtFzG6ondo", "Solana"),
            ("MSTRon", "FSz4ouiqXpHuGPcpacZfTzbMjScoj5FfzHkiyu2ondo", "Solana"),
            ("MUon", "Fz9edBpaURPPzpKVRR1A8PENYDEgHqwx5D5th28ondo", "Solana"),
            ("NFLXon", "g4KnPrxPLeeKkwvDmZFMtYQPM64eHeShbD55vK6ondo", "Solana"),
            ("NKEon", "g646pcdG2Rt5DH9WZzL7VVnVDWCCMTTrnktwE74ondo", "Solana"),
            ("NOWon", "G7pTVoSECz5RQWubEnTP7AC83KHUsSyoiqYR1R2ondo", "Solana"),
            ("NVDAon", "gEGtLTPNQ7jcg25zTetkbmF7teoDLcrfTnQfmn2ondo", "Solana"),
            ("NVOon", "GeV7S8vjP8qdYZpdGv2Xi6e7MUMCk8NAAp2z7g5ondo", "Solana"),
            ("ORCLon", "GmDADFpfwjfzZq9MfCafMDTS69MgVjtzD7Fd9a4ondo", "Solana"),
            ("PANWon", "M7hVQomhw4Q2D2op3HvBrZjHu9SryjNvD5haEZ1ondo", "Solana"),
            ("PBRon", "GRciFCqJ5y2hbiD6U5mGkohY65BZTXGuGUrCqf7ondo", "Solana"),
            ("PEPon", "gud6b3fYekjhMG5F818BALwbg2vt4JKoow59Md9ondo", "Solana"),
            ("PFEon", "Gwh9fPsX1qWATXy63vNaJnAFfwebWQtZaVmPko6ondo", "Solana"),
            ("PGon", "GZ8v4NdSG7CTRZqHMgNsTPRULeVi8CpdWd9wZY8ondo", "Solana"),
            ("PLTRon", "HfsnTS5qtdStwec9DfBrunRqnAMYMMz1kjv9Hu9ondo", "Solana"),
            ("PYPLon", "hM7B3UQTTR81mS27SxDDPzBbjejmo8fnpFjzgv9ondo", "Solana"),
            ("QBTSon", "hqJXutLF6f7DxStrWCrnZDfXzbNTZmvi3KheVi6ondo", "Solana"),
            ("QCOMon", "hrmX7MV5hifoaBVjnrdpz698yABxrbBNAcWtWo9ondo", "Solana"),
            ("QQQon", "HrYNm6jTQ71LoFphjVKBTdAE4uja7WsmLG8VxB8ondo", "Solana"),
            ("RDDTon", "HXFrTf9v9NdjGUTnx4sojR3Cf92hoBsQFUxKTN7ondo", "Solana"),
            ("RIOTon", "i6f3DvZBuLpnGSqS8x6WPeStJ7jNe5KewD6afD5ondo", "Solana"),
            ("SBETon", "iLDu2jjp2i3Uqc2Vm7K7GLiUj3hR4Un49MtD7c4ondo", "Solana"),
            ("SBUXon", "iPFqjcZQTNMNXA4kbShbMhfAVD8yr8Uq9UtXMV6ondo", "Solana"),
            ("SHOPon", "ivdDracs2s7jCP698dJXKSEQdVrNj9hasJL1Uq1ondo", "Solana"),
            ("SLVon", "iy11ytbSGcUnrjE6Lfv78TFqxKyUESfku1FugS9ondo", "Solana"),
            ("SMCIon", "jLca79XzcewRuBZyaJxVxuKpUHcEix1X4CP1RP9ondo", "Solana"),
            ("SNOWon", "JmFLCBwoNvcXy6B2VqABg6m784ubkXpaEx3p7S5ondo", "Solana"),
            ("SPGIon", "JrTYw7A9jihX5TwpRStYviEbsYf2X2VJpZ13719ondo", "Solana"),
            ("SPOTon", "jzCvs2Pk8tDcfsFRqnEMjurgaQW4iQfEkandUR8ondo", "Solana"),
            ("SPYon", "k18WJUULWheRkSpSquYGdNNmtuE2Vbw1hpuUi92ondo", "Solana"),
            ("TIPon", "k6BPp2Xmf2TYgrZiUyWfUoZBKeqaDbvPoAVgSx2ondo", "Solana"),
            ("TLTon", "KaSLSWByKy6b9FrCYXPEJoHmLpuFZtTCJk1F1Z9ondo", "Solana"),
            ("TMon", "kbmF7ERJWMaaDswMprrH9gHSLya5D2RMBNgKqg3ondo", "Solana"),
            ("TSLAon", "KeGv7bsfR4MheC1CkmnAVceoApjrkvBhHYjWb67ondo", "Solana"),
            ("TSMon", "keybg184d4vyXeQdFqs4o99YsMg7xBthxTJ6Ky3ondo", "Solana"),
            ("UBERon", "KJNeFW3kk3ycPjXpC6cbuyckjeYHacc2ekhtAi5ondo", "Solana"),
            ("UNHon", "kPBGL8vAwKN3UGmr9cjkM2dU79SC3nzTC9yu7F8ondo", "Solana"),
            ("Von", "kxEW4oJL75K37VeXaZF1ynbHQATQwhECQKN1374ondo", "Solana"),
            ("WFCon", "L6ZE5qCpVVSqLePz64CrwkgyWoPF9M7tB8BeFH4ondo", "Solana"),
            ("WMTon", "LZddqAqKqJW9oMZSjTxCUmbmzBRQtv9gMkD9hZ3ondo", "Solana"),
            ("AALon", "9wYZetvT8J2ptfsRca5gzLBGvcUug38mp9yT3xaondo", "Solana"),
            ("ABBVon", "MFerpBVGKZh2jXN7cbJdXRXQTp6j6pbSnSZrfWrondo", "Solana"),
            ("ACHRon", "KcCVQxG9LhFYP5o9DWFKTFgFShPPQkDEemVbiFyondo", "Solana"),
            ("ADIon", "LmTMwmZLNZszn3qpjmnbhfP12U4qWDivaEBwSBSondo", "Solana"),
            ("AMATon", "7eRX747PSbVtGVx3qD5UFdkNM2BfTy86ikUiCMhondo", "Solana"),
            ("AMCon", "C9xNaNujcF1a5fidWAAFReFYqhLRVbyk4yPyGqzondo", "Solana"),
            ("AMGNon", "SS6AEWhzRrxhL2cXzKKjhFt3rCzmHHGKmFyugDTondo", "Solana"),
            ("ANETon", "Cq6QtvHpXbJWtFaiMhUDtHy8YVZ95gcD1oZ1cohondo", "Solana"),
            ("BACon", "Wk8gC6iTNp8dqd4ghkJ3h1giiUnyhykwHh7tYWjondo", "Solana"),
            ("BBAIon", "YXE7mph6XhsgnyezkMEcTuohSuWhbLWfwx2Hh6mondo", "Solana"),
            ("BILIon", "14kLsQVmc64qZexYuR4XGop9y8BeMkd77pJUm1Rhondo", "Solana"),
            ("BINCon", "mhZ69E1vDnAsQJXAwarLYSX5tmgeMajXBJ2rXAcondo", "Solana"),
            ("BLSHon", "A9PFmw9Hu8zzxDUoU351pio1E1XWBWBfWnjT9qoondo", "Solana"),
            ("BMNRon", "MYXqkDYbzr7vjXAz2BapR4AiYRXzoikGirrLoRzondo", "Solana"),
            ("BTGon", "cBnVXDyZgaaLZM18wAmqsUKnRUFAEJWbq6VuUoaondo", "Solana"),
            ("BZon", "doPqjCxi6UkANkvMz5fSuYGEo5PGppVpTZMeB5vondo", "Solana"),
            ("CATon", "AErxJJxGbc9cZzZoZepN62BNfg5RXns8tmEc3Zpondo", "Solana"),
            ("CEGon", "7NWHifsBnn9DimUeNnsHdEXkTZhXmJTiXxcCngBondo", "Solana"),
            ("CIFRon", "WNZBSkNBNP3Ct1pcFn6Fu4sZQFhnu48EsM9voCEondo", "Solana"),
            ("CLOAon", "t71FyTYHVkPAb5g48adDHmkVxXYbUuP2eq6jDZLondo", "Solana"),
            ("CLOIon", "ucQ3VfWAx9pkCN4Kg84zE56FtB4FJN2kQH4ArYYondo", "Solana"),
            ("COFon", "R2uDbMtmHq5xSS5SserrovdRKdpiqnVBCd2AHLhondo", "Solana"),
            ("Con", "PjtfUiw6Hwd8PZ94EcUw8mBSYxp7SjjzSLeNTDKondo", "Solana"),
            ("COPon", "X68p9qTpEMkR1TLpXUP2ZJo8PG4Qge2Y2ZLdjA2ondo", "Solana"),
            ("COPXon", "X7j77hTmjZJbepkXXBcsEapM8qNgdfihkFj6CZ5ondo", "Solana"),
            ("CPNGon", "NKyzy31w2J7odLb2CW3Ft4fpKXkW3LBt1pvpkVLondo", "Solana"),
            ("CRWDon", "cdKfoNjbXgnSuxvoajhtH3uixfZhq1YXhQsS1Rwondo", "Solana"),
            ("CVNAon", "FGmUDXqA3AbWfo5b3NUcsvwoUFCF4tr9ea6uercondo", "Solana"),
            ("DBCon", "td1aY5AvYQuwGD75qNq9aPipMexraN9mQXJwqifondo", "Solana"),
            ("DEon", "CqQyAZjB9LGFTG95eiadGTkfhd9QA12ProeKsQmondo", "Solana"),
            ("DGRWon", "gnoSQSNTNZHViqVfxCcPDVxcRA29mrJL7C6JqYLondo", "Solana"),
            ("DNNon", "12J2LD3tuLfdiVKnWZMHRMrbnXDY9rM4yqVLUa5yondo", "Solana"),
            ("FIGRon", "ZmHxc6Gt27RJKxD2ay6UL4n9yQ7mKAq4XZQUeVhondo", "Solana"),
            ("Fon", "5hT2o25X9tGXipwhLckaUdgnxrZ6Y8eiUwdhpLeondo", "Solana"),
            ("FTGCon", "ivBnfPTyuHDNWmMSnbavckhJK6SHZW8h77nZKsEondo", "Solana"),
            ("GEMIon", "NrTdGMA3ujUvWXkwXyZKnhoByb32KTjRh5Vo47yondo", "Solana"),
            ("GLDon", "hWfiw4mcxT8rnNFkk6fsCQSxoxgZ9yVhB6tyeVcondo", "Solana"),
            ("GRABon", "m9GcsVgdjaL3KsdtSFHimnhtsUMpTHkjtwEG4Tzondo", "Solana"),
            ("GRNDon", "Gc1aT3ay7FXL3qdAW7cNSXYPDsGavy7qiACuxwxondo", "Solana"),
            ("HDon", "MtEXKVN3Pcggy8MPA3eJr15H6SK3RXheScqj9qtondo", "Solana"),
            ("IRENon", "13QHuepdhtJ3urNsV9i1hdL8nQoca2G7ZaLzb5FYondo", "Solana"),
            ("ISRGon", "1MGRpPrkhEsCm2GCWD3rsvEU77xTTLAzfKXeFgFondo", "Solana"),
            ("JAAAon", "KZtqx9BJbpcGY7vdzhqPXM3ECKChxE5YhXaDiwRondo", "Solana"),
            ("JNJon", "KUXt7LzHWSQXp5eyqMZRxWjAP6yM8BUh4LRHwiwondo", "Solana"),
            ("KLACon", "149o8ppQf9SzKCKXZ4v3dzHkwumvtQSRzSEkr29uondo", "Solana"),
            ("LIon", "v12TwfofSbvVqQ5N5KGG4d3J8rtEi4BjGfn2apyondo", "Solana"),
            ("LOWon", "edLdFJVVR532qhcrNTJjLAmhmyV7NsctbWVokMBondo", "Solana"),
            ("LRCXon", "wFJoeEYpKg9oRhyJy6BWTT3J95gmXBLvoeikDQNondo", "Solana"),
            ("MPon", "XwFm5GiKPVTvPiEbQpdc6vJbFEpsUXRMf6TcSxnondo", "Solana"),
            ("MRKon", "bn1fb8dwzafGePqNPrM8m8cbAKQiFqeEPuZkPySondo", "Solana"),
            ("MRNAon", "14VP7DvCAdBCc5XGNZkPt6zhtPzJrWWS64Koxtxyondo", "Solana"),
            ("MTZon", "R3ywbVQ5t8LNmjQsn2Ngv43dSqyZscQwNag9G3Eondo", "Solana"),
            ("NEEon", "t7eN6cGwRMFaZvsNW2SmVwkedmHtDdrxA4ycNE5ondo", "Solana"),
            ("NIKLon", "V8LRV7kWjrx6Prke9oHEHNUiR122BVtyuPciTCTondo", "Solana"),
            ("NIOon", "yQ37dFiGAbzrb2FRAEhGNzRy5zFfoYGWYhAepFEondo", "Solana"),
            ("NTESon", "YeK2TdPtGLAme3Phg4pb1GBN2YxKgX5UNVyD4asondo", "Solana"),
            ("OKLOon", "m6oDLvJT7rY7M1TxuLWP3pWmAPg2cCWDQR1NKiEondo", "Solana"),
            ("ONDSon", "7qy1j4Mechfyr6uAST3djH4vk4kiEYC2cjEytXdondo", "Solana"),
            ("ONon", "13qtwy5fZi9Przz14pzo9xqFSr8QHmLyUpUCvP1xondo", "Solana"),
            ("OPENon", "ou1uE526v7zmUYP2qCb2LJgfXAyWAtWS9SETtr8ondo", "Solana"),
            ("OPRAon", "gbHFTMkuMQUy5xrgoCBdaQ2XYvNyjWAYcnRPh9Condo", "Solana"),
            ("OSCRon", "ThwGDsXZ6iKubWuEQjmDxGwF3bUERDGbBXvcbjFondo", "Solana"),
            ("OXYon", "1GNFMryQ6c9ZpMhgNimmsbtgYM21qnBJgRAFoNiondo", "Solana"),
            ("PALLon", "P7hTXnKk2d2DyqWnefp5BSroE1qjjKpKxg9SxQqondo", "Solana"),
            ("PCGon", "UP5s1srLaHDc4SwJqLPa3A48x5R7ofN3hZWxWEZondo", "Solana"),
            ("PDBCon", "M6agiXbNgy8Xon9ngiW4ZDPbMFcNCTMkMMkshZyondo", "Solana"),
            ("PDDon", "PnjETBCLC318DRejo9cMQKAmET9PvW8AEFGWMNtondo", "Solana"),
            ("PINSon", "sxyg1VTSzy5zYANUK7hntNtmFAWoXGJq95AcHuVondo", "Solana"),
            ("PLUGon", "TnfswqdE1jAJ8sfnf5J7kSVLEH1cfpAYZ8MWmKfondo", "Solana"),
            ("PSQon", "qKtU9A7ij34XmtxaSzYfxCpkgAZzzFsqnUb2kW2ondo", "Solana"),
            ("REMXon", "tiitb2Z1HtpB2DpVr6V7tdCFS3jmTinLeuGj9EVondo", "Solana"),
            ("RGTIon", "dwEPNKQab3iwRmjGvZPXhAmws1W5NsQGwuXwi8oondo", "Solana"),
            ("RIVNon", "AXRsYFt7TXNQ3DcY6BkvRgPV6VsYMURyDtaeudjondo", "Solana"),
            ("RTXon", "12BvLZtzjdssAycxPeBQUjukhmgQpULAvy6SroYdondo", "Solana"),
            ("SCHWon", "cnc6M1zXLdrGR5LAQVcaJDfgezMiVWNtGQsVy1Kondo", "Solana"),
            ("SGOVon", "HjrN6ChZK2QRL6hMXayjGPLFvxhgjwKEy135VRjondo", "Solana"),
            ("SNAPon", "a2cXfonVgQ6cKB4Lm8YZsPry39VZSA562bwmRSiondo", "Solana"),
            ("SOFIon", "mqL8yXQpeSvc7NgrAtLLPtRvUiWyLoG5RWLv16iondo", "Solana"),
            ("SOon", "aKzjn2ZdWySSGPSSDTY2HUpcSCmemSahTXihrpyondo", "Solana"),
            ("SOUNon", "vE2qArmjto6VfeMngyGAnzp2ipLYeXsxiARDnnXondo", "Solana"),
            ("SQQQon", "D1tu7Fnm3cCpKyyPXrqm5GXShPqMj7a2SEjjq9fondo", "Solana"),
            ("TCOMon", "9PMjLqd8zPdKkJUXarnit5t7tPL3cCscwHzy7ATondo", "Solana"),
            ("TLNon", "RTb54gpqAx6RpLAHRGnqQ3ciQ845CHqhg21ZzEJondo", "Solana"),
            ("TMOon", "T699bgtXQw4CJ59rQ4VzLsupVQUzoL5RmuhHnKrondo", "Solana"),
            ("TMUSon", "pDY4GPJfZcNETPG7myXeafQfgJqqVkn81bMYDyfondo", "Solana"),
            ("Ton", "WKMZummev5UcXz5nNKQZvTD6QjNSM2X58uwmDReondo", "Solana"),
            ("TQQQon", "14W1itEkV7k1W819mLSknFTaMmkCtPokbF2tRkPUondo", "Solana"),
            ("TXNon", "81xLFvCzFaUM3KDxSHC75pXu3RPCeSeCbmGBY8aondo", "Solana"),
            ("USFRon", "o6U1Sm6Vd7EofMyCrL28mrp2QLzgYGgjveHiEQ5ondo", "Solana"),
            ("USOon", "rpydAzWdCy85HEmoQkH5PVxYtDYQWjmLxgHHadxondo", "Solana"),
            ("VRTon", "MkN2TZSYTFBdMRLf9EVcfhstTwnazH8knd9hpepondo", "Solana"),
            ("VSTon", "h6MW8GFpfzxFa1JNn6hZNnBF3t4fj9SHAXKy6LXondo", "Solana"),
            ("VTIon", "jCCU4GwukjNxAXJowG2S4KCrr5g6YyUB61WHYvGondo", "Solana"),
            ("VTVon", "KuiYLPVq65qixD9TgvxBC576C4gG6vVTCdbh2zFondo", "Solana"),
            ("VZon", "igu1coP6n3GPaWmbd8J9Z7UAyLpV254uQFFNfydondo", "Solana"),
            ("WULFon", "exYfSJt6Fgfhfnp3bAD4roYy97hLF9npjYaLyEXondo", "Solana"),
            ("XOMon", "qCYD74QnXzd9pzv6pGHQKJVwoibL6sNcPQDnpDiondo", "Solana"),
            ("XYZon", "BWxe2FVciUbwrCUZQPUKiREBh5LmVa5AiUqNLAkondo", "Solana"),
            ("BTGOon", "bgJWGuQxyoyFeXwzYZKBmoujVdatGFYPNFnv1a6ondo", "Solana"),
            ("ALBon", "B5KufqHkskgGYwMXtL8FSHgREAkMQvE3ykhH5Kmondo", "Solana"),
            ("APLDon", "B6WqvLGXdGqpw7qgxeb5EGiRZEYo2apWpQybjYuondo", "Solana"),
            ("ASTSon", "B6ry9goGNvVbhq7gWHzs3p6emJ1gLaMhu4By9TTondo", "Solana"),
            ("BNOon", "BAU83kqEqhyiexfAMQhZZE5KnGogSqh17fJc44Sondo", "Solana"),
            ("CAPRon", "BS8zoc6pmALQnBhBDFak6eFhgGHjpebnHzsxApgondo", "Solana"),
            ("CIBRon", "BVdL3WUxtxUD4vXRWwqChJLbGxvfzZjBGPp63Wtondo", "Solana"),
            ("COHRon", "BXMkru8ded26p71gJ3AMMwJmwZaYYfQjRo8vbZzondo", "Solana"),
            ("CRWVon", "BfPGpgNyxe6rjAru1EJarjSBAcCABuMF5L32v7nondo", "Solana"),
            ("ECHon", "BmXVAFyfpW7VuVYeWDtbFtLx7sek2mZt3BEsGgAondo", "Solana"),
            ("ENLVon", "BncvtBGs4JqgYZwUoq3EN9q9HUFqJKTfWpvCsHCondo", "Solana"),
            ("ENPHon", "Bp26APthMuM46gMFTo5KYpo7b92GN2xSCor7f9oondo", "Solana"),
            ("ETHAon", "LitNUakTges74cjDJm6HHfFNKGPdySkp3MWSYzYondo", "Solana"),
            ("ETNon", "BpYiU1dBXU1fdB64jbR93wHEw3Y47QeRLZvUyLQondo", "Solana"),
            ("EWJon", "C6c7VcxuUYcV5YTsky5HM4PUmfwHTwsDD5DNwwPondo", "Solana"),
            ("EWYon", "C8pSaSgjkiTWixS3GM6Hxd6HKnKrgAbY9WDgfVeondo", "Solana"),
            ("EWZon", "CBKcmEvVg5EgE3W5hVSPcBYWh6TFVjQwbmYod9Pondo", "Solana"),
            ("EXODon", "CJRoTbu98waCCuLFfLuJ2kXawLk889fqW4UAAbwondo", "Solana"),
            ("FCXon", "CY8ttw5rYCT6fFBJwqXofefqa7Ji9E8zfLmhRLmondo", "Solana"),
            ("FFOGon", "CYAwMGyuNSDu7NpuccNwcxMNS5Bu9akxU2Jooyiondo", "Solana"),
            ("FGDLon", "CYqLHM92EhmF83iNgfN4A1j2ckjsHigRvXu7xHCondo", "Solana"),
            ("FLHYon", "CZ3FxxSto7tsjkSkqMek1C5p3RCFFmkwKqW57nbondo", "Solana"),
            ("FLQLon", "CZ9GBn1okotqKNUUqoxk4PF2JVi59bw5GWvVo6Dondo", "Solana"),
            ("FSOLon", "BJhPr9SM7uZTZXHeSLYmUk7CjGQq1esFkVxPF5tondo", "Solana"),
            ("FXIon", "CeFbGYXDmkyfo1TXXzzZ512mtnCCewNohu6V15vondo", "Solana"),
            ("GEVon", "CgZSv89BL58ybWfWobANKEU8nV9jYfFw23G2DZEondo", "Solana"),
            ("GLTRon", "CgnZbDNzBfaLyJqUtd4esKLShRp7RznQuwP4uQaondo", "Solana"),
            ("GLXYon", "CkWmEM2J79k6AjAwyQVHXteFucAL1zQrKLxLqJHondo", "Solana"),
            ("HYSon", "CsN1Tyz467bSFLPGd6MJyZhPNtwDaWZtX8ixHWyondo", "Solana"),
            ("IBITon", "6JLG8iUkAuqiBhL3j2ckDMDf5oWAa6awmyaWezKondo", "Solana"),
            ("IEFon", "D4uWxzR5StYC6sTRhVts8Eboy3pmVtHeNC62dnQondo", "Solana"),
            ("INCEon", "D8KT4Jd8qiKKTfkM8ejSKCpWGR1o3GFvnQGp5ERondo", "Solana"),
            ("INDAon", "DBNwt3FoYCKQWdfzxKFNZ4mzuz4Jz1iRzFf7HFzondo", "Solana"),
            ("IONQon", "DDZQijTbaSd3Kas1r1bgCnHPayk8vTP8SfZWp5Tondo", "Solana"),
            ("ITAon", "DDcAL93Urf7KrPntvKULnZoFs4Wdee1LkkJqLpjondo", "Solana"),
            ("KWEBon", "DVPSYdqWPLvNa8afnEqa3B9eDfTTWpGyUZeXvdMondo", "Solana"),
            ("LUNRon", "DiDWPZ7vQXfpaeQ8BX68XuDYeiQLv7diDxdeUpaondo", "Solana"),
            ("NBISon", "DiRshqNDE68bWbGdLHm1GwQ76MvWQG3af6w1NdQondo", "Solana"),
            ("NEMon", "Dig28Tf1ufhCBAsjTmFkXCgcNgMqDMYj5A2rDQmondo", "Solana"),
            ("NOCon", "Dm6FpQ76SsbVmAZ4NvD2mjZP7cxbw1CASr4WwCiondo", "Solana"),
            ("OIHon", "DnvbCqRuUYssmKVRBRNwkUnptHitH4ZZTt1KVuZondo", "Solana"),
            ("PAVEon", "DsLQ18ooPjiHYuiuQ5Jz8PNCpVaKe3FhAYpvMxWondo", "Solana"),
            ("PPLTon", "DwRtkbsaQMGAS3oMeEGYh6M5vH4X9WECsQgqHjAondo", "Solana"),
            ("QUBTon", "E4YowrHx5wm4RtSjfuvTqtNH3Wf7NEj5tYZGD9Bondo", "Solana"),
            ("RDWon", "E6KSaqjvqe2HiUpbEweRxLK4RimQddigm95H9Jaondo", "Solana"),
            ("REGNon", "E86mX2yb3HLbJM6gRtZQ6dCYmLh6MSDZadu9SCPondo", "Solana"),
            ("RKLBon", "E9VQY3VnrpVSekFByzRmfeK1kxgM3UiKCoVVbdUondo", "Solana"),
            ("SCCOon", "EANjzFjj3nPXHdzN5CE3Z8LLVn69Ce77FE8X4cvondo", "Solana"),
            ("SEDGon", "EAwP9LGNjTkQ2YeKE6CGKqBYtrJ6APFvRe7KCMmondo", "Solana"),
            ("SHYon", "EEy57xbaLcUrN1HXj2vz8VWxeWFK1eZQZo4aWbrondo", "Solana"),
            ("SNDKon", "EJmUVvDqAdfH5zEohkdS4234bi3c6iunqEMobjmondo", "Solana"),
            ("SOXXon", "EN5pHc1LccUSojxb7kkyQi7v7iJN5RpDq6qz3DHondo", "Solana"),
            ("STXon", "EXtprP1wzrNo2bByrU9JyzqEg2hQMSCVJakeHHYondo", "Solana"),
            ("UECon", "EYo8D3cLdF1CDeGms5M5VHyU52HJYinkMZ1cqvYondo", "Solana"),
            ("UNGon", "Es2ipHL7qXBcLmZ4N7LP9PHBHaWaTMTAkxDwGGjondo", "Solana"),
            ("UNPon", "EvsME8gdnEwPLbTnhrGVDwrY35zBuB8hEGCq59Hondo", "Solana"),
            ("URAon", "EvzskrQ3vUUkiMGG1DzfSDyG6H2WCMy3v9G8fzzondo", "Solana"),
            ("VFSon", "F3V1fKLKv7H8aNdt9TC6GQ3X4LayEfGHsPi8Umaondo", "Solana"),
            ("VNQon", "F3dMJ9H137YUNc9cpN3gBWDSq4MSRbTFtojH65Uondo", "Solana"),
            ("VRTXon", "FL7QzUq58pvkDxkftJm7RqRWgqYEFZwXuvAMsUnondo", "Solana"),
            ("WDCon", "FLqH2jB2DZPJP5nnVFAakRKaNTcDZtq71Pnpp6Aondo", "Solana"),
            ("WMon", "FPvKvWzSzDZqgYmSZUetrkpUXSwo2VtpR4BynVYondo", "Solana"),
# ── Ethereum deployments (Ondo Global Markets, alphabetical) ──
            ("AALon", "0xBE8eb7b51a08f9d52Bb6C8c7EcA699f0f89BfC02", "Ethereum"),
            ("AAPLon", "0x14c3abF95Cb9C93a8b82C1CdCB76D72Cb87b2d4c", "Ethereum"),
            ("ABBVon", "0x7c7378143a9c8839e0502e2178F058F46c6ea504", "Ethereum"),
            ("ABNBon", "0xb035c3d5083bdc80074F380aeBc9Fcb68aBa0A28", "Ethereum"),
            ("ABTon", "0x3859385363f7BB4Dfe42811cCF3F294FcD41dd1D", "Ethereum"),
            ("ACHRon", "0x9cFA08002d606e638Fe91941bE725e1b970B84a6", "Ethereum"),
            ("ACNon", "0xAbA9Ae731Aad63335C604E5f6E6A5db2e05f549d", "Ethereum"),
            ("ADBEon", "0x7042a8fFc7c7049684BfBc2fcb41b72380755a43", "Ethereum"),
            ("ADIon", "0x2dDc2391CC89E3e716A938F089AE755174cfDf1f", "Ethereum"),
            ("AGGon", "0xfF7CF16aA2fFc463b996DB2f7B7cf0130336899D", "Ethereum"),
            ("ALBon", "0x1b468d5535Ed7C19Ce42f0073db7Fdf441028131", "Ethereum"),
            ("AMATon", "0x6bE935eaDc71c49c414b1175985946ee40365c67", "Ethereum"),
            ("AMCon", "0x592643a667633bCa51Cb2387C98B6dE6CE549A45", "Ethereum"),
            ("AMDon", "0x0C1f3412A44Ff99E40bF14e06e5Ea321aE7B3938", "Ethereum"),
            ("AMGNon", "0x1c5FA55eAdE69ae98571059332520F73733C2D82", "Ethereum"),
            ("AMZNon", "0xbb8774FB97436d23d74C1b882E8E9A69322cFD31", "Ethereum"),
            ("ANETon", "0x20e113E9235dF6A2A9BFc6f244c2ccC380c8f546", "Ethereum"),
            ("APLDon", "0x318Dcb4f07C3e6ccEcc12A252100Fb3Bf76Eeb02", "Ethereum"),
            ("APOon", "0x4D21aFfD27183B07335935F81A5C26b6A5A15355", "Ethereum"),
            ("APPon", "0xd5C5B2883735Fa9B658Dd52e2FCC8d7c0f1A42Ce", "Ethereum"),
            ("ARMon", "0x5Bf1b2A808598C0eF4Af1673a5457d86fE6d7B3d", "Ethereum"),
            ("ASMLon", "0xE51bA774ebF6392c45Bf1d9E6b334d07992460d3", "Ethereum"),
            ("ASTSon", "0x0D1fa4E1E3719945899Ef7b02840627Df46aF44A", "Ethereum"),
            ("AVGOon", "0x0d54D4279B9E8c54cD8547c2C75A8Ee81A0BcaE8", "Ethereum"),
            ("AXPon", "0x2Bc7Ff0C5dA9F1a4A51F96e77C5b0F7165DC06d2", "Ethereum"),
            ("BABAon", "0x41765F0FCddC276309195166C7A62AE522FA09ef", "Ethereum"),
            ("BACon", "0x576E9CA70e3a040c00d8139b0665a2b7b7B64844", "Ethereum"),
            ("BAon", "0x57270D35A840BC5C094da6FBeCA033FB71eA6Ab0", "Ethereum"),
            ("BBAIon", "0x1b8d3e59b31981385C066eE0916Ec964628ff1f9", "Ethereum"),
            ("BIDUon", "0x9d4C6AD12B55E4645b585209F90Cc26614061E91", "Ethereum"),
            ("BILIon", "0x7e08cE07acA80CefE61ebbFA0CedFe5C7b07eDB9", "Ethereum"),
            ("BINCon", "0x88703C1E71f44a2D329C99e8E112F7a4E7dD6312", "Ethereum"),
            ("BLKon", "0x7a0F89c1606f71499950AA2590d547c3975B728E", "Ethereum"),
            ("BLSHon", "0x334ccd8Df4013bac99Af8C5C61d3605B315302a0", "Ethereum"),
            ("BMNRon", "0x33483A58079b4225b10e57958Ca28ad7b9CDbAF7", "Ethereum"),
            ("BNOon", "0x9ddb2524782684942FAD28b44E76552cB7f3F548", "Ethereum"),
            ("BTGon", "0x8AC6AD49b3344024834f373f3CA491f22ceb952e", "Ethereum"),
            ("BTGOon", "0x510Dd21055188Eda378714DE3bb5591Ffa0CC468", "Ethereum"),
            ("BZon", "0x858e985126543b5a066C4E8a5DAB0249C1d683f7", "Ethereum"),
            ("CAPRon", "0x70Ec0f5B23404C0cD6f29ce88f4af00A0b0d895D", "Ethereum"),
            ("CATon", "0xF719b02079E0FaA5450392DA2D3e11a1e5b0EADB", "Ethereum"),
            ("CEGon", "0x060505527C83E8BfEb9b4Ff08248B82e688800F1", "Ethereum"),
            ("CIBRon", "0x42d6E274B8631e5289a8F853E8d1A7bAEff3C8d1", "Ethereum"),
            ("CIFRon", "0x24E5Bc45d5b6Cef6F38989AC33dF587a3FC850cf", "Ethereum"),
            ("CLOAon", "0x8cEFd49b703dE9C0486d9bf6Cb559F0895268Ee8", "Ethereum"),
            ("CLOIon", "0xe8B09e8175AEcB35A171Fa059647434fE47F114c", "Ethereum"),
            ("CMGon", "0x25018520138bbaB60684AD7983D4432E8B8E926B", "Ethereum"),
            ("COFon", "0x3D07c3161F355Cb9E5B524beF8d113c96e0263AB", "Ethereum"),
            ("COHRon", "0x2B7727076B9C9B1834a2f95B81f12EEdD30db9f1", "Ethereum"),
            ("COINon", "0xF042cfa86cf1D598a75Bdb55c3507a1F39f9493b", "Ethereum"),
            ("Con", "0xC46e7eF70d7Cf8C17863a6B0b9be2aF6a4C41aBe", "Ethereum"),
            ("COPon", "0x8E6A5338eaC4B6fE8D51a7653Fad3B9DA755Eea6", "Ethereum"),
            ("COPXon", "0x423A63dfE8d82CD9C6568C92210AA537d8Ef6885", "Ethereum"),
            ("COSTon", "0x0c8276E4FeC072cf7854Be69c70F7773D1610857", "Ethereum"),
            ("CPNGon", "0xFA9f0Bf8baa9A3D5E0a8E5c0AeAF186ACaBef63D", "Ethereum"),
            ("CRCLon", "0x3632DEa96A953C11dac2f00b4A05a32CD1063fAE", "Ethereum"),
            ("CRMon", "0x55720eF5b023Fd043AE5F8D2e526030207978950", "Ethereum"),
            ("CRWDon", "0xCaC9AAfb2cf51645Ae1ab4Fb1F35F07d42437f80", "Ethereum"),
            ("CRWVon", "0x66908813cd7676269494B2c6F6DBAB8B4f9E95df", "Ethereum"),
            ("CSCOon", "0x980a1001ee94e54142b231f44C7CA7c9DF71FBe1", "Ethereum"),
            ("CVNAon", "0xFe4eC50E0413148021d2f50d114CC44De6fFBF23", "Ethereum"),
            ("CVXon", "0x8F3E41b378ae010c46d255F36bFC1D303b52dceb", "Ethereum"),
            ("DASHon", "0x241958c86c7744d15d5f6314BA1Ea4c81DDA2896", "Ethereum"),
            ("DBCon", "0x20224080aD516769723c9a4A18325fC4E8C9Ab5D", "Ethereum"),
            ("DEon", "0x32d7c413fD3477E86b8eC6B0BB8F3Ac510eAfaae", "Ethereum"),
            ("DGRWon", "0x81Eb954936A7062d1758Fc0E6E3b88d42D9C361c", "Ethereum"),
            ("DISon", "0xc3D93B45249E8E06cfeB01d25A96337E8893265d", "Ethereum"),
            ("DNNon", "0x7AA59A63d1D0C435a08bC96E11bef2E95aB66c40", "Ethereum"),
            ("ECHon", "0x74C8f41f57948Bfd8aa0D48C882d69d12d1Cc579", "Ethereum"),
            ("EEMon", "0x77A1a02e4a888ADA8620b93C30dE8a41E621126c", "Ethereum"),
            ("EFAon", "0x4111b60bc87F2Bd1e81E783E271D7F0ec6EE088B", "Ethereum"),
            ("ENLVon", "0xc4e6E80295154D3968519851F73f8Dc1a227286F", "Ethereum"),
            ("ENPHon", "0x4F3B49aC895a29c0908c57538932967Cdc8e3c80", "Ethereum"),
            ("EQIXon", "0x73d2ccEE12C120E7DA265a2dE9d9f952a0101b4f", "Ethereum"),
            ("ETHAon", "0x98284FbC11eDD7540E29b896a49817Bbe52DdCBd", "Ethereum"),
            ("ETNon", "0x9b56f5ED5A94Ae3266b7FF21953e6626F94008F1", "Ethereum"),
            ("EWJon", "0x625Fb557cAD6D4638dae420626F3F08A485b43a8", "Ethereum"),
            ("EWYon", "0xbd660e96D45e7C175512d1ed0cCc119Cb980b81a", "Ethereum"),
            ("EWZon", "0x54021fDe36B7c4C4F9C35b02fb9A153eD8F5938A", "Ethereum"),
            ("EXODon", "0x185E5FA1B84F94D46ef2A33052aD39bD5f326fd8", "Ethereum"),
            ("FCXon", "0xeb08d539Be0f6a6C90eA24276196E348f5688A02", "Ethereum"),
            ("FFOGon", "0x70B4082F4e3F9067dB9A2aA7520C77719E8626EE", "Ethereum"),
            ("FGDLon", "0x1cB673005fc58447D881486919c14D8e7C741Bb1", "Ethereum"),
            ("FIGon", "0x073E7a0669833d356fa88ca65CC6D454EFaAa3c5", "Ethereum"),
            ("FIGRon", "0xc2dBFE026f17e7BbC17a9e41F9b8D69531887d47", "Ethereum"),
            ("FLHYon", "0x54C1Ff361b402f66c13107421E6A431C3375EF24", "Ethereum"),
            ("FLQLon", "0xC53D2e7321aB83B28aF2360559Aa303676a23f98", "Ethereum"),
            ("Fon", "0xf72936FA8afC808c99eb76E620A98DDC6a7A53d1", "Ethereum"),
            ("FSOLon", "0x224B381CFAe8CCAf2e4d32D827467C2331Ce04bE", "Ethereum"),
            ("FTGCon", "0xaCf3FECAA787F268351A86409C3bD3b96Ef924fb", "Ethereum"),
            ("FUTUon", "0x5Ce215d9c37a195DF88e294a06B8396C296B4e15", "Ethereum"),
            ("FXIon", "0x2dD57b497c777D9825A5902114BE81dF98eDE958", "Ethereum"),
            ("GEMIon", "0xb51db25c920C16F2865C37011c3Eec91Db946B07", "Ethereum"),
            ("GEon", "0xD904bCf89B7CedF5c89f9Df7e829191D695F847E", "Ethereum"),
            ("GEVon", "0xA043FDc5A6E2E381e3532d5A97404b82fb7A0af8", "Ethereum"),
            ("GLDon", "0x423D42E505e64F99b6E277eb7ED324CC5606F139", "Ethereum"),
            ("GLTRon", "0xD0a265a32D0211a7f61F11de014B854F7ce716F8", "Ethereum"),
            ("GLXYon", "0xE668e08a6f5792CEF0e63E9D98524968fDB5882f", "Ethereum"),
            ("GMEon", "0x71d24Baeb0A033ec5F90FF65C4210545AF378D97", "Ethereum"),
            ("GOOGLon", "0xbA47214eDd2bb43099611b208f75E4b42FDcfEDc", "Ethereum"),
            ("GRABon", "0x1C174711f3FD63C4165d6F296b3eB19D17fde94a", "Ethereum"),
            ("GRNDon", "0xe5b26BA77E6a4d79a7c54a5296d81254269D9700", "Ethereum"),
            ("GSon", "0xdB57d9C14e357Fc01E49035a808779Df41E9B4e2", "Ethereum"),
            ("HDon", "0x7DBd435aa4eCAB5471CFCeF4527a022feF0b7e1C", "Ethereum"),
            ("HIMSon", "0xCa468554e5C0423Ee858fe3942c9568C51FcAa79", "Ethereum"),
            ("HOODon", "0x998f02A9E343EF6E3E6f28700d5A20F839fD74E6", "Ethereum"),
            ("HYGon", "0xeD3618Bb8778F8eBBe2f241Da532227591771D04", "Ethereum"),
            ("HYSon", "0xEc169F9Ac2161723a2D4febd9748BB529D6C12B5", "Ethereum"),
            ("IAUon", "0x4f0CA3df1c2e6b943cf82E649d576ffe7B2fABCF", "Ethereum"),
            ("IBITon", "0x122940c4C5F9cCFAe7Fa86455a42D3EC140855cE", "Ethereum"),
            ("IBMon", "0x25d3f236B2d61656eebdeA86Ac6D42168e340011", "Ethereum"),
            ("IEFAon", "0xFEFf7a377A86462F5a2A872009722C154707F09e", "Ethereum"),
            ("IEFon", "0xA2eC76139028F279A1C790d323c57Cc4158098d6", "Ethereum"),
            ("IEMGon", "0xcDD60D15125bf3362b6838D2506b0Fa33bc1a515", "Ethereum"),
            ("IJHon", "0xFd50Fc4E3686a8DA814c5C3D6121d8aB98a537F0", "Ethereum"),
            ("INCEon", "0xfA690b2B6f6b4dA518035F1d0aa8B968c23341bb", "Ethereum"),
            ("INDAon", "0xa6CdB19b22B03e03EA89e133A8a46aDc3017aa6d", "Ethereum"),
            ("INTCon", "0xFdA09936DbD717368De0835bA441d9E62069d36f", "Ethereum"),
            ("INTUon", "0x6cc0afD51CE4Cb6920B775F3D6376Ab82b9A93Bb", "Ethereum"),
            ("IONQon", "0x2F2e4b09B99fbf018F600e031aAfD9Da6347Cc75", "Ethereum"),
            ("IRENon", "0x0b59fDb1A233A7477ea14061004b9DD776e73CB3", "Ethereum"),
            ("ISRGon", "0x2691b13fca1E02322685b9554B5ae0F5F3f05C55", "Ethereum"),
            ("ITAon", "0x68622855dcf14ced1B0Cc2A69cc34843708e2E0f", "Ethereum"),
            ("ITOTon", "0x0692481C369E2BDc728A69ae31b848343a4567Be", "Ethereum"),
            ("IVVon", "0x62cA254a363dc3c748e7E955c20447aB5bF06fF7", "Ethereum"),
            ("IWFon", "0x8d05432C2786e3F93f1a9A62b9572DBf54f3ea06", "Ethereum"),
            ("IWMon", "0x070D79021dD7e841123cB0CF554993bF683c511D", "Ethereum"),
            ("IWNon", "0x9DCf7f739B8C0270E2FC0Cc8D0DaBe355a150dBa", "Ethereum"),
            ("JAAAon", "0x219a1b27baA08D72fAC836665a3B752F3C9aCBBC", "Ethereum"),
            ("JDon", "0xdeB6B89088cA9B7d7756087c8a0F7C6DF46f319C", "Ethereum"),
            ("JNJon", "0xdd0E1e6162666a210905fFE8d368661B313c00e9", "Ethereum"),
            ("JPMon", "0x03C1EC4CA9DBb168E6Db0DeF827c085999CBffaF", "Ethereum"),
            ("KLACon", "0xa637ae510cB50E61236a89AC480B93B8c3bcCc46", "Ethereum"),
            ("KOon", "0x74a03d741226f738098C35da8188E57acA50d146", "Ethereum"),
            ("KWEBon", "0xeAA2287290544eD9F481012aa348619A4D2F9e51", "Ethereum"),
            ("LINon", "0x01B19c68f8A9eE3a480dA788ba401cFAbdf19B93", "Ethereum"),
            ("LIon", "0xb6E362a39db703f0F7cF582C9fc043A51624e53d", "Ethereum"),
            ("LLYon", "0xf192957AE52dB3eb088654403CC2eDeD014ae556", "Ethereum"),
            ("LMTon", "0x691b126cF619707Ed5d16CaB1B27C000aa8De300", "Ethereum"),
            ("LOWon", "0x84328D8B85019FdCeCf4c82FBE076Bf350FC0cab", "Ethereum"),
            ("LRCXon", "0x21bE23f5bF87A749670c088F6DEe26760F1Ab80F", "Ethereum"),
            ("LUNRon", "0x58a2EDF0169eDE82904e47a0E2a3a4008eDebB60", "Ethereum"),
            ("MAon", "0xA29dC2102dfc2a0A4A5dCb84Af984315567c9858", "Ethereum"),
            ("MARAon", "0x4604b0b581269843ac7a6b70A5FC019E7762e511", "Ethereum"),
            ("MCDon", "0x4C82c8cD9a218612DCe60b156B73A36705645e3b", "Ethereum"),
            ("MELIon", "0x2816169A49953C548BfEb3948dCF05c4A0E4657D", "Ethereum"),
            ("METAon", "0x59644165402b611b350645555B50Afb581C71EB2", "Ethereum"),
            ("MPon", "0x75846A2b2Eeee6575Ac775f9984be54fd1D08189", "Ethereum"),
            ("MRKon", "0xdc8a7Db05EA704227D56F5D4a4b77A2d1bbA29c0", "Ethereum"),
            ("MRNAon", "0xA2C1c0b4683a871187d4565Eb63ABF9AEF5947Ee", "Ethereum"),
            ("MRVLon", "0xF404E5f887dBd5508e16a1198fCDD5DE1A4296B8", "Ethereum"),
            ("MSFTon", "0xB812837b81a3a6b81d7CD74CfB19A7f2784555E5", "Ethereum"),
            ("MSTRon", "0xCabD955322dfbf94C084929ac5E9Eca3fEB5556F", "Ethereum"),
            ("MTZon", "0x5cb95099a2C7e3C8187fbca6eFe5ba222b5bA820", "Ethereum"),
            ("MUon", "0x050362Ab1072Cb2Ce74d74770E22A3203Ad04ee5", "Ethereum"),
            ("NBISon", "0xE4babaa960bA7D37860f3fe00D7b95D3868e8EDc", "Ethereum"),
            ("NEEon", "0xF46BA88694cd7933Ca28Be84EE787Ad5732e856B", "Ethereum"),
            ("NEMon", "0x0C802aCB21Cb2AadB2EB5E5090868E1361B26B69", "Ethereum"),
            ("NFLXon", "0x032deC3372F25C41EA8054B4987a7c4832CDB338", "Ethereum"),
            ("NIKLon", "0xBf54eb503bb350583D11f4348086DC3608FA245c", "Ethereum"),
            ("NIOon", "0xEe2542F442a5ed8008e2fe3590e14F90DB69f70d", "Ethereum"),
            ("NKEon", "0xD8e26FcC879b30cB0a0B543925a2B3500f074D81", "Ethereum"),
            ("NOCon", "0x8Ce34F749796F82A6990fFa2d80622ef75CA7aD5", "Ethereum"),
            ("NOWon", "0x8bCF9012f4b0c1C3D359eDb7133C294f82f80790", "Ethereum"),
            ("NTESon", "0x3d1cF8692A6f2Fc9048A9cc1A06aBF77F3465f0a", "Ethereum"),
            ("NVDAon", "0x2D1F7226Bd1F780AF6B9A49DCC0aE00E8Df4bDEE", "Ethereum"),
            ("NVOon", "0x28151F5888833D3d767C4d6945a0Ee50D1B193E3", "Ethereum"),
            ("OIHon", "0xBcA31049ab4782f0FfA9CfFCB2cF48e8D6dE4fb8", "Ethereum"),
            ("OKLOon", "0xF0372e226553aF4F343b44111A789f87A9fa427A", "Ethereum"),
            ("ONDSon", "0x818234860A647D480b9BBCC9a47A23889f2Ec900", "Ethereum"),
            ("ONon", "0xa52B2D6cA1CD9B1E8b931645428380c340cAEF9A", "Ethereum"),
            ("OPENon", "0xB22d83E228c4266075Ec75c32aCc3BC059B6f248", "Ethereum"),
            ("OPRAon", "0xB40aFd1d55eA61FC1A6fBe093B817B673C8E78D7", "Ethereum"),
            ("ORCLon", "0x8a23C6BaadB88512b30475C83Df6A63881e33e1E", "Ethereum"),
            ("OSCRon", "0x244EFb92f76a57da49B5F71045dcE3E546E13106", "Ethereum"),
            ("OXYon", "0xeedC48205852E9D83Ea5cA92fa8656597788601f", "Ethereum"),
            ("PALLon", "0x0cE36d199bd6851788e03392568849394cBdE722", "Ethereum"),
            ("PANWon", "0x34bfdFF25F0fdA6d3ad0c33F1e06c0D40bD68885", "Ethereum"),
            ("PAVEon", "0xD4c6CCE0Cadf2fe4C0AF9eE6777989EFD8fB7670", "Ethereum"),
            ("PBRon", "0xD08DDb436e731f32455Fe302723eE0FD2E9E8706", "Ethereum"),
            ("PCGon", "0x193Fdf644451CC394b28B9Cec2F5D32E2b4dE515", "Ethereum"),
            ("PDBCon", "0x46c0A02A877C1412CB32B57028B2F771c0364a7E", "Ethereum"),
            ("PDDon", "0xcC40965d3621362C3EE1dD946bA98d6A708ea86B", "Ethereum"),
            ("PEPon", "0x3cE219D498D807317F840f4CB0f03FA27dd65046", "Ethereum"),
            ("PFEon", "0x06954faa913fA14c28Eb1b2e459594F22f33f3dE", "Ethereum"),
            ("PGon", "0x339ce23a355ed6D513DD3e1462975C4eCD86823a", "Ethereum"),
            ("PINSon", "0xC017C622cd05698580E2decD0F97d4A17DaB70F9", "Ethereum"),
            ("PLTRon", "0x0c666485b02F7A87d21AdD7AEb9F5e64975AA490", "Ethereum"),
            ("PLUGon", "0xe7ee911172bDD557B9Ab6Be7701F86BBc8FD772E", "Ethereum"),
            ("PPLTon", "0xf1883461Ec7BD883A3668749c5CF5f351080d059", "Ethereum"),
            ("PSQon", "0x9ebd34d99Cc3a45B39CAFc14Ad7994263fa2Be56", "Ethereum"),
            ("PYPLon", "0x4EFD92F372898B57F292De69fCe377dd7D912bDd", "Ethereum"),
            ("QBTSon", "0x3807562A482B824C08A564DFefcc471806d3E00a", "Ethereum"),
            ("QCOMon", "0xE3419710c1f77D44B4DaB02316d3f048818C4E59", "Ethereum"),
            ("QQQon", "0x0e397938C1Aa0680954093495B70A9F5e2249aBa", "Ethereum"),
            ("QUBTon", "0xc95c9e3fA311664b5e744B3C2716547BEc2Ba7dA", "Ethereum"),
            ("RDDTon", "0xA9431d354cFAD3c6B76E50f0e73b43D48Be80CD0", "Ethereum"),
            ("RDWon", "0x3a16aF9Ef328D087cc781053A2a2a27549aE6768", "Ethereum"),
            ("REGNon", "0x33aC34DA58168De69cE74a66fbaD81a88F974BD5", "Ethereum"),
            ("REMXon", "0x1140043f02d8EE34b10eae2e32AE921cda1459eE", "Ethereum"),
            ("RGTIon", "0xfDFDf5db2F4A72cb754FfA8896EA012dC2cc0F5e", "Ethereum"),
            ("RIOTon", "0x21deafD91116FCe9fE87C8f15Bde03f99a309b72", "Ethereum"),
            ("RIVNon", "0x04d94914Cd1D7FF749eFedEe764335777225b962", "Ethereum"),
            ("RKLBon", "0x36E3b8d9aAd0e51aC08E56a75A8f6005bF68535B", "Ethereum"),
            ("RTXon", "0x67c5902F5210F62f37157cd9C735c693164c1378", "Ethereum"),
            ("SBETon", "0xfDb46864A7C476F0914c5E82CdED3364a9F56F8a", "Ethereum"),
            ("SBUXon", "0xf15FbC1349ab99ABAd63db3f9A510BF413bE3BeF", "Ethereum"),
            ("SCCOon", "0xdA81DA76070a7377eAEEB2978F0E13C5d57FaDb7", "Ethereum"),
            ("SCHWon", "0xe737f948bDFe3bEAe9423292853EC0579173cebB", "Ethereum"),
            ("SEDGon", "0x8E82a0d7347329703FB6c6A745B1C2b3aBB1658c", "Ethereum"),
            ("SGOVon", "0x8De5D49725550f7b318b2FA0f1B1F118E98E8D0F", "Ethereum"),
            ("SHOPon", "0x908266C1192628371Cff7AD2F5Eba4dE061a0ac5", "Ethereum"),
            ("SHYon", "0x5C424B9b60383FCE7fE7069D2a2B1047BCd04a73", "Ethereum"),
            ("SLVon", "0xF3e4872e6a4cF365888D93b6146a2bAA7348F1A4", "Ethereum"),
            ("SMCIon", "0x2ca12a3F9635fD69C21580def14F25C210cA9612", "Ethereum"),
            ("SNAPon", "0xB2924278cc92E60DB9b673d6A311d7a331dD703D", "Ethereum"),
            ("SNDKon", "0x71E2400CF1Cb83204f33794eD326636A71a9AAfC", "Ethereum"),
            ("SNOWon", "0x5D1a9a9B118fF19721e0111f094f2360b6Ef7A2f", "Ethereum"),
            ("SOFIon", "0x9f2e3EB0160117c56b07652Fe66a08a48b5bD7B5", "Ethereum"),
            ("SOon", "0x99aA107e55250a9fE52bB4b5541A59239EB6D974", "Ethereum"),
            ("SOUNon", "0x966dB065199A3edEa2228C6E5Eb6Ac49FF251AcC", "Ethereum"),
            ("SOXXon", "0x1fE2126bC05E4BB0468C4a198e930c889e1054a3", "Ethereum"),
            ("SPGIon", "0xbc843b147DB4C7E00721d76037b8b92e13AfE13f", "Ethereum"),
            ("SPOTon", "0x590F21186489cA1612f49a4B1ff5c66acD6796A9", "Ethereum"),
            ("SPYon", "0xFeDC5f4a6c38211c1338aa411018DFAf26612c08", "Ethereum"),
            ("SQQQon", "0x0a00c19246Fc41B2524d56C87EC44Ce8b30Ba0f8", "Ethereum"),
            ("STXon", "0xB53894f82a6B2d3B7365f24932B5bDE1c5Fb51FF", "Ethereum"),
            ("TCOMon", "0x398f7f759380F3d309B9fC0E6cB3D36E0D67818d", "Ethereum"),
            ("TIPon", "0x2Df38cA485D01fC15e4FD85847ed26b7EF871c1c", "Ethereum"),
            ("TLNon", "0xc80C91BC6215E1333eA98314b8671d6e26c58470", "Ethereum"),
            ("TLTon", "0x992651BFeB9A0DCC4457610E284ba66D86489d4d", "Ethereum"),
            ("TMon", "0xaB02fc332e9278eBCbbC6B4a8038050c01D15F69", "Ethereum"),
            ("TMOon", "0x60808f2a0d035e16F57e9043842BD1BFBda24fA2", "Ethereum"),
            ("TMUSon", "0xdEb3C23f93349229823A006657CfE1a6552B6340", "Ethereum"),
            ("Ton", "0x3361A73262199873b74D6835760a59B8817fa592", "Ethereum"),
            ("TQQQon", "0xa45cd7ac9865b9539166ebaf2aBc362Df4736580", "Ethereum"),
            ("TSLAon", "0xf6b1117ec07684D3958caD8BEb1b302bfD21103f", "Ethereum"),
            ("TSMon", "0x3Cafdbfe682aec17d5acE2f97A2f3ab3dCf6a4A9", "Ethereum"),
            ("TXNon", "0x58fC9d573Ea773ef9a25c3DE66F990B87Ee5f50E", "Ethereum"),
            ("UBERon", "0x5Bcd8195E3Ef58f677aeF9eBC276B5087c027050", "Ethereum"),
            ("UECon", "0x0F8887772262c449793890DCD3Bf320308dB423B", "Ethereum"),
            ("UNGon", "0x7C488cFc874Ca9F34e7bdBd0410C27CE6d6af5f9", "Ethereum"),
            ("UNHon", "0x075756F3b6381a79633438fAA8964946bf40163d", "Ethereum"),
            ("UNPon", "0x39930751d4569F7DD45d1bA46E82CD3680EC2e0a", "Ethereum"),
            ("URAon", "0xf98Ec282300892b3518B5cB996012b18d9B7D435", "Ethereum"),
            ("USDon", "0xAcE8E719899F6E91831B18AE746C9A965c2119F1", "Ethereum"),
            ("USFRon", "0xFb82561A955bF59B9263301126AF490D3799e231", "Ethereum"),
            ("USOon", "0x1F5fc5c3c8B0F15c7E21AF623936FF2b210b6415", "Ethereum"),
            ("VFSon", "0xBFe6e76A2FE099392064fbB3E868558C82bEb917", "Ethereum"),
            ("VNQon", "0xcCAe8843E26259278C200C6506F6E5A3bdD524cd", "Ethereum"),
            ("Von", "0xaC37c20C1d0E5285035e056101a64e263Ff94a41", "Ethereum"),
            ("VRTon", "0x0752163d221d3D5d4B6e98bD616B22bd2b453964", "Ethereum"),
            ("VRTXon", "0xFC003a764a7B5054Cc6fDb6b511F35deC8022751", "Ethereum"),
            ("VSTon", "0xf1573EdDDB75BF7Ce165f142A17Ed6b5E7f5aA13", "Ethereum"),
            ("VTIon", "0x57B392146848C6321bb2A3D4358DF1bDEACdc62A", "Ethereum"),
            ("VTVon", "0x84E8f1b9b40DD1832925702459D12FFb14d97bF3", "Ethereum"),
            ("VZon", "0x0e3D889D5B857C3e6eb361B9C9aE35bb7DdbD254", "Ethereum"),
            ("WDCon", "0x44e89d34601b8D0155e16634d2553EF7F54DBab2", "Ethereum"),
            ("WFCon", "0x4AD2118Da8a65eaa81402A3d583FEF6eE76BDf3F", "Ethereum"),
            ("WMon", "0x1fb5a8DCd70bE750a97Eaf8a47bBe74Ab7d3183e", "Ethereum"),
            ("WMTon", "0x82106347dDbB23cE44Cf4cE4053Ef1adf8b9323B", "Ethereum"),
            ("WULFon", "0x110CAe53912C2Ed9bF279CD70B3b699e26C79E58", "Ethereum"),
            ("XOMon", "0xF05Ad9840924EA6f977EBccb3b1da87e31DcD0B4", "Ethereum"),
            ("XYZon", "0x6cC41275ef02B4EecCC04fC4424849A96f3272aa", "Ethereum"),
            # ── BNB Chain deployments (Ondo Global Markets, alphabetical) ──
            ("AALon", "0x02D608506cA0048D0D991a11F1E7Fb8CAD1e44f8", "BinanceSmartChain"),
            ("AAPLon", "0x390a684EF9cADE28A7AD0DFa61AB1Eb3842618c4", "BinanceSmartChain"),
            ("ABBVon", "0x8677aBAD7B458bF16a0fB2676DFC7d3f55Ac202A", "BinanceSmartChain"),
            ("ABNBon", "0xEf80743f78d98FC2B47a2253B293152ce8B879ba", "BinanceSmartChain"),
            ("ABTon", "0x5a20886b575058dd7299785f0Ea9b1172942a3E0", "BinanceSmartChain"),
            ("ACHRon", "0x91C62325F901EE29Da8E521CfE68980332A4Ca06", "BinanceSmartChain"),
            ("ACNon", "0x7aF44d51d1fb88c5b74Fc71d3cba649Bb8099D14", "BinanceSmartChain"),
            ("ADBEon", "0xcB22Db0EcB6fe58B7B47db443dCFdfDFbF729CEf", "BinanceSmartChain"),
            ("ADIon", "0x0E246E05212DbBD78A354C072a92B4e5723B2fa0", "BinanceSmartChain"),
            ("AGGon", "0x08cE97F3D5Cf11E577D091ab048bC5e2EaE3FABB", "BinanceSmartChain"),
            ("ALBon", "0x0B790ABd6594918DE1022233b7cc79baDB84d92a", "BinanceSmartChain"),
            ("AMATon", "0x5ecC352C4640f1d26BD231dbBd171f40f7d0Eec6", "BinanceSmartChain"),
            ("AMCon", "0x1d7B5e06fdbe4FD33f5C64C081E32B5d539751D0", "BinanceSmartChain"),
            ("AMDon", "0x9f16E46c73b43BDB70861247d537bEE4eA18F639", "BinanceSmartChain"),
            ("AMGNon", "0xFBdF0366F800CC79d6663DA26bc0BF21FB455Aa6", "BinanceSmartChain"),
            ("AMZNon", "0x4553cFe1C09f37f38b12dC509F676964e392F8Fc", "BinanceSmartChain"),
            ("ANETon", "0x538e2838f9ebC9B891399DF4a8dcc42890D9dc20", "BinanceSmartChain"),
            ("APLDon", "0x18De24acb876C0B8392d9C55583Bb21c0355980b", "BinanceSmartChain"),
            ("APOon", "0x5630B5741A33371D9d935283849A16dC808f7F3A", "BinanceSmartChain"),
            ("APPon", "0xEDB3124e96c64C177Eb709CbC64F9977dB40Ea74", "BinanceSmartChain"),
            ("ARMon", "0x527C6436E1eAa4f2065CDE4090F798Cb5D031dD6", "BinanceSmartChain"),
            ("ASMLon", "0xb034f6Cb52b7f2Fd5a7EeeffCa6b9aDCD6b9A6F6", "BinanceSmartChain"),
            ("ASTSon", "0x45Abf29515bc23F8c0Ed2a06584444cE473A75FB", "BinanceSmartChain"),
            ("AVGOon", "0x0ED2E3180EDf393e6bf8Db124bD15DDD54dE150A", "BinanceSmartChain"),
            ("AXPon", "0xD803f8777187D6DEe1eA57854aEB957043fb1675", "BinanceSmartChain"),
            ("BABAon", "0xd5964f3fcee8D649995AB88F04b8982539c282D2", "BinanceSmartChain"),
            ("BACon", "0xd615468088B19FB9d4F03CB3CE9E33876fF3Db99", "BinanceSmartChain"),
            ("BAon", "0xf21132A811Ad1A878E21Af60F64d4e690C9DaA42", "BinanceSmartChain"),
            ("BBAIon", "0x7BA995f1662A01f3bE0DC299ce94Bb7e9C7075f5", "BinanceSmartChain"),
            ("BIDUon", "0x467e59ce5D5fe01686D4A80dd1E1DAE13549AA6c", "BinanceSmartChain"),
            ("BILIon", "0x91fc7371d6dE682A1e8CFcB4EB7dA693312A03a4", "BinanceSmartChain"),
            ("BINCon", "0x940F442746D9AE699e63c378D52C4494ea02684f", "BinanceSmartChain"),
            ("BLKon", "0x24F5471183eA549987f245D6ce236B6108869C92", "BinanceSmartChain"),
            ("BLSHon", "0xFBE22D27B6e153244882fD7bdfE7C6109918281B", "BinanceSmartChain"),
            ("BMNRon", "0x52AD57A7eA642E99A892AFc79E937B383f1b59e9", "BinanceSmartChain"),
            ("BNOon", "0x5f2d37192576a6804F44722eB828E280D5FB43Dc", "BinanceSmartChain"),
            ("BTGon", "0xe2Ac868F2FD097086d83Bc939248E5aE08d35DA4", "BinanceSmartChain"),
            ("BTGOon", "0x5fA699c0c1319b8D86489AF77dFDe4Fa97B47DF8", "BinanceSmartChain"),
            ("BZon", "0xC2C7fcdDC37f6737ca2481ebda6B81Ee279fE20C", "BinanceSmartChain"),
            ("CAPRon", "0x812Fc2943371C952c6c8Daf99Fe665Eb0e40Cd27", "BinanceSmartChain"),
            ("CATon", "0x274B0cB6DB9473245A31cDEA9B789786f4108e4B", "BinanceSmartChain"),
            ("CEGon", "0x65D84F0990B7394209d591380C2952c83D778aA3", "BinanceSmartChain"),
            ("CIBRon", "0xDcd4536508060dab8F43C334B3a6C72c39528DA5", "BinanceSmartChain"),
            ("CIFRon", "0xDad07D0Ca26ed4109Bc00893dBee3Ed4cE8ce2a4", "BinanceSmartChain"),
            ("CLOAon", "0x4Ef383f521e803863a33FcA8F3f861e53eF9Ef9B", "BinanceSmartChain"),
            ("CLOIon", "0xd7E3317D54473DAb04135fb0676623f237Ff5CA9", "BinanceSmartChain"),
            ("CMGon", "0xAed5985afC12aA09d87F55B4b1e6bC3B8f7B0208", "BinanceSmartChain"),
            ("COFon", "0x53a8c5fc5643b437779742f494691e6b7C660a8b", "BinanceSmartChain"),
            ("COHRon", "0x0585756aAFB241b0f8A9Df62Db26c566091Bde0b", "BinanceSmartChain"),
            ("COINon", "0xf8589b526FdD65F7F301c605a6e04F0F1b4B3620", "BinanceSmartChain"),
            ("Con", "0x8dDB97556F6ae98B4d408c56B167139fE1Cbe3e8", "BinanceSmartChain"),
            ("COPon", "0x0D586b51A90Dc999f9bB6A0506Da7F034a1D3A2E", "BinanceSmartChain"),
            ("COPXon", "0xEC93fE7Ff4B09CA3CCAFBc4CC9615E62BE412780", "BinanceSmartChain"),
            ("COSTon", "0x34375f826fD3dD4E15F883d4F4786bB45eb705ac", "BinanceSmartChain"),
            ("CPNGon", "0x19904Bc04c09e5d29Ed216dDD105bdf103A0bA2D", "BinanceSmartChain"),
            ("CRCLon", "0x992879Cd8ce0c312d98648875B5A8D6D042cbF34", "BinanceSmartChain"),
            ("CRMon", "0xD04a2BB053277721a8321d7441eEd5b42FDF7250", "BinanceSmartChain"),
            ("CRWDon", "0xe6837794FBC6DD024733A1A31F86061296FA2752", "BinanceSmartChain"),
            ("CRWVon", "0x76E39171Cb665a35981e744e2CEB7012F76caEAc", "BinanceSmartChain"),
            ("CSCOon", "0x34304f2f7cC487eb4186e6D69F5905A613474aA2", "BinanceSmartChain"),
            ("CVNAon", "0xc145DC2eBDbe8EAD1fEcDeBF46c76Eb1Fdd0104D", "BinanceSmartChain"),
            ("CVXon", "0xD3113A0AD20a46F6a662C63fe8E637f7713E59c7", "BinanceSmartChain"),
            ("DASHon", "0x7567c2a46BCE46373b454682F3d95e6535BDe144", "BinanceSmartChain"),
            ("DBCon", "0xFC2067E3e6a289C205151d96Ef67A032f339566D", "BinanceSmartChain"),
            ("DEon", "0x90cCbB75d61Cb65cd73a3ABb5Df04a75961612b7", "BinanceSmartChain"),
            ("DGRWon", "0x1CD89241B26FCDC421FD02907d6504C8AbBfe1bc", "BinanceSmartChain"),
            ("DISon", "0xeEe9eeE593cB8f7946260B4066CBa7907f40ACFa", "BinanceSmartChain"),
            ("DNNon", "0x70bd780076E25D087Ed9c35f4e4A540522Abe8cF", "BinanceSmartChain"),
            ("ECHon", "0x551f8Db0da800C910E12Cf991eac306714481685", "BinanceSmartChain"),
            ("EEMon", "0x00c81d35edDF44c75d4Db9E07bDCdC236eB0ebcf", "BinanceSmartChain"),
            ("EFAon", "0x38B9A53bfDc5dba58a29bD6992341927C2fca637", "BinanceSmartChain"),
            ("ENLVon", "0x5A9D924FC336A5EC8cf3B1909aA660533B50b015", "BinanceSmartChain"),
            ("ENPHon", "0x30938154E2697694F41592C2e48459287dEBe4BF", "BinanceSmartChain"),
            ("EQIXon", "0xE4E12c9CEc3e8cAE405202A97f66AFA695075fa0", "BinanceSmartChain"),
            ("ETHAon", "0x04B16ff1F9673146F68AA5d5F57aA45AdcF068E1", "BinanceSmartChain"),
            ("ETNon", "0x4697b2A050f7B5A8e1ebc27c325f9D78D094f041", "BinanceSmartChain"),
            ("EWJon", "0x82715299f3f132FB85f3De1F7e8fAfd3d79f3eB5", "BinanceSmartChain"),
            ("EWYon", "0x12B7aDC48416A103F63E7e6210f62C81dfB91fD0", "BinanceSmartChain"),
            ("EWZon", "0x9876F4b879cDe9Aa49Ffd260034A0698B7B33A49", "BinanceSmartChain"),
            ("EXODon", "0x92d504158A8Dc69de989dB5EDe3230D958fb8630", "BinanceSmartChain"),
            ("FCXon", "0xE3b17E6D290A0F28bd32aF4064637057627004D5", "BinanceSmartChain"),
            ("FFOGon", "0xEA130432a9feE9ca1A7eDa84028650d38Bd0E232", "BinanceSmartChain"),
            ("FGDLon", "0xee0d57462f20434030B8262204c00c0eA0399C41", "BinanceSmartChain"),
            ("FIGon", "0x93fac02b22B6743423381D163aec418178019B7a", "BinanceSmartChain"),
            ("FIGRon", "0x620477782cEa4C4171165396F8014EdEF83a13da", "BinanceSmartChain"),
            ("FLHYon", "0x240Eb4859B4537D250cf784cc758c404dA5Fe4bd", "BinanceSmartChain"),
            ("FLQLon", "0x48187890D16aEE64798E02C5BeD510f4dB5694A9", "BinanceSmartChain"),
            ("Fon", "0xb1ABA049C42B6fe811766EBA61F51f11C57acC4b", "BinanceSmartChain"),
            ("FSOLon", "0x54B92Fd77229269Ff6484942C123cCa72f2D6fEC", "BinanceSmartChain"),
            ("FTGCon", "0xe96F94e10F1265dcc15F83D251f1F6758d2CD67D", "BinanceSmartChain"),
            ("FUTUon", "0x5acf40056ED51C8bBCD1b125Ef803581Ac89A627", "BinanceSmartChain"),
            ("FXIon", "0x9b8E987e6fEc8Cf1380C4dcA7071e2C7853AEEA1", "BinanceSmartChain"),
            ("GEMIon", "0x817942D5DE16092656568e9f67F54CCb462f8989", "BinanceSmartChain"),
            ("GEon", "0x5151A22421Ed4277F1e4ca4785a07b035D548a36", "BinanceSmartChain"),
            ("GEVon", "0x2Aea1D415D45CCF3EaBE565d45DcaF4ea2035b9c", "BinanceSmartChain"),
            ("GLDon", "0xfA9a1E901085e269f6D428F79Cd5252d8b919344", "BinanceSmartChain"),
            ("GLTRon", "0x15580092796f69825CFf4738Cac55D05D41eaa42", "BinanceSmartChain"),
            ("GLXYon", "0xF98B89825233808CD37706A53D2b4Ae3e359d442", "BinanceSmartChain"),
            ("GMEon", "0xdABb9afF4cf02f26D2014e4cA9f94aC6fe6572a3", "BinanceSmartChain"),
            ("GOOGLon", "0x091FC7778e6932d4009B087B191D1EE3bac5729A", "BinanceSmartChain"),
            ("GRABon", "0xab2F74804C022C5249d52e743AF4340E42F5f3b6", "BinanceSmartChain"),
            ("GRNDon", "0x20CCe48D767eD68CBbA7727c4c504eFE5Bcb626c", "BinanceSmartChain"),
            ("GSon", "0x0D4f9b25F81163Fb4840BA4F434672543823000C", "BinanceSmartChain"),
            ("HDon", "0x31DAbf49E4BC1Af1456c1819cb6A2562154e92F3", "BinanceSmartChain"),
            ("HIMSon", "0x4693f6F5EF257381a28afd0673e64d8b32d5C6aD", "BinanceSmartChain"),
            ("HOODon", "0x19601179A60f55Ff6636F5D1A8b6671053Bd60a8", "BinanceSmartChain"),
            ("HYGon", "0x0DAE81A905b645a3D1E67129b89CD0Acda224E9A", "BinanceSmartChain"),
            ("HYSon", "0x75E9D68e99e76714ed1a7663ab48ba3AaBd7A6c5", "BinanceSmartChain"),
            ("IAUon", "0xcB2a0F46f67dC4c58a316F1c008EDef5c2311795", "BinanceSmartChain"),
            ("IBITon", "0x68B07cEf227Cea1b2b6683921C8c825cd5C69Ec7", "BinanceSmartChain"),
            ("IBMon", "0xE8ff70859Ce4cbd72E4352b4fb45F5BF39d07464", "BinanceSmartChain"),
            ("IEFAon", "0x918008C3d29496C37b478b611967BeaCA365aF36", "BinanceSmartChain"),
            ("IEFon", "0xA486a0A05250E8621bA3B26C3bbc517145eba619", "BinanceSmartChain"),
            ("IEMGon", "0x22092c94a91d019Ad15536725598B0A6BE0a73C0", "BinanceSmartChain"),
            ("IJHon", "0x167E93A849A0cc479769132552B99aa1cFA0948c", "BinanceSmartChain"),
            ("INCEon", "0x5e24DB6DE4C21E2C8f9E81bAcfCedFBAC2DeE4aa", "BinanceSmartChain"),
            ("INDAon", "0xdB0748297fbEf0B33dF89e86519A0BD3adAf6459", "BinanceSmartChain"),
            ("INTCon", "0xA528CaaA2f96090e379d43F90834C75dF54D6E74", "BinanceSmartChain"),
            ("INTUon", "0x6E3e077A6C0E3c27fD6D00B97387D9b7Bd451BAB", "BinanceSmartChain"),
            ("IONQon", "0x40d8E1fbaf69173c47fa493FeB50a84eeC6b57eE", "BinanceSmartChain"),
            ("IRENon", "0x8FD70eE385f470c8D6FDA2D93a4E49C849BAC6a6", "BinanceSmartChain"),
            ("ISRGon", "0x784584933C2192Caa062E90d8140D94768CE62d8", "BinanceSmartChain"),
            ("ITAon", "0x88B90f45bd6a4F97f7D85d280eD64A40880e4935", "BinanceSmartChain"),
            ("ITOTon", "0xcf9CAf83053213c44dd7027Db3e1E4aC98E55f8f", "BinanceSmartChain"),
            ("IVVon", "0x1104EB7e85E25eB45F88e638b0C27A06C1A91CB2", "BinanceSmartChain"),
            ("IWFon", "0x40755F06aB7F8dE1ab3a9413b1ef562d63DE19B1", "BinanceSmartChain"),
            ("IWMon", "0x500EAFc69b68Acd6F27064f9B75F1C7d91CC4d9F", "BinanceSmartChain"),
            ("IWNon", "0xF54b94eA21e1Da5d51eF00fd4502225e5394F874", "BinanceSmartChain"),
            ("JAAAon", "0x84719A1082ed487c7eeac7d69885E3CC2009Ea78", "BinanceSmartChain"),
            ("JDon", "0xE92bE960aE64F6a914Ca77014CaC9E56DE7f36C1", "BinanceSmartChain"),
            ("JNJon", "0xd1F799cB9f5d0a02951b0755BeCeD6C43882712F", "BinanceSmartChain"),
            ("JPMon", "0x317bF42b43A394860718266Dec445DCC9FD9dA49", "BinanceSmartChain"),
            ("KLACon", "0xFc263946439b0d802bF4C5a6fCd34E2885259f91", "BinanceSmartChain"),
            ("KOon", "0x405F38B90beBF1259062CF29Da299f3398662bcb", "BinanceSmartChain"),
            ("KWEBon", "0x7437203800140BA7d9081ddE8cEF09EE40E3Bf03", "BinanceSmartChain"),
            ("LINon", "0xE1743616f705954620aa351465c8885fBDE5A8A9", "BinanceSmartChain"),
            ("LIon", "0x9810beac9af3C30d14cFB61cDd557E160f60FD50", "BinanceSmartChain"),
            ("LLYon", "0x341D31b2be1Fee9C00e395A62bA41837F4322EEd", "BinanceSmartChain"),
            ("LMTon", "0xd09F7B75b9659b864C6F82bb00Ff096f9D277998", "BinanceSmartChain"),
            ("LOWon", "0x2ec46EeD30c94caa5979e6A0395Abe824138335f", "BinanceSmartChain"),
            ("LRCXon", "0x35895a1fa1AFf7FB3204fB01257409Fd75acB24C", "BinanceSmartChain"),
            ("LUNRon", "0xA3b7B7cfEb023a6C4f444f5ca9a3Fc85809Ece15", "BinanceSmartChain"),
            ("MAon", "0x25FfDA07F585c39848dB6573E533D7585679C52d", "BinanceSmartChain"),
            ("MARAon", "0xd226d8170EE38793430C7Dec6903df4B818BB74C", "BinanceSmartChain"),
            ("MCDon", "0x995ADd4bA29a628A57930a8a185c62cA044eC090", "BinanceSmartChain"),
            ("MELIon", "0x60a8f8e05200fF73aFde9E2caE819bF1605f0BdD", "BinanceSmartChain"),
            ("METAon", "0xD7dF5863A3e742F0c767768cDfcb63f09E0422f6", "BinanceSmartChain"),
            ("MPon", "0x4BAf4dC56Cf6a525a0874e25cc6372A6A8915135", "BinanceSmartChain"),
            ("MRKon", "0x869027261075c3C239D6A26842579b93802606f4", "BinanceSmartChain"),
            ("MRNAon", "0x01486675Da0764ee780Ea7cB65C33062E9B2D28c", "BinanceSmartChain"),
            ("MRVLon", "0x1501EC83FFEf405B4331CC4f73277a40fb0C627d", "BinanceSmartChain"),
            ("MSFTon", "0x6Bfe75D1ad432050eA973C3A3DcD88F02e2444C3", "BinanceSmartChain"),
            ("MSTRon", "0x7313EA16493b2f55054Df0131A3A14B043ec8992", "BinanceSmartChain"),
            ("MTZon", "0xf49046aAE76EAeb7ffd3EF116ce0f7CD0F52d93e", "BinanceSmartChain"),
            ("MUon", "0x8b6ACf6041A81567f012Ff6A4C6D96d5818d74bF", "BinanceSmartChain"),
            ("NBISon", "0xee268780473E7a0e47baC41547C6E01512555A16", "BinanceSmartChain"),
            ("NEEon", "0xe9d43f7E6b2237e8873a7003b3F43c6B03160bE5", "BinanceSmartChain"),
            ("NEMon", "0x5E63232993789601CE362e0240a299C1DfCBfbEc", "BinanceSmartChain"),
            ("NFLXon", "0x7048F5227b032326cC8DBC53cF3FdDD947a2c757", "BinanceSmartChain"),
            ("NIKLon", "0xe23f03d2907CdC38a10F6CcDc1a157bf1AFe51De", "BinanceSmartChain"),
            ("NIOon", "0xC6f9eDbeE6042a237D72493bBdA3eE2c3c62F708", "BinanceSmartChain"),
            ("NKEon", "0x04b5E199F2eC84f78B111035F57b16BeE448dB6F", "BinanceSmartChain"),
            ("NOCon", "0x4D3442D884202584F1729bCA20Db05472B886B52", "BinanceSmartChain"),
            ("NOWon", "0xeb19c13c54B1cD48AFc62F6503375e92D5f1e856", "BinanceSmartChain"),
            ("NTESon", "0x282973969118F9fe39bf2FF3D8DD1EFEE82CCb11", "BinanceSmartChain"),
            ("NVDAon", "0xA9eE28C80f960B889dFbd1902055218cBa016F75", "BinanceSmartChain"),
            ("NVOon", "0x08a513779f46FFb7A34F16094a94016d010128a8", "BinanceSmartChain"),
            ("OIHon", "0x31D6011023D6c7695Efc29bB016830F3F36De40a", "BinanceSmartChain"),
            ("OKLOon", "0xaF6C03acf72355Ce98d0741302B78870b376428C", "BinanceSmartChain"),
            ("ONDSon", "0xd85d4Ce29b4cA361FF72Ef0E53D6236e334C5DB6", "BinanceSmartChain"),
            ("ONon", "0xB35a9EAB5D25282f4e668798B629a9294e9A47aa", "BinanceSmartChain"),
            ("OPENon", "0xa09699fc0cbb1F85128450A0ff6a3c4d3A7E7B9B", "BinanceSmartChain"),
            ("OPRAon", "0x88672043905BdD272df55a5A7Bb1b7E1e693cBc5", "BinanceSmartChain"),
            ("ORCLon", "0x03E4bd1Ea53f1da84513da0319D1f03dD1BBCf93", "BinanceSmartChain"),
            ("OSCRon", "0xB0752Aa50B089EE6eA9aCD51373207Fa460E87bb", "BinanceSmartChain"),
            ("OXYon", "0x01b5a4aC600bE98448DbEFBB78BcDF38262552cc", "BinanceSmartChain"),
            ("PALLon", "0x3fCD741646A9790635b938Cdb69af5Df356CbaAB", "BinanceSmartChain"),
            ("PANWon", "0x0eAa1a75bd682A5669AB2371A559fBD039C6b9Eb", "BinanceSmartChain"),
            ("PAVEon", "0x6f28Cb07790c1049ecd7482d09Fd13B977B47201", "BinanceSmartChain"),
            ("PBRon", "0x2b1d5cDeCC356530a746C5754231EfaEAca64022", "BinanceSmartChain"),
            ("PCGon", "0x47B36DDB9dd12A8411f78226f55e8c3f0D65481f", "BinanceSmartChain"),
            ("PDBCon", "0xCF3E84E62002ca459Db81B2032d7Fe13715BAd51", "BinanceSmartChain"),
            ("PDDon", "0xF3e82EA164CB344B2b11Bad4c24b0Ea4F7BA4714", "BinanceSmartChain"),
            ("PEPon", "0xf99F8f3a95257d82006183bd524efa7aaCc9Ef7A", "BinanceSmartChain"),
            ("PFEon", "0x8A83C31D6751833B4940b6e871c48d9A15a07b46", "BinanceSmartChain"),
            ("PGon", "0x400F1e257f86D25578A0928C94dC95115F09d5c9", "BinanceSmartChain"),
            ("PINSon", "0xCfD1F0DF84300EA1a4e2BA5238043A2fA5A7237c", "BinanceSmartChain"),
            ("PLTRon", "0x9351AbD19f42101dD36025E495B98E910b255d78", "BinanceSmartChain"),
            ("PLUGon", "0x4752AE8f910b25e64E4406eAad50c1B4E8DE7E6D", "BinanceSmartChain"),
            ("PPLTon", "0x3EC23F52F6573FC0587A0631dd8C3b107f6bcb35", "BinanceSmartChain"),
            ("PSQon", "0x3802dc739eF9E226f36421A9c15eFa519153bBBe", "BinanceSmartChain"),
            ("PYPLon", "0x374D03A6c0d5bd4bE0A5117eBE1B49D52aC8a53F", "BinanceSmartChain"),
            ("QBTSon", "0x8C7Bf0ED6bc778bde1489De1592C1aAd3E66371d", "BinanceSmartChain"),
            ("QCOMon", "0xfBD4D681C92ead6Af0E49950c8B2e47EeAcbB2dB", "BinanceSmartChain"),
            ("QQQon", "0x0cdE6936d305d5B34667fC46425E852efd73559a", "BinanceSmartChain"),
            ("QUBTon", "0x82E07C1017032cFd889b1Ca81EBe722c4D4de825", "BinanceSmartChain"),
            ("RDDTon", "0x4DA12f47578ef89c76179b760C778E70b668f80b", "BinanceSmartChain"),
            ("RDWon", "0x23e39D94807a8bb7e3f8294b4911D04EE26DcE39", "BinanceSmartChain"),
            ("REGNon", "0x30BD85fD4286c5c9857679F5B188f737B4a7B8C0", "BinanceSmartChain"),
            ("REMXon", "0xC16f47C4A7eD39372B9a0e3e2016cEDe9b4cB83a", "BinanceSmartChain"),
            ("RGTIon", "0xEd2a500eB2b66679e0BbD76e51a60049aE5f3271", "BinanceSmartChain"),
            ("RIOTon", "0xC4a88a72B848255Fd24Da3C1aD6755D980535FB1", "BinanceSmartChain"),
            ("RIVNon", "0x277E1FA8704c5511FEd7E30Bc691F922aa30101B", "BinanceSmartChain"),
            ("RKLBon", "0xb4D695569236273745B4CD54B539b1b9Cc1513af", "BinanceSmartChain"),
            ("RTXon", "0x44fde2C6Bc2C2b54962C69fcEF57A2A50121DBD7", "BinanceSmartChain"),
            ("SBETon", "0x99e01F02d66455Bb106D91D469c9EAF6aB4904f6", "BinanceSmartChain"),
            ("SBUXon", "0x94d7754541B829A87321d56121Bc544167Ac490D", "BinanceSmartChain"),
            ("SCCOon", "0xF15B8f7465b92799F6EE440F86B3CAB5A4dbc65A", "BinanceSmartChain"),
            ("SCHWon", "0xe5BA472C98b7E4695bD856290De66bDEDaffC123", "BinanceSmartChain"),
            ("SEDGon", "0x8755c5C39b1AA9053a83AC731242a2cf4D04B0Fe", "BinanceSmartChain"),
            ("SGOVon", "0xc008c5F579ec1450F20099c39F587547e27c7523", "BinanceSmartChain"),
            ("SHOPon", "0x43d0B380c33cD004a6A69aBD61843881a2de4113", "BinanceSmartChain"),
            ("SHYon", "0xf95e50BE5Efc96117c28775F80C7Cdb41Ebc4888", "BinanceSmartChain"),
            ("SLVon", "0x8b872732b07be325a8803CDB480D9d20B6f8d11B", "BinanceSmartChain"),
            ("SMCIon", "0xC142ba8ccD36d80C3a001342fb83E4C3d218A873", "BinanceSmartChain"),
            ("SNAPon", "0xF325884D9bcac457271fE7F7B6be1765348fCCa2", "BinanceSmartChain"),
            ("SNDKon", "0x4Fd67CB8CFEdc718BAc984b5936abE3330d0a2A4", "BinanceSmartChain"),
            ("SNOWon", "0x138ED6833ff4E8811E1FEa0D005E13726c8886F9", "BinanceSmartChain"),
            ("SOFIon", "0x71507068e98049cBA81E9bbc8d901E4A2f4222Eb", "BinanceSmartChain"),
            ("SOon", "0xD7a6353a23ED2c4fcaC29A63CBBe3f65ffEf41F5", "BinanceSmartChain"),
            ("SOUNon", "0xeDCF71B2e2217064038AdCb54A3C3a5fC3488eF1", "BinanceSmartChain"),
            ("SOXXon", "0x2A3cbF64C8181DB4a25D41D4d7a7Db9984C59DAC", "BinanceSmartChain"),
            ("SPGIon", "0x55B370B704240a914f42B5bBB3195431C031f9f8", "BinanceSmartChain"),
            ("SPOTon", "0x50356167a4DbC38BeA6779C045e24E25fAcEdfdc", "BinanceSmartChain"),
            ("SPYon", "0x6a708EAD771238919D85930b5a0f10454E1C331a", "BinanceSmartChain"),
            ("SQQQon", "0x17515B68378d86C38F394c666e79907dA05dcBA9", "BinanceSmartChain"),
            ("STXon", "0x966EbCBA3c51E81f5CF159a1EaBeFd2327aB5E8D", "BinanceSmartChain"),
            ("TCOMon", "0x6459303F58244Ff1E7A42b90aA3782Dfb6Ca6969", "BinanceSmartChain"),
            ("TIPon", "0x2Ac26EC236df5D1d2Ad1A6Dd4E448A90E45DC35D", "BinanceSmartChain"),
            ("TLNon", "0xbbE4Dfe7a349Fb72aEc6f52d5CD9bDD78AE8f313", "BinanceSmartChain"),
            ("TLTon", "0xf69e40069aC227C11459E3f4e8a446b3401616b6", "BinanceSmartChain"),
            ("TMon", "0xECC1299F183b6a720A6F4729Bf24F82Cd8D50828", "BinanceSmartChain"),
            ("TMOon", "0xbCf7D958791152128710565a5fC6f68342Ed71C8", "BinanceSmartChain"),
            ("TMUSon", "0x2588F20BAd92Da8dCCE7FaC8311B5F8Ab4690E43", "BinanceSmartChain"),
            ("Ton", "0x4255279aF47cf10eFB9A5C8839F90170F4EF759f", "BinanceSmartChain"),
            ("TQQQon", "0xe42CfB20e00912409B77A602B5BDcfF3c7aCC5F4", "BinanceSmartChain"),
            ("TSLAon", "0x2494b603319d4D9F9715c9f4496d9E0364B59d93", "BinanceSmartChain"),
            ("TSMon", "0xC37042A7a4fa510D8884a433762aB87257B91965", "BinanceSmartChain"),
            ("TXNon", "0xCa3a5c955F1F01f20aAcF9501B03E4aa235e478B", "BinanceSmartChain"),
            ("UBERon", "0xDE9D6036FCA870f7efc5A82722Ae694c371Ac909", "BinanceSmartChain"),
            ("UECon", "0xE7ddF606841ee278A30E5C90486681e68ddd8cbF", "BinanceSmartChain"),
            ("UNGon", "0xA5351C9bf08055E03642b6b8649A0f7e895501BF", "BinanceSmartChain"),
            ("UNHon", "0x3385Cb29cCA0aC66f5d2354d13ef977b49A2510f", "BinanceSmartChain"),
            ("UNPon", "0xfe9aA194E3C4604f3872f220eb41C33A287FCD90", "BinanceSmartChain"),
            ("URAon", "0xc7806943663158D68740a14ab0B270bD60BDe87D", "BinanceSmartChain"),
            ("USDon", "0x1f8955E640Cbd9abc3C3Bb408c9E2E1f5F20DfE6", "BinanceSmartChain"),
            ("USFRon", "0xf4fd75764A5C086fb12F822be2cA318b3a362DC3", "BinanceSmartChain"),
            ("USOon", "0x94174e3D1335db402dD03A092f7aA7ac2cb32be4", "BinanceSmartChain"),
            ("VFSon", "0x1D2EaAF0aE00382893aa4318Bd88d1Cd0e9B858A", "BinanceSmartChain"),
            ("VNQon", "0x10b58A3d9DCeC59bB1c3bf6b9c9414eAfCE711C9", "BinanceSmartChain"),
            ("Von", "0x1CdE419faE0Ef7f7931aE3E29e5f411C8C5E5Fa1", "BinanceSmartChain"),
            ("VRTon", "0x9Cea8A7Be1Ab0320b709d368Ad60d8500f55995f", "BinanceSmartChain"),
            ("VRTXon", "0x8c9979Dc208f74a5602c38691aa920F121e2f863", "BinanceSmartChain"),
            ("VSTon", "0xf2C24c47805f4f72d3919C8674bFDd401505794B", "BinanceSmartChain"),
            ("VTIon", "0x158734153f354CB326EE690C3d55f810DCb0fc90", "BinanceSmartChain"),
            ("VTVon", "0xc2dD31B1B3a2f515cE0D48De712c6744C3475170", "BinanceSmartChain"),
            ("VZon", "0xA3B089C886E6d721f49DEF8E050F3b9D4362560B", "BinanceSmartChain"),
            ("WDCon", "0xcEb29848d04Ad3Cb46E1fE8E45B82ffAc39D797d", "BinanceSmartChain"),
            ("WFCon", "0x629520dEE1620dEf11596F84E85de9F1Ff653012", "BinanceSmartChain"),
            ("WMon", "0xCE0466Bae0e867239719dC386CA84b1F3eFE6914", "BinanceSmartChain"),
            ("WMTon", "0xa7d1e886acf66Ec0656DF2DECB4B7C893A3bAb4C", "BinanceSmartChain"),
            ("WULFon", "0xaD56701D9e57957e28e546Db7db508A16d4f86CC", "BinanceSmartChain"),
            ("XOMon", "0x4D209D275e3492aC08497A7A42915899c4dD5E86", "BinanceSmartChain"),
            ("XYZon", "0xe778A2e5D953c82EB9475cf3B87654226a867344", "BinanceSmartChain"),
        ],
    ),
]


# ══════════════════════════════════════════════════════════════════════════════
# 6. PULLER REGISTRY
#    ↳ Add new DataPullers here — that's the only file you need to touch.
# ══════════════════════════════════════════════════════════════════════════════


def _build_insights_context(pullers: List[DataPuller]) -> str:
    """
    Collect a concise, structured text summary of the latest cached data from
    every puller. This is passed as context to Claude when generating insights.
    """
    today = datetime.utcnow().strftime("%Y-%m-%d")
    lines: list[str] = [
        f"Crypto dashboard data snapshot — {today} UTC",
        "=" * 60,
        "",
    ]

    def _fmt(v: float | None, prefix: str = "$") -> str:
        if v is None or (isinstance(v, float) and math.isnan(v)):
            return "N/A"
        if abs(v) >= 1_000_000_000:
            return f"{prefix}{v/1e9:.2f}B"
        if abs(v) >= 1_000_000:
            return f"{prefix}{v/1e6:.2f}M"
        if abs(v) >= 1_000:
            return f"{prefix}{v/1e3:.1f}K"
        return f"{prefix}{v:.2f}"

    for p in pullers:
        df = p.get_latest()
        if df is None or df.empty:
            continue

        group = getattr(p, "GROUP", "")
        name  = getattr(p, "TOKEN_NAME",
                getattr(p, "GROUP_LABEL",
                getattr(p, "name", "?")))

        # ── helpers shared across sections ───────────────────────────────────
        def _wow_mom(series: pd.Series, tail_n: int = 7) -> tuple[str, str]:
            """Return (WoW string, MoM string) for a daily volume series."""
            last_n    = series.tail(tail_n)
            prev_n    = series.iloc[-(tail_n * 2):-tail_n] if len(series) >= tail_n * 2 else pd.Series(dtype=float)
            last30    = series.tail(30)
            prev30    = series.iloc[-60:-30] if len(series) >= 60 else pd.Series(dtype=float)
            wow = mom = ""
            if not prev_n.empty and prev_n.mean() > 0:
                wow = f"  WoW avg vol change : {(last_n.mean() - prev_n.mean()) / prev_n.mean() * 100:+.1f}%"
            if not prev30.empty and prev30.mean() > 0:
                mom = f"  MoM avg vol change : {(last30.mean() - prev30.mean()) / prev30.mean() * 100:+.1f}%"
            return wow, mom

        def _ath_lines(series: pd.Series, dates: pd.Series, label: str = "vol") -> list[str]:
            """Return lines describing the all-time high and distance from it."""
            ath_idx  = series.idxmax()
            ath_val  = series[ath_idx]
            ath_date = pd.to_datetime(dates[ath_idx]).strftime("%Y-%m-%d")
            latest_v = series.iloc[-1]
            pct_from = (latest_v - ath_val) / ath_val * 100 if ath_val > 0 else 0
            is_ath   = abs(pct_from) < 1.0   # within 1 % → call it the ATH
            out = [f"  All-time high {label} : {_fmt(ath_val)} on {ath_date}"]
            if is_ath:
                out.append(f"  *** Latest day IS the all-time high (or within 1%) ***")
            else:
                out.append(f"  Latest vs ATH      : {pct_from:+.1f}%")
            return out

        # ── Single-token puller (Solana tokens, stablecoins) ─────────────────
        if group in ("solana_tokens", "stablecoins"):
            try:
                df["date"] = pd.to_datetime(df["date"])
                df = df.sort_values("date")
                latest = df.iloc[-1]
                last7  = df.tail(7)
                last30 = df.tail(30)

                avg_vol_7d  = last7["volume_usd"].mean()
                avg_vol_30d = last30["volume_usd"].mean()
                wow, mom    = _wow_mom(df["volume_usd"])
                ath_lines   = _ath_lines(df["volume_usd"].reset_index(drop=True),
                                         df["date"].reset_index(drop=True))

                mc_or_supply = latest.get("market_cap_usd")
                lines += [
                    f"## {name} ({'Stablecoin' if group == 'stablecoins' else 'L1/Wrapped token on Solana'})",
                    f"  Latest price      : {_fmt(latest['price_usd'], '$')}",
                    f"  Latest 24h vol    : {_fmt(latest['volume_usd'])}",
                    f"  7-day avg vol     : {_fmt(avg_vol_7d)}",
                    f"  30-day avg vol    : {_fmt(avg_vol_30d)}",
                    *(
                        [f"  Mkt cap / supply  : {_fmt(mc_or_supply)}"]
                        if pd.notna(mc_or_supply) else []
                    ),
                    wow, mom,
                    *ath_lines,
                    "",
                ]
            except Exception:
                pass

        # ── Multi-token group puller (tokenized stocks) ───────────────────────
        elif group == "tokenized_stocks":
            try:
                df["date"] = pd.to_datetime(df["date"])
                df = df.sort_values("date")
                vol_cols = [
                    c for c in df.columns
                    if c.startswith("vol_") and c.endswith("_usd")
                ]
                if not vol_cols:
                    continue

                df["total_vol"] = df[vol_cols].sum(axis=1)
                latest = df.iloc[-1]
                last7  = df.tail(7)
                last30 = df.tail(30)

                avg7  = last7["total_vol"].mean()
                avg30 = last30["total_vol"].mean()
                wow, mom  = _wow_mom(df["total_vol"])
                ath_lines = _ath_lines(df["total_vol"].reset_index(drop=True),
                                       df["date"].reset_index(drop=True))

                token_vols = {c: latest[c] for c in vol_cols if latest[c] > 0}
                top5 = sorted(token_vols.items(), key=lambda x: x[1], reverse=True)[:5]
                top5_str = ", ".join(
                    f"{c.replace('vol_','').replace('_usd','').upper()} {_fmt(v)}"
                    for c, v in top5
                )
                lines += [
                    f"## {name} (Tokenized stocks — {len(vol_cols)} tokens)",
                    f"  Latest-day total vol : {_fmt(latest['total_vol'])}",
                    f"  7-day avg total vol  : {_fmt(avg7)}",
                    f"  30-day avg total vol : {_fmt(avg30)}",
                    wow, mom,
                    *ath_lines,
                    *(
                        [f"  Top tokens (latest day): {top5_str}"]
                        if top5_str else []
                    ),
                    "",
                ]
            except Exception:
                pass

        # ── USDC / multi-chain puller ─────────────────────────────────────────
        else:
            try:
                df["date"] = pd.to_datetime(df["date"])
                df = df.sort_values("date")
                chain_cols = [c for c in df.columns if c != "date"]
                if not chain_cols:
                    continue

                df["total"] = df[chain_cols].sum(axis=1)
                latest = df.iloc[-1]
                last7  = df.tail(7)

                wow, mom  = _wow_mom(df["total"])
                ath_lines = _ath_lines(df["total"].reset_index(drop=True),
                                       df["date"].reset_index(drop=True))

                chain_latest = {
                    c: latest[c]
                    for c in chain_cols
                    if c != "total" and pd.notna(latest[c]) and latest[c] > 0
                }
                chain_str = ", ".join(
                    f"{c}: {_fmt(v)}"
                    for c, v in sorted(
                        chain_latest.items(), key=lambda x: x[1], reverse=True
                    )
                )
                lines += [
                    f"## {getattr(p, 'name', 'USDC').upper()} (Multi-chain stablecoin volume)",
                    f"  Latest-day total vol : {_fmt(latest['total'])}",
                    f"  7-day avg total vol  : {_fmt(last7['total'].mean())}",
                    wow, mom,
                    *ath_lines,
                    f"  Chain breakdown (latest): {chain_str}",
                    "",
                ]
            except Exception:
                pass

    return "\n".join(lines)


def init_pullers(settings: Settings, db: CacheDB) -> List[DataPuller]:
    """
    Instantiate and return every active DataPuller.

    To add a new Solana token, append a row to _SOLANA_TOKENS above.
    To add a new token group (tokenized stocks), append a row to _TOKENIZED_STOCK_GROUPS.
    To add a completely new source, subclass DataPuller and append it below.
    """
    # Stocks pullers: enable Birdeye Token Overview MC. For tokenized
    # stocks (Backed.fi xStocks, PreStocks, Ondo tokenized equities)
    # Birdeye's `marketCap` field = price × on-chain `totalSupply` =
    # the correct **tokenized** MC (not the underlying company's market
    # cap — that was a stale assumption from earlier when the field
    # was sometimes computed off company shares-outstanding). Verified
    # against AAPLx Solana: \$48.29M reported = \$313.21 × 154,175.
    # Result: mc_<symbol>_<chain>_usd populates per chain on each pull,
    # and the per-token MC chart sums across chains on the All-chain
    # view (was empty before because no MC source was configured).
    # Solana-native foreign-L1 tokens — revived from the original
    # _SOLANA_TOKENS registry. Each gets its own SolanaTokenMetricsPuller
    # subclass via the factory (one puller per token, GROUP='solana_tokens').
    # Used by solana_dashboard.py's 'Foreign L1 tokens' vertical; no UI
    # in the main stocks_dashboard since this side has no foreign-L1 tab.
    solana_pullers = [
        _make_solana_puller(name, addr, start)(settings, db)
        for name, addr, start in _SOLANA_TOKENS
    ]
    stock_pullers = [
        _make_stock_group_puller(pname, label, tokens,
                                 market_cap_source="birdeye_overview")(settings, db)
        for pname, label, tokens in _TOKENIZED_STOCK_GROUPS
    ]
    commodity_pullers = [
        _make_stock_group_puller(pname, label, tokens,
                                 group="tokenized_commodities",
                                 market_cap_source="birdeye_overview",
                                 defillama_tokens=_COMMODITY_DEFILLAMA)(settings, db)
        for pname, label, tokens in _TOKENIZED_COMMODITY_GROUPS
    ]
    stablecoin_pullers = [
        _make_stock_group_puller(
            pname, label, tokens,
            group="stablecoins",
            market_cap_source="birdeye_overview",
            defillama_tokens=_STABLECOIN_DEFILLAMA,
            # _HIDDEN_STABLECOINS is empty by default now — add symbols to
            # the per-chain dict here if a specific chain's data goes bad.
            hidden_tokens_by_chain={"solana": _HIDDEN_STABLECOINS},
        )(settings, db)
        for pname, label, tokens in _STABLECOIN_GROUPS
    ]
    treasury_pullers = [
        _make_stock_group_puller(pname, label, tokens,
                                 group="treasuries",
                                 defillama_tokens=_TREASURY_DEFILLAMA,
                                 skip_volume=True)(settings, db)
        for pname, label, tokens in _TREASURY_GROUPS
    ]
    return [*solana_pullers, *stock_pullers, *commodity_pullers,
            *stablecoin_pullers, *treasury_pullers]


# ── Tokenized commodity group registry — gold-backed tokens (OHLCV V3 volume) ──
_TOKENIZED_COMMODITY_GROUPS: list[tuple[str, str, list]] = [
    (
        "commodities_group",
        "Tokenized Commodities",
        [
            # ── Solana-native (Birdeye OHLCV volume + Birdeye overview MC) ────
            ("XAUM",  "5aLhp9VnUEKcsdtkfsf2DUgpJfomx7GmYVny24dHUZoB", "Solana"),
            ("GOLD",  "GoLDppdjB1vDTPSGxyMJFqdnj134yH6Prg9eqsGDiw6A", "Solana"),
            ("VNXAU", "9TPL8droGJ7jThsq4momaoz6uhTcvX2SeMqipoPmNa8R", "Solana"),
            # PAXG on Solana is the Wormhole-bridged Paxos Gold (~\$70K MC).
            # DefiLlama doesn't currently include Solana in the paxos-gold
            # protocol's chain breakdown, so this entry's data comes from
            # Birdeye only — MC will accrue from each pull's snapshot.
            ("PAXG",  "C6oFsE8nXRDThzrMEQ5SxaNFGKoyyfWDDVPw37JKvPTe",  "Solana"),
            # XAUt0 on Solana is the LayerZero OFT mirror of Tether Gold
            # (the `0` suffix is LayerZero's OFT naming convention). MC
            # ~\$13.9M, ~5.6K holders, ~\$680K/day vol. Tracked separately
            # under its own symbol so it doesn't conflict with the Ethereum
            # XAUT entry; DefiLlama's tether-gold slug groups Sol into the
            # protocol-level chainTvls, so mc_xaut0_solana_usd is Birdeye-
            # snapshot only (accrues from each pull).
            ("XAUt0", "AymATz4TCL9sWNEEV9Kvyz45CHVhDZ6kUgjTJPzLpU9P",  "Solana"),
            # ── Ethereum-native gold tokens (DefiLlama-only; Birdeye will
            #    skip them since x-chain=solana). Addresses kept for
            #    reference / future per-chain Birdeye calls. ─────────────────
            ("PAXG", "0x45804880De22913dAFE09f4980848ECE6EcbAf78", "Ethereum"),
            ("XAUT", "0x68749665FF8D2d112Fa859AA293F07A622782F38", "Ethereum"),
            # DGLD: Gold Token SA (~\$7M MC, ~3K holders). Birdeye-verified
            # on Ethereum mainnet.
            ("DGLD", "0xa9299c296d7830a99414d1e5546f5171fa01e9c8", "Ethereum"),
            # TXAU: tGOLD (~\$1.47M MC). Birdeye-verified. Note: priced
            # around \$34 not \$4,400 — appears to be a fractional/100-th-
            # oz wrapper rather than per-oz like XAUM/PAXG.
            ("TXAU", "0xe4a6f23fb9e00fca037aa0ea0a6954de0a6c53bf", "Ethereum"),
            # ── Arbitrum-native gold ──────────────────────────────────────
            # PGOLD: Pleasing Gold (~\$87M MC, ~343 holders). Birdeye works
            # on Arbitrum via x-chain=arbitrum; the puller infers chain
            # from this 3-tuple so no additional config needed. Will surface
            # on the All-chain view (chain-specific tabs only render the
            # 4 chains in our sidebar — Sol/Eth/BSC/Base — so PGOLD is
            # silently visible there until/unless Arbitrum gets a tab).
            ("PGOLD", "0x3e76bb02286bfeaa89dd35f11253f2cbce634f91", "Arbitrum"),
            # ── XDC-native gold (DefiLlama-only — Birdeye doesn't support XDC) ─
            # CGO: ComTech Gold (~\$5.6M MC on XDC). Lives in our cache via
            # the additive _COMMODITY_DEFILLAMA path (comtech-gold slug);
            # Birdeye is skipped because no x-chain=xdc exists in their API.
            ("CGO",  "0x8f9920283470f52128bf11b0c14e798be704fd15", "XDC"),
        ],
    ),
]

# Historical market cap source: Birdeye has none, so use CoinGecko coin ids.
_COMMODITY_COINGECKO_IDS: dict[str, str] = {
    "XAUM":  "matrixdock-gold",
    "GOLD":  "gold-11",
    "VNXAU": "vnx-gold",
}

# ── Stablecoin group registry — Solana stablecoins (OHLCV V3 volume + MC) ──────
_STABLECOIN_GROUPS: list[tuple[str, str, list]] = [
    (
        "stablecoins_group",
        "Stablecoins",
        [
            # ── Solana-native ───────────────────────────────────────────────
            ("USDC",   "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v", "Solana"),
            ("USDT",   "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB", "Solana"),
            ("CASH",   "CASHx9KJUStyftLFWGvEVf59SGeG9sh5FfcnZMVPCASH", "Solana"),
            ("USDG",   "2u1tszSeqZ3qBWF3uNGPFc8TzMk2tdiwknnRMWGWjGWH", "Solana"),
            ("USD1",   "USD1ttGY1N17NEEHLmELoaybftRBUSErhqYiQzvEmuB",  "Solana"),
            ("PYUSD",  "2b1kV6DkPAnxd5ixfnxCpjxmKwqjjaYmCZfHsFu24GXo", "Solana"),
            ("USDe",   "DEkqHyPN7GMRJ5cArtQFAWefqbZb33Hyf6s5iCwjEonT", "Solana"),
            ("JupUSD", "JuprjznTrTSp2UFa3ZBUFgwdAmtZCq4MQCwysN55USD",  "Solana"),
            ("USDS",   "USDSwr9ApdHk5bvJKMjzff41FfuX8bSxdKcR81vTwcA",  "Solana"),
            # ── Ethereum mirrors (USDC/USDT/USDe/USD1/USDG/PYUSD/USDS only —
            #    CASH and JupUSD are Solana-native and have no Ethereum
            #    deployment). Birdeye chain inferred from address; DefiLlama
            #    also provides historical MC via _STABLECOIN_DEFILLAMA. ────
            ("USDC",  "0xA0b86991c6218b36c1d19d4a2e9eb0ce3606eb48", "Ethereum"),
            ("USDT",  "0xdAC17F958D2ee523a2206206994597C13D831ec7", "Ethereum"),
            ("USDe",  "0x4c9EDD5852cd905f086C759E8383e09bff1E68B3", "Ethereum"),
            ("USD1",  "0x8d0D000Ee44948FC98c9B98A4FA4921476f08B0d", "Ethereum"),
            ("USDG",  "0xe343167631d89B6Ffc58B88d6b7fb0228795491D", "Ethereum"),
            ("PYUSD", "0x6c3ea9036406852006290770BEdFcAbA0e23A0e8", "Ethereum"),
            ("USDS",  "0xdC035D45d973E3EC169d2276DDab16f1e407384F", "Ethereum"),
            # ── Base — only Sky USDS exists there for now; other stables
            #    will be added as they get tracked. ─────────────────────────
            ("USDS",  "0x820c137fa70c8691f0e44dc420a5e53c168921dc", "Base"),
            # ── BNB Chain — Sky USDS has no BSC deployment (Sky bridged to
            #    Ethereum/Base/Arbitrum/OP/Solana/Avalanche/Unichain only).
            #    USDT/USDC etc. on BSC come from the existing DefiLlama
            #    multi-chain coverage on the Ethereum mirror entries. ───
        ],
    ),
]

# Tokens hidden from the stablecoins chart on specific chains. Add a symbol
# here to suppress its display while still keeping the data flowing. Was
# originally populated to mask a sawtooth pattern caused by mixing live cache
# data with seed JSON; the fresh Solscan seeds and the seed-application fix
# (which now writes to the per-chain Solana col + uses `setdefault` so live
# data takes precedence) resolved that, so the set is back to empty —
# everything renders on every chain by default.
_HIDDEN_STABLECOINS: set[str] = set()

# DefiLlama IDs for stablecoins. These supply per-chain MC history for every chain
# DefiLlama tracks (Ethereum, Binance, Base, Tron, Arbitrum, …). Solana keeps its
# Birdeye + seed-file lineage; the DefiLlama series is additive, stored in
# mc_<symbol>_<chain>_usd columns.
_STABLECOIN_DEFILLAMA: dict = {
    "USDT":  {"type": "stablecoin", "id": 1},
    "USDC":  {"type": "stablecoin", "id": 2},
    "PYUSD": {"type": "stablecoin", "id": 120},
    "USDe":  {"type": "stablecoin", "id": 146},
    "USD1":  {"type": "stablecoin", "id": 262},
    "USDG":  {"type": "stablecoin", "id": 286},
    "USDS":  {"type": "stablecoin", "id": 209},   # Sky Dollar (Maker rebrand)
    # CASH / JupUSD: no DefiLlama coverage at the moment — Solana-only via Birdeye.
}

# DefiLlama protocol slugs for tokenized commodities. Additive per-chain MC on
# top of Birdeye/Solana cache.
_COMMODITY_DEFILLAMA: dict = {
    "XAUM": {"type": "protocol", "slug": "matrixdock-xaum"},
    # PAXG: Ethereum-only (~$2.07B). XAUT: Ethereum dominant (~$3.15B) +
    # smaller balances on Arbitrum / Avalanche / Celo / Monad / Plasma /
    # Polygon / Ink. Both come in as separate mc_<token>_<chain>_usd cols
    # via the additive DefiLlama path.
    "PAXG": {"type": "protocol", "slug": "paxos-gold"},
    "XAUT": {"type": "protocol", "slug": "tether-gold"},
    # CGO: ComTech Gold on XDC Network (~$5.6M). XDC isn't supported by
    # Birdeye, so DefiLlama is the only available MC source for this token.
    "CGO":  {"type": "protocol", "slug": "comtech-gold"},
    # GOLD / VNXAU / DGLD / TXAU / PGOLD: no clean DefiLlama protocol
    # mapping. DGLD/TXAU/PGOLD covered by Birdeye snapshots; GOLD/VNXAU
    # backfilled from the Solscan-derived mc_seed_*.json files.
}


# ── Tokenized treasuries & MMFs registry (market cap only, no volume) ─────────
_TREASURY_GROUPS: list[tuple[str, str, list]] = [
    (
        "treasuries_group",
        "Treasuries & MMFs",
        [
            # ── Solana-native treasury / MMF tokens ─────────────────────────
            ("BUIDL", "GyWgeqpy5GueU2YbkE8xqUeVEokCMMCEeUrfbtMw6phr", "Solana"),
            ("ULTRA", "9DRPPWYud8i6CaSsDsFESs1xyVr8dBCMtjPZji2xiZEa", "Solana"),
            ("VBILL", "34mJztT9am2jybSukvjNqRjgJBZqHJsHnivArx1P4xy1", "Solana"),
            ("USYC", "7LWanZteUKtvFjv4MHYgKXXdAuCQYFPJysL9pxxdRQGn", "Solana"),
            ("USTB", "CCz3SGVziFeLYk2xfEstkiqJfYkjaSWb2GCABYsVcjo2", "Solana"),
            ("CASHx", "5d3zUSzje2saHwgzwJwFE8SDR8S5sGpE9wHhXdsCfu7j", "Solana"),
            ("TBILL", "4MmJVdwYN8LwvbGeCowYjSx7KoEi6BJWg8XXnW4fDDp6", "Solana"),
            ("BENJI", "5Tu84fKBpe9vfXeotjvfvWdWbAjy3hqsExvuHgFqFxA1", "Solana"),
            ("nTBILL", "2sA2jW9e8EYJkLFpq9hkhxfVUQBwVGJwq6iP4TmTKrL4", "Solana"),
            ("CMBMINT", "4uuqdpVPE9JdPyTRkAppQLB3x4QNmTjCZqdhAkwPmoMY", "Solana"),
            ("deJTRSY", "DeJXZwShCZYJnRX2ruVASfhUhsC44qPW1pacbxRFuGLR", "Solana"),
            ("WTGXX", "Em46fxxwgY2RRoUbBMSbEjJwY62x3ESMNdhnsGpEKewm", "Solana"),
            ("FLTTX", "5Qjgvd1mKaishqbrnz2tPsZFnMWpjpLZdqdPoVdTY4Vi", "Solana"),
            ("TIPSX", "B3Lc8KhBHVK3fKzh92xvsqvzJPr3wc5rMENmexAcsiDf", "Solana"),
            ("WTLGX", "51fSuDgEYgGiRBfTykMudLBQeJcwR3hqncyPFzXQ85R1", "Solana"),
            ("WTSTX", "A46zj57APuTZyBkNh2jhNs2GAzz5LcybA97zAwpP7Uck", "Solana"),
            ("WTTSX", "DpkuH46BBV4KhFvsBk8dLXuMbwrhijpBtyx3DqMmJCY3", "Solana"),
            ("WTSYX", "7aXJS2mgKzj2fCqZGx2TbXD3nxVXexxuK3BTyCq6BN4H", "Solana"),
            ("OUSG", "i7u4r16TcsJTgq1kAG8opmVZyVnAKBwLKu6ZPMwzxNc", "Solana"),
            ("USDY", "A1KLoBrKBde8Ty9qtNQUtq3C2ortoC3u7twggz7sEto6", "Solana"),
            ("USDM1", "BNgsQdjfWmjoy3cw8T3VXWswHfgCzEMyQzUno8gmzmRC", "Solana"),
            ("USTRY", "USTRYnGgcHAhdWsanv8BG6vHGd4p7UGgoB9NRd8ei7j", "Solana"),
            # ── Ethereum-native treasury / MMF tokens ───────────────────────
            # Duplicated symbol names (BUIDL/USDY/USYC/etc) are deduplicated
            # by name at render time so the chart legend stays clean. Each
            # entry still triggers a Birdeye Ethereum fetch (x-chain inferred
            # from 0x prefix) so we get per-token snapshots + volume.
            ("USYC", "0x136471a34f6ef19fe571effc1ca711fdb8e49f2b", "Ethereum"),
            ("BUIDL", "0x6a9da2d710bb9b700acde7cb81f10f1ff8c89041", "Ethereum"),
            ("USDY", "0x96f6ef951840721adbf46ac996b59e0235cb985c", "Ethereum"),
            ("iBENJI", "0x90276e9d4a023b5229e0c2e9d4b2a83fe3a2b48c", "Ethereum"),
            ("WTGXX", "0x1fecf3d9d4fee7f2c02917a66028a48c6706c179", "Ethereum"),
            ("JTRSY", "0x8c213ee79581ff4984583c6a801e5263418c4b86", "Ethereum"),
            ("BENJI", "0x3ddc84940ab509c11b20b76b466933f40b750dc9", "Ethereum"),
            ("USTB", "0x43415eb6ff9db7e26a15b704e7a3edce97d31c4e", "Ethereum"),
            ("OUSG", "0x1b19c19393e2d034d8ff31ff34c81252fcbbee92", "Ethereum"),
            ("CUMIU", "0x85d38585c3ac08268f598282a84b7c0ddfc0d04f", "Ethereum"),
            ("USTBL", "0xe4880249745eac5f1ed9d8f7df844792d560e750", "Ethereum"),
            ("FDIT", "0x48ab4e39ac59f4e88974804b04a991b3a402717f", "Ethereum"),
            ("ULTRA", "0x50293dd8889b931eb3441d2664dce8396640b419", "Ethereum"),
            ("THBILL", "0x5fa487bca6158c64046b2813623e20755091da0b", "Ethereum"),
            ("BELIF", "0x237c717df1b60501f8d029d3fe7385fd090df180", "Ethereum"),
            ("MONY", "0x6a7c6aa2b8b8a6a891de552bdeffa87c3f53bd46", "Ethereum"),
            ("TBILL", "0xdd50c053c096cb04a3e3362e2b622529ec5f2e8a", "Ethereum"),
            ("VBILL", "0x2255718832bc9fd3be1caf75084f4803da14ff01", "Ethereum"),
            ("MTBILL", "0xdd629e5241cbc5919847783e6c96b2de4754e438", "Ethereum"),
            ("CASHx", "0x42975aae7a124257e7fda7f5e8382f51449b784a", "Ethereum"),
            ("DCP", "0xb5710a6fede27d1048c75b157bd3403ba08cdbe0", "Ethereum"),
            ("FILQ", "0x54a4fc78431f9201824643e99bec891bb7462a1d", "Ethereum"),
            ("CUMBU", "0x1aaa3339572cf88dc487dbeef263f5aabc5f3bbf", "Ethereum"),
            ("UMINT", "0xc06036793272219179f846ef6bfc3b16e820df0b", "Ethereum"),
            ("CUMFU", "0xdbf879f356c6b8c5f1edfdcb2950eda8b3ad25d9", "Ethereum"),
            ("usfr.d", "0xaEB0A5d56de94479cdA178977570FD9079500527", "Ethereum"),
            ("deJTRSY", "0xa6233014b9b7aaa74f38fa1977ffc7a89642dc72", "Ethereum"),
            ("CMBMINT", "0xc9a71c8fa0f505e690cbab1012d4a4a518e03231", "Ethereum"),
            ("USDM1", "0x90a1717e0dabe37693f79afe43ae236dc3b65957", "Ethereum"),
        ],
    ),
]

# DefiLlama lookup for treasury tokens covered by the free API. Each pull fetches
# the full per-chain historical MC series. Tokens missing from this map will
# simply have empty MC series until you drop an mc_seed_<symbol>.json file in.
_TREASURY_DEFILLAMA: dict = {
    # ── Single-fund slugs (precise) ─────────────────────────────────────────
    "BUIDL":  {"type": "protocol",   "slug": "blackrock-buidl"},
    "OUSG":   {"type": "protocol",   "slug": "ondo-yield-assets"},
    "USDY":   {"type": "stablecoin", "id":   129},
    "VBILL":  {"type": "protocol",   "slug": "vaneck-treasury-fund"},
    "ULTRA":  {"type": "protocol",   "slug": "ondo-global-markets"},
    "USTB":   {"type": "protocol",   "slug": "superstate-ustb"},
    "TBILL":  {"type": "protocol",   "slug": "openeden-tbill"},
    "USDM1":  {"type": "stablecoin", "id":   342},
    "USYC":   {"type": "protocol",   "slug": "circle-usyc"},
    "FDIT":   {"type": "protocol",   "slug": "fidelity-digital-interest-token"},
    "THBILL": {"type": "protocol",   "slug": "theo-network-thbill"},
    "DCP":    {"type": "protocol",   "slug": "apollo-diversified-credit-securitize-fund"},
    # ── Multi-fund protocol slugs (aggregate at protocol level — better
    #    than nothing while waiting for DefiLlama to split out per token) ──
    "WTGXX":  {"type": "protocol",   "slug": "wisdomtree"},
    "MTBILL": {"type": "protocol",   "slug": "midas-rwa"},
    "USTBL":  {"type": "protocol",   "slug": "spiko"},
    "CASHx":  {"type": "protocol",   "slug": "asseto-cash+"},
    # Not yet mapped (no clean DefiLlama equivalent found):
    #   iBENJI, BENJI, JTRSY, CUMIU, BELIF, MONY, FILQ, CUMBU, UMINT,
    #   CUMFU, usfr.d, deJTRSY, CMBMINT, nTBILL, FLTTX, TIPSX, WTLGX,
    #   WTSTX, WTTSX, WTSYX, USTRY
}


# ══════════════════════════════════════════════════════════════════════════════
# 6b. HEADLESS PULL MODE (for the scheduled cron job)
#     Run:  PULL_ONLY=1 python3 stocks_dashboard.py
#     Pulls every source once into the SQLite cache and exits — no Streamlit UI.
# ══════════════════════════════════════════════════════════════════════════════
import os as _os

if _os.getenv("PULL_ONLY") == "1":
    # Optional: limit to GROUP(s), comma-separated
    # (e.g. PULL_GROUP=tokenized_commodities,stablecoins). Empty = all.
    _groups = [g.strip() for g in _os.getenv("PULL_GROUP", "").split(",") if g.strip()]
    log.info("PULL_ONLY mode (groups=%s) — pulling into %s",
             ",".join(_groups) or "all", settings.db_path)
    for _p in init_pullers(settings, cache_db):
        if _groups and getattr(_p, "GROUP", "") not in _groups:
            continue
        try:
            _p.pull()
            log.info("pulled %s", _p.name)
        except Exception as _exc:
            log.error("pull failed for %s: %s", _p.name, _exc)
    log.info("PULL_ONLY done")
    raise SystemExit(0)


# ══════════════════════════════════════════════════════════════════════════════
# 7. STREAMLIT ENTRY POINT
#    streamlit run stocks_dashboard.py
# ══════════════════════════════════════════════════════════════════════════════

def _combined_stocks_df(pullers: list) -> pd.DataFrame | None:
    """Merge per-group daily DataFrames into one wide table.

    Result columns: date | <GROUP_LABEL> …
    Each project column = sum of all its per-token vol_*_usd columns for that day.
    """
    frames: list[pd.DataFrame] = []
    for p in pullers:
        raw = p.get_latest()
        if raw is None or raw.empty:
            continue
        raw = raw.copy()
        raw["date"] = pd.to_datetime(raw["date"])
        vol_cols = [c for c in raw.columns
                    if c.startswith("vol_") and c.endswith("_usd")]
        proj = raw[["date"]].copy()
        proj[p.GROUP_LABEL] = raw[vol_cols].sum(axis=1)
        frames.append(proj)

    if not frames:
        return None

    result = frames[0]
    for f in frames[1:]:
        result = result.merge(f, on="date", how="outer")
    return result.sort_values("date").reset_index(drop=True)


def _build_combined_stocks_fig(df: pd.DataFrame, labels: list[str],
                                period: str, height: int) -> go.Figure:
    """Stacked bar figure of tokenized-stock volume by project.

    period: 'D' daily, 'W' weekly, 'M' monthly.
    """
    def _aligned_ticks(vmax: float, n: int = 6) -> list[float]:
        if vmax <= 0 or not pd.notna(vmax):
            return [0.0] * n
        raw  = vmax / (n - 1)
        mag  = 10 ** math.floor(math.log10(raw))
        step = math.ceil(raw / mag) * mag
        return [i * step for i in range(n)]

    plot_df = df.copy()

    if period != "D":
        col = "week" if period == "W" else "month"
        present = [lbl for lbl in labels if lbl in plot_df.columns]
        plot_df = (
            plot_df.assign(**{col: plot_df["date"].dt.to_period(period).dt.start_time})
            .groupby(col, as_index=False)
            .agg({lbl: "sum" for lbl in present})
            .rename(columns={col: "date"})
        )

    present = [lbl for lbl in labels if lbl in plot_df.columns]
    vol_max = plot_df[present].sum(axis=1).max() if present else 1.0
    ticks   = _aligned_ticks(vol_max)

    fig = go.Figure()
    for label in present:
        y     = plot_df[label].fillna(0).replace(0, float("nan"))
        color = _STOCKS_PROJECT_COLORS.get(label, "#888888")
        fig.add_trace(go.Bar(x=plot_df["date"], y=y, name=label,
                             marker_color=color, opacity=0.85))

    # Invisible total trace — shows a summary row at the bottom of the hover tooltip.
    total = plot_df[present].fillna(0).sum(axis=1).replace(0, float("nan"))
    fig.add_trace(go.Scatter(
        x=plot_df["date"], y=total,
        name="Total",
        mode="lines",
        line=dict(width=0),
        showlegend=False,
        hovertemplate="<b>Total: $%{y:~s}</b><extra></extra>",
    ))

    fig.update_layout(
        barmode="stack",
        hovermode="x unified",
        height=height,
        margin=dict(t=10, b=10, l=10, r=10),
        legend=dict(orientation="h", yanchor="bottom", y=1.02,
                    xanchor="right", x=1),
        yaxis=dict(
            tickprefix="$", tickformat="~s",
            tickmode="array", tickvals=ticks, range=[0, ticks[-1]],
            showgrid=True,
        ),
    )
    return fig


# Per-project line color for the combined stocks chart (module-level so
# _build_combined_stocks_fig can reach it from anywhere).
_STOCKS_PROJECT_COLORS: dict[str, str] = {
    "PreStocks": "#d2b58f",  # tan/7
    "xStocks":   "#6F97D5",  # navy/6
    "Ondo":      "#6FD58F",  # green/6
}


# ── Raw-data modal (module-level so solana_dashboard.py can import it) ───────
@st.dialog("📋 Raw Data", width="large")
def _raw_data_modal(df: pd.DataFrame, fmt: dict | None = None,
                    filename: str = "data") -> None:
    """Pop-open dialog showing a chart's underlying data with a Download
    CSV button. `fmt` is a Pandas Styler format dict ({col: '${:,.0f}'});
    when None, auto-formats any non-date column as USD with thousands
    separators (sensible default for the finance-oriented charts here).
    """
    if fmt is None:
        skip = {"date", "month", "week"}
        fmt = {c: "${:,.0f}" for c in df.columns if c not in skip}
    st.download_button(
        "⬇️ Download CSV",
        df.to_csv(index=False).encode("utf-8"),
        file_name=f"{filename}.csv", mime="text/csv",
        key=f"dl_{filename}",
    )
    st.dataframe(df.style.format(fmt, na_rep="—"),
                 use_container_width=True, height=520)


# Version key: bump whenever the puller list / class hierarchy changes so that
# stale session-state instances (from before a code reload) are discarded.
# Exposed at module level so solana_dashboard.py can use it for its own
# session-state version-gating without re-defining a parallel constant.
_PULLERS_VERSION = "stocks-commodities-stables-treasuries-multichain-v52-mc-snapshot"


# ── Module guard ────────────────────────────────────────────────────────────
# When imported as a library (e.g. by solana_dashboard.py) the UI rendering
# block below is skipped — only the helpers, classes, Settings, CacheDB,
# and init_pullers() are exposed. When run directly via `streamlit run
# stocks_dashboard.py` (__name__ == '__main__'), the original RWA dashboard
# renders as before. The PULL_ONLY block above this guard runs regardless,
# so `python scripts/run_pull.py` still pulls without depending on UI state.
if __name__ == "__main__":
    st.set_page_config(
        page_title="Solana Tokenized Stocks · Birdeye Peak",
        page_icon="assets/logos/Birdeye_Peak_Logomark_White.svg",
        layout="wide",
    )

    # ── Birdeye Peak branding (theme, fonts, logo) ────────────────────────────────
    import base64 as _b64
    from pathlib import Path as _Path

    _ASSET_DIR = _Path(__file__).parent / "assets"


    def _asset_b64(rel: str) -> str:
        try:
            return _b64.b64encode((_ASSET_DIR / rel).read_bytes()).decode()
        except Exception:
            return ""


    def _asset_text(rel: str) -> str:
        try:
            return (_ASSET_DIR / rel).read_text()
        except Exception:
            return ""


    _PEAK_FONT_B64 = _asset_b64("fonts/Indivisible_SemiBold.otf")
    _PEAK_LOGO_SVG = _asset_text("logos/Birdeye_Peak_Horizontal_White.svg")

    # Streamlit Cloud injects extra header buttons (Share, pencil-edit, GitHub icon)
    # that local runs don't have. Detect via the Cloud-only mount path so we can
    # push our floated header controls further left and avoid overlapping them.
    _IS_CLOUD = os.path.exists("/mount/src")
    _TOPBAR_FORCE_PULL_RIGHT = "13rem" if _IS_CLOUD else "8rem"
    _TOPBAR_CAPTION_RIGHT    = "20rem" if _IS_CLOUD else "15rem"

    st.markdown(
        f"""
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600&display=swap');
        @font-face {{
            font-family: 'Indivisible';
            src: url(data:font/otf;base64,{_PEAK_FONT_B64}) format('opentype');
            font-weight: 600; font-style: normal; font-display: swap;
        }}
        :root {{
            --peak-bg: #141414; --peak-container: #1d1b19; --peak-elevated: #262320;
            --peak-primary: #d2b58f; --peak-primary-hover: #c59c72; --peak-primary-active: #cc8943;
            --peak-text: #ffffff; --peak-text-secondary: #BBA68F;
            --peak-divider: rgba(255,255,255,0.10);
        }}
        html, body, .stApp, [data-testid="stAppViewContainer"] {{
            background-color: var(--peak-bg) !important;
            color: var(--peak-text);
            font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
        }}
        [data-testid="stHeader"] {{ background: transparent !important; }}
        /* Hide the native running / Stop status widget (we show our own spinner) */
        [data-testid="stStatusWidget"] {{ display: none !important; }}
        /* Tighten top padding so the page title lines up with the sidebar CHAINS label */
        [data-testid="stMainBlockContainer"], .block-container {{ padding-top: 18px !important; }}
        [data-testid="stSidebar"] {{
            background-color: var(--peak-container) !important;
            border-right: 1px solid var(--peak-divider);
        }}
        h1, h2, h3, h4, h5, h6 {{
            font-family: 'Inter', sans-serif !important;
            color: var(--peak-text); font-weight: 600;
        }}
        /* Chart section titles (st.subheader → h3) — smaller than the page title */
        [data-testid="stHeading"] h3, h3 {{
            font-size: 20px !important; line-height: 1.35 !important;
        }}
        /* Hero header */
        .peak-header {{ display: flex; align-items: center; gap: 16px; }}
        .peak-logo svg {{ height: 30px; width: auto; display: block; }}
        .peak-title {{
            font-family: 'Indivisible', 'Inter', sans-serif !important;
            font-weight: 600 !important; font-size: 50px !important; line-height: 56px !important;
            letter-spacing: 0.02em; text-transform: uppercase;
            color: var(--peak-text-secondary) !important; margin: 0 !important;
        }}
        .peak-subtitle {{
            font-family: 'Inter', sans-serif; font-size: 16px !important; font-weight: 500;
            color: rgba(255, 255, 255, 0.6) !important; letter-spacing: 0.02em; margin: 2px 0 0 !important;
        }}
        .peak-sub {{ color: var(--peak-text-secondary); font-size: 14px; margin: 0; }}
        .peak-sub b {{ color: var(--peak-text); font-weight: 600; }}
        .peak-sub code {{
            background: var(--peak-elevated); color: var(--peak-primary);
            padding: 2px 6px; border-radius: 4px; font-size: 12px;
        }}
        /* Tabs — tan accent instead of default red */
        .stTabs [data-baseweb="tab-list"] {{ border-bottom: 1px solid var(--peak-divider); }}
        .stTabs [data-baseweb="tab"] {{ color: var(--peak-text-secondary); }}
        .stTabs [aria-selected="true"] {{ color: var(--peak-primary) !important; }}
        .stTabs [data-baseweb="tab-highlight"],
        .stTabs [data-baseweb="tab-border"] {{ background-color: var(--peak-primary) !important; }}
        /* Buttons */
        .stButton > button {{
            background-color: var(--peak-primary); color: #1d1b19;
            border: 1px solid var(--peak-primary); border-radius: 4px; font-weight: 600;
        }}
        .stButton > button:hover {{
            background-color: var(--peak-primary-hover);
            border-color: var(--peak-primary-hover); color: #141414;
        }}
        .stButton > button:active {{ background-color: var(--peak-primary-active) !important; }}
        /* Misc */
        hr {{ border-color: var(--peak-divider) !important; }}
        a, a:visited {{ color: var(--peak-primary) !important; }}
        [data-testid="stMetricValue"] {{ color: var(--peak-text); }}
        /* Chain navigation (sidebar) */
        .peak-nav-title {{
            font-family: 'Inter', sans-serif; font-size: 12px; font-weight: 600;
            letter-spacing: 0.08em; color: var(--peak-text-secondary);
            margin: 4px 0 10px; text-transform: uppercase;
        }}
        [data-testid="stSidebar"] [role="radiogroup"] label {{ padding: 6px 4px; }}
        /* Empty-chain placeholder */
        .peak-empty {{
            margin-top: 1.5rem; padding: 4rem 1rem; text-align: center;
            color: var(--peak-text-secondary); font-size: 18px;
            border: 1px dashed var(--peak-divider); border-radius: 8px;
            background: var(--peak-container);
        }}
        /* Caption (refresh / pull cadence / timestamp) floated into the top bar */
        .peak-sub-anchor {{ display: none; }}
        [data-testid="stElementContainer"]:has(.peak-sub-anchor),
        .element-container:has(.peak-sub-anchor) {{
            position: fixed; top: 0; right: {_TOPBAR_CAPTION_RIGHT}; height: 3.75rem;
            display: flex; align-items: center; justify-content: flex-end;
            width: auto !important; margin: 0 !important; padding: 0 !important;
            z-index: 999991;
        }}
        .peak-sub-topbar {{
            margin: 0 !important; font-size: 13px;
            white-space: nowrap; text-align: right;
            position: relative; top: -8px;
        }}
        /* Force-pull button floated into the top bar, styled like the Deploy button */
        .st-key-force_pull_header {{
            position: fixed; top: 0; right: {_TOPBAR_FORCE_PULL_RIGHT}; height: 3.75rem;
            display: flex; align-items: center;
            width: auto !important; min-height: 0 !important;
            margin: 0 !important; padding: 0 !important;
            z-index: 999992;
        }}
        .st-key-force_pull_header button {{
            background: transparent !important; border: none !important;
            box-shadow: none !important; color: var(--peak-text) !important;
            height: 2.1rem; min-height: 0; padding: 0.2rem 0.5rem;
            font-size: 14px; font-weight: 400; border-radius: 4px;
        }}
        .st-key-force_pull_header button:hover {{
            background: rgba(255,255,255,0.08) !important;
            color: var(--peak-text) !important;
        }}
        /* Raw-data icons — pinned to each chart's tab row, far right, borderless */
        .st-key-combined_chart,
        [class*="st-key-chartwrap_"] {{ position: relative; }}
        [class*="st-key-raw_"] {{
            position: absolute; top: 8px; right: 0; z-index: 5;
            width: auto !important; min-height: 0 !important;
            margin: 0 !important; padding: 0 !important;
        }}
        [class*="st-key-raw_"] button {{
            background: transparent !important; border: none !important;
            box-shadow: none !important; color: var(--peak-text-secondary) !important;
            min-height: 0 !important; height: auto !important;
            padding: 2px 4px !important; font-size: 18px; line-height: 1;
        }}
        [class*="st-key-raw_"] button:hover {{
            color: var(--peak-text) !important; background: transparent !important;
        }}
        </style>
        """,
        unsafe_allow_html=True,
    )

    # ── Bootstrap scheduler once per process (survives Streamlit reruns) ──────────
    _need_init = (
        "scheduler" not in st.session_state
        or st.session_state.get("_pullers_version") != _PULLERS_VERSION
    )
    if _need_init:
        # Shut down old scheduler if one exists
        _old_sched = st.session_state.get("scheduler")
        if _old_sched is not None:
            try:
                _old_sched.shutdown()
            except Exception:
                pass
        _pullers = init_pullers(settings, cache_db)
        # On Streamlit Cloud the GitHub Actions cron pulls every 6h, so an
        # in-process APScheduler is duplicate work AND a big memory hog
        # (each puller holds its TOKENS list + APScheduler holds the whole
        # scheduler thread + job state). Skip it on Cloud; keep it locally so
        # `streamlit run` still auto-refreshes data during dev.
        if _IS_CLOUD:
            _sched = None
        else:
            _sched = PullScheduler(settings)
            for _p in _pullers:
                _sched.register(_p)
            _sched.start()
        st.session_state["scheduler"]       = _sched
        st.session_state["pullers"]         = _pullers
        st.session_state["_pullers_version"] = _PULLERS_VERSION

    scheduler: PullScheduler | None = st.session_state["scheduler"]
    pullers: List[DataPuller] = st.session_state["pullers"]

    # ── Puller groupings — bind once for the rest of the UI render ───────────────
    solana_pullers      = [p for p in pullers if getattr(p, "GROUP", "") == "solana_tokens"]
    stocks_pullers      = [p for p in pullers if getattr(p, "GROUP", "") == "tokenized_stocks"]
    commodity_pullers   = [p for p in pullers if getattr(p, "GROUP", "") == "tokenized_commodities"]
    stablecoin_pullers  = [p for p in pullers if getattr(p, "GROUP", "") == "stablecoins"]
    treasury_pullers    = [p for p in pullers if getattr(p, "GROUP", "") == "treasuries"]
    usdc_pullers        = [p for p in pullers
                           if getattr(p, "GROUP", "") not in
                              ("solana_tokens", "tokenized_stocks", "stablecoins")]

    # ── Auto-refresh ──────────────────────────────────────────────────────────────
    st_autorefresh(interval=settings.ui_refresh_seconds * 1_000, key="dashboard_refresh")

    # ── Sidebar: chain navigation ─────────────────────────────────────────────────
    _CHAINS = ["All chain", "Solana", "Ethereum", "BNB Chain", "Base"]
    with st.sidebar:
        st.markdown('<p class="peak-nav-title">Chains</p>', unsafe_allow_html=True)
        selected_chain = st.radio(
            "Chain", _CHAINS, index=1,
            label_visibility="collapsed", key="chain_nav",
        )

    _chain_label = "ALL CHAINS" if selected_chain == "All chain" else selected_chain.upper()

    # ── Top-bar controls — caption + Force Pull, floated next to Deploy ────────────
    _refresh_disp = (f"{settings.ui_refresh_seconds // 60}m"
                     if settings.ui_refresh_seconds >= 60
                     else f"{settings.ui_refresh_seconds}s")
    st.markdown(
        f'<span class="peak-sub-anchor"></span>'
        f'<p class="peak-sub peak-sub-topbar">Refresh <b>{_refresh_disp}</b> · '
        f'Pull <b>{settings.pull_interval_seconds // 3600}h</b> · '
        f'<code>{datetime.utcnow().strftime("%H:%M")} UTC</code></p>',
        unsafe_allow_html=True,
    )
    if st.button("⟳ Force Pull", key="force_pull_header",
                 help="Pull all data sources now"):
        with st.spinner("Pulling all data sources…"):
            for _p in pullers:
                try:
                    _p.pull()
                except Exception as _exc:
                    st.toast(f"Pull failed: {_p.name}", icon="⚠️")
        st.toast("Force pull complete", icon="✅")
        st.rerun()

    # ── Header ────────────────────────────────────────────────────────────────────
    st.markdown(
        f'<p class="peak-title">RWA DASHBOARD</p>'
        f'<p class="peak-subtitle">{selected_chain}</p>',
        unsafe_allow_html=True,
    )
    st.divider()

    # ── Combined tokenized-stocks overview helpers ────────────────────────────────

    # Moved to module level — see _STOCKS_PROJECT_COLORS def above the guard.




    @st.dialog("📈 Full Chart", width="large")
    def _fullscreen_chart() -> None:
        token_name = st.session_state.get("_fullscreen_token", "")
        puller = next((p for p in solana_pullers if p.TOKEN_NAME == token_name), None)
        if puller is None:
            st.warning("Token not found.")
            return
        st.subheader(f"{puller.TOKEN_NAME} — Market Cap & Volume (Solana)")
        puller.render()

    # Map the sidebar chain label to DefiLlama's chain name (None = aggregate all).
    _CHAIN_TO_DL = {
        "Solana":    "Solana",
        "Ethereum":  "Ethereum",
        "BNB Chain": "Binance",
        "Base":      "Base",
        "All chain": None,
    }

    if selected_chain != "Solana":
        # Other chains mirror Solana's tab layout (Tokenized stocks / Commodities /
        # Stablecoins / Treasuries & MMFs) so future charts have a stable home on
        # every chain. Each tab renders the chain-filtered MC chart for its group.
        # Birdeye is queried first (Solana addresses only return on Solana), so
        # non-Solana series come from DefiLlama. Groups without coverage for the
        # selected chain show an empty-state message instead of disappearing.
        dl_chain = _CHAIN_TO_DL.get(selected_chain)
        scope_label = "all chains" if dl_chain is None else selected_chain

        # Sidebar chain label → Birdeye x-chain value. None when the puller
        # already infers the chain from each token's address (no extra volume
        # chart) or when "All chain" is selected.
        _BIRDEYE_CHAIN = {
            "Ethereum":  "ethereum",
            "BNB Chain": "bsc",
            "Base":      "base",
        }
        birdeye_chain = _BIRDEYE_CHAIN.get(selected_chain)

        def _puller_has_chain(puller, chain_canonical: str | None) -> bool:
            """True if `puller.TOKENS` has at least one entry whose declared
            chain matches `chain_canonical` (DefiLlama-style name). When
            `chain_canonical is None` (the All-chain view) returns True so every
            puller renders. DefiLlama-only coverage doesn't count here — we only
            suppress pullers whose TOKENS registry doesn't even list the chain."""
            if not chain_canonical:
                return True
            want = chain_canonical.lower()
            for tok in puller.TOKENS:
                try:
                    if puller._token_chain(tok).lower() == want:
                        return True
                except Exception:
                    continue
            return False

        def _render_chain_group(group_label: str, group_pullers: list,
                                show_volume: bool = False) -> None:
            """Render every puller in a group, filtered to the active chain.
            Pullers whose TOKENS registry has no entries on the active chain are
            skipped entirely (e.g. PreStocks on Ethereum/BNB Chain — Solana-only)
            instead of emitting an empty 'no data yet' panel.
            If show_volume=True, also render a per-chain Birdeye volume chart
            above the market-cap chart."""
            if not group_pullers:
                st.info(
                    f"No {group_label.lower()} tracked on {scope_label} yet. "
                    "Drop tokens into the group registry to populate this tab.")
                return
            active_pullers = [p for p in group_pullers
                              if _puller_has_chain(p, dl_chain)]
            if not active_pullers:
                st.info(
                    f"No {group_label.lower()} deployed on {scope_label} yet. "
                    "Tokens in this group are tracked on other chains only.")
                return
            any_data = False
            for p in active_pullers:
                heading = getattr(p, "GROUP_LABEL", "") or group_label
                if show_volume and birdeye_chain:
                    st.subheader(f"{heading} — Trading Volume ({scope_label})")
                    p.render_volume_chain(chain=birdeye_chain)
                st.subheader(f"{heading} — Market Cap ({scope_label})")
                st.caption(
                    f"Per-token market cap on {scope_label}. Birdeye first; "
                    "DefiLlama free API supplies multi-chain history where "
                    "Birdeye has no coverage.")
                p.render_market_cap_chain(chain=dl_chain, stacked=True)
                any_data = True
            if not any_data:
                st.info(f"No {group_label.lower()} data on {scope_label} yet.")

        chain_tabs = st.tabs([
            "Tokenized stocks",
            "Tokenized commodities",
            "Stablecoins",
            "Treasuries & MMFs",
        ])
        with chain_tabs[0]:
            # xStocks now has Birdeye-native EVM volume (Backed.fi deploys the
            # same 0x proxy on Ethereum + BSC), so the non-Solana stock tabs get
            # a real volume chart on top of the per-chain MC chart.
            _render_chain_group("Tokenized Stocks", stocks_pullers,
                                show_volume=True)
        with chain_tabs[1]:
            # Commodities is the only group with Birdeye-native volume on
            # Ethereum today (PAXG / XAUT). Other chains will populate once
            # their addresses are wired into the TOKENS list.
            _render_chain_group("Tokenized Commodities", commodity_pullers,
                                show_volume=True)
        with chain_tabs[2]:
            # Stablecoins trade in size on Ethereum (USDT/USDC do billions/day),
            # so the non-Solana tab gets a Birdeye volume chart above MC.
            if dl_chain is None:
                # All-chain: render the cross-source aggregate view (DefiLlama
                # per-chain stacked + CoinGecko top-50 catalog). Skip
                # _render_chain_group here — its volume sub-chart needs a
                # specific birdeye chain (which is None on All-chain) and the
                # per-token MC chart it would render is summed across chains
                # already covered by the aggregate above. Instead, render just
                # the per-token MC chart directly under a fresh heading.
                _render_all_chain_stablecoins()
                st.divider()
                st.subheader("Stablecoins Market Cap by Token")
                st.caption(
                    "Each tracked stablecoin's market cap summed across every "
                    "chain it's deployed on. Useful for comparing token-level "
                    "trajectories that the per-chain stack above doesn't show."
                )
                for p in stablecoin_pullers:
                    p.render_market_cap_chain(chain=None, stacked=True)
            else:
                _render_chain_group("Stablecoins", stablecoin_pullers,
                                    show_volume=True)
        with chain_tabs[3]:
            _render_chain_group("Treasuries & MMFs", treasury_pullers)
        st.stop()

    tab_stocks, tab_commodities, tab_stablecoins, tab_treasuries = st.tabs(
        ["Tokenized stocks", "Tokenized commodities", "Stablecoins", "Treasuries & MMFs"])

    with tab_stocks:
        if not stocks_pullers:
            st.info("No tokenized stock group pullers registered.")
        else:
            # ── Combined overview: all projects in one chart ───────────────────
            st.subheader("All Tokenized Stocks — Volume by Project")
            combined_df = _combined_stocks_df(stocks_pullers)
            if combined_df is None:
                st.info("Waiting for first pull…")
            else:
                labels = [p.GROUP_LABEL for p in stocks_pullers]
                _raw = combined_df.copy()
                _present = [l for l in labels if l in _raw.columns]
                _raw["Total"] = _raw[_present].fillna(0).sum(axis=1)
                _fmt = {col: "${:,.0f}" for col in _present + ["Total"]}

                with st.container(key="combined_chart"):
                    # Raw-data icon — pinned onto the tab row, far right (see CSS)
                    if st.button("📋", key="raw_combined_stocks", help="View raw data"):
                        _raw_data_modal(_raw.sort_values("date", ascending=False), _fmt)
                    ctab_d, ctab_w, ctab_m = st.tabs(["Daily", "Weekly", "Monthly"])
                    with ctab_d:
                        _chart(
                            _build_combined_stocks_fig(combined_df, labels, "D", 380),
                            use_container_width=True,
                        )
                    with ctab_w:
                        _chart(
                            _build_combined_stocks_fig(combined_df, labels, "W", 380),
                            use_container_width=True,
                        )
                    with ctab_m:
                        _chart(
                            _build_combined_stocks_fig(combined_df, labels, "M", 380),
                            use_container_width=True,
                        )
            st.divider()

            # ── Per-group breakdowns — 2 per row ──────────────────────────────
            for row_start in range(0, len(stocks_pullers), 2):
                col_a, col_b = st.columns(2, gap="medium")
                for col, p in zip(
                    (col_a, col_b),
                    stocks_pullers[row_start : row_start + 2],
                ):
                    with col:
                        st.subheader(p.GROUP_LABEL)
                        # Chain-aware renderer reads the chain-suffixed vol col
                        # and dedupes by symbol, so multi-chain TOKENS entries
                        # (e.g. xStocks / Ondo on Solana + Ethereum + BSC) don't
                        # produce duplicate columns or pollute the Solana chart
                        # with EVM volume data. clip_outliers suppresses
                        # Birdeye v_usd glitch days (>10× per-token median).
                        p.render_volume_chain(chain="solana",
                                              clip_outliers=True)
                st.divider()

    with tab_commodities:
        if not commodity_pullers:
            st.info("No tokenized commodity pullers registered.")
        else:
            for p in commodity_pullers:
                st.subheader(f"{p.GROUP_LABEL} — Trading Volume (Solana)")
                # Restrict to Solana-native tokens so the Ethereum-only PAXG /
                # XAUT entries (which carry Birdeye Ethereum volume, not Solana)
                # don't pollute this stack. clip_outliers suppresses Birdeye
                # v_usd glitch days (>10× per-token median).
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

    with tab_stablecoins:
        if not stablecoin_pullers:
            st.info("No stablecoin pullers registered.")
        else:
            for p in stablecoin_pullers:
                st.subheader(f"{p.GROUP_LABEL} — Market Cap (Solana)")
                st.caption(
                    "Solana-only market cap per token, stacked. Sourced from "
                    "DefiLlama (free API, daily history) plus the Solscan-derived "
                    "seed JSONs and same-day Birdeye Token Overview snapshots."
                )
                # Chain-aware renderer reads mc_<sym>_solana_usd (DefiLlama +
                # seed + Birdeye snapshot all converge into this col) and dedupes
                # by symbol so multi-chain TOKENS entries don't break the chart.
                p.render_market_cap_chain(chain="Solana", stacked=True)

                # Split the volume view: USDC's cross-pair v_usd dwarfs every
                # other Solana stable by 10-100×, flattening the rest into the
                # x-axis. Render two stacks so both views are readable. The USDC
                # chart also clips Birdeye-glitch days (>10× global median) so
                # the late-Dec-2024 / early-Jan-2025 outlier cluster doesn't
                # squash the rest of the series visually.
                st.subheader(f"{p.GROUP_LABEL} — USDC + USDT Daily Trading Volume (Solana)")
                st.caption(
                    "USDC + USDT stacked · Birdeye OHLCV V3, v_usd, daily · "
                    "outlier days (>25× median) suppressed for readability — "
                    "keeps the legit Jan 18-20 2025 TRUMP-launch burst (~20×)."
                )
                p.render_volume_chain(chain="solana",
                                      include_tokens={"USDC", "USDT"},
                                      key_suffix="usdc_usdt",
                                      clip_outliers=True)

                st.subheader(f"{p.GROUP_LABEL} — Other Stables Daily Trading Volume (Solana)")
                st.caption(
                    "Everything except USDC + USDT, stacked · Birdeye OHLCV V3, "
                    "v_usd, daily · outlier days (>25× per-token median) suppressed."
                )
                p.render_volume_chain(chain="solana",
                                      exclude_tokens={"USDC", "USDT"},
                                      key_suffix="others",
                                      clip_outliers=True)

    with tab_treasuries:
        if not treasury_pullers:
            st.info("No treasury pullers registered.")
        else:
            for p in treasury_pullers:
                st.subheader(f"{p.GROUP_LABEL} — Market Cap (Solana)")
                st.caption(
                    "Per-token market cap on Solana, from DefiLlama's free API "
                    "(daily history). These tokens have no on-chain trading "
                    "activity tracked; only market cap is shown. Pick a different "
                    "chain in the sidebar to see the same data for that chain."
                )
                p.render_market_cap_chain(chain="Solana", stacked=True)

