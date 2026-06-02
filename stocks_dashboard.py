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

    # Streamlit UI auto-refresh
    ui_refresh_seconds: int = 30

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
        """Load the most recent cached snapshot."""
        return self.db.latest(self.name)

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
            xaxis=dict(rangeslider=dict(visible=True), type="date"),
            yaxis_tickformat="$~s",
        )
        st.plotly_chart(fig, use_container_width=True)

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
    def _try_token_ohlcv(self, headers: dict, time_to: int,
                         circ_supply: float | None) -> pd.DataFrame | None:
        rows = self._paginated_ohlcv(headers, self.ADDRESS, self.START_TS, time_to,
                                     endpoint="token")
        if not rows:
            return None
        df = pd.DataFrame(rows)
        df["date"]           = pd.to_datetime(df["unixTime"], unit="s").dt.strftime("%Y-%m-%d")
        df["price_usd"]      = df["c"]
        df["volume_usd"]     = df["v"] * df["c"]   # v = native token units; ×price → USD
        df["market_cap_usd"] = df["price_usd"] * circ_supply if circ_supply else None
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

    def _try_pair_ohlcv(self, headers: dict, time_to: int,
                        circ_supply: float | None) -> pd.DataFrame:
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
                "market_cap_usd": price * circ_supply if price and circ_supply else None,
            })
        df = pd.DataFrame(out_rows)
        df.attrs["volume_source"] = "pair-aggregation (top pairs)"
        return df

    # ── Public fetch ──────────────────────────────────────────────────────────
    def fetch(self) -> pd.DataFrame:
        headers    = {"X-API-KEY": self.settings.birdeye_api_key, "x-chain": "solana"}
        time_to    = int(datetime.utcnow().timestamp())
        circ_supply = self._fetch_circ_supply(headers)

        # Try fast token-level OHLCV first; fall back to pair aggregation
        df = self._try_token_ohlcv(headers, time_to, circ_supply)
        if df is None or df.empty:
            self.logger.info("%s: token OHLCV empty — falling back to pair aggregation",
                             self.TOKEN_NAME)
            df = self._try_pair_ohlcv(headers, time_to, circ_supply)

        # For stablecoins with a DeFiLlama ID, overwrite market_cap_usd with
        # real historical circulating supply so the chart line is meaningful.
        if not df.empty and self.DEFILLAMA_STABLE_ID:
            supply_by_day = self._fetch_defillama_supply()
            if supply_by_day:
                df["market_cap_usd"] = df["date"].map(supply_by_day)

        return df

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
                title_text=self.MC_CHART_LABEL, tickprefix="$", tickformat="~s",
                tickmode="array", tickvals=mc_ticks, range=[0, mc_ticks[-1]],
                showgrid=True,
            )
            layout["yaxis2"] = dict(
                title_text="Volume (USD)", tickprefix="$", tickformat="~s",
                tickmode="array", tickvals=vol_ticks, range=[0, vol_ticks[-1]],
                showgrid=False, overlaying="y", side="right",
            )
        else:
            layout["yaxis"] = dict(
                title_text="Volume (USD)", tickprefix="$", tickformat="~s",
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
            st.plotly_chart(self._build_fig(df, height=520), use_container_width=True)
        with tab_w:
            st.plotly_chart(self._build_fig(self._resample(df, "W"), height=520),
                            use_container_width=True)
        with tab_m:
            st.plotly_chart(self._build_fig(self._resample(df, "M"), height=520),
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
        st.plotly_chart(self._build_fig(df, height=300), use_container_width=True)


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
    def _token_chain(token_tuple) -> str:
        """Return the canonical chain (DefiLlama-style name) for a TOKENS row.
        Supports 2-tuple (name, addr) [chain inferred from addr] or 3-tuple
        (name, addr, chain). Normalises BSC aliases to 'Binance' to match the
        DefiLlama-written `mc_<token>_binance_usd` column suffix."""
        if len(token_tuple) >= 3 and token_tuple[2]:
            return _norm_chain(str(token_tuple[2]))
        addr = token_tuple[1] if len(token_tuple) >= 2 else ""
        return "Ethereum" if str(addr).startswith("0x") else "Solana"

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
        scheduler threads don't trip CoinGecko's free-tier rate limit.
        """
        now = time.time()
        with _CG_MC_LOCK:
            hit = _CG_MC_CACHE.get(cg_id)
            if hit and now - hit[0] < _CG_MC_TTL:
                return hit[1]
            caps = None
            for attempt in range(3):
                try:
                    r = requests.get(
                        f"https://api.coingecko.com/api/v3/coins/{cg_id}/market_chart",
                        # days="max" needs a paid key; 365 works on the free tier.
                        params={"vs_currency": "usd", "days": "365"}, timeout=30,
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
                address    = tok[1] if len(tok) > 1 else ""
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
        if self.MARKET_CAP_SOURCE == "birdeye_overview":
            # Legacy (chain-agnostic) MC col, one per UNIQUE symbol — kept so
            # the existing Solana per-token MC chart (render_market_cap) keeps
            # working without migration.
            _seen_names: set[str] = set()
            mc_cols = []
            for tok in self.TOKENS:
                t = tok[0]
                if t in _seen_names: continue
                _seen_names.add(t)
                mc_cols.append(self._mc_col(t))
            # (0) optional historical seed (mc_history_seed.json) — keyed by symbol/mint.
            _seed = _load_mc_seed()
            if _seed:
                for tok in self.TOKENS:
                    token_name = tok[0]
                    address    = tok[1] if len(tok) > 1 else ""
                    col = self._mc_col(token_name)
                    ser = (_seed.get(token_name.lower())
                           or _seed.get(str(address).lower()))
                    if ser:
                        for d, mc in ser.items():
                            mc_cols_by_date.setdefault(d, {})[col] = mc
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
                address    = tok[1] if len(tok) > 1 else ""
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
            for idx, (token_name, _) in enumerate(self.TOKENS):
                cg_id = self.COINGECKO_IDS.get(token_name)
                if not cg_id:
                    continue
                if idx > 0:
                    time.sleep(2.5)   # respect CoinGecko free-tier rate limit
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
        for tok in self.TOKENS:
            t = tok[0]
            a = tok[1] if len(tok) > 1 else ""
            if t in self.HIDDEN_TOKENS or t in seen:
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
                title_text="Total Market Cap (USD)", tickprefix="$", tickformat="~s",
                tickmode="array", tickvals=mc_ticks, range=[0, mc_ticks[-1]],
                showgrid=True,
            )
            layout_kwargs["yaxis2"] = dict(
                title_text="Daily Volume (USD)", tickprefix="$", tickformat="~s",
                tickmode="array", tickvals=vol_ticks, range=[0, vol_ticks[-1]],
                showgrid=False, overlaying="y", side="right",
            )
        else:
            layout_kwargs["yaxis"] = dict(
                title_text="Daily Volume (USD)", tickprefix="$", tickformat="~s",
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
        for _tn, _ in self.TOKENS:
            _safe = _tn.lower().replace("-", "_").replace(" ", "_")
            _fmt[f"vol_{_safe}_usd"] = "${:,.0f}"

        st.caption(f"Last pull: {df.attrs.get('pulled_at', '?')} UTC · Source: Birdeye")
        with st.container(key=f"chartwrap_{self.name}"):
            # Raw-data icon — pinned onto the tab row, far right (see CSS)
            if st.button("📋", key=f"raw_{self.name}", help="View raw data"):
                _raw_data_modal(df.sort_values("date", ascending=False), _fmt)
            tab_d, tab_w, tab_m = st.tabs(["Daily", "Weekly", "Monthly"])
            with tab_d:
                st.plotly_chart(
                    self._build_fig(df, sorted_tokens, height=450),
                    use_container_width=True,
                )
            with tab_w:
                st.plotly_chart(
                    self._build_fig(self._resample(df, "W"), sorted_tokens, height=450),
                    use_container_width=True,
                )
            with tab_m:
                st.plotly_chart(
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
        for token_name, _ in self.TOKENS:
            if token_name in self.HIDDEN_TOKENS or token_name in _seen:
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

        fig = go.Figure()
        for i, (token_name, _) in enumerate(token_series):
            color = self._COLORS[i % len(self._COLORS)]
            y = mdf[token_name]
            if stacked:
                y = y.fillna(0.0)
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
        fig.update_layout(
            height=380, hovermode="x unified",
            margin=dict(t=10, b=10, l=10, r=10),
            legend=dict(orientation="h", yanchor="bottom", y=1.02,
                        xanchor="right", x=1),
            yaxis=dict(title_text="Market Cap (USD)", tickprefix="$",
                       tickformat="~s", showgrid=True, rangemode="tozero"),
        )
        st.plotly_chart(fig, use_container_width=True)

    # ── Volume chart filtered to one chain (Birdeye OHLCV V3) ──────────────────
    def render_volume_chain(self, chain: str | None = None) -> None:
        """Daily trading-volume chart restricted to tokens whose addresses
        live on `chain` (per `_birdeye_chain_for`). When chain=None every
        token in TOKENS is included regardless of source. Reuses _build_fig
        so the layout matches the Solana volume chart exactly."""
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
        if not sorted_tokens:
            st.info(f"No trading volume recorded on {chain or 'any chain'} yet.")
            return

        st.caption(
            f"Last pull: {df.attrs.get('pulled_at', '?')} UTC · "
            f"Source: Birdeye OHLCV V3 (x-chain: {chain or 'all'})"
        )
        chain_tag = (chain or "all").lower().replace(" ", "_")
        with st.container(key=f"chartwrap_{self.name}_vol_{chain_tag}"):
            # Raw-data icon — pinned via existing CSS rules.
            _fmt = {self._safe_col(t, chain): "${:,.0f}"
                    for t, _, _ in sorted_tokens}
            if st.button("📋", key=f"raw_{self.name}_vol_{chain_tag}",
                         help="View raw data"):
                _raw_data_modal(df.sort_values("date", ascending=False), _fmt)
            # Aliased view: rename the chain-suffixed col → legacy col name so
            # _build_fig (which reads _safe_col(name) = vol_<name>_usd) works
            # without modification. Each render gets its own alias view.
            if chain:
                _aliases = {self._safe_col(t, chain): self._safe_col(t)
                            for t, _, _ in sorted_tokens}
                df_view = df.rename(columns=_aliases)
            else:
                df_view = df
            tab_d, tab_w, tab_m = st.tabs(["Daily", "Weekly", "Monthly"])
            with tab_d:
                st.plotly_chart(
                    self._build_fig(df_view, sorted_tokens, height=380),
                    use_container_width=True,
                )
            with tab_w:
                st.plotly_chart(
                    self._build_fig(self._resample(df_view, "W"), sorted_tokens,
                                    height=380),
                    use_container_width=True,
                )
            with tab_m:
                st.plotly_chart(
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
        present = [
            (t, self._mc_col(t)) for t, _ in self.TOKENS
            if t not in self.HIDDEN_TOKENS
            and self._mc_col(t) in df.columns and df[self._mc_col(t)].notna().any()
        ]
        if not present:
            st.info("Market-cap history is building — a snapshot is cached each "
                    "pull, so the chart fills in over the coming days.")
            return

        # Rows that carry at least one MC reading (MC accrues from tracking start).
        mc_cols = [c for _, c in present]
        mdf = df.loc[df[mc_cols].notna().any(axis=1),
                     ["date"] + mc_cols].sort_values("date")

        fig = go.Figure()
        for i, (token_name, col) in enumerate(present):
            color = self._COLORS[i % len(self._COLORS)]
            if stacked:
                y = mdf[col].fillna(0.0)
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
        fig.update_layout(
            height=380, hovermode="x unified",
            margin=dict(t=10, b=10, l=10, r=10),
            legend=dict(orientation="h", yanchor="bottom", y=1.02,
                        xanchor="right", x=1),
            yaxis=dict(title_text="Market Cap (USD)", tickprefix="$",
                       tickformat="~s", showgrid=True, rangemode="tozero"),
        )
        st.plotly_chart(fig, use_container_width=True)


def _make_stock_group_puller(puller_name: str, label: str,
                              tokens: list[tuple[str, str]],
                              group: str = "tokenized_stocks",
                              market_cap_source: str = "",
                              coingecko_ids: dict | None = None,
                              defillama_tokens: dict | None = None,
                              hidden_tokens: set | None = None,
                              skip_volume: bool = False) -> type:
    """Factory: return a TokenGroupMetricsPuller subclass for one group."""
    safe = puller_name.lower().replace("-", "_").replace(" ", "_")
    return type(
        f"{label.replace(' ', '').replace('-', '')}GroupMetricsPuller",
        (TokenGroupMetricsPuller,),
        {
            "name"             : f"{safe}_metrics",
            "GROUP"            : group,
            "GROUP_LABEL"      : label,
            "TOKENS"           : tokens,
            "MARKET_CAP_SOURCE": market_cap_source,
            "COINGECKO_IDS"    : coingecko_ids or {},
            "DEFILLAMA_TOKENS" : defillama_tokens or {},
            "HIDDEN_TOKENS"    : frozenset(hidden_tokens or ()),
            "SKIP_VOLUME"      : bool(skip_volume),
        },
    )


# ── Tokenized stock group registry (puller_name, display_label, [(token, address)]) ──
_TOKENIZED_STOCK_GROUPS: list[tuple[str, str, list]] = [
    (
        "prestocks_group",
        "PreStocks",
        [
            ("ANTHROPIC",  "Pren1FvFX6J3E4kXhJuCiAD5aDmGEb7qJRncwA8Lkhw"),
            ("ANDURIL",    "PresTj4Yc2bAR197Er7wz4UUKSfqt6FryBEdAriBoQB"),
            ("OPENAI",     "PreweJYECqtQwBtpxHL171nL2K6umo692gTm7Q3rpgF"),
            ("XAI",        "PreC1KtJ1sBPPqaeeqL6Qb15GTLCYVvyYEwxhdfTwfx"),
            ("SPACEX",     "PreANxuXjsy2pvisWWMNB6YaJNzr7681wJJr2rHsfTh"),
            ("KALSHI",     "PreLWGkkeqG1s4HEfFZSy9moCrJ7btsHuUtfcCeoRua"),
            ("POLYMARKET", "Pre8AREmFPtoJFT8mQSXQLh56cwJmM7CFDRuoGBZiUP"),
        ],
    ),
    (
        "xstocks_group",
        "xStocks",
        [
            ("AAPLx",  "XsbEhLAtcf6HdfpFZ5xEMdqW8nfAvcsP5bdudRLJzJp"),
            ("ABBVx",  "XswbinNKyPmzTa5CskMbCPvMW6G5CMnZXZEeQSSQoie"),
            ("ABTx",   "XsHtf5RpxsQ7jeJ9ivNewouZKJHbPxhPoEy6yYvULr7"),
            ("ACNx",   "Xs5UJzmCRQ8DWZjskExdSQDnbE6iLkRu2jjrRAB1JSU"),
            ("AMBRx",  "XsaQTCgebC2KPbf27KUhdv5JFvHhQ4GDAPURwrEhAzb"),
            ("AMDx",   "XsXcJ6GZ9kVnjqGsjBnktRcuwMBmvKWh8S93RefZ1rF"),
            ("AMZNx",  "Xs3eBt7uRfJX8QUs4suhyU8p2M6DoUDrJyWBa8LLZsg"),
            ("APPx",   "XsPdAVBi8Zc1xvv53k4JcMrQaEDTgkGqKYeh7AYgPHV"),
            ("AVGOx",  "XsgSaSvNSqLTtFuyWPBhK9196Xb9Bbdyjj4fH3cPJGo"),
            ("AZNx",   "Xs3ZFkPYT2BN7qBMqf1j1bfTeTm1rFzEFSsQ1z3wAKU"),
            ("BACx",   "XswsQk4duEQmCbGzfqUUWYmi7pV7xpJ9eEmLHXCaEQP"),
            ("BMNRx",  "XsrBCwaH8c46xiqXBChzobgufRKxQxAWUWbndgBNzFn"),
            ("BRK.Bx", "Xs6B6zawENwAbWVi7w92rjazLuAr5Az59qgWKcNb45x"),
            ("BTBTx",  "XsPLBFy59Q3hY59KLAJur8QyvziMF4xUxGTxXqXE7cT"),
            ("BTGOx",  "XsvHMmbDcd14DHHW16PkxPGW7ks77ehxUv1E9Zmxgj4"),
            ("CMCSAx", "XsvKCaNsxg2GN8jjUmq71qukMJr7Q1c5R2Mk9P8kcS8"),
            ("COINx",  "Xs7ZdzSHLU9ftNJsii5fCeJhoRWSC32SQGzGQtePxNu"),
            ("COPXx",  "XsybfiKkD4UmjkAGT2uR8X2sq9AWFtvGJM2KTffoALZ"),
            ("CRCLx",  "XsueG8BtpquVJX9LVLLEGuViXUungE6WmK5YZ3p3bd1"),
            ("CRMx",   "XsczbcQ3zfcgAEt9qHQES8pxKAVG5rujPSHQEXi4kaN"),
            ("CRWDx",  "Xs7xXqkcK7K8urEqGg52SECi79dRp2cEKKuYjUePYDw"),
            ("CSCOx",  "Xsr3pdLQyXvDJBFgpR5nexCEZwXvigb8wbPYp4YoNFf"),
            ("CVXx",   "XsNNMt7WTNA2sV3jrb1NNfNgapxRF5i4i6GcnTRRHts"),
            ("DFDVx",  "Xs2yquAgsHByNzx68WJC55WHjHBvG9JsMB7CWjTLyPy"),
            ("DHRx",   "Xseo8tgCZfkHxWS9xbFYeKFyMSbWEvZGFV1Gh53GtCV"),
            ("GLDx",   "Xsv9hRk1z5ystj9MhnA7Lq4vjSsLwzL2nxrwmwtD3re"),
            ("GMEx",   "Xsf9mBktVB9BSU5kf4nHxPq5hCBJ2j2ui3ecFGxPRGc"),
            ("GOOGLx", "XsCPL9dNWBMvFtTmwcCA5v3xWPSMEBCszbQdiLLq6aN"),
            ("GSx",    "XsgaUyp4jd1fNBCxgtTKkW64xnnhQcvgaxzsbAq5ZD1"),
            ("HDx",    "XszjVtyhowGjSC5odCqBpW1CtXXwXjYokymrk7fGKD3"),
            ("HONx",   "XsRbLZthfABAPAfumWNEJhPyiKDW6TvDVeAeW7oKqA2"),
            ("HOODx",  "XsvNBAYkrDRNhA7wPHQfX3ZUXZyZLdnCQDfHZ56bzpg"),
            ("IBMx",   "XspwhyYPdWVM8XBHZnpS9hgyag9MKjLRyE3tVfmCbSr"),
            ("IEMGx",  "XsFnZawJdLdXfBSEt5Vw29K5vdBiHotdPLjUPafpfHs"),
            ("IJRx",   "XsyZcb97BzETAqi9BoP2C9D196MiMNBisGMVNje2Thz"),
            ("INTCx",  "XshPgPdXFRWB8tP1j82rebb2Q9rPgGX37RuqzohmArM"),
            ("IWMx",   "XsbELVbLGBkn7xfMfyYuUipKGt1iRUc2B7pYRvFTFu3"),
            ("JNJx",   "XsGVi5eo1Dh2zUpic4qACcjuWGjNv8GCt3dm5XcX6Dn"),
            ("JPMx",   "XsMAqkcKsUewDrzVkait4e5u4y8REgtyS7jWgCpLV2C"),
            ("KOx",    "XsaBXg8dU5cPM6ehmVctMkVqoiRG2ZjMo1cyBJ3AykQ"),
            ("KRAQx",  "XsAiRejKuvLAdq9KtedrMSrabz7SWdzKoVK6Qgac1Ki"),
            ("LINx",   "XsSr8anD1hkvNMu8XQiVcmiaTP7XGvYu7Q58LdmtE8Z"),
            ("LLYx",   "Xsnuv4omNoHozR6EEW5mXkw8Nrny5rB3jVfLqi6gKMH"),
            ("MAx",    "XsApJFV9MAktqnAc6jqzsHVujxkGm9xcSUffaBoYLKC"),
            ("MCDx",   "XsqE9cRRpzxcGKDXj1BJ7Xmg4GRhZoyY1KpmGSxAWT2"),
            ("MDTx",   "XsDgw22qRLTv5Uwuzn6T63cW69exG41T6gwQhEK22u2"),
            ("METAx",  "Xsa62P5mvPszXL1krVUnU5ar38bBSVcWAB6fmPCo5Zu"),
            ("MRKx",   "XsnQnU7AdbRZYe2akqqpibDdXjkieGFfSkbkjX1Sd1X"),
            ("MRVLx",  "XsuxRGDzbLjnJ72v74b7p9VY6N66uYgTCyfwwRjVCJA"),
            ("MSFTx",  "XspzcW1PRtgf6Wj92HCiZdjzKCyFekVD8P5Ueh3dRMX"),
            ("MSTRx",  "XsP7xzNPvEHS1m6qfanPUGjNmdnmsLKEoNAnHjdxxyZ"),
            ("NFLXx",  "XsEH7wWfJJu2ZT3UCFeVfALnVA6CP5ur7Ee11KmzVpL"),
            ("NVDAx",  "Xsc9qvGR1efVDFGLrVsmkzv3qi45LTBjeUKSPmx9qEh"),
            ("NVOx",   "XsfAzPzYrYjd4Dpa9BU3cusBsvWfVB9gBcyGC87S57n"),
            ("OPENx",  "XsGtpmjhmC8kyjVSWL4VicGu36ceq9u55PTgF8bhGv6"),
            ("ORCLx",  "XsjFwUPiLofddX5cWFHW35GCbXcSu1BCUGfxoQAQjeL"),
            ("PALLx",  "XsTTtPA5V19YwHKDv4xeVXNM6kdsQNJvg3MyWkRUckt"),
            ("PEPx",   "Xsv99frTRUeornyvCfvhnDesQDWuvns1M852Pez91vF"),
            ("PFEx",   "XsAtbqkAP1HJxy7hFDeq7ok6yM43DQ9mQ1Rh861X8rw"),
            ("PGx",    "XsYdjDjNUygZ7yGKfQaB6TxLh2gC6RRjzLtLAGJrhzV"),
            ("PLTRx",  "XsoBhf2ufR8fTyNSjqfU71DYGaE6Z3SUGAidpzriAA4"),
            ("PMx",    "Xsba6tUnSjDae2VcopDB6FGGDaxRrewFCDa5hKn5vT3"),
            ("PPLTx",  "Xst6eFD4YT6sz9RLMysN9SyvaZWtraSdVJQGu5ZkAme"),
            ("QQQx",   "Xs8S1uUs1zvS2p7iwtsG3b6fkhpvmwz4GYU3gWAmWHZ"),
            ("SCHFx",  "XsWAnFM77x6YvpdaZoos79R12o4Yj4r7EVkaTWddzhU"),
            ("SLVx",   "XsxAd6okt8y1RRK6gNg7iJaqiWNiq5Md5EDf3ZrF2dm"),
            ("SPYx",   "XsoCS1TfEyfFhfvj8EtZ528L3CaKBDBRqRapnBbDF2W"),
            ("STRCx",  "Xs78JED6PFZxWc2wCEPspZW9kL3Se5J7L5TChKgsidH"),
            ("TBLLx",  "XsqBC5tcVQLYt8wqGCHRnAUUecbRYXoJCReD6w7QEKp"),
            ("TMOx",   "Xs8drBWy3Sd5QY3aifG9kt9KFs2K3PGZmx7jWrsrk57"),
            ("TONXx",  "XscE4GUcsYhcyZu5ATiGUMmhxYa1D5fwbpJw4K6K4dp"),
            ("TQQQx",  "XsjQP3iMAaQ3kQScQKthQpx9ALRbjKAjQtHg6TFomoc"),
            ("TSLAx",  "XsDoVfqeBukxuZHWhdvWHBhgEHjGNst4MLodqsJHzoB"),
            ("UNHx",   "XszvaiXGPwvk2nwb3o9C1CX4K6zH8sez11E6uyup6fe"),
            ("VTIx",   "XsssYEQjzxBCFgvYFFNuhJFBeHNdLWYeUSP8F45cDr9"),
            ("VTx",    "XsEdDDTcVGJU6nvdRdVnj53eKTrsCkvtrVfXGmUK68V"),
            ("Vx",     "XsqgsbXwWogGJsNcVZ3TyVouy2MbTkfCFhCGGGcQZ2p"),
            ("WMTx",   "Xs151QeqTCiuKtinzfRATnUESM2xTU6V9Wy8Vy538ci"),
            ("XOMx",   "XsaHND8sHyfMfsWPj6kSdd5VwvCayZvjYgKmmcNL5qh"),
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
            ("AAPLon",  "123mYEnRLM2LLYsJW3K6oyYh8uP1fngj732iG638ondo"),
            ("ABNBon",  "128qNYovdGv2YqayErcJgU7gDwbNVX1VuoxbtWz8ondo"),
            ("ABTon",   "129gRoHKhVg7CvPMrqVsEB4uYZo6zV4yDZX6NBg9ondo"),
            ("ACNon",   "12LxMMJYVSf4LoeqjFE47BQQNRciaH9E3nbDfjH4ondo"),
            ("ADBEon",  "12Rh6JhfW4X5fKP16bbUdb4pcVCKDHFB48x8GG33ondo"),
            ("AGGon",   "13qTjKx53y6LKGGStiKeieGbnVx3fx1bbwopKFb3ondo"),
            ("AMDon",   "14diAn5z8kjrKwSC8WLqvBqqe5YmihJhjxRxd8Z6ondo"),
            ("AMZNon",  "14Tqdo8V1FhzKsE3W2pFsZCzYPQxxupXRcqw9jv6ondo"),
            ("APOon",   "14VXAhoa1R74vi1ZuiQyGLJrnDMfoFBPJSCpGVz3ondo"),
            ("APPon",   "14Z8rQQe2Aza33YgEUmj3g3QGNz8DXLiFPuCnsD1ondo"),
            ("ARMon",   "15SsCZqCsM9fZGhTmP4rdJTPT9WGZKazDSsgeQ8ondo"),
            ("ASMLon",  "1eLZPRsn8bAKmoxsqDMH9Q2m2k7GMNp6RLSQGm8ondo"),
            ("AVGOon",  "1FWZtdWN7y38BSXGzbs8D6Shk88oL9atDNgbVz9ondo"),
            ("AXPon",   "1WxT6NdK7uqpfXuKpALxL2n3f7Rq61XXeHA8UM4ondo"),
            ("BABAon",  "1zvb9ELBFShBCWKEk5jRTJAaPAwtVt7quEXx1X4ondo"),
            ("BAon",    "1YVZ4LGpq8CAhpdpm3mgy7GgPb83gJczCpxLUQ3ondo"),
            ("BIDUon",  "54CoRF2FYMZNJg9tS36xq5BUcLZ7rju1r59jGc2ondo"),
            ("BLKon",   "5H1VpMzRuoNtRbPTRCz35ETtEUtnkt8hJuQb9v7ondo"),
            ("CMGon",   "5owVsVFSHACQuippFYdLp3qWRobp2EGcwxMmsr6ondo"),
            ("COINon",  "5u6KDiNJXxX4rGMfYT4BApZQC5CuDNrG6MHkwp1ondo"),
            ("COSTon",  "6btaz134wjHkR8sqhAYrtSM6tavftfxnRvnyMd8ondo"),
            ("CRCLon",  "6xHEyem9hmkGtVq6XGCiQUGpPsHBaoYuYdFNZa5ondo"),
            ("CRMon",   "7D7ukbcnUNYt7Et5vtsDZhAy28MKu9pkHka1Hp9ondo"),
            ("CSCOon",  "7DWcZE1uVc8m2mf9pV8KNov28ET7HsvHkhrhgr9ondo"),
            ("CVXon",   "7tgKziACteG26VjV5xKufojKxwTgCFyTwmWUmz5ondo"),
            ("DASHon",  "83P1gCFBZfGRCwJuBt9juxJKEsZwejJoG66eTZ6ondo"),
            ("DISon",   "mJf1xT3suXtkXBCfZcE9oUUuyxkvSgqYBWiX7v1ondo"),
            ("EEMon",   "916SDKz7y5ZcEZC9CtnQ5Djs1Y8Yv3UAPb6bak8ondo"),
            ("EFAon",   "AbvryMGnaba9oADMZk8Vp2Av6MtczsncGyfWaC4ondo"),
            ("EQIXon",  "aheEdmuryJU8ymy8LjYheZH5i2BW1UMsfuWQKD2ondo"),
            ("FIGon",   "aLDdFsr3VTUQaHFK6yNvQxztvxQ8nxW4AMuSGC7ondo"),
            ("FUTUon",  "Ao5rKFRQ54W3DKSAtqfhBRPNHewwWRLNLao2JL9ondo"),
            ("GEon",    "aTBfDuLRqYHBiG82bHA7DzwjSDTFre2dRtGH3S5ondo"),
            ("GMEon",   "aznKt8v32CwYMEcTcB4bGTv8DXWStCpHrcCtyy7ondo"),
            ("GOOGLon", "bbahNA5vT9WJeYft8tALrH1LXWffjwqVoUbqYa1ondo"),
            ("GSon",    "BchJRy2snmhJZf3rQ9LJ3ePs2BGfYgfvQNo31d2ondo"),
            ("HIMSon",  "bdh3njeo19d2TBLAKTGvCWdSoArfVw8uZBAJHY4ondo"),
            ("HOODon",  "BVdXGvmgi6A9oAiwWvBvP76fyTqcCNRJMM7zMN6ondo"),
            ("HYGon",   "c5ug15fwZRfQhhVa6LHscFY33ebVDHcVCezYpj7ondo"),
            ("IAUon",   "M77ZvkZ8zW5udRbuJCbuwSwavRa7bGAZYMTwru8ondo"),
            ("IBMon",   "C8bZkgSxXkyT1RgxByp2teJ24hgimPLoyEYoNa9ondo"),
            ("IEFAon",  "C9J9vZ8N79GzzxFoRkPWCkGtMKU8akg4FhUk4r9ondo"),
            ("IEMGon",  "cdVNL7wK8mf1UCDqM6zdrziRv4hmvqWhXeTcck2ondo"),
            ("IJHon",   "cfPLN9WXD2BTkbZhRZMVXPmVSiRo44hJWRtnaC8ondo"),
            ("INTCon",  "cJpUMp5R7rZ6fGeLHbHhrRuJzK9mkyKDjZqNpT3ondo"),
            ("INTUon",  "CozoH5HBTyyeYSQxHcWpGzd4Sq5XBaKzBzvTtN3ondo"),
            ("ITOTon",  "CPWkMURVvcnX8hGjqCTb8i5LkzV3VSvyk7SeJi8ondo"),
            ("IVVon",   "CqW2pd6dCPG9xKZfAsTovzDsMmAGKJSDBNcwM96ondo"),
            ("IWFon",   "dSHPFuMMjZqt7xDYGWrexXTSkdEZAiZngqymQF2ondo"),
            ("IWMon",   "dvj2kKFSyjpnyYSYppgFdAEVfgjMEoQGi9VaV23ondo"),
            ("IWNon",   "DX7g7WNjDpVzNK9CG81v7wb6ZbiNzYfkdzH2Xs5ondo"),
            ("JDon",    "E1aUS5nyv7kaBzdQzPVJW5zfaMgoUJpKYzdnFS2ondo"),
            ("JPMon",   "E5Gczsavxcomqf6Cw1sGCKLabL1xYD2FzKxVoB4ondo"),
            ("KOon",    "e6G4pfFcrdKxJuZ4YXixRFfMbpMvgXG2Mjcus71ondo"),
            ("LINon",   "Edik9MoFp8LAXS9HNu2gRFyihwYqDqv4ZmNmVT9ondo"),
            ("LLYon",   "eGGxZwNSfuNKRqQLKaz2hc4QkA2mau7skyxPdj7ondo"),
            ("LMTon",   "EoReHwUnGGekbXFHLj5rbCVKiwWqu32GrETMfw4ondo"),
            ("MAon",    "EsVHcyRxXFJCLMiuYLWhoDygrNe1BJGpYeZ17X7ondo"),
            ("MARAon",  "ETCJUmuhs5aY62xgEVWCZ5JR8KPdeXUaJz3LuC5ondo"),
            ("MCDon",   "EUbJjmDt8JA222M91bVLZs211siZ2jzbFArH9N3ondo"),
            ("MELIon",  "EWwdgGshGngcMpDV34pWZRSu5bkAuiKuKTTHKQ8ondo"),
            ("METAon",  "fDxs5y12E7x7jBwCKBXGqt71uJmCWsAQ3Srkte6ondo"),
            ("MRVLon",  "FovBwhoV5KQjZCdhoM6jgXYwXLX3F8vgAfvmLH7ondo"),
            ("MSFTon",  "FRmH6iRkMr33DLG6zVLR7EM4LojBFAuq6NtFzG6ondo"),
            ("MSTRon",  "FSz4ouiqXpHuGPcpacZfTzbMjScoj5FfzHkiyu2ondo"),
            ("MUon",    "Fz9edBpaURPPzpKVRR1A8PENYDEgHqwx5D5th28ondo"),
            ("NFLXon",  "g4KnPrxPLeeKkwvDmZFMtYQPM64eHeShbD55vK6ondo"),
            ("NKEon",   "g646pcdG2Rt5DH9WZzL7VVnVDWCCMTTrnktwE74ondo"),
            ("NOWon",   "G7pTVoSECz5RQWubEnTP7AC83KHUsSyoiqYR1R2ondo"),
            ("NVDAon",  "gEGtLTPNQ7jcg25zTetkbmF7teoDLcrfTnQfmn2ondo"),
            ("NVOon",   "GeV7S8vjP8qdYZpdGv2Xi6e7MUMCk8NAAp2z7g5ondo"),
            ("ORCLon",  "GmDADFpfwjfzZq9MfCafMDTS69MgVjtzD7Fd9a4ondo"),
            ("PANWon",  "M7hVQomhw4Q2D2op3HvBrZjHu9SryjNvD5haEZ1ondo"),
            ("PBRon",   "GRciFCqJ5y2hbiD6U5mGkohY65BZTXGuGUrCqf7ondo"),
            ("PEPon",   "gud6b3fYekjhMG5F818BALwbg2vt4JKoow59Md9ondo"),
            ("PFEon",   "Gwh9fPsX1qWATXy63vNaJnAFfwebWQtZaVmPko6ondo"),
            ("PGon",    "GZ8v4NdSG7CTRZqHMgNsTPRULeVi8CpdWd9wZY8ondo"),
            ("PLTRon",  "HfsnTS5qtdStwec9DfBrunRqnAMYMMz1kjv9Hu9ondo"),
            ("PYPLon",  "hM7B3UQTTR81mS27SxDDPzBbjejmo8fnpFjzgv9ondo"),
            ("QBTSon",  "hqJXutLF6f7DxStrWCrnZDfXzbNTZmvi3KheVi6ondo"),
            ("QCOMon",  "hrmX7MV5hifoaBVjnrdpz698yABxrbBNAcWtWo9ondo"),
            ("QQQon",   "HrYNm6jTQ71LoFphjVKBTdAE4uja7WsmLG8VxB8ondo"),
            ("RDDTon",  "HXFrTf9v9NdjGUTnx4sojR3Cf92hoBsQFUxKTN7ondo"),
            ("RIOTon",  "i6f3DvZBuLpnGSqS8x6WPeStJ7jNe5KewD6afD5ondo"),
            ("SBETon",  "iLDu2jjp2i3Uqc2Vm7K7GLiUj3hR4Un49MtD7c4ondo"),
            ("SBUXon",  "iPFqjcZQTNMNXA4kbShbMhfAVD8yr8Uq9UtXMV6ondo"),
            ("SHOPon",  "ivdDracs2s7jCP698dJXKSEQdVrNj9hasJL1Uq1ondo"),
            ("SLVon",   "iy11ytbSGcUnrjE6Lfv78TFqxKyUESfku1FugS9ondo"),
            ("SMCIon",  "jLca79XzcewRuBZyaJxVxuKpUHcEix1X4CP1RP9ondo"),
            ("SNOWon",  "JmFLCBwoNvcXy6B2VqABg6m784ubkXpaEx3p7S5ondo"),
            ("SPGIon",  "JrTYw7A9jihX5TwpRStYviEbsYf2X2VJpZ13719ondo"),
            ("SPOTon",  "jzCvs2Pk8tDcfsFRqnEMjurgaQW4iQfEkandUR8ondo"),
            ("SPYon",   "k18WJUULWheRkSpSquYGdNNmtuE2Vbw1hpuUi92ondo"),
            ("TIPon",   "k6BPp2Xmf2TYgrZiUyWfUoZBKeqaDbvPoAVgSx2ondo"),
            ("TLTon",   "KaSLSWByKy6b9FrCYXPEJoHmLpuFZtTCJk1F1Z9ondo"),
            ("TMon",    "kbmF7ERJWMaaDswMprrH9gHSLya5D2RMBNgKqg3ondo"),
            ("TSLAon",  "KeGv7bsfR4MheC1CkmnAVceoApjrkvBhHYjWb67ondo"),
            ("TSMon",   "keybg184d4vyXeQdFqs4o99YsMg7xBthxTJ6Ky3ondo"),
            ("UBERon",  "KJNeFW3kk3ycPjXpC6cbuyckjeYHacc2ekhtAi5ondo"),
            ("UNHon",   "kPBGL8vAwKN3UGmr9cjkM2dU79SC3nzTC9yu7F8ondo"),
            ("Von",     "kxEW4oJL75K37VeXaZF1ynbHQATQwhECQKN1374ondo"),
            ("WFCon",   "L6ZE5qCpVVSqLePz64CrwkgyWoPF9M7tB8BeFH4ondo"),
            ("WMTon",   "LZddqAqKqJW9oMZSjTxCUmbmzBRQtv9gMkD9hZ3ondo"),
            ("AALon",   "9wYZetvT8J2ptfsRca5gzLBGvcUug38mp9yT3xaondo"),
            ("ABBVon",  "MFerpBVGKZh2jXN7cbJdXRXQTp6j6pbSnSZrfWrondo"),
            ("ACHRon",  "KcCVQxG9LhFYP5o9DWFKTFgFShPPQkDEemVbiFyondo"),
            ("ADIon",   "LmTMwmZLNZszn3qpjmnbhfP12U4qWDivaEBwSBSondo"),
            ("AMATon",  "7eRX747PSbVtGVx3qD5UFdkNM2BfTy86ikUiCMhondo"),
            ("AMCon",   "C9xNaNujcF1a5fidWAAFReFYqhLRVbyk4yPyGqzondo"),
            ("AMGNon",  "SS6AEWhzRrxhL2cXzKKjhFt3rCzmHHGKmFyugDTondo"),
            ("ANETon",  "Cq6QtvHpXbJWtFaiMhUDtHy8YVZ95gcD1oZ1cohondo"),
            ("BACon",   "Wk8gC6iTNp8dqd4ghkJ3h1giiUnyhykwHh7tYWjondo"),
            ("BBAIon",  "YXE7mph6XhsgnyezkMEcTuohSuWhbLWfwx2Hh6mondo"),
            ("BILIon",  "14kLsQVmc64qZexYuR4XGop9y8BeMkd77pJUm1Rhondo"),
            ("BINCon",  "mhZ69E1vDnAsQJXAwarLYSX5tmgeMajXBJ2rXAcondo"),
            ("BLSHon",  "A9PFmw9Hu8zzxDUoU351pio1E1XWBWBfWnjT9qoondo"),
            ("BMNRon",  "MYXqkDYbzr7vjXAz2BapR4AiYRXzoikGirrLoRzondo"),
            ("BTGon",   "cBnVXDyZgaaLZM18wAmqsUKnRUFAEJWbq6VuUoaondo"),
            ("BZon",    "doPqjCxi6UkANkvMz5fSuYGEo5PGppVpTZMeB5vondo"),
            ("CATon",   "AErxJJxGbc9cZzZoZepN62BNfg5RXns8tmEc3Zpondo"),
            ("CEGon",   "7NWHifsBnn9DimUeNnsHdEXkTZhXmJTiXxcCngBondo"),
            ("CIFRon",  "WNZBSkNBNP3Ct1pcFn6Fu4sZQFhnu48EsM9voCEondo"),
            ("CLOAon",  "t71FyTYHVkPAb5g48adDHmkVxXYbUuP2eq6jDZLondo"),
            ("CLOIon",  "ucQ3VfWAx9pkCN4Kg84zE56FtB4FJN2kQH4ArYYondo"),
            ("COFon",   "R2uDbMtmHq5xSS5SserrovdRKdpiqnVBCd2AHLhondo"),
            ("Con",     "PjtfUiw6Hwd8PZ94EcUw8mBSYxp7SjjzSLeNTDKondo"),
            ("COPon",   "X68p9qTpEMkR1TLpXUP2ZJo8PG4Qge2Y2ZLdjA2ondo"),
            ("COPXon",  "X7j77hTmjZJbepkXXBcsEapM8qNgdfihkFj6CZ5ondo"),
            ("CPNGon",  "NKyzy31w2J7odLb2CW3Ft4fpKXkW3LBt1pvpkVLondo"),
            ("CRWDon",  "cdKfoNjbXgnSuxvoajhtH3uixfZhq1YXhQsS1Rwondo"),
            ("CVNAon",  "FGmUDXqA3AbWfo5b3NUcsvwoUFCF4tr9ea6uercondo"),
            ("DBCon",   "td1aY5AvYQuwGD75qNq9aPipMexraN9mQXJwqifondo"),
            ("DEon",    "CqQyAZjB9LGFTG95eiadGTkfhd9QA12ProeKsQmondo"),
            ("DGRWon",  "gnoSQSNTNZHViqVfxCcPDVxcRA29mrJL7C6JqYLondo"),
            ("DNNon",   "12J2LD3tuLfdiVKnWZMHRMrbnXDY9rM4yqVLUa5yondo"),
            ("FIGRon",  "ZmHxc6Gt27RJKxD2ay6UL4n9yQ7mKAq4XZQUeVhondo"),
            ("Fon",     "5hT2o25X9tGXipwhLckaUdgnxrZ6Y8eiUwdhpLeondo"),
            ("FTGCon",  "ivBnfPTyuHDNWmMSnbavckhJK6SHZW8h77nZKsEondo"),
            ("GEMIon",  "NrTdGMA3ujUvWXkwXyZKnhoByb32KTjRh5Vo47yondo"),
            ("GLDon",   "hWfiw4mcxT8rnNFkk6fsCQSxoxgZ9yVhB6tyeVcondo"),
            ("GRABon",  "m9GcsVgdjaL3KsdtSFHimnhtsUMpTHkjtwEG4Tzondo"),
            ("GRNDon",  "Gc1aT3ay7FXL3qdAW7cNSXYPDsGavy7qiACuxwxondo"),
            ("HDon",    "MtEXKVN3Pcggy8MPA3eJr15H6SK3RXheScqj9qtondo"),
            ("IRENon",  "13QHuepdhtJ3urNsV9i1hdL8nQoca2G7ZaLzb5FYondo"),
            ("ISRGon",  "1MGRpPrkhEsCm2GCWD3rsvEU77xTTLAzfKXeFgFondo"),
            ("JAAAon",  "KZtqx9BJbpcGY7vdzhqPXM3ECKChxE5YhXaDiwRondo"),
            ("JNJon",   "KUXt7LzHWSQXp5eyqMZRxWjAP6yM8BUh4LRHwiwondo"),
            ("KLACon",  "149o8ppQf9SzKCKXZ4v3dzHkwumvtQSRzSEkr29uondo"),
            ("LIon",    "v12TwfofSbvVqQ5N5KGG4d3J8rtEi4BjGfn2apyondo"),
            ("LOWon",   "edLdFJVVR532qhcrNTJjLAmhmyV7NsctbWVokMBondo"),
            ("LRCXon",  "wFJoeEYpKg9oRhyJy6BWTT3J95gmXBLvoeikDQNondo"),
            ("MPon",    "XwFm5GiKPVTvPiEbQpdc6vJbFEpsUXRMf6TcSxnondo"),
            ("MRKon",   "bn1fb8dwzafGePqNPrM8m8cbAKQiFqeEPuZkPySondo"),
            ("MRNAon",  "14VP7DvCAdBCc5XGNZkPt6zhtPzJrWWS64Koxtxyondo"),
            ("MTZon",   "R3ywbVQ5t8LNmjQsn2Ngv43dSqyZscQwNag9G3Eondo"),
            ("NEEon",   "t7eN6cGwRMFaZvsNW2SmVwkedmHtDdrxA4ycNE5ondo"),
            ("NIKLon",  "V8LRV7kWjrx6Prke9oHEHNUiR122BVtyuPciTCTondo"),
            ("NIOon",   "yQ37dFiGAbzrb2FRAEhGNzRy5zFfoYGWYhAepFEondo"),
            ("NTESon",  "YeK2TdPtGLAme3Phg4pb1GBN2YxKgX5UNVyD4asondo"),
            ("OKLOon",  "m6oDLvJT7rY7M1TxuLWP3pWmAPg2cCWDQR1NKiEondo"),
            ("ONDSon",  "7qy1j4Mechfyr6uAST3djH4vk4kiEYC2cjEytXdondo"),
            ("ONon",    "13qtwy5fZi9Przz14pzo9xqFSr8QHmLyUpUCvP1xondo"),
            ("OPENon",  "ou1uE526v7zmUYP2qCb2LJgfXAyWAtWS9SETtr8ondo"),
            ("OPRAon",  "gbHFTMkuMQUy5xrgoCBdaQ2XYvNyjWAYcnRPh9Condo"),
            ("OSCRon",  "ThwGDsXZ6iKubWuEQjmDxGwF3bUERDGbBXvcbjFondo"),
            ("OXYon",   "1GNFMryQ6c9ZpMhgNimmsbtgYM21qnBJgRAFoNiondo"),
            ("PALLon",  "P7hTXnKk2d2DyqWnefp5BSroE1qjjKpKxg9SxQqondo"),
            ("PCGon",   "UP5s1srLaHDc4SwJqLPa3A48x5R7ofN3hZWxWEZondo"),
            ("PDBCon",  "M6agiXbNgy8Xon9ngiW4ZDPbMFcNCTMkMMkshZyondo"),
            ("PDDon",   "PnjETBCLC318DRejo9cMQKAmET9PvW8AEFGWMNtondo"),
            ("PINSon",  "sxyg1VTSzy5zYANUK7hntNtmFAWoXGJq95AcHuVondo"),
            ("PLUGon",  "TnfswqdE1jAJ8sfnf5J7kSVLEH1cfpAYZ8MWmKfondo"),
            ("PSQon",   "qKtU9A7ij34XmtxaSzYfxCpkgAZzzFsqnUb2kW2ondo"),
            ("REMXon",  "tiitb2Z1HtpB2DpVr6V7tdCFS3jmTinLeuGj9EVondo"),
            ("RGTIon",  "dwEPNKQab3iwRmjGvZPXhAmws1W5NsQGwuXwi8oondo"),
            ("RIVNon",  "AXRsYFt7TXNQ3DcY6BkvRgPV6VsYMURyDtaeudjondo"),
            ("RTXon",   "12BvLZtzjdssAycxPeBQUjukhmgQpULAvy6SroYdondo"),
            ("SCHWon",  "cnc6M1zXLdrGR5LAQVcaJDfgezMiVWNtGQsVy1Kondo"),
            ("SGOVon",  "HjrN6ChZK2QRL6hMXayjGPLFvxhgjwKEy135VRjondo"),
            ("SNAPon",  "a2cXfonVgQ6cKB4Lm8YZsPry39VZSA562bwmRSiondo"),
            ("SOFIon",  "mqL8yXQpeSvc7NgrAtLLPtRvUiWyLoG5RWLv16iondo"),
            ("SOon",    "aKzjn2ZdWySSGPSSDTY2HUpcSCmemSahTXihrpyondo"),
            ("SOUNon",  "vE2qArmjto6VfeMngyGAnzp2ipLYeXsxiARDnnXondo"),
            ("SQQQon",  "D1tu7Fnm3cCpKyyPXrqm5GXShPqMj7a2SEjjq9fondo"),
            ("TCOMon",  "9PMjLqd8zPdKkJUXarnit5t7tPL3cCscwHzy7ATondo"),
            ("TLNon",   "RTb54gpqAx6RpLAHRGnqQ3ciQ845CHqhg21ZzEJondo"),
            ("TMOon",   "T699bgtXQw4CJ59rQ4VzLsupVQUzoL5RmuhHnKrondo"),
            ("TMUSon",  "pDY4GPJfZcNETPG7myXeafQfgJqqVkn81bMYDyfondo"),
            ("Ton",     "WKMZummev5UcXz5nNKQZvTD6QjNSM2X58uwmDReondo"),
            ("TQQQon",  "14W1itEkV7k1W819mLSknFTaMmkCtPokbF2tRkPUondo"),
            ("TXNon",   "81xLFvCzFaUM3KDxSHC75pXu3RPCeSeCbmGBY8aondo"),
            ("USFRon",  "o6U1Sm6Vd7EofMyCrL28mrp2QLzgYGgjveHiEQ5ondo"),
            ("USOon",   "rpydAzWdCy85HEmoQkH5PVxYtDYQWjmLxgHHadxondo"),
            ("VRTon",   "MkN2TZSYTFBdMRLf9EVcfhstTwnazH8knd9hpepondo"),
            ("VSTon",   "h6MW8GFpfzxFa1JNn6hZNnBF3t4fj9SHAXKy6LXondo"),
            ("VTIon",   "jCCU4GwukjNxAXJowG2S4KCrr5g6YyUB61WHYvGondo"),
            ("VTVon",   "KuiYLPVq65qixD9TgvxBC576C4gG6vVTCdbh2zFondo"),
            ("VZon",    "igu1coP6n3GPaWmbd8J9Z7UAyLpV254uQFFNfydondo"),
            ("WULFon",  "exYfSJt6Fgfhfnp3bAD4roYy97hLF9npjYaLyEXondo"),
            ("XOMon",   "qCYD74QnXzd9pzv6pGHQKJVwoibL6sNcPQDnpDiondo"),
            ("XYZon",   "BWxe2FVciUbwrCUZQPUKiREBh5LmVa5AiUqNLAkondo"),
            ("BTGOon",  "bgJWGuQxyoyFeXwzYZKBmoujVdatGFYPNFnv1a6ondo"),
            ("ALBon",   "B5KufqHkskgGYwMXtL8FSHgREAkMQvE3ykhH5Kmondo"),
            ("APLDon",  "B6WqvLGXdGqpw7qgxeb5EGiRZEYo2apWpQybjYuondo"),
            ("ASTSon",  "B6ry9goGNvVbhq7gWHzs3p6emJ1gLaMhu4By9TTondo"),
            ("BNOon",   "BAU83kqEqhyiexfAMQhZZE5KnGogSqh17fJc44Sondo"),
            ("CAPRon",  "BS8zoc6pmALQnBhBDFak6eFhgGHjpebnHzsxApgondo"),
            ("CIBRon",  "BVdL3WUxtxUD4vXRWwqChJLbGxvfzZjBGPp63Wtondo"),
            ("COHRon",  "BXMkru8ded26p71gJ3AMMwJmwZaYYfQjRo8vbZzondo"),
            ("CRWVon",  "BfPGpgNyxe6rjAru1EJarjSBAcCABuMF5L32v7nondo"),
            ("ECHon",   "BmXVAFyfpW7VuVYeWDtbFtLx7sek2mZt3BEsGgAondo"),
            ("ENLVon",  "BncvtBGs4JqgYZwUoq3EN9q9HUFqJKTfWpvCsHCondo"),
            ("ENPHon",  "Bp26APthMuM46gMFTo5KYpo7b92GN2xSCor7f9oondo"),
            ("ETHAon",  "LitNUakTges74cjDJm6HHfFNKGPdySkp3MWSYzYondo"),
            ("ETNon",   "BpYiU1dBXU1fdB64jbR93wHEw3Y47QeRLZvUyLQondo"),
            ("EWJon",   "C6c7VcxuUYcV5YTsky5HM4PUmfwHTwsDD5DNwwPondo"),
            ("EWYon",   "C8pSaSgjkiTWixS3GM6Hxd6HKnKrgAbY9WDgfVeondo"),
            ("EWZon",   "CBKcmEvVg5EgE3W5hVSPcBYWh6TFVjQwbmYod9Pondo"),
            ("EXODon",  "CJRoTbu98waCCuLFfLuJ2kXawLk889fqW4UAAbwondo"),
            ("FCXon",   "CY8ttw5rYCT6fFBJwqXofefqa7Ji9E8zfLmhRLmondo"),
            ("FFOGon",  "CYAwMGyuNSDu7NpuccNwcxMNS5Bu9akxU2Jooyiondo"),
            ("FGDLon",  "CYqLHM92EhmF83iNgfN4A1j2ckjsHigRvXu7xHCondo"),
            ("FLHYon",  "CZ3FxxSto7tsjkSkqMek1C5p3RCFFmkwKqW57nbondo"),
            ("FLQLon",  "CZ9GBn1okotqKNUUqoxk4PF2JVi59bw5GWvVo6Dondo"),
            ("FSOLon",  "BJhPr9SM7uZTZXHeSLYmUk7CjGQq1esFkVxPF5tondo"),
            ("FXIon",   "CeFbGYXDmkyfo1TXXzzZ512mtnCCewNohu6V15vondo"),
            ("GEVon",   "CgZSv89BL58ybWfWobANKEU8nV9jYfFw23G2DZEondo"),
            ("GLTRon",  "CgnZbDNzBfaLyJqUtd4esKLShRp7RznQuwP4uQaondo"),
            ("GLXYon",  "CkWmEM2J79k6AjAwyQVHXteFucAL1zQrKLxLqJHondo"),
            ("HYSon",   "CsN1Tyz467bSFLPGd6MJyZhPNtwDaWZtX8ixHWyondo"),
            ("IBITon",  "6JLG8iUkAuqiBhL3j2ckDMDf5oWAa6awmyaWezKondo"),
            ("IEFon",   "D4uWxzR5StYC6sTRhVts8Eboy3pmVtHeNC62dnQondo"),
            ("INCEon",  "D8KT4Jd8qiKKTfkM8ejSKCpWGR1o3GFvnQGp5ERondo"),
            ("INDAon",  "DBNwt3FoYCKQWdfzxKFNZ4mzuz4Jz1iRzFf7HFzondo"),
            ("IONQon",  "DDZQijTbaSd3Kas1r1bgCnHPayk8vTP8SfZWp5Tondo"),
            ("ITAon",   "DDcAL93Urf7KrPntvKULnZoFs4Wdee1LkkJqLpjondo"),
            ("KWEBon",  "DVPSYdqWPLvNa8afnEqa3B9eDfTTWpGyUZeXvdMondo"),
            ("LUNRon",  "DiDWPZ7vQXfpaeQ8BX68XuDYeiQLv7diDxdeUpaondo"),
            ("NBISon",  "DiRshqNDE68bWbGdLHm1GwQ76MvWQG3af6w1NdQondo"),
            ("NEMon",   "Dig28Tf1ufhCBAsjTmFkXCgcNgMqDMYj5A2rDQmondo"),
            ("NOCon",   "Dm6FpQ76SsbVmAZ4NvD2mjZP7cxbw1CASr4WwCiondo"),
            ("OIHon",   "DnvbCqRuUYssmKVRBRNwkUnptHitH4ZZTt1KVuZondo"),
            ("PAVEon",  "DsLQ18ooPjiHYuiuQ5Jz8PNCpVaKe3FhAYpvMxWondo"),
            ("PPLTon",  "DwRtkbsaQMGAS3oMeEGYh6M5vH4X9WECsQgqHjAondo"),
            ("QUBTon",  "E4YowrHx5wm4RtSjfuvTqtNH3Wf7NEj5tYZGD9Bondo"),
            ("RDWon",   "E6KSaqjvqe2HiUpbEweRxLK4RimQddigm95H9Jaondo"),
            ("REGNon",  "E86mX2yb3HLbJM6gRtZQ6dCYmLh6MSDZadu9SCPondo"),
            ("RKLBon",  "E9VQY3VnrpVSekFByzRmfeK1kxgM3UiKCoVVbdUondo"),
            ("SCCOon",  "EANjzFjj3nPXHdzN5CE3Z8LLVn69Ce77FE8X4cvondo"),
            ("SEDGon",  "EAwP9LGNjTkQ2YeKE6CGKqBYtrJ6APFvRe7KCMmondo"),
            ("SHYon",   "EEy57xbaLcUrN1HXj2vz8VWxeWFK1eZQZo4aWbrondo"),
            ("SNDKon",  "EJmUVvDqAdfH5zEohkdS4234bi3c6iunqEMobjmondo"),
            ("SOXXon",  "EN5pHc1LccUSojxb7kkyQi7v7iJN5RpDq6qz3DHondo"),
            ("STXon",   "EXtprP1wzrNo2bByrU9JyzqEg2hQMSCVJakeHHYondo"),
            ("UECon",   "EYo8D3cLdF1CDeGms5M5VHyU52HJYinkMZ1cqvYondo"),
            ("UNGon",   "Es2ipHL7qXBcLmZ4N7LP9PHBHaWaTMTAkxDwGGjondo"),
            ("UNPon",   "EvsME8gdnEwPLbTnhrGVDwrY35zBuB8hEGCq59Hondo"),
            ("URAon",   "EvzskrQ3vUUkiMGG1DzfSDyG6H2WCMy3v9G8fzzondo"),
            ("VFSon",   "F3V1fKLKv7H8aNdt9TC6GQ3X4LayEfGHsPi8Umaondo"),
            ("VNQon",   "F3dMJ9H137YUNc9cpN3gBWDSq4MSRbTFtojH65Uondo"),
            ("VRTXon",  "FL7QzUq58pvkDxkftJm7RqRWgqYEFZwXuvAMsUnondo"),
            ("WDCon",   "FLqH2jB2DZPJP5nnVFAakRKaNTcDZtq71Pnpp6Aondo"),
            ("WMon",    "FPvKvWzSzDZqgYmSZUetrkpUXSwo2VtpR4BynVYondo"),
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
    stock_pullers = [
        _make_stock_group_puller(pname, label, tokens)(settings, db)
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
        _make_stock_group_puller(pname, label, tokens,
                                 group="stablecoins",
                                 market_cap_source="birdeye_overview",
                                 defillama_tokens=_STABLECOIN_DEFILLAMA,
                                 hidden_tokens=_HIDDEN_STABLECOINS)(settings, db)
        for pname, label, tokens in _STABLECOIN_GROUPS
    ]
    treasury_pullers = [
        _make_stock_group_puller(pname, label, tokens,
                                 group="treasuries",
                                 defillama_tokens=_TREASURY_DEFILLAMA,
                                 skip_volume=True)(settings, db)
        for pname, label, tokens in _TREASURY_GROUPS
    ]
    return [*stock_pullers, *commodity_pullers,
            *stablecoin_pullers, *treasury_pullers]


# ── Tokenized commodity group registry — gold-backed tokens (OHLCV V3 volume) ──
_TOKENIZED_COMMODITY_GROUPS: list[tuple[str, str, list]] = [
    (
        "commodities_group",
        "Tokenized Commodities",
        [
            # ── Solana-native (Birdeye OHLCV volume + Birdeye overview MC) ────
            ("XAUM",  "5aLhp9VnUEKcsdtkfsf2DUgpJfomx7GmYVny24dHUZoB"),
            ("GOLD",  "GoLDppdjB1vDTPSGxyMJFqdnj134yH6Prg9eqsGDiw6A"),
            ("VNXAU", "9TPL8droGJ7jThsq4momaoz6uhTcvX2SeMqipoPmNa8R"),
            # ── Ethereum-native gold tokens (DefiLlama-only; Birdeye will
            #    skip them since x-chain=solana). Addresses kept for
            #    reference / future per-chain Birdeye calls. ─────────────────
            ("PAXG",  "0x45804880De22913dAFE09f4980848ECE6EcbAf78"),
            ("XAUT",  "0x68749665FF8D2d112Fa859AA293F07A622782F38"),
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
            ("USDC",  "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"),
            ("USDT",  "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"),
            ("CASH",  "CASHx9KJUStyftLFWGvEVf59SGeG9sh5FfcnZMVPCASH"),
            ("USDG",  "2u1tszSeqZ3qBWF3uNGPFc8TzMk2tdiwknnRMWGWjGWH"),
            ("USD1",   "USD1ttGY1N17NEEHLmELoaybftRBUSErhqYiQzvEmuB"),
            ("PYUSD",  "2b1kV6DkPAnxd5ixfnxCpjxmKwqjjaYmCZfHsFu24GXo"),
            ("USDe",   "DEkqHyPN7GMRJ5cArtQFAWefqbZb33Hyf6s5iCwjEonT"),
            ("JupUSD", "JuprjznTrTSp2UFa3ZBUFgwdAmtZCq4MQCwysN55USD"),
        ],
    ),
]

# Tokens still pulled/cached but hidden from the charts for now (display only).
# Empty this set to bring them back.
_HIDDEN_STABLECOINS = {"PYUSD", "USDC", "USDG", "USD1", "USDe"}

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
    # GOLD / VNXAU: no clean DefiLlama mapping yet — Solana-only via Birdeye.
}


# ── Tokenized treasuries & MMFs registry (market cap only, no volume) ─────────
_TREASURY_GROUPS: list[tuple[str, str, list]] = [
    (
        "treasuries_group",
        "Treasuries & MMFs",
        [
            # ── Solana-native treasury / MMF tokens ─────────────────────────
            ("BUIDL",    "GyWgeqpy5GueU2YbkE8xqUeVEokCMMCEeUrfbtMw6phr"),
            ("ULTRA",    "9DRPPWYud8i6CaSsDsFESs1xyVr8dBCMtjPZji2xiZEa"),
            ("VBILL",    "34mJztT9am2jybSukvjNqRjgJBZqHJsHnivArx1P4xy1"),
            ("USYC",     "7LWanZteUKtvFjv4MHYgKXXdAuCQYFPJysL9pxxdRQGn"),
            ("USTB",     "CCz3SGVziFeLYk2xfEstkiqJfYkjaSWb2GCABYsVcjo2"),
            ("CASHx",    "5d3zUSzje2saHwgzwJwFE8SDR8S5sGpE9wHhXdsCfu7j"),
            ("TBILL",    "4MmJVdwYN8LwvbGeCowYjSx7KoEi6BJWg8XXnW4fDDp6"),
            ("BENJI",    "5Tu84fKBpe9vfXeotjvfvWdWbAjy3hqsExvuHgFqFxA1"),
            ("nTBILL",   "2sA2jW9e8EYJkLFpq9hkhxfVUQBwVGJwq6iP4TmTKrL4"),
            ("CMBMINT",  "4uuqdpVPE9JdPyTRkAppQLB3x4QNmTjCZqdhAkwPmoMY"),
            ("deJTRSY",  "DeJXZwShCZYJnRX2ruVASfhUhsC44qPW1pacbxRFuGLR"),
            ("WTGXX",    "Em46fxxwgY2RRoUbBMSbEjJwY62x3ESMNdhnsGpEKewm"),
            ("FLTTX",    "5Qjgvd1mKaishqbrnz2tPsZFnMWpjpLZdqdPoVdTY4Vi"),
            ("TIPSX",    "B3Lc8KhBHVK3fKzh92xvsqvzJPr3wc5rMENmexAcsiDf"),
            ("WTLGX",    "51fSuDgEYgGiRBfTykMudLBQeJcwR3hqncyPFzXQ85R1"),
            ("WTSTX",    "A46zj57APuTZyBkNh2jhNs2GAzz5LcybA97zAwpP7Uck"),
            ("WTTSX",    "DpkuH46BBV4KhFvsBk8dLXuMbwrhijpBtyx3DqMmJCY3"),
            ("WTSYX",    "7aXJS2mgKzj2fCqZGx2TbXD3nxVXexxuK3BTyCq6BN4H"),
            ("OUSG",     "i7u4r16TcsJTgq1kAG8opmVZyVnAKBwLKu6ZPMwzxNc"),
            ("USDY",     "A1KLoBrKBde8Ty9qtNQUtq3C2ortoC3u7twggz7sEto6"),
            ("USDM1",    "BNgsQdjfWmjoy3cw8T3VXWswHfgCzEMyQzUno8gmzmRC"),
            ("USTRY",    "USTRYnGgcHAhdWsanv8BG6vHGd4p7UGgoB9NRd8ei7j"),
            # ── Ethereum-native treasury / MMF tokens ───────────────────────
            # Duplicated symbol names (BUIDL/USDY/USYC/etc) are deduplicated
            # by name at render time so the chart legend stays clean. Each
            # entry still triggers a Birdeye Ethereum fetch (x-chain inferred
            # from 0x prefix) so we get per-token snapshots + volume.
            ("USYC",     "0x136471a34f6ef19fe571effc1ca711fdb8e49f2b"),
            ("BUIDL",    "0x6a9da2d710bb9b700acde7cb81f10f1ff8c89041"),
            ("USDY",     "0x96f6ef951840721adbf46ac996b59e0235cb985c"),
            ("iBENJI",   "0x90276e9d4a023b5229e0c2e9d4b2a83fe3a2b48c"),
            ("WTGXX",    "0x1fecf3d9d4fee7f2c02917a66028a48c6706c179"),
            ("JTRSY",    "0x8c213ee79581ff4984583c6a801e5263418c4b86"),
            ("BENJI",    "0x3ddc84940ab509c11b20b76b466933f40b750dc9"),
            ("USTB",     "0x43415eb6ff9db7e26a15b704e7a3edce97d31c4e"),
            ("OUSG",     "0x1b19c19393e2d034d8ff31ff34c81252fcbbee92"),
            ("CUMIU",    "0x85d38585c3ac08268f598282a84b7c0ddfc0d04f"),
            ("USTBL",    "0xe4880249745eac5f1ed9d8f7df844792d560e750"),
            ("FDIT",     "0x48ab4e39ac59f4e88974804b04a991b3a402717f"),
            ("ULTRA",    "0x50293dd8889b931eb3441d2664dce8396640b419"),
            ("THBILL",   "0x5fa487bca6158c64046b2813623e20755091da0b"),
            ("BELIF",    "0x237c717df1b60501f8d029d3fe7385fd090df180"),
            ("MONY",     "0x6a7c6aa2b8b8a6a891de552bdeffa87c3f53bd46"),
            ("TBILL",    "0xdd50c053c096cb04a3e3362e2b622529ec5f2e8a"),
            ("VBILL",    "0x2255718832bc9fd3be1caf75084f4803da14ff01"),
            ("MTBILL",   "0xdd629e5241cbc5919847783e6c96b2de4754e438"),
            ("CASHx",    "0x42975aae7a124257e7fda7f5e8382f51449b784a"),
            ("DCP",      "0xb5710a6fede27d1048c75b157bd3403ba08cdbe0"),
            ("FILQ",     "0x54a4fc78431f9201824643e99bec891bb7462a1d"),
            ("CUMBU",    "0x1aaa3339572cf88dc487dbeef263f5aabc5f3bbf"),
            ("UMINT",    "0xc06036793272219179f846ef6bfc3b16e820df0b"),
            ("CUMFU",    "0xdbf879f356c6b8c5f1edfdcb2950eda8b3ad25d9"),
            ("usfr.d",   "0xaEB0A5d56de94479cdA178977570FD9079500527"),
            ("deJTRSY",  "0xa6233014b9b7aaa74f38fa1977ffc7a89642dc72"),
            ("CMBMINT",  "0xc9a71c8fa0f505e690cbab1012d4a4a518e03231"),
            ("USDM1",    "0x90a1717e0dabe37693f79afe43ae236dc3b65957"),
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
# Version key: bump whenever the puller list or class hierarchy changes so that
# stale session-state instances (from before a code reload) are discarded.
_PULLERS_VERSION = "stocks-commodities-stables-treasuries-multichain-v14-xstocks-evm"

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
    _sched = PullScheduler(settings)
    for _p in _pullers:
        _sched.register(_p)
    _sched.start()
    st.session_state["scheduler"]       = _sched
    st.session_state["pullers"]         = _pullers
    st.session_state["_pullers_version"] = _PULLERS_VERSION

scheduler: PullScheduler = st.session_state["scheduler"]
pullers: List[DataPuller] = st.session_state["pullers"]

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
st.markdown(
    f'<span class="peak-sub-anchor"></span>'
    f'<p class="peak-sub peak-sub-topbar">Refresh <b>{settings.ui_refresh_seconds}s</b> · '
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

_STOCKS_PROJECT_COLORS: dict[str, str] = {
    "PreStocks": "#d2b58f",  # tan/7
    "xStocks":   "#6F97D5",  # navy/6
    "Ondo":      "#6FD58F",  # green/6
}


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
            title_text="Volume (USD)", tickprefix="$", tickformat="~s",
            tickmode="array", tickvals=ticks, range=[0, ticks[-1]],
            showgrid=True,
        ),
    )
    return fig


# ── Tabs ──────────────────────────────────────────────────────────────────────
solana_pullers      = [p for p in pullers if getattr(p, "GROUP", "") == "solana_tokens"]
stocks_pullers      = [p for p in pullers if getattr(p, "GROUP", "") == "tokenized_stocks"]
commodity_pullers   = [p for p in pullers if getattr(p, "GROUP", "") == "tokenized_commodities"]
stablecoin_pullers  = [p for p in pullers if getattr(p, "GROUP", "") == "stablecoins"]
treasury_pullers    = [p for p in pullers if getattr(p, "GROUP", "") == "treasuries"]
usdc_pullers        = [p for p in pullers
                       if getattr(p, "GROUP", "") not in
                          ("solana_tokens", "tokenized_stocks", "stablecoins")]

# ── Raw-data modal ────────────────────────────────────────────────────────────
@st.dialog("📋 Raw Data", width="large")
def _raw_data_modal(df: pd.DataFrame, fmt: dict) -> None:
    st.dataframe(df.style.format(fmt), use_container_width=True)


# Full-screen dialog — opens when any "⛶" button is clicked
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

    def _render_chain_group(group_label: str, group_pullers: list,
                            show_volume: bool = False) -> None:
        """Render every puller in a group, filtered to the active chain.
        If show_volume=True, also render a per-chain Birdeye volume chart
        above the market-cap chart."""
        if not group_pullers:
            st.info(
                f"No {group_label.lower()} tracked on {scope_label} yet. "
                "Drop tokens into the group registry to populate this tab.")
            return
        any_data = False
        for p in group_pullers:
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
        _render_chain_group("Stablecoins", stablecoin_pullers)
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
                    st.plotly_chart(
                        _build_combined_stocks_fig(combined_df, labels, "D", 380),
                        use_container_width=True,
                    )
                with ctab_w:
                    st.plotly_chart(
                        _build_combined_stocks_fig(combined_df, labels, "W", 380),
                        use_container_width=True,
                    )
                with ctab_m:
                    st.plotly_chart(
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
                    p.render()
            st.divider()

with tab_commodities:
    if not commodity_pullers:
        st.info("No tokenized commodity pullers registered.")
    else:
        for p in commodity_pullers:
            st.subheader(f"{p.GROUP_LABEL} — Trading Volume (Solana)")
            # Restrict to Solana-native tokens so the Ethereum-only PAXG /
            # XAUT entries (which carry Birdeye Ethereum volume, not Solana)
            # don't pollute this stack.
            p.render_volume_chain(chain="solana")

            st.subheader(f"{p.GROUP_LABEL} — Market Cap by Token")
            st.caption(
                "Solana-only market cap per token, snapshotted each pull from "
                "Birdeye Token Overview and cached over time — the history builds "
                "up from when tracking began."
            )
            p.render_market_cap()

with tab_stablecoins:
    if not stablecoin_pullers:
        st.info("No stablecoin pullers registered.")
    else:
        for p in stablecoin_pullers:
            st.subheader(f"{p.GROUP_LABEL} — Market Cap")
            st.caption(
                "Solana-only market cap per token (Birdeye Token Overview), "
                "stacked. Snapshotted each pull and cached over time — the "
                "history builds up from when tracking began."
            )
            p.render_market_cap(stacked=True)

            st.subheader(f"{p.GROUP_LABEL} — Daily Trading Volume")
            st.caption("Daily on-chain volume per token, stacked "
                       "(Birdeye OHLCV V3, v_usd).")
            p.render()

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

