"""Disk-snapshot cache helpers shared across pullers.

The pattern: every fetcher tries the live API first; on success it
writes its DataFrame (or raw JSON payload) to disk; on failure it
falls back to the most recent disk snapshot. The Streamlit dashboards
keep rendering even when an upstream API is down or rate-limited.

Same intent as the existing seed dirs (`allium_seeds/`, `rwa_seeds/`,
`blockworks_seeds/`, `solana_seeds/dex/`, `mc_seed_*.json`) — just
factored into one module so every new puller doesn't reinvent the I/O
plumbing.

Paths are resolved relative to this module so the helpers work both
under `streamlit run` (CWD = repo root) and from any cron script that
imports them with a different CWD.

Layout convention:
  <repo>/<seeds_dir>/<filename>
where `seeds_dir` is per-source (e.g. "paymentscan_seeds",
"defillama_seeds") and `filename` is per-query.

DataFrames go to CSV (cheap to diff in git, human-readable). Nested /
non-tabular payloads (DefiLlama's `chainTvls` dict, stablecoin
chainBalances) go to JSON. JSON is dumped sorted-keys + indent=2 so
git diffs of refreshed snapshots stay legible.
"""
from __future__ import annotations

import json
import logging
import os
from typing import Optional

import pandas as pd


log = logging.getLogger(__name__)

# Repo root resolved from THIS file's location — works regardless of CWD.
_REPO_ROOT = os.path.dirname(os.path.abspath(__file__))


def _seed_path(seeds_dir: str, filename: str) -> str:
    """Build the absolute path to a seed file. Creates the parent dir
    on demand so the first write doesn't fail."""
    full_dir = os.path.join(_REPO_ROOT, seeds_dir)
    os.makedirs(full_dir, exist_ok=True)
    return os.path.join(full_dir, filename)


def write_seed_df(df: pd.DataFrame, seeds_dir: str, filename: str) -> bool:
    """Persist a DataFrame to <seeds_dir>/<filename>. Returns True on
    success. No-ops (and returns False) for empty frames — we don't
    want to overwrite a good prior snapshot with an empty live result.
    Date columns are serialized as ISO YYYY-MM-DD strings via pandas'
    default CSV behavior.

    Failure to write is logged but never raised — caching is best-
    effort. The live API success that triggered the write already
    succeeded, so the user still sees data this run."""
    if df is None or df.empty:
        return False
    path = _seed_path(seeds_dir, filename)
    try:
        df.to_csv(path, index=False)
        log.info("seed write: %s/%s (%d rows)",
                  seeds_dir, filename, len(df))
        return True
    except Exception as exc:
        log.warning("seed write failed %s/%s: %s",
                     seeds_dir, filename, exc)
        return False


def read_seed_df(seeds_dir: str, filename: str,
                 parse_dates: Optional[list[str]] = None) -> pd.DataFrame:
    """Load a previously-written DataFrame back. Returns an empty
    DataFrame if the file is missing or unreadable — caller handles
    the empty case (typically by surfacing an st.info).

    `parse_dates` is forwarded to pd.read_csv when provided so date-
    typed columns come back as datetime64 instead of strings. Most
    chart builders downstream expect this."""
    path = _seed_path(seeds_dir, filename)
    if not os.path.exists(path):
        return pd.DataFrame()
    try:
        kwargs: dict = {}
        if parse_dates:
            kwargs["parse_dates"] = parse_dates
        df = pd.read_csv(path, **kwargs)
        log.info("seed read: %s/%s (%d rows)",
                  seeds_dir, filename, len(df))
        return df
    except Exception as exc:
        log.warning("seed read failed %s/%s: %s",
                     seeds_dir, filename, exc)
        return pd.DataFrame()


def write_seed_json(payload, seeds_dir: str, filename: str,
                    pretty: bool = False) -> bool:
    """Persist a JSON-serializable payload to <seeds_dir>/<filename>.
    Skips empty payloads (empty dict / empty list / None) — same
    guard as write_seed_df, prevents overwriting good prior snapshots
    with degraded live results.

    Compact-encoded by default (no whitespace) — saves ~30–40% disk
    vs pretty-print for the big DefiLlama payloads. Pass pretty=True
    for small config-shape payloads where git-diff readability beats
    bytes-on-disk."""
    if not payload:
        return False
    path = _seed_path(seeds_dir, filename)
    try:
        with open(path, "w") as f:
            if pretty:
                json.dump(payload, f, indent=2, sort_keys=True,
                          default=str)
            else:
                # Compact: drop spaces, drop the default-2-space sep.
                json.dump(payload, f, separators=(",", ":"),
                          sort_keys=True, default=str)
        # Size used for a quick sanity-check in the log line.
        size = os.path.getsize(path)
        log.info("seed write: %s/%s (%d bytes)",
                  seeds_dir, filename, size)
        return True
    except Exception as exc:
        log.warning("seed write failed %s/%s: %s",
                     seeds_dir, filename, exc)
        return False


def read_seed_json(seeds_dir: str, filename: str):
    """Load a previously-written JSON payload. Returns None if missing
    or unreadable — caller decides what an empty-fallback means
    (typically: return empty dict/list to match the API's shape)."""
    path = _seed_path(seeds_dir, filename)
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r") as f:
            payload = json.load(f)
        log.info("seed read: %s/%s",
                  seeds_dir, filename)
        return payload
    except Exception as exc:
        log.warning("seed read failed %s/%s: %s",
                     seeds_dir, filename, exc)
        return None
