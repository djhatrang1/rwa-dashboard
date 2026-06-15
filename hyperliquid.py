"""Hyperliquid public-API fetcher (RWA perp annotations only).

Scope: this module exists SOLELY to fetch the
`perpConciseAnnotations` payload — a per-token `{category}` map
covering every HIP-3 perp listed on Hyperliquid (xyz, flx, km, vntl,
para, …). Used by `birdeye_perps.categorize()` to assign each
`xyz:*` ticker to its asset class on the RWA OI chart.

We do NOT use Hyperliquid for OI / volume / price data — that comes
from Birdeye's `/perps/v1/token/list` proxy, which is already an
approved data source. This module exists only because Birdeye
doesn't expose the annotation/category field.

Endpoint:
  POST https://api.hyperliquid.xyz/info
  body: {"type": "perpConciseAnnotations"}
  response: [[token, {"category": str}], ...]
No auth required. ~10 KB response.

Disk-snapshot fallback (`hyperliquid_seeds/`) — same pattern as
sosovalue.py / paymentscan.py / defillama.py. The cron writes the
seed weekly via `scripts/refresh_hyperliquid_seed.py`; the dashboard
ALWAYS reads from disk (never from this fetcher at render time) so
Supabase egress is unaffected.
"""
from __future__ import annotations

import logging
from typing import Optional

import requests

import seed_cache


log = logging.getLogger(__name__)

_BASE = "https://api.hyperliquid.xyz/info"
_SEEDS_DIR = "hyperliquid_seeds"
_SEED_FN = "perp_annotations.json"


def fetch_perp_annotations() -> tuple[list, Optional[str]]:
    """Pull the full per-token annotation list from Hyperliquid.

    Returns (annotations_list, error_message). On success the list is
    the raw `[[token, {"category": str}], ...]` shape the API returns
    and err is None; on failure annotations is whatever the most-recent
    on-disk seed has (possibly empty) and err is a short string.

    Network call only — the on-disk seed is read by
    `load_seed()`, not by this function."""
    try:
        r = requests.post(
            _BASE,
            json={"type": "perpConciseAnnotations"},
            timeout=30,
        )
        r.raise_for_status()
        data = r.json()
        if not isinstance(data, list) or not data:
            raise RuntimeError(f"unexpected payload shape: {type(data).__name__}")
        log.info("Hyperliquid perpConciseAnnotations: %d entries", len(data))
        return data, None
    except Exception as exc:
        msg = f"{type(exc).__name__}: {exc}"
        log.warning("Hyperliquid perpConciseAnnotations failed: %s", msg)
        # Best-effort fallback so a caller running outside of cron
        # context (e.g. ad-hoc bootstrap) still gets something useful.
        seed = seed_cache.read_seed_json(_SEEDS_DIR, _SEED_FN) or {}
        return seed.get("annotations", []), msg


def load_seed() -> dict:
    """Read the on-disk annotation seed. Returns the parsed JSON dict
    (with `fetched_at`, `source`, `annotations` keys) or {} if missing.

    This is what `birdeye_perps.categorize()` reads at chart render
    time — no network call, no Postgres round-trip. The seed is
    populated/refreshed by `scripts/refresh_hyperliquid_seed.py`,
    which the weekly GitHub Actions cron runs."""
    return seed_cache.read_seed_json(_SEEDS_DIR, _SEED_FN) or {}


def write_seed(annotations: list) -> bool:
    """Persist annotations to the disk seed with a `fetched_at` stamp.
    Called by the refresh script. Returns True on success."""
    if not annotations:
        return False
    # Wrap in a metadata envelope so future readers can sanity-check
    # the source and freshness without re-parsing the array.
    # Using datetime.utcnow().isoformat() rather than a fixed-format
    # string so the seed roundtrips cleanly via json.loads.
    from datetime import datetime
    payload = {
        "fetched_at": datetime.utcnow().isoformat() + "Z",
        "source": "POST https://api.hyperliquid.xyz/info "
                  "type=perpConciseAnnotations",
        "annotations": annotations,
    }
    # Pretty-print: this file is small (~10 KB), human-edit-friendly,
    # and changes weekly — readable git diffs are worth the few extra
    # bytes vs the compact-encoded default we use for the big DefiLlama
    # blobs.
    return seed_cache.write_seed_json(
        payload, _SEEDS_DIR, _SEED_FN, pretty=True)
