#!/usr/bin/env python3
"""Headless pull entry point for the GitHub Actions cron.

Mirrors what `PULL_ONLY=1 python stocks_dashboard.py` does locally, but lives
in its own file so the workflow doesn't have to know about Streamlit-internal
side effects of importing the app module.

Usage:
    DATABASE_URL=... BIRDEYE_API_KEY=... python scripts/run_pull.py \\
        --groups tokenized_commodities,stablecoins,treasuries

Each puller is invoked once; results are written to whatever backend CacheDB
selects (Postgres when DATABASE_URL is set, else local SQLite).
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

# Make the project root importable regardless of where the cron CWDs to.
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

# Set PULL_ONLY before importing the app — the module-level guard at the
# bottom of stocks_dashboard.py reads this and short-circuits Streamlit setup.
os.environ.setdefault("PULL_ONLY", "1")


def main() -> int:
    parser = argparse.ArgumentParser(description="Run RWA dashboard pullers headlessly.")
    parser.add_argument(
        "--groups",
        default=os.getenv(
            "PULL_GROUP",
            "tokenized_stocks,tokenized_commodities,stablecoins,treasuries",
        ),
        help="Comma-separated list of GROUP names to pull (default: all).",
    )
    args = parser.parse_args()
    os.environ["PULL_GROUP"] = args.groups

    # Importing the module triggers the PULL_ONLY branch which iterates every
    # registered puller and exits. We catch SystemExit so the script returns 0
    # on success rather than propagating the exit.
    try:
        import stocks_dashboard  # noqa: F401  — side effect is the pull loop.
    except SystemExit as exc:
        return int(exc.code or 0)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
