#!/usr/bin/env python3
"""One-time migration: copy `pulls` rows from local SQLite into Postgres.

Run once after you've created the Supabase project and added the connection
string to your local `.env` as `DATABASE_URL`. Safe to run multiple times —
duplicate (puller, pulled_at) rows are skipped via an idempotency check.

Usage:
    DATABASE_URL="postgresql://…" python scripts/migrate_sqlite_to_postgres.py \\
        --sqlite crypto_data_stocks.db

Tips:
- The script streams in batches; memory usage stays modest even for 400 MB DBs.
- Each batch is wrapped in its own transaction so a network hiccup costs at
  most one batch (default 200 rows).
- After it completes, check Supabase → Table Editor → `pulls` for a sanity
  count by puller; then you can drop your local `.db` from the repo (it's
  already gitignored).
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

import psycopg
from psycopg.types.json import Json


def _parse_ts(raw: str) -> datetime:
    """SQLite stores isoformat strings; Postgres wants real datetimes (UTC)."""
    if not raw:
        return datetime.now(timezone.utc)
    try:
        # Accept "2026-06-01T12:34:56.789" or "2026-06-01T12:34:56"
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return datetime.now(timezone.utc)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def migrate(sqlite_path: Path, database_url: str, batch_size: int = 200,
            dry_run: bool = False) -> None:
    if not sqlite_path.exists():
        sys.exit(f"❌ SQLite file not found: {sqlite_path}")

    print(f"🔌 Connecting to SQLite at {sqlite_path}")
    src = sqlite3.connect(str(sqlite_path))
    src.row_factory = sqlite3.Row

    print(f"🔌 Connecting to Postgres ({database_url.split('@', 1)[-1]})")
    dst = psycopg.connect(database_url, autocommit=False)

    # Ensure target table exists with the same schema CacheDB._init creates.
    with dst.cursor() as cur:
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
        cur.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS uq_pulls_puller_time "
            "ON pulls(puller, pulled_at)"
        )
    dst.commit()

    # Count totals so we can report progress.
    total = src.execute("SELECT COUNT(*) FROM pulls").fetchone()[0]
    print(f"📊 SQLite has {total:,} rows to migrate")

    # Per-puller pre-migration counts in Postgres (resume-safe).
    existing: dict[str, int] = {}
    with dst.cursor() as cur:
        cur.execute("SELECT puller, COUNT(*) FROM pulls GROUP BY puller")
        existing = dict(cur.fetchall())
    if existing:
        print("ℹ️  Postgres already has rows; running idempotent (rows with "
              "matching puller+pulled_at are skipped):")
        for p, n in sorted(existing.items()):
            print(f"     {p:35} {n:,}")

    if dry_run:
        print("\n🔎 dry-run: no inserts will be performed.")
        return

    moved = skipped = 0
    cur_src = src.execute(
        "SELECT puller, pulled_at, payload, status FROM pulls "
        "ORDER BY puller, pulled_at"
    )

    batch: list[tuple] = []

    def _flush() -> tuple[int, int]:
        if not batch:
            return 0, 0
        with dst.cursor() as cur:
            cur.executemany(
                "INSERT INTO pulls (puller, pulled_at, payload, status) "
                "VALUES (%s, %s, %s, %s) "
                "ON CONFLICT (puller, pulled_at) DO NOTHING",
                batch,
            )
            inserted = cur.rowcount
        dst.commit()
        return inserted, len(batch) - max(inserted, 0)

    for i, row in enumerate(cur_src, start=1):
        try:
            payload_obj = json.loads(row["payload"]) if row["payload"] else []
        except json.JSONDecodeError:
            print(f"⚠️  Skipping malformed payload at row {i} ({row['puller']})")
            continue
        batch.append((
            row["puller"],
            _parse_ts(row["pulled_at"]),
            Json(payload_obj),
            row["status"] or "ok",
        ))
        if len(batch) >= batch_size:
            ins, sk = _flush()
            moved += ins
            skipped += sk
            batch.clear()
            print(f"   … {i:>7,}/{total:,}  moved={moved:,}  skipped={skipped:,}")

    if batch:
        ins, sk = _flush()
        moved += ins
        skipped += sk
        batch.clear()

    src.close()
    dst.close()
    print(f"\n✅ Done. Inserted {moved:,} new rows; skipped {skipped:,} duplicates.")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sqlite", default="crypto_data_stocks.db",
                        help="Path to the local SQLite file to migrate.")
    parser.add_argument("--batch-size", type=int, default=200,
                        help="Rows per Postgres transaction (default 200).")
    parser.add_argument("--dry-run", action="store_true",
                        help="Connect + report totals but do not insert.")
    args = parser.parse_args()

    database_url = os.environ.get("DATABASE_URL", "").strip()
    if not database_url:
        sys.exit("❌ DATABASE_URL is not set. Add it to your .env first.")

    migrate(Path(args.sqlite), database_url, args.batch_size, args.dry_run)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
