"""Enable Row-Level Security on the cache `pulls` table.

WHY: Supabase exposes every public-schema table via PostgREST (anon +
authenticated roles) by default. With RLS disabled, anyone with the
project URL can read / insert / update / delete every row via the
auto-generated REST endpoints — this is the "rls_disabled_in_public"
warning Supabase surfaces. Our cache holds upstream API snapshots
keyed by puller name + timestamp; we don't want a random scraper
inserting bogus payloads or wiping our cached pulls.

WHY THIS IS SAFE FOR THE DASHBOARD:
- The dashboard + the GitHub-Actions cron pull connect via psycopg
  using the `postgres.<project_ref>` superuser role on the pooler
  (port 6543). Supabase's `postgres` role has the BYPASSRLS attribute
  by default, so RLS policies don't apply to its queries. Reads /
  writes from our app continue exactly as before.
- RLS only affects roles WITHOUT bypass — specifically the `anon`
  and `authenticated` roles that PostgREST hands out. After enabling
  RLS with no policies, those two roles see zero rows and can't write
  anything. That's the intended outcome.

WHAT WE DO:
  1. ALTER TABLE public.pulls ENABLE ROW LEVEL SECURITY
  2. REVOKE ALL ON public.pulls FROM anon, authenticated
     (defense-in-depth — RLS-with-no-policy already denies, but
     explicit revoke makes the lockdown obvious in the catalog)

Run with: `python3 scripts/enable_rls_on_pulls.py`
Requires DATABASE_URL in env or .env.

Idempotent — safe to re-run.
"""
from __future__ import annotations

import os
import sys

import psycopg
from dotenv import load_dotenv


def main() -> int:
    load_dotenv()
    url = os.getenv("DATABASE_URL", "").strip()
    if not url:
        print("ERROR: DATABASE_URL not set (check .env)", file=sys.stderr)
        return 1

    with psycopg.connect(url, autocommit=True) as c:
        with c.cursor() as cur:
            # 1. Confirm the table exists
            cur.execute("""
                SELECT EXISTS (
                    SELECT 1 FROM pg_tables
                    WHERE schemaname = 'public' AND tablename = 'pulls'
                )
            """)
            if not cur.fetchone()[0]:
                print("ERROR: public.pulls not found — nothing to lock down",
                      file=sys.stderr)
                return 1

            # 2. Pre-state report
            cur.execute("""
                SELECT relrowsecurity
                FROM pg_class
                WHERE oid = 'public.pulls'::regclass
            """)
            rls_before = cur.fetchone()[0]
            print(f"Pre-state: RLS on public.pulls = {rls_before}")

            # 3. Enable RLS
            cur.execute("ALTER TABLE public.pulls ENABLE ROW LEVEL SECURITY")
            print("✓ Enabled RLS on public.pulls")

            # 4. Belt-and-suspenders: revoke from anon/authenticated.
            # RLS-with-no-policy already denies for non-bypass roles, but
            # an explicit REVOKE puts the lockdown in pg_class.relacl
            # where it's easy to audit.
            cur.execute(
                "REVOKE ALL ON public.pulls FROM anon, authenticated")
            print("✓ Revoked all grants on public.pulls from anon + "
                  "authenticated")

            # 5. Post-state confirmation
            cur.execute("""
                SELECT relrowsecurity
                FROM pg_class
                WHERE oid = 'public.pulls'::regclass
            """)
            rls_after = cur.fetchone()[0]
            print(f"Post-state: RLS on public.pulls = {rls_after}")

            # 6. Smoke-test: prove the dashboard role can still read.
            # If this errors, RLS broke us — revert by running:
            #   ALTER TABLE public.pulls DISABLE ROW LEVEL SECURITY;
            cur.execute("SELECT count(*) FROM public.pulls")
            n = cur.fetchone()[0]
            print(f"✓ Smoke-test: dashboard role read {n} rows from "
                  "public.pulls (RLS doesn't block superuser)")

    print("\nDone. The Supabase 'rls_disabled_in_public' warning should "
          "clear on the next scan.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
