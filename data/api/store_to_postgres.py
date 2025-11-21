#!/usr/bin/env python3
import os
import sys
import argparse
from datetime import datetime

import psycopg2
from psycopg2.extras import execute_values
from psycopg2 import OperationalError as PsycopgOperationalError
import socket
from dotenv import load_dotenv

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from scripts.fetch_eia import fetch_period

load_dotenv()

AZ_BAS = {"AZPS", "SRP", "TEPC"}


def connect_db():
    dbname = os.getenv("PG_DB")
    user = os.getenv("PG_USER")
    password = os.getenv("PG_PASSWORD")
    host = os.getenv("PG_HOST")
    port = os.getenv("PG_PORT", 5432)
    sslmode = os.getenv("PG_SSLMODE", "require")

    if not host:
        raise RuntimeError("PG_HOST environment variable is not set. Check your .env file or environment.")

    host = host.strip()
    if (host.startswith('"') and host.endswith('"')) or (host.startswith("'") and host.endswith("'")):
        host = host[1:-1]

    if "://" in host:
        dsn = host
        try:
            return psycopg2.connect(dsn)
        except PsycopgOperationalError as e:
            print("Failed to connect using DSN provided in PG_HOST (treated as full connection URL).")
            print("psycopg2 OperationalError:", e)
            raise

    try:
        socket.gethostbyname(host)
    except socket.gaierror:
        print(f"DNS error: cannot resolve PG_HOST {repr(host)}.")
        print("Things to check: is the host name correct? Is your network / VPN / DNS configured?")
        print("Try: `nslookup <host>` or `dig <host> +short` from your shell.")
        raise

    try:
        return psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port,
            sslmode=sslmode,
        )
    except PsycopgOperationalError as e:
        masked_pwd = "***" if password else "(none)"
        print("Failed to connect to Postgres. Connection parameters:")
        print(f"  host={host}")
        print(f"  port={port}")
        print(f"  dbname={dbname}")
        print(f"  user={user}")
        print(f"  password={masked_pwd}")
        print(f"  sslmode={sslmode}")
        print("")
        print("psycopg2 OperationalError:", e)
        print("Common causes:")
        print(" - incorrect PG_HOST (typo or wrong project)")
        print(" - network/DNS/VPN blocking name resolution")
        print(" - firewall blocking outbound connections to the DB host/port")
        print(" - database is paused or not publicly accessible (Supabase project settings)")
        raise


def save_interchange(records):
    if not records:
        print("No records to insert.")
        return

    print(f"[store_to_postgres] Received {len(records)} records from fetch_eia")

    filtered = [
        r for r in records
        if r.get("fromba") in AZ_BAS or r.get("toba") in AZ_BAS
    ]

    print(
        f"[store_to_postgres] Filtered to {len(filtered)} Arizona-related records "
        f"(fromba/toba in {sorted(AZ_BAS)})"
    )

    if not filtered:
        print("No Arizona-related records to insert after filtering.")
        return

    conn = connect_db()

    def ensure_table_exists(conn):
        create_sql = """
        CREATE TABLE IF NOT EXISTS eia_interchange (
            period timestamp without time zone,
            fromba text,
            fromba_name text,
            toba text,
            toba_name text,
            value integer,
            value_units text
        )
        """
        c = conn.cursor()
        c.execute(create_sql)
        conn.commit()
        c.close()

    ensure_table_exists(conn)
    cur = conn.cursor()

    # Convert all rows
    rows = []
    for r in filtered:
        period = datetime.strptime(r["period"], "%Y-%m-%dT%H")
        rows.append(
            (
                period,
                r.get("fromba"),
                r.get("fromba-name"),
                r.get("toba"),
                r.get("toba-name"),
                int(r.get("value", 0)) if r.get("value") is not None else None,
                r.get("value-units"),
            )
        )

    total = len(rows)
    CHUNK = 5000
    inserted = 0

    print(f"[store_to_postgres] Beginning chunked insert of {total} rows...")

    query = """
        INSERT INTO eia_interchange
        (period, fromba, fromba_name, toba, toba_name, value, value_units)
        VALUES %s
    """

    for i in range(0, total, CHUNK):
        batch = rows[i:i+CHUNK]
        execute_values(cur, query, batch)
        conn.commit()

        inserted += len(batch)
        pct = (inserted / total) * 100

        last_period = batch[-1][0]  # period of last inserted row

        print(
            f"[store_to_postgres] Inserted {inserted:,} / {total:,} rows "
            f"({pct:.2f}%) — last period inserted: {last_period}"
        )
        sys.stdout.flush()

    cur.close()
    conn.close()
    print(f"[store_to_postgres] Finished inserting {total:,} rows into eia_interchange")


def main():
    parser = argparse.ArgumentParser(description="Fetch EIA data and store in Supabase Postgres.")
    parser.add_argument("--api-key", help="EIA API key or set EIA_API_KEY")
    parser.add_argument("--start-date", required=True, help="Start date YYYY-MM-DD")
    parser.add_argument("--days", type=int, default=7, help="Number of days to fetch (default 7)")
    parser.add_argument("--state", help="Optional state facet")
    parser.add_argument("--pretty", action="store_true", help="Pretty-print fetched JSON")
    parser.add_argument("--no-db", action="store_true", help="Do not insert into DB, only fetch/print")

    args = parser.parse_args()

    api_key = args.api_key or os.getenv("EIA_API_KEY")
    if not api_key:
        print("Error: Missing API key. Provide --api-key or set EIA_API_KEY in .env")
        sys.exit(1)

    print(
        f"[store_to_postgres] Starting fetch for start-date={args.start_date}, "
        f"days={args.days}, state={args.state}"
    )

    records = fetch_period(
        api_key=api_key,
        start_date=args.start_date,
        days=args.days,
        state=args.state,
        pretty=args.pretty,
    )

    if not records:
        print("[store_to_postgres] No records returned from fetch_period, exiting.")
        return

    if not args.no_db:
        save_interchange(records)
    else:
        print("[store_to_postgres] --no-db flag set, skipping DB insert.")


if __name__ == "__main__":
    main()
