#!/usr/bin/env python3
import os
import sys
import argparse
from datetime import datetime, timedelta

import requests
from psycopg2.extras import execute_values
from dotenv import load_dotenv

# Reuse DB connection from your existing script
# Ensure repo root is on sys.path so `data` package imports work when running scripts
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from data.api.store_to_postgres import connect_db

load_dotenv()

PRICE_URL = "https://api.eia.gov/v2/electricity/retail-sales/data/"
PAGE_SIZE = 5000


def month_range(start: datetime, days: int):
    """
    Convert (start_date, days) into a list of YYYY-MM months.
    """
    end = start + timedelta(days=days - 1)
    months = set()

    curr = datetime(start.year, start.month, 1)
    last = datetime(end.year, end.month, 1)

    while curr <= last:
        months.add(curr.strftime("%Y-%m"))
        # move to next month
        if curr.month == 12:
            curr = datetime(curr.year + 1, 1, 1)
        else:
            curr = datetime(curr.year, curr.month + 1, 1)
    return sorted(months)


def fetch_az_prices(api_key: str) -> list[dict]:
    """
    Fetch ALL AZ price data (only monthly available).
    """
    all_records: list[dict] = []
    offset = 0
    page = 1

    while True:
        print(f"[fetch_prices] Page {page} offset={offset}")

        params = [
            ("api_key", api_key),
            ("frequency", "monthly"),
            ("data[0]", "price"),
            ("facets[stateid][]", "AZ"),
            ("facets[sectorid][]", "ALL"),
            ("offset", offset),
            ("length", PAGE_SIZE),
        ]

        resp = requests.get(PRICE_URL, params=params, timeout=20)
        resp.raise_for_status()
        data = resp.json()

        records = (
            data.get("response", {}).get("data")
            or data.get("data")
            or data.get("results")
        )

        if not records:
            break

        all_records.extend(records)

        if len(records) < PAGE_SIZE:
            break

        offset += PAGE_SIZE
        page += 1

    print(f"[fetch_prices] Total rows fetched: {len(all_records)}")
    return all_records


def save_prices(records: list[dict]):
    conn = connect_db()
    cur = conn.cursor()

    create_sql = """
        CREATE TABLE IF NOT EXISTS eia_az_price (
            period_month date,
            stateid text,
            sectorid text,
            price_cents_per_kwh numeric,
            price_per_mwh numeric,
            unit text
        )
    """
    cur.execute(create_sql)
    conn.commit()

    rows = []
    for r in records:
        period_month_str = r["period"] + "-01"
        period_month = datetime.strptime(period_month_str, "%Y-%m-%d").date()

        price_cents = float(r["price"])
        price_per_mwh = price_cents / 100.0 * 1000.0

        rows.append(
            (
                period_month,
                r.get("stateid"),
                r.get("sectorid"),
                price_cents,
                price_per_mwh,
                r.get("unit") or "cents/kwh"
            )
        )

    insert_sql = """
        INSERT INTO eia_az_price
        (period_month, stateid, sectorid, price_cents_per_kwh, price_per_mwh, unit)
        VALUES %s
    """

    execute_values(cur, insert_sql, rows)
    conn.commit()
    cur.close()
    conn.close()

    print(f"[fetch_prices] Inserted {len(rows)} price rows.")


def main():
    parser = argparse.ArgumentParser(description="Fetch AZ monthly electricity prices (ALL sectors).")
    parser.add_argument("--api-key", help="EIA API key or set EIA_API_KEY")
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--days", type=int, required=True)
    parser.add_argument("--pretty", action="store_true")
    parser.add_argument("--no-db", action="store_true")

    args = parser.parse_args()

    api_key = args.api_key or os.getenv("EIA_API_KEY")
    if not api_key:
        print("Error: Missing EIA API key")
        sys.exit(1)

    # Compute allowed months from window
    try:
        start = datetime.strptime(args.start_date, "%Y-%m-%d")
    except ValueError:
        print("Error: start-date must be YYYY-MM-DD")
        sys.exit(1)

    months_needed = month_range(start, args.days)

    # Fetch all AZ price data
    all_price_rows = fetch_az_prices(api_key)

    # Filter to only the months inside user's range
    filtered = [
        r for r in all_price_rows
        if r["period"] in months_needed
    ]

    print(f"[fetch_prices] Filtered to {len(filtered)} rows for months: {months_needed}")

    if args.pretty:
        import json
        print(json.dumps(filtered, indent=2))

    if not args.no_db:
        save_prices(filtered)
    else:
        print("[fetch_prices] --no-db set, skipping database insert.")


if __name__ == "__main__":
    main()