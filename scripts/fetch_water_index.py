#!/usr/bin/env python3
import os
import sys
import argparse
import json
from datetime import datetime, timedelta

import requests
from psycopg2.extras import execute_values
from dotenv import load_dotenv

# Make repo root importable so we can reuse connect_db()
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from data.api.store_to_postgres import connect_db

load_dotenv()

BLS_URL = os.getenv("BLS_API_URL", "https://api.bls.gov/publicAPI/v2/timeseries/data/")
BLS_SERIES = os.getenv("BLS_WATER_SERIES", "CUUR0000SEHG")
BLS_KEY = os.getenv("BLS_API_KEY")


def fetch_water_index(start_date_str: str, days: int) -> list[dict]:
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d").date()
    end_date = start_date + timedelta(days=days - 1)

    start_year = start_date.year
    end_year = end_date.year

    headers = {"Content-type": "application/json"}

    payload = {
        "seriesid": [BLS_SERIES],
        "startyear": str(start_year),
        "endyear": str(end_year),
    }

    if BLS_KEY:
        payload["registrationkey"] = BLS_KEY

    resp = requests.post(BLS_URL, headers=headers, data=json.dumps(payload))
    resp.raise_for_status()
    data = resp.json()

    status = data.get("status")
    if status != "REQUEST_SUCCEEDED":
        raise RuntimeError(f"BLS API error: {data.get('message')}")

    series_list = data.get("Results", {}).get("series", [])
    if not series_list:
        return []

    series = series_list[0]
    series_id = series.get("seriesID", BLS_SERIES)

    records: list[dict] = []

    for item in series.get("data", []):
        period = item.get("period")
        if not period or not period.startswith("M"):
            continue

        year = int(item["year"])
        month = int(period[1:])
        period_date = datetime(year, month, 1).date()

        if period_date < start_date or period_date > end_date:
            continue

        value = float(item["value"])
        records.append(
            {
                "period_date": period_date,
                "cpi_value": value,
                "series_id": series_id,
            }
        )

    return records


def save_water_index(records: list[dict], no_db: bool = False) -> None:
    if not records:
        print("[fetch_water_index] No records to save.")
        return

    if no_db:
        print("[fetch_water_index] --no-db set, skipping database insert.")
        return

    conn = connect_db()
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS water_price_index (
            period_date date PRIMARY KEY,
            cpi_value numeric NOT NULL,
            series_id text NOT NULL
        )
        """
    )
    conn.commit()

    rows = [
        (r["period_date"], r["cpi_value"], r["series_id"])
        for r in records
    ]

    insert_sql = """
        INSERT INTO water_price_index (period_date, cpi_value, series_id)
        VALUES %s
        ON CONFLICT (period_date) DO UPDATE
        SET cpi_value = EXCLUDED.cpi_value,
            series_id = EXCLUDED.series_id
    """

    execute_values(cur, insert_sql, rows)
    conn.commit()
    cur.close()
    conn.close()

    print(f"[fetch_water_index] Inserted/updated {len(rows)} rows into water_price_index.")


def main():
    parser = argparse.ArgumentParser(
        description="Fetch BLS water/sewer CPI index and store in Supabase."
    )
    parser.add_argument("--start-date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--days", type=int, default=365, help="Number of days to cover")
    parser.add_argument("--pretty", action="store_true")
    parser.add_argument("--no-db", action="store_true")

    args = parser.parse_args()

    records = fetch_water_index(args.start_date, args.days)

    if args.pretty:
        print(json.dumps(records, default=str, indent=2))

    save_water_index(records, no_db=args.no_db)


if __name__ == "__main__":
    main()