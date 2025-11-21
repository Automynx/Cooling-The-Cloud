#!/usr/bin/env python3
import os
import sys
import argparse
import json
from datetime import datetime, timedelta

import requests
from dotenv import load_dotenv

load_dotenv()

BASE_URL = "https://api.eia.gov/v2/electricity/rto/interchange-data/data/"
PAGE_SIZE = 5000  # EIA v2 max per request
AZ_BAS = ["AZPS", "SRP", "TEPC"]


def _fetch_for_dimension(
    api_key: str,
    start_str: str,
    end_str: str,
    dim: str,
    state: str | None = None,
) -> list[dict]:
    """
    Fetch all pages where `dim` (either 'fromba' or 'toba') is one of AZ_BAS.
    """
    all_records: list[dict] = []
    offset = 0
    page = 1

    while True:
        print(f"[fetch_eia] ---- {dim.upper()} Page {page} (offset={offset}) ----")

        params = [
            ("api_key", api_key),
            ("frequency", "hourly"),
            ("data[0]", "value"),
            ("start", start_str),
            ("end", end_str),
            ("sort[0][column]", "period"),
            ("sort[0][direction]", "asc"),
            ("offset", offset),
            ("length", PAGE_SIZE),
        ]

        if state:
            params.append(("facets[state][]", state))

        for ba in AZ_BAS:
            params.append((f"facets[{dim}][]", ba))

        try:
            resp = requests.get(BASE_URL, params=params, timeout=20)
            resp.raise_for_status()
        except Exception as e:
            print(f"[fetch_eia] Request error ({dim}, offset={offset}): {e}")
            sys.exit(1)

        data = resp.json()

        records = None
        if isinstance(data, dict):
            records = (
                data.get("response", {}).get("data")
                or data.get("data")
                or data.get("results")
            )

        if not records:
            if offset == 0:
                print(f"[fetch_eia] No records returned on first page for {dim}.")
                print(json.dumps(data, indent=2))
            else:
                print(
                    f"[fetch_eia] No records returned at offset {offset} for {dim}, "
                    "stopping pagination."
                )
            break

        first_period = records[0].get("period")
        last_period = records[-1].get("period")
        prev_total = len(all_records)
        page_count = len(records)
        new_total = prev_total + page_count

        print(
            f"[fetch_eia] {dim.upper()} Page {page}: fetched {page_count} rows "
            f"(period {first_period} → {last_period}), total for {dim} so far = {new_total}"
        )
        sys.stdout.flush()

        all_records.extend(records)

        if len(records) < PAGE_SIZE:
            print(
                f"[fetch_eia] Last page for {dim} had fewer than PAGE_SIZE rows, "
                "stopping pagination."
            )
            break

        offset += PAGE_SIZE
        page += 1

    return all_records


def fetch_period(
    api_key: str,
    start_date: str,
    days: int = 7,
    state: str | None = None,
    pretty: bool = False,
):
    try:
        start = datetime.strptime(start_date, "%Y-%m-%d").date()
    except ValueError:
        print("Error: start-date must be in format YYYY-MM-DD")
        sys.exit(1)

    if days < 1:
        print("Error: days must be at least 1")
        sys.exit(1)

    end = start + timedelta(days=days - 1)
    start_str = f"{start.isoformat()}T00"
    end_str = f"{end.isoformat()}T23"

    print(f"[fetch_eia] Requesting AZ-related data from {start_str} to {end_str}")
    expected = 24 * days
    print(f"[fetch_eia] Expected ~{expected} hourly timestamps for {days} day(s)")

    # Fetch where AZ BAs appear as FROM
    from_records = _fetch_for_dimension(
        api_key=api_key,
        start_str=start_str,
        end_str=end_str,
        dim="fromba",
        state=state,
    )

    # Fetch where AZ BAs appear as TO
    to_records = _fetch_for_dimension(
        api_key=api_key,
        start_str=start_str,
        end_str=end_str,
        dim="toba",
        state=state,
    )

    print(
        f"[fetch_eia] Raw counts before dedupe: "
        f"fromba-side={len(from_records)}, toba-side={len(to_records)}"
    )

    # Merge and dedupe on (period, fromba, toba)
    combined: list[dict] = []
    seen: set[tuple[str | None, str | None, str | None]] = set()

    for r in from_records + to_records:
        key = (r.get("period"), r.get("fromba"), r.get("toba"))
        if key in seen:
            continue
        seen.add(key)
        combined.append(r)

    if not combined:
        print("[fetch_eia] No AZ-related records after merge/dedupe.")
        return []

    first_overall = combined[0].get("period")
    last_overall = combined[-1].get("period")
    print(
        f"[fetch_eia] DONE: fetched {len(combined)} unique AZ-related rows "
        f"from {first_overall} to {last_overall}"
    )

    if pretty:
        print(json.dumps(combined, indent=2))

    return combined


def main():
    parser = argparse.ArgumentParser(description="Fetch EIA hourly grid data for a date range.")
    parser.add_argument("--api-key", help="EIA API key or set EIA_API_KEY")
    parser.add_argument("--start-date", required=True, help="Start date YYYY-MM-DD")
    parser.add_argument("--days", type=int, default=7, help="Number of days to fetch (default 7)")
    parser.add_argument("--state", help="Optional state facet")
    parser.add_argument("--save", help="Save JSON output to file")
    parser.add_argument("--pretty", action="store_true", help="Pretty-print JSON")

    args = parser.parse_args()

    api_key = args.api_key or os.getenv("EIA_API_KEY")
    if not api_key:
        print("Error: Missing API key. Provide --api-key or set EIA_API_KEY in .env")
        sys.exit(1)

    records = fetch_period(
        api_key=api_key,
        start_date=args.start_date,
        days=args.days,
        state=args.state,
        pretty=args.pretty,
    )

    if args.save:
        with open(args.save, "w") as f:
            json.dump(records, f, indent=2 if args.pretty else None)
        print(f"Saved {len(records)} records to {args.save}")
    else:
        print(json.dumps(records, indent=2 if args.pretty else None))


if __name__ == "__main__":
    main()
