#!/usr/bin/env python3
"""Fetch hourly interchange data from EIA for AZ and print or save results.

Usage:
  - Set env var `EIA_API_KEY` or pass `--api-key YOUR_KEY`.
  - Example: `python scripts/fetch_eia.py --api-key YOUR_KEY --pretty`
"""
import os
import sys
import argparse
import json
import requests
from dotenv import load_dotenv

# Load environment variables from .env (if present)
load_dotenv()


def fetch_and_collect(api_key, pretty=False, save=None):
    # Candidate endpoints to probe if the first one returns 404
    candidates = [
        "https://api.eia.gov/v2/electricity/rto/interchange/data/",
        "https://api.eia.gov/v2/electricity/interchange/data/",
        "https://api.eia.gov/v2/electricity/rto/interchange/",
        "https://api.eia.gov/v2/electricity/interchange/",
        "https://api.eia.gov/v2/electricity/rto/interchange/data",
        "https://api.eia.gov/v2/electricity/interchange/data",
    ]

    # Build params as list of tuples to allow repeated keys like facets[state][]
    params = [
        ("api_key", api_key),
        ("frequency", "hourly"),
        ("data[0]", "value"),
        ("facets[state][]", "AZ"),
        ("start", "2024-01-01"),
        ("end", "2024-12-31"),
        ("page", "1"),
        ("per_page", "1000"),
    ]

    session = requests.Session()
    all_records = []

    # Probe candidates to find a working endpoint (or at least get better diagnostics)
    base = None
    probe_results = []
    for cand in candidates:
        try:
            probe_resp = session.get(cand, params=params, timeout=20)
        except Exception as e:
            probe_results.append((cand, None, str(e), None))
            continue

        probe_results.append((cand, probe_resp.status_code, probe_resp.text[:1000], probe_resp))
        if probe_resp.status_code == 200:
            base = cand
            break

    # If none of the candidates returned 200, print diagnostics and exit
    if base is None:
        print("No working endpoint found. Probe results:")
        for cand, status, snippet, _ in probe_results:
            print(f"- {cand} -> status={status} snippet={snippet!r}")
        print("If the API docs show a different path, update the script or provide the correct endpoint.")
        return

    # We have a working base; continue fetching pages
    # Use the found base and existing params
    while True:
        resp = session.get(base, params=params, timeout=30)
        try:
            resp.raise_for_status()
        except requests.HTTPError as e:
            print(f"HTTP error: {e}\nResponse body:\n{resp.text}")
            sys.exit(1)

        data = resp.json()

        # Try common places for the returned records
        records = None
        pagination = None

        if isinstance(data, dict):
            records = data.get("response", {}).get("data") or data.get("data") or data.get("results")
            pagination = data.get("response", {}).get("pagination") or data.get("pagination")

        # If no list of records found, save/print whole response and exit
        if not records:
            if save:
                with open(save, "w") as f:
                    json.dump(data, f, indent=2 if pretty else None)
                print(f"Saved response to {save}")
            else:
                print(json.dumps(data, indent=2 if pretty else None))
            return

        all_records.extend(records)

        # If pagination info exists, advance pages; otherwise break
        if pagination:
            cur = pagination.get("current_page") or pagination.get("page")
            total = pagination.get("total_pages") or pagination.get("pages") or pagination.get("total_pages")
            if cur is None or total is None:
                break
            if int(cur) >= int(total):
                break
            next_page = int(cur) + 1
            # replace page in params
            params = [(k, v) for (k, v) in params if k != "page"]
            params.append(("page", str(next_page)))
        else:
            break

    # Output collected records
    if save:
        with open(save, "w") as f:
            json.dump(all_records, f, indent=2 if pretty else None)
        print(f"Saved {len(all_records)} records to {save}")
    else:
        if pretty:
            print(json.dumps(all_records, indent=2))
        else:
            print(json.dumps(all_records))


def main():
    p = argparse.ArgumentParser(description="Fetch EIA hourly interchange data for AZ (2024) and print/save it.")
    p.add_argument("--api-key", help="EIA API key (or set EIA_API_KEY env var)")
    p.add_argument("--save", help="Save JSON output to a file")
    p.add_argument("--pretty", action="store_true", help="Pretty-print JSON output")

    args = p.parse_args()

    api_key = args.api_key or os.getenv("EIA_API_KEY")
    if not api_key:
        print("Error: API key not provided. Use --api-key or set EIA_API_KEY environment variable.")
        sys.exit(2)

    fetch_and_collect(api_key, pretty=args.pretty, save=args.save)


if __name__ == "__main__":
    main()
