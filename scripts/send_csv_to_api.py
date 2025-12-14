"""Send a CSV file to the running DOT API.

Usage:
  python scripts/send_csv_to_api.py --csv ./sample.csv --customer customer_a

Notes:
  - Server must be running (e.g. uvicorn digital_operation_twin.main:app --reload)
  - The REST API requires X-Customer-Id header.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import httpx


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="POST a CSV to /api/ingest_csv")
    p.add_argument("--csv", required=True, help="Path to CSV file")
    p.add_argument("--customer", required=True, help="Customer id (X-Customer-Id)")
    p.add_argument("--url", default="http://127.0.0.1:8000", help="Base URL")
    p.add_argument("--event-type", default=None, help="Optional event_type override")
    p.add_argument("--timeout", type=float, default=30.0, help="Request timeout seconds")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    csv_path = Path(args.csv)
    if not csv_path.exists():
        raise SystemExit(f"CSV not found: {csv_path}")

    endpoint = args.url.rstrip("/") + "/api/ingest_csv"
    params = {}
    if args.event_type:
        params["event_type"] = args.event_type

    headers = {"X-Customer-Id": args.customer}

    with csv_path.open("rb") as f:
        files = {"file": (csv_path.name, f, "text/csv")}
        with httpx.Client(timeout=args.timeout) as client:
            resp = client.post(endpoint, headers=headers, params=params, files=files)

    print(f"POST {endpoint} -> {resp.status_code}")
    try:
        print(resp.json())
    except Exception:
        print(resp.text)

    resp.raise_for_status()


if __name__ == "__main__":
    main()
