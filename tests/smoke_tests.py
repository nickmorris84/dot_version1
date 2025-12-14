from __future__ import annotations

import base64
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, Optional

import requests


BASE_URL = os.getenv("DOT_BASE_URL", "http://127.0.0.1:8000")
CUSTOMER_ID = os.getenv("DOT_CUSTOMER_ID", "customer_a")


def _print_result(name: str, r: requests.Response) -> None:
    print(f"\n=== {name} ===")
    print("URL:", r.request.method, r.url)
    print("Status:", r.status_code)
    try:
        print("JSON:", json.dumps(r.json(), indent=2))
    except Exception:
        print("Text:", r.text[:1000])


def test_healthz() -> None:
    r = requests.get(f"{BASE_URL}/api/healthz", timeout=10)
    _print_result("healthz", r)
    r.raise_for_status()


def test_ingest_single() -> None:
    body: Dict[str, Any] = {
        "envelope": {"event_id": "smoke-single-1", "event_type": "x", "source": "rest", "attributes": {}},
        "payload": {"id": 1, "name": "one"},
    }
    r = requests.post(
        f"{BASE_URL}/api/ingest",
        params={"customer_id": CUSTOMER_ID},
        json=body,
        timeout=20,
    )
    _print_result("ingest_single", r)
    r.raise_for_status()


def test_ingest_list() -> None:
    body: Dict[str, Any] = {
        "envelope": {"event_id": "smoke-list-1", "event_type": "x", "source": "rest", "attributes": {}},
        "payload": [{"id": 1}, {"id": 2}],
    }
    r = requests.post(
        f"{BASE_URL}/api/ingest",
        params={"customer_id": CUSTOMER_ID},
        json=body,
        timeout=20,
    )
    _print_result("ingest_list", r)
    r.raise_for_status()


def test_ingest_csv(csv_path: Optional[str] = None) -> None:
    # Create a temp CSV if none provided
    path = Path(csv_path) if csv_path else Path("tmp_smoke.csv")
    if not csv_path:
        path.write_text("id,name\n1,alpha\n2,beta\n", encoding="utf-8")

    with path.open("rb") as f:
        files = {"file": (path.name, f, "text/csv")}
        r = requests.post(
            f"{BASE_URL}/api/ingest_csv",
            params={"customer_id": CUSTOMER_ID},
            files=files,
            timeout=60,
        )
    _print_result("ingest_csv", r)
    r.raise_for_status()

    if not csv_path:
        path.unlink(missing_ok=True)


def test_pubsub_push() -> None:
    # Your pubsub endpoint expects a Pub/Sub push body with base64 encoded data
    data = {
        "envelope": {"event_id": "smoke-pubsub-1", "event_type": "x", "source": "pubsub", "attributes": {}},
        "payload": {"id": 1, "name": "pubsub"},
    }
    b64 = base64.b64encode(json.dumps(data).encode("utf-8")).decode("utf-8")

    body = {
        "message": {
            "data": b64,
            "attributes": {"customer_id": CUSTOMER_ID, "event_type": "x"},
            "messageId": "m-smoke-1",
            "publishTime": "2025-12-14T00:00:00Z",
        },
        "subscription": "projects/x/subscriptions/y",
    }

    r = requests.post(f"{BASE_URL}/pubsub/gcp", json=body, timeout=20)
    _print_result("pubsub_push", r)
    r.raise_for_status()


def main() -> int:
    try:
        test_healthz()
        test_ingest_single()
        test_ingest_list()
        test_ingest_csv()
        test_pubsub_push()
    except requests.HTTPError as e:
        print("\nFAILED:", str(e))
        return 1

    print("\nALL SMOKE TESTS PASSED ✅")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())