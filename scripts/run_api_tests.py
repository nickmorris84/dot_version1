#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

import requests


@dataclass
class TestCase:
    name: str
    payload: Dict[str, Any]
    expected_status_code: int
    expect_detail_contains: Optional[str] = None
    expect_json_path_equals: Optional[Tuple[str, Any]] = None  # ("status", "accepted") etc.


def _get(d: Dict[str, Any], path: str) -> Any:
    """Minimal JSON path getter: "a.b.c" """
    cur: Any = d
    for part in path.split("."):
        if not isinstance(cur, dict) or part not in cur:
            return None
        cur = cur[part]
    return cur


def post_ingest(base_url: str, customer_id: str, payload: Dict[str, Any], timeout: int = 15) -> requests.Response:
    url = base_url.rstrip("/") + "/api/ingest"
    headers = {
        "Content-Type": "application/json",
        "X-Customer-Id": customer_id,
    }
    return requests.post(url, headers=headers, json=payload, timeout=timeout)


def run_test_case(base_url: str, customer_id: str, tc: TestCase, verbose: bool = False) -> bool:
    try:
        resp = post_ingest(base_url, customer_id, tc.payload)
    except Exception as e:
        print(f"❌ {tc.name}: request failed: {e}")
        return False

    ok = True

    if resp.status_code != tc.expected_status_code:
        ok = False
        print(f"❌ {tc.name}: expected status {tc.expected_status_code}, got {resp.status_code}")

    # Try parse JSON (FastAPI returns JSON for errors too)
    try:
        body = resp.json()
    except Exception:
        body = {"_raw": resp.text}

    if tc.expect_detail_contains is not None:
        detail = body.get("detail")
        detail_str = json.dumps(detail, default=str) if detail is not None else ""
        if tc.expect_detail_contains not in detail_str:
            ok = False
            print(f"❌ {tc.name}: expected detail to contain {tc.expect_detail_contains!r}, got {detail_str!r}")

    if tc.expect_json_path_equals is not None:
        path, expected_value = tc.expect_json_path_equals
        actual_value = _get(body, path)
        if actual_value != expected_value:
            ok = False
            print(f"❌ {tc.name}: expected body[{path!r}] == {expected_value!r}, got {actual_value!r}")

    if ok:
        print(f"✅ {tc.name}")
    else:
        if verbose:
            print("   Response body:")
            print(json.dumps(body, indent=2, default=str))

    return ok


def _now_id(prefix: str) -> str:
    return f"{prefix}-{int(time.time() * 1000)}"


def build_test_cases() -> list[TestCase]:
    schema_version = "v1"

    # ---- Case A: already-standardised payload (matches Event fields exactly) ----
    good_single_event_model_fields = {
        "envelope": {
            "event_id": _now_id("evt"),
            "event_type": "application_created",
            "source": "credit_card_system",
            "dedupe_key": None,
            "attributes": {"schema_version": schema_version},
        },
        "payload": {
            # ✅ REQUIRED BY Event
            "event_id": _now_id("evt-payload"),
            "journey_id": "journey-001",               # REQUIRED (not application_id)
            "step": "application_created",            # REQUIRED
            "start_ts": "2025-12-14T10:00:00Z",        # REQUIRED
            "end_ts": "2025-12-14T10:00:05Z",          # REQUIRED
            "event_type": "application_created",       # REQUIRED
            "event_description": "Credit card application created",  # REQUIRED
            "event_codes": ["APP_CREATE"],             # REQUIRED in your current dataclass
            # extras are OK
            "customer_id": "cust-123",
        },
    }

    # ---- Case B: customer_a raw-ish fields (tests your standardiser rename mapping) ----
    # This should work only if your YAML contains:
    # stage -> step
    # stage_start_time -> start_ts
    # stage_end_time -> end_ts
    # activity_type -> event_type
    # activity_notes -> event_description
    # risk_grade -> event_codes
    # application_id -> journey_id
    good_single_customer_a_raw_fields = {
        "envelope": {
            "event_id": _now_id("evt-raw"),
            "event_type": "application_events",
            "source": "credit_card_csv",
            "dedupe_key": None,
            "attributes": {"schema_version": schema_version},
        },
        "payload": {
            # include event_id so it can flow through
            "event_id": _now_id("evt-raw-payload"),
            "application_id": "journey-002",
            "stage": "application_created",
            "stage_start_time": "2025-12-14T10:00:00Z",
            "stage_end_time": "2025-12-14T10:00:05Z",
            "activity_type": "application_created",
            "activity_notes": "Created from CSV upload",
            "risk_grade": "APP_CREATE",  # will rename to event_codes; your Event.from_dict will normalise to list if you added that
        },
    }

    # ---- Batch using Event fields directly ----
    batch_good_event_fields = {
        "envelope": {
            "event_id": _now_id("evt-batch"),
            "event_type": "application_events",
            "source": "credit_card_system",
            "attributes": {"schema_version": schema_version},
        },
        "payload": [
            {
                "event_id": _now_id("evt-b1"),
                "journey_id": "journey-003",
                "step": "application_created",
                "start_ts": "2025-12-14T10:00:00Z",
                "end_ts": "2025-12-14T10:00:02Z",
                "event_type": "application_created",
                "event_description": "Application created",
                "event_codes": ["APP_CREATE"],
            },
            {
                "event_id": _now_id("evt-b2"),
                "journey_id": "journey-003",
                "step": "application_submitted",
                "start_ts": "2025-12-14T10:02:00Z",
                "end_ts": "2025-12-14T10:02:05Z",
                "event_type": "application_submitted",
                "event_description": "Application submitted",
                "event_codes": ["APP_SUBMIT"],
            },
        ],
    }

    # ---- Negative case: missing required fields ----
    bad_missing_required = {
        "envelope": {
            "event_id": _now_id("evt-bad"),
            "event_type": "application_created",
            "source": "credit_card_system",
            "attributes": {"schema_version": schema_version},
        },
        "payload": {
            "event_id": _now_id("evt-bad-payload"),
            "journey_id": "journey-bad",
            # missing: step
            # missing: start_ts
            # missing: end_ts
            "event_type": "application_created",
            # missing: event_description
            # missing: event_codes
        },
    }

    return [
        TestCase(
            name="happy_path_single_event_model_fields",
            payload=good_single_event_model_fields,
            expected_status_code=200,
            expect_json_path_equals=("status", "accepted"),
        ),
        TestCase(
            name="happy_path_single_event_customer_a_raw_fields",
            payload=good_single_customer_a_raw_fields,
            expected_status_code=200,
            expect_json_path_equals=("status", "accepted"),
        ),
        TestCase(
            name="happy_path_batch_event_fields",
            payload=batch_good_event_fields,
            expected_status_code=200,
            expect_json_path_equals=("status", "accepted"),
        ),
        TestCase(
            name="reject_missing_required_fields",
            payload=bad_missing_required,
            expected_status_code=400,
            expect_detail_contains="Missing required fields",
        ),
    ]


def main() -> int:
    parser = argparse.ArgumentParser(description="Run ingest endpoint smoke tests.")
    parser.add_argument("--base-url", default="http://127.0.0.1:8000", help="Base URL of API")
    parser.add_argument("--customer-id", default="customer_a", help="Value for X-Customer-Id header")
    parser.add_argument("--verbose", action="store_true", help="Print response bodies on failures")
    args = parser.parse_args()

    test_cases = build_test_cases()

    print(f"Running {len(test_cases)} ingest tests against {args.base_url} (X-Customer-Id={args.customer_id})")
    passed = 0

    for tc in test_cases:
        if run_test_case(args.base_url, args.customer_id, tc, verbose=args.verbose):
            passed += 1

    failed = len(test_cases) - passed
    print(f"\nDone: {passed} passed, {failed} failed")

    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
