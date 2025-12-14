from __future__ import annotations

import base64
import json
from typing import Any, Dict

import pytest
from fastapi.testclient import TestClient

from digital_operation_twin.main import app


CUSTOMER_HEADERS = {"X-Customer-Id": "customer_a"}


@pytest.fixture()
def client() -> TestClient:
    return TestClient(app)


def test_healthz(client: TestClient) -> None:
    r = client.get("/api/healthz")
    assert r.status_code == 200
    assert r.json()["status"] == "ok"


def test_ingest_single(client: TestClient) -> None:
    body: Dict[str, Any] = {
        "envelope": {"event_id": "t-unit-1", "event_type": "x", "source": "rest", "attributes": {}},
        "payload": {"id": 1, "name": "one"},
    }
    r = client.post("/api/ingest", headers=CUSTOMER_HEADERS, json=body)
    assert r.status_code in (200, 202)
    out = r.json()
    assert out["status"] in ("accepted", "duplicate", "rejected")


def test_ingest_list(client: TestClient) -> None:
    body: Dict[str, Any] = {
        "envelope": {"event_id": "t-unit-2", "event_type": "x", "source": "rest", "attributes": {}},
        "payload": [{"id": 1}, {"id": 2}],
    }
    r = client.post("/api/ingest", headers=CUSTOMER_HEADERS, json=body)
    assert r.status_code in (200, 202, 422)
    out = r.json()
    if r.status_code == 422:
        assert "detail" in out
    else:
        assert out["status"] in ("accepted", "duplicate", "rejected")


def test_ingest_csv(client: TestClient) -> None:
    csv_bytes = b"id,name\n1,alpha\n2,beta\n"
    files = {"file": ("sample.csv", csv_bytes, "text/csv")}
    r = client.post("/api/ingest_csv", headers=CUSTOMER_HEADERS, files=files)
    assert r.status_code in (200, 202, 422)
    out = r.json()
    if r.status_code == 422:
        assert "detail" in out
    else:
        assert out["status"] in ("accepted", "duplicate", "rejected")
        assert out.get("filename") == "sample.csv"


def test_pubsub_push(client: TestClient) -> None:
    data = {
        "envelope": {"event_id": "t-unit-pubsub-1", "event_type": "x", "source": "pubsub", "attributes": {}},
        "payload": {"id": 1, "name": "pubsub"},
    }
    b64 = base64.b64encode(json.dumps(data).encode("utf-8")).decode("utf-8")

    body = {
        "message": {
            "data": b64,
            "attributes": {"customer_id": "customer_a", "event_type": "x"},
            "messageId": "m-unit-1",
            "publishTime": "2025-12-14T00:00:00Z",
        },
        "subscription": "projects/x/subscriptions/y",
    }

    r = client.post("/pubsub/gcp", json=body)
    assert r.status_code == 200
    out = r.json()
    assert out["status"] in ("accepted", "duplicate", "rejected", "ignored")