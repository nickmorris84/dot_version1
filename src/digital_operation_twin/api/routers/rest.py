from __future__ import annotations

import os
from typing import Any, Dict
from fastapi import APIRouter, BackgroundTasks, HTTPException
from src.digital_operation_twin.core.models.api import APIModel

from src.digital_operation_twin.pipelines.orchestrators.master_orchestration import MasterOrchestrator
from config.config_loader import load_yaml, deep_merge

import logging
logger = logging.getLogger(__name__)


def load_cfg(customer_id: str) -> Dict[str, Any]:
    base_cfg = load_yaml("config/base.yaml")
    cust_cfg = load_yaml(f"config/{customer_id}.yaml")
    return deep_merge(base_cfg, cust_cfg)

router = APIRouter(prefix="/api", tags=["rest"])

EXPECTED_API_KEY = os.getenv("API_KEY")  # set in .env, or leave None to disable


@router.get("/healthz")
def healthz():
    return {"status": "ok"}


@router.post("/ingest", status_code=202)
async def ingest(body: Dict[str, Any], background_tasks: BackgroundTasks, customer_id: str = "customer_a"):
    msg = APIModel(**body)

    cfg = load_cfg(customer_id)
    orch = MasterOrchestrator(cfg)

    gate_state = await orch.gate(msg)

    if gate_state.status == "rejected":
        raise HTTPException(status_code=422, detail=gate_state.errors)

    if gate_state.status == "duplicate":
        return {"status": "duplicate", "event_id": gate_state.event_id}

    # Start heavy processing in the background
    background_tasks.add_task(orch.process, gate_state)

    # Respond immediately
    return {
        "status": "accepted",
        "event_id": gate_state.event_id,
        "received": gate_state.metrics.get("received", len(gate_state.records)),
    
    }