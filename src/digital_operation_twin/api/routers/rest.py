from __future__ import annotations

from typing import Any, Dict, Optional

from fastapi import APIRouter, BackgroundTasks, Depends, File, HTTPException, UploadFile

from digital_operation_twin.core.models.api_gate import APIModel
from digital_operation_twin.pipelines.orchestrators.master_orchestration import MasterOrchestrator
from digital_operation_twin.services.csv_ingestion import CsvIngestionService
from digital_operation_twin.api.api_submit import submit_api

from digital_operation_twin.api.deps import get_settings_rest, get_orchestrator_rest
from digital_operation_twin.config.schema import Settings

import logging
logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api", tags=["rest"])


@router.get("/healthz")
def healthz():
    return {"status": "ok"}


# ----------------------
# JSON ingest
# ----------------------
@router.post("/ingest", status_code=202)
async def ingest(
    body: Dict[str, Any],
    background_tasks: BackgroundTasks,
    orch: MasterOrchestrator = Depends(get_orchestrator_rest),
):
    msg = APIModel(**body)

    result = await submit_api(
        orch=orch,
        msg=msg,
        schedule=background_tasks.add_task,
    )

    if result["status"] == "rejected":
        raise HTTPException(status_code=422, detail=result["errors"])

    return result


# ----------------------
# CSV ingest
# ----------------------
@router.post("/ingest_csv", status_code=202)
async def ingest_csv(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    event_type: Optional[str] = None,
    settings: Settings = Depends(get_settings_rest),
    orch: MasterOrchestrator = Depends(get_orchestrator_rest),
):
    raw = await file.read()
    if not raw:
        raise HTTPException(status_code=400, detail="Empty file.")

    csv_service = CsvIngestionService(default_source="rest_csv")
    df = csv_service.load_to_df(raw)

    api_body = csv_service.build_api_body(
        df=df,
        event_type=event_type or "record.ingest",
        attributes={
            "filename": file.filename,
            "content_type": file.content_type,
            "customer_id": settings.customer_id,
        },
    )

    msg = APIModel(**api_body)

    result = await submit_api(
        orch=orch,
        msg=msg,
        schedule=background_tasks.add_task,
    )

    if result["status"] == "rejected":
        raise HTTPException(status_code=422, detail=result["errors"])

    return {**result, "filename": file.filename}