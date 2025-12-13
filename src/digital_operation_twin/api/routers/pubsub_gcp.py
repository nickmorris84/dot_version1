from __future__ import annotations

import base64
import json
from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse

from src.digital_operation_twin.core.models.api import APIModel
from src.digital_operation_twin.services.ingestion import IngestionService

router = APIRouter(prefix="/api", tags=["pubsub"])
service = IngestionService()


@router.post("/pubsub")
async def pubsub_push(body: dict):
    """
    GCP Pub/Sub push format:
    {
      "message": {
        "data": "<base64-encoded JSON string>",
        ...
      },
      "subscription": "..."
    }
    """
    try:
        data_b64 = body["message"]["data"]
        raw = base64.b64decode(data_b64).decode("utf-8")
        payload = json.loads(raw)
        msg = APIModel.model_validate(payload)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Bad Pub/Sub message: {e}")

    result = await service.process(msg)
    status = 200 if result.get("status") in ("ok", "duplicate", "partial_ok") else 422
    return JSONResponse(content=result, status_code=status)
