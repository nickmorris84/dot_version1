from __future__ import annotations

import base64
import json
import logging
from typing import Any, Dict

from fastapi import APIRouter, BackgroundTasks, Depends

from digital_operation_twin.pipelines.api_submit import submit_api
from digital_operation_twin.api.deps import (
    get_orchestrator_pubsub,
    get_pubsub_body,
    get_settings_pubsub,
)
from digital_operation_twin.config.schema import Settings
from digital_operation_twin.core.models.api_gate import APIModel
from digital_operation_twin.pipelines.orchestrators.master_orchestration import MasterOrchestrator

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/pubsub", tags=["pubsub"])


def _b64_to_json_dict(data_b64: str) -> Dict[str, Any]:
    decoded = base64.b64decode(data_b64).decode("utf-8")
    return json.loads(decoded)


def _wrap_as_api_body(payload: Any, attrs: Dict[str, Any], message_id: str | None) -> Dict[str, Any]:
    """
    Normalise Pub/Sub payload into the APIModel body shape.
    If payload already matches APIModel, we merge attributes in.
    """
    if isinstance(payload, dict):
        out: Dict[str, Any] = dict(payload)
    else:
        out = {"payload": payload}

    out.setdefault("attributes", {})
    # Ensure attributes are always a dict
    if not isinstance(out["attributes"], dict):
        out["attributes"] = {}

    out["attributes"].update(attrs or {})
    if message_id:
        out["attributes"]["message_id"] = message_id

    return out


@router.post("/gcp")
async def pubsub_push(
    background_tasks: BackgroundTasks,
    body: Dict[str, Any] = Depends(get_pubsub_body),
    settings: Settings = Depends(get_settings_pubsub),
    orch: MasterOrchestrator = Depends(get_orchestrator_pubsub),
) -> Dict[str, Any]:
    """
    GCP Pub/Sub push format:
    {
      "message": {
        "data": "base64-encoded-string",
        "attributes": {...},
        "messageId": "...",
        "publishTime": "..."
      },
      "subscription": "..."
    }

    Pub/Sub retries on non-2xx. For poison messages, return 200 with status=ignored/rejected
    to avoid retry loops (unless you *want* retries / DLQ behavior).
    """
    message = body.get("message") or {}
    attrs = message.get("attributes") or {}
    message_id = message.get("messageId")

    customer_id = settings.customer_id  # single source of truth

    logger.info("PubSub received", extra={"message_id": message_id, "customer_id": customer_id})

    if "data" not in message:
        logger.warning(
            "PubSub ignored: missing message.data",
            extra={"message_id": message_id, "customer_id": customer_id},
        )
        return {"status": "ignored", "reason": "missing_data", "message_id": message_id}

    try:
        payload_dict = _b64_to_json_dict(message["data"])
    except Exception:
        logger.exception(
            "PubSub ignored: decode/parse failed",
            extra={"message_id": message_id, "customer_id": customer_id},
        )
        return {"status": "ignored", "reason": "decode_failed", "message_id": message_id}

    try:
        api_body = _wrap_as_api_body(payload_dict, attrs=attrs, message_id=message_id)
        msg = APIModel(**api_body)
    except Exception as e:
        logger.exception(
            "PubSub ignored: APIModel validation failed",
            extra={"message_id": message_id, "customer_id": customer_id},
        )
        return {
            "status": "ignored",
            "reason": "model_invalid",
            "message_id": message_id,
            "error": str(e),
        }

    result = await submit_api(
        orch=orch,
        msg=msg,
        schedule=background_tasks.add_task,
        message_id=message_id,
    )

    logger.info(
        "PubSub ingest result",
        extra={
            "message_id": message_id,
            "customer_id": customer_id,
            "event_id": result.get("event_id"),
            "status": result.get("status"),
            "received": result.get("received"),
        },
    )

    return result
