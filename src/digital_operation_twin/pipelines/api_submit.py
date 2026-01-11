from __future__ import annotations

import logging
from typing import Any, Awaitable, Callable, Dict, Optional

from digital_operation_twin.core.models.api_gate import APIModel
from digital_operation_twin.core.request_context import event_id_var, customer_id_var
from digital_operation_twin.pipelines.orchestrators.master_orchestration import MasterOrchestrator

logger = logging.getLogger(__name__)

Scheduler = Callable[[Callable[..., Awaitable[Any]], Any], None]


async def submit_api(
    *,
    orch: MasterOrchestrator,
    msg: APIModel,
    schedule: Optional[Scheduler] = None,
    message_id: Optional[str] = None,
) -> Dict[str, Any]:
    """Shared ingestion entrypoint used by REST and Pub/Sub."""

    token_event = event_id_var.set(msg.envelope.event_id)
    token_cust = customer_id_var.set((msg.envelope.attributes or {}).get("customer_id"))

    try:
        logger.info(
            "ingest received",
            extra={
                "event_id": msg.envelope.event_id,
                "source": msg.envelope.source,
                "event_type": msg.envelope.event_type,
            },
        )

        gate_state = await orch.gate(msg)
        gate_state.context["message_id"] = message_id

        base = {
            "event_id": gate_state.event_id,
            "message_id": message_id,
            "received": gate_state.metrics.get("received", len(gate_state.records)),
        }

        if gate_state.status == "rejected":
            logger.warning("ingest rejected", extra={"event_id": gate_state.event_id, "errors": len(gate_state.errors)})
            return {**base, "status": "rejected", "errors": gate_state.errors}

        if gate_state.status == "duplicate":
            logger.info("ingest duplicate", extra={"event_id": gate_state.event_id})
            return {**base, "status": "duplicate"}

        if schedule is not None:
            logger.info("ingest accepted (scheduled)", extra={"event_id": gate_state.event_id})
            schedule(orch.process, gate_state)
        else:
            logger.info("ingest accepted", extra={"event_id": gate_state.event_id})

        return {**base, "status": "accepted"}
    finally:
        event_id_var.reset(token_event)
        customer_id_var.reset(token_cust)
