from __future__ import annotations

from typing import Any, Awaitable, Callable, Dict, Optional

from digital_operation_twin.core.models.api_gate import APIModel
from digital_operation_twin.pipelines.orchestrators.master_orchestration import MasterOrchestrator
from digital_operation_twin.core.models.pipeline_state import PipelineState

# A scheduler function that enqueues background work
Scheduler = Callable[[Callable[..., Awaitable[Any]], Any], None]
# e.g. background_tasks.add_task(fn, arg)

async def submit_api(
    *,
    orch: MasterOrchestrator,
    msg: APIModel,
    schedule: Optional[Scheduler] = None,
    message_id: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Shared ingestion entrypoint used by REST and Pub/Sub.

    - Runs gate
    - If accepted, schedules orch.process(state) using provided schedule()
    - Returns a neutral result dict (caller chooses HTTP status codes)
    """
    gate_state: PipelineState = await orch.gate(msg)

    base = {
        "event_id": gate_state.event_id,
        "message_id": message_id,
        "received": gate_state.metrics.get("received", len(gate_state.records)),
    }

    if gate_state.status == "rejected":
        return {**base, "status": "rejected", "errors": gate_state.errors}

    if gate_state.status == "duplicate":
        return {**base, "status": "duplicate"}

    # accepted
    if schedule is not None:
        schedule(orch.process, gate_state)

    return {**base, "status": "accepted"}
