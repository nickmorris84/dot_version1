from __future__ import annotations

import time
import logging
from typing import Protocol, List

from digital_operation_twin.core.models.pipeline_state import PipelineState

logger = logging.getLogger(__name__)


class Step(Protocol):
    name: str
    async def run(self, state: PipelineState) -> PipelineState: ...


class PipelineRunner:
    def __init__(self, steps: List[Step], *, name: str = "pipeline") -> None:
        self.steps = steps
        self.name = name

    async def run(self, state: PipelineState) -> PipelineState:
        logger.info("[%s] starting", self.name, extra={"event_id": state.event_id})

        for step in self.steps:
            t0 = time.perf_counter()
            try:
                state = await step.run(state)
            except Exception as e:
                elapsed = int((time.perf_counter() - t0) * 1000)
                state.timings_ms[step.name] = elapsed
                state.status = "failed"
                state.errors.append({"step": step.name, "error": str(e)})
                logger.exception("[%s] step failed: %s", self.name, step.name, extra={"event_id": state.event_id})
                return state

            elapsed = int((time.perf_counter() - t0) * 1000)
            state.timings_ms[step.name] = elapsed
            logger.info("[%s] step ok: %s (%sms)", self.name, step.name, elapsed, extra={"event_id": state.event_id})

            # allow a step to short-circuit (e.g., rejected/duplicate)
            if state.status in {"rejected", "duplicate"}:
                logger.info("[%s] short-circuit (%s)", self.name, state.status, extra={"event_id": state.event_id})
                return state

        state.status = "done" if state.status not in {"rejected", "duplicate"} else state.status
        logger.info("[%s] finished (%s)", self.name, state.status, extra={"event_id": state.event_id})
        return state
