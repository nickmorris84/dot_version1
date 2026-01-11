from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Protocol
import pandas as pd
import time

from digital_operation_twin.core.models.pipeline_state import PipelineState

logger = logging.getLogger(__name__)

_LOG_DATA = os.getenv("DOT_LOG_DATA", "0").lower() in {"1", "true", "yes", "y"}
_MAX_CHARS = int(os.getenv("DOT_LOG_DATA_MAX_CHARS", "2000"))


def _preview(obj: Any) -> str:
    try:
        s = json.dumps(obj, default=str)
    except Exception:
        s = str(obj)
    if len(s) > _MAX_CHARS:
        return s[:_MAX_CHARS] + "...(truncated)"
    return s


class Step(Protocol):
    name: str

    async def run(self, state: PipelineState) -> PipelineState:
        raise NotImplementedError
    
    
@dataclass
class StepReport:
    step_name: str
    ok: bool
    duration_ms: int
    rows_before: int
    rows_after: int
    cols_before: List[str]
    cols_after: List[str]
    added_cols: List[str]
    removed_cols: List[str]
    nulls_top: Dict[str, int]
    output_path: Optional[str] = None
    error: Optional[str] = None
    log_path: Optional[str] = None


class PipelineRunner:
    def __init__(self, steps: List[Step], *, name: str = "pipeline") -> None:
        self.steps = steps
        self.name = name

    async def run(self, state: PipelineState) -> PipelineState:
        logger.info("[%s] starting", self.name, extra={"event_id": state.event_id})

        if _LOG_DATA:
            logger.debug(
                "[%s] input records=%s schema=%s",
                self.name,
                getattr(state, "metrics", {}).get("received", len(getattr(state, "records", []) or [])),
                getattr(state, "schema_version", None),
                extra={"event_id": state.event_id},
            )

        for step in self.steps:
            t0 = time.perf_counter()
            logger.debug("[%s] step start: %s", self.name, step.name, extra={"event_id": state.event_id})

            try:
                state = await step.run(state)
            except Exception:
                logger.exception(
                    "[%s] step failed: %s",
                    self.name,
                    step.name,
                    extra={"event_id": state.event_id, "step": step.name},
                )
                state.status = "rejected"
                state.errors.append({"code": "step_failed", "message": f"Step failed: {step.name}"})
                return state

            elapsed = int((time.perf_counter() - t0) * 1000)
            state.timings_ms[step.name] = elapsed
            logger.info(
                "[%s] step ok: %s (%sms)",
                self.name,
                step.name,
                elapsed,
                extra={"event_id": state.event_id, "step": step.name},
            )

            if _LOG_DATA:
                # lightweight step outputs
                data = {
                    "status": state.status,
                    "errors": len(state.errors),
                    "records": len(state.records or []),
                    "df_shape": getattr(getattr(state, "df", None), "shape", None),
                }
                logger.debug(
                    "[%s] step state: %s => %s",
                    self.name,
                    step.name,
                    _preview(data),
                    extra={"event_id": state.event_id, "step": step.name},
                )

            # allow a step to short-circuit (e.g., rejected/duplicate)
            if state.status in {"rejected", "duplicate"}:
                logger.info("[%s] short-circuit (%s)", self.name, state.status, extra={"event_id": state.event_id})
                return state

        state.status = "done" if state.status not in {"rejected", "duplicate"} else state.status
        logger.info("[%s] finished (%s)", self.name, state.status, extra={"event_id": state.event_id})
        return state
