from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Literal, Optional, TypeVar
import pandas as pd

from digital_operation_twin.core.models.pipeline_state import PipelineState

import logging

logger = logging.getLogger(__name__)

result_status = Literal["continue", "accepted", "rejected"]
ResultStatus = Literal["continue", "accepted", "rejected", "duplicate"]
Mode = Literal["report", "enforce"]

@dataclass(frozen=True)
class ApiGateRequest: 
    msg: Any
    cfg: Dict[str, Any]
    step_type: str = field(default="gate", init=False)

@dataclass(frozen=True)
class TransformationRequest:
    state: PipelineState
    cfg: Dict[str, Any] = field(default_factory=dict)
    step_type: str = field(default="transformation", init=False)

# ============================================================
# Standard result (same everywhere)
# ============================================================

@dataclass
class ProcessResult:
    """
    Standard result for all steps across all segments.
    Runner merges per-step meta/data into acc.meta['__steps__'] / acc.data['__steps__'].
    """
    state: PipelineState
    status: ResultStatus = "continue"

    # Optional convenience mirrors (state is still source of truth)
    records: Optional[List[Dict[str, Any]]] = None
    df: Optional[pd.DataFrame] = None
    metrics: Dict[str, Any] = field(default_factory=dict)

    errors: List[Dict[str, Any]] = field(default_factory=list)
    meta: Dict[str, Any] = field(default_factory=dict)
    data: Dict[str, Any] = field(default_factory=dict)

    def reject(self, *, code: str, message: str, **details: Any) -> "ProcessResult":
        self.status = "rejected"
        self.errors.append({"code": code, "message": message, **details})
        return self

    def accept(self) -> "ProcessResult":
        self.status = "accepted"
        return self


ProcessFn = Callable[[Any], ProcessResult]
Registry = Dict[str, Callable[[Any], ProcessFn]]
ReqType = TypeVar("ReqType")


class ProcessConfigError(ValueError):
    pass

