from __future__ import annotations

from dataclasses import MISSING, fields, is_dataclass, field
from dataclasses import dataclass
from typing import Any, Callable, Dict, Generic, List, Optional, Sequence, Type, TypeVar, Union, Literal
import logging

# Import your actual types
from digital_operation_twin.core.models.processIO import ApiGateRequest,TransformationRequest, ReqType
from digital_operation_twin.core.models.pipeline_state import PipelineState

logger = logging.getLogger(__name__)

ProcessType = Literal["gate", "transform"]

class ProcessAdapterError(ValueError):
    """Raised when adapter cannot build request/result or validation fails."""
    pass


# ============================================================
# Dataclass-driven builder helpers
# ============================================================

def _required_field_names(cls: Type[Any]) -> set[str]:
    req: set[str] = set()
    for f in fields(cls):
        if f.default is MISSING and f.default_factory is MISSING:  # type: ignore
            req.add(f.name)
    return req

def _all_field_names(cls: Type[Any]) -> set[str]:
    return {f.name for f in fields(cls)}

def _looks_like_df(x: Any) -> bool:
    return hasattr(x, "shape") and hasattr(x, "columns") and hasattr(x, "dtypes")

def build_dataclass(*, cls: Type[ReqType], strict_unknown: bool, context: str, **kwargs: Any) -> ReqType:
    if not is_dataclass(cls):
        raise ProcessAdapterError(f"[{context}] cls must be a dataclass, got: {cls}")

    all_names = _all_field_names(cls)
    required = _required_field_names(cls)

    missing = sorted([k for k in required if k not in kwargs])
    if missing:
        raise ProcessAdapterError(f"[{context}] Missing required fields for {cls.__name__}: {missing}")

    if strict_unknown:
        unknown = sorted([k for k in kwargs.keys() if k not in all_names])
        if unknown:
            raise ProcessAdapterError(f"[{context}] Unknown fields for {cls.__name__}: {unknown}. Allowed={sorted(all_names)}")

    payload = {k: v for k, v in kwargs.items() if k in all_names}
    logger.debug("[%s] build_dataclass cls=%s fields=%s", context, cls.__name__, sorted(payload.keys()))
    return cls(**payload)  # type: ignore[arg-type]

# ============================================================
# Adapter: builds request from (state + inputs) using request dataclass
# ============================================================

@dataclass(frozen=True)
class ProcessIOAdapter(Generic[ReqType]):
    kind: Literal["gate", "transform"]
    request_cls: Type[ReqType]
    strict_unknown_input: bool = True
    validators: Dict[str, Callable[[Any], bool]] = field(default_factory=dict)

    def build_request(self, *, state: PipelineState, cfg: Dict[str, Any], segment_name: str, inputs: Dict[str, Any]) -> ReqType:
        if not isinstance(inputs, dict):
            raise ProcessAdapterError(f"[{segment_name}:{self.kind}] inputs must be dict, got: {type(inputs).__name__}")

        merged = {"state": state, **inputs, "cfg": cfg}

        for k, fn in self.validators.items():
            if k in merged and not fn(merged[k]):
                raise ProcessAdapterError(f"[{segment_name}:{self.kind}] Invalid '{k}' (validator failed). type={type(merged[k]).__name__}")

        return build_dataclass(
            cls=self.request_cls,
            strict_unknown=self.strict_unknown_input,
            context=f"{segment_name}:{self.kind}:request",
            **merged,
        )

# Concrete adapters
GATE_ADAPTER = ProcessIOAdapter[ApiGateRequest](
    kind="gate",
    request_cls=ApiGateRequest,
    validators={"msg": lambda v: v is not None},
)

TRANSFORM_ADAPTER = ProcessIOAdapter[TransformationRequest](
    kind="transform",
    request_cls=TransformationRequest,
    validators={"df": _looks_like_df},
)
