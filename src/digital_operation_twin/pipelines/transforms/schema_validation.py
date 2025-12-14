from __future__ import annotations
from typing import Dict, Any, Tuple, Optional, Literal, Set

from digital_operation_twin.core.models.event import Event

try:
    from pydantic import ValidationError
except Exception:
    ValidationError = Exception  # type: ignore


class DQError(Exception):
    def __init__(self, code: str, message: str, details: Dict[str, Any] | None = None):
        super().__init__(message)
        self.code = code
        self.details = details or {}


Mode = Literal["report", "enforce"]


def _event_field_sets() -> tuple[Set[str], Set[str]]:
    """
    Returns (required_fields, all_fields) for Event.
    Works for pydantic v1/v2.
    """
    # pydantic v2
    if hasattr(Event, "model_fields"):
        fields = Event.model_fields  # type: ignore[attr-defined]
        all_fields = set(fields.keys())
        required = {k for k, f in fields.items() if getattr(f, "is_required", lambda: False)()}
        return required, all_fields

    # pydantic v1 fallback
    if hasattr(Event, "__fields__"):
        fields = Event.__fields__  # type: ignore[attr-defined]
        all_fields = set(fields.keys())
        required = {k for k, f in fields.items() if getattr(f, "required", False)}
        return required, all_fields

    # if Event isn't pydantic (unlikely in your setup)
    return set(), set()


def schema_validation(
    payload: Dict[str, Any],
    schema_version: Optional[str],
    *,
    mode: Mode = "enforce",
) -> Tuple[str, Dict[str, Any], Dict[str, Any]]:
    """
    Returns (version_used, normalized_dict, diff)

    diff = {
      "missing_required": [...],
      "extra_fields": [...],
      "present_fields": [...],
    }

    - report: never raises; normalized_dict is original payload (shallow copy)
    - enforce: validates against Event, raises DQError on failure
    """
    sv = schema_version or "v1"
    if sv != "v1":
        raise DQError("unsupported_version", f"Unsupported schema_version: {sv}", {"schema_version": sv})

    p = dict(payload)
    required, allowed = _event_field_sets()
    present = set(p.keys())

    missing_required = sorted(list(required - present))
    extra_fields = sorted(list(present - allowed))

    diff = {
        "missing_required": missing_required,
        "extra_fields": extra_fields,
        "present_fields": sorted(list(present)),
    }

    if mode == "report":
        # Do not validate or normalize; just return diff
        return sv, p, diff

    # enforce mode
    try:
        rec = Event(**p)
    except ValidationError as e:
        raise DQError(
            "schema_validation_failed",
            "Payload failed Event schema validation",
            {"schema_version": sv, "diff": diff, "errors": getattr(e, "errors", lambda: str(e))()},
        )
    except Exception as e:
        raise DQError(
            "schema_validation_error",
            f"Unexpected error validating payload: {e}",
            {"schema_version": sv, "diff": diff},
        )

    normalized = rec.model_dump() if hasattr(rec, "model_dump") else rec.dict()
    return sv, normalized, diff
