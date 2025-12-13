from __future__ import annotations
from typing import Dict, Any, Tuple, Optional

from src.digital_operation_twin.core.models.event import Event

try:
    from pydantic import ValidationError
except Exception:  # pydantic might not be installed in some contexts
    ValidationError = Exception  # type: ignore


class DQError(Exception):
    def __init__(self, code: str, message: str, details: Dict[str, Any] | None = None):
        super().__init__(message)
        self.code = code
        self.details = details or {}


def schema_validation(payload: Dict[str, Any], schema_version: Optional[str]) -> Tuple[str, Dict[str, Any]]:
    """
    Returns (version_used, normalized_dict).
    """
    sv = schema_version or "v1"

    if sv == "v1":
        p = dict(payload)

        # Example normalization hooks (optional)
        # p["id"] = str(p["id"]).strip() if "id" in p else p.get("id")

        try:
            rec = Event(**p)
        except ValidationError as e:
            # Convert validation errors into pipeline DQ errors
            raise DQError(
                "schema_validation_failed",
                "Payload failed Event schema validation",
                {"schema_version": sv, "errors": getattr(e, "errors", lambda: str(e))()},
            )
        except Exception as e:
            raise DQError(
                "schema_validation_error",
                f"Unexpected error validating payload: {e}",
                {"schema_version": sv},
            )

        normalized = rec.model_dump() if hasattr(rec, "model_dump") else rec.dict()
        return "v1", normalized

    raise DQError(
        "unsupported_version",
        f"Unsupported schema_version: {sv}",
        {"schema_version": sv},
    )