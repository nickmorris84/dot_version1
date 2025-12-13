from __future__ import annotations
from typing import Dict, Any, Tuple
from ...services.initialise_events_service import Event

class DQError(Exception):
    def __init__(self, code: str, message: str, details: Dict[str, Any] | None = None):
        super().__init__(message)
        self.code = code
        self.details = details or {}

def normalize_and_validate(payload: Dict[str, Any], schema_version: str) -> Tuple[str, Dict[str, Any]]:
    """
    Returns (version_used, normalized_dict).
    You can add more version handlers as you evolve the model.
    """
    if schema_version in (None, "", "v1"):
        # --- light normalization examples ---
        p = dict(payload)

        # strict validation now
        rec = Event(**p)
        return "v1", rec.dict()

    # Unknown version → treat as DQ failure
    raise DQError("unsupported_version", f"Unsupported schema_version: {schema_version}", {"schema_version": schema_version})
