from __future__ import annotations
from dataclasses import dataclass, field, asdict
from typing import Any, Callable, Dict, Iterable, List, Optional, Union
from datetime import datetime
import pandas as pd

# ---------- Record model (captures extras) ----------
@dataclass(frozen=True)
class EventDefault:
    #Core requirements for the event
    event_id: str
    journey_id: str
    step: str
    start_ts: datetime
    end_ts: datetime
    event_type: str
    event_description: str
    #Optional requirements for the event
    event_codes: Optional [Union[list, str]]
    journey_status: Optional[str] = None
    process_status: Optional[str] = None
    performed_by: Optional[str] = None
    agent_notes: Optional[str] = None
    decision: Optional[str] = None
    next_step: Optional[str] = None
    #Any Dims can be added in here for future merges, other variables that the customer wants on the event are stored in json.
    extra: Dict[str, Any] = field(default_factory=dict) 

    @classmethod
    def from_dict(cls, row: Dict[str, Any]) -> "EventDefault":
        # Known fields (everything except extra)
        known = {f for f in cls.__dataclass_fields__.keys() if f != "extra"}  # type: ignore[attr-defined]

        # Bucket unknown keys into extra
        extra = {k: v for k, v in row.items() if k not in known}

        def to_dt(x):
            if x is None or x == "":
                return None
            try:
                dt = pd.to_datetime(x, errors="coerce")
                return None if pd.isna(dt) else dt.to_pydatetime()
            except Exception:
                return None

        def norm_codes(x):
            # allow list OR scalar OR empty
            if x is None or x == "":
                return None
            if isinstance(x, list):
                return x
            # common CSV case: "A,B,C"
            if isinstance(x, str) and "," in x:
                return [p.strip() for p in x.split(",") if p.strip()]
            return [x]  # wrap scalar into list for consistency

        # Build core dict with explicit coercions
        core = {k: row.get(k) for k in known}

        # Coerce datetimes
        core["start_ts"] = to_dt(core.get("start_ts"))
        core["end_ts"] = to_dt(core.get("end_ts"))

        # Normalize codes
        core["event_codes"] = norm_codes(core.get("event_codes"))

        # Validate required fields early (clear errors)
        required = ["event_id", "journey_id", "step", "start_ts", "end_ts", "event_type", "event_description"]
        missing = [k for k in required if core.get(k) in (None, "")]
        if missing:
            raise TypeError(f"Missing required fields: {missing}")

        return cls(**core, extra=extra)