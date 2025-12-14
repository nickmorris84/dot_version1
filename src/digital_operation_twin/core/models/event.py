from __future__ import annotations
from dataclasses import dataclass, field, asdict
from typing import Any, Callable, Dict, Iterable, List, Optional, Union
from datetime import datetime
import pandas as pd

# ---------- Record model (captures extras) ----------
@dataclass(frozen=True)
class Event:
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
    def from_dict(cls, row: Dict[str, Any]) -> "Event":
        # map known fields; bucket everything else into .extra
        known = {f for f in cls.__dataclass_fields__.keys() if f != "extra"}  # type: ignore[attr-defined]
        core = {k: row.get(k) for k in known if k in row}
        extra = {k: v for k, v in row.items() if k not in known}

        # light datetime coercion
        def to_dt(x):
            try:
                return pd.to_datetime(x, errors="coerce").to_pydatetime() if x is not None else None
            except Exception:
                return None

        return cls(**core, extra=extra)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)