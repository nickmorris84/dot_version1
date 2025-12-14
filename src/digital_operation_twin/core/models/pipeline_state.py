from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
import pandas as pd


@dataclass
class PipelineState:
    # identifiers / metadata
    event_id: str
    schema_version: str = "v1"
    customer_id: Optional[str] = None

    # data representations (use whichever is available at a given stage)
    records: List[Dict[str, Any]] = field(default_factory=list)   # list-of-dicts (edge/gate)
    df: Optional[pd.DataFrame] = None                              # DataFrame (heavy transform)

    # results / observability
    status: str = "init"                                           # init|accepted|rejected|running|done|failed
    errors: List[Dict[str, Any]] = field(default_factory=list)
    metrics: Dict[str, Any] = field(default_factory=dict)
    timings_ms: Dict[str, int] = field(default_factory=dict)

    # carry config & misc context
    cfg: Dict[str, Any] = field(default_factory=dict)
    context: Dict[str, Any] = field(default_factory=dict)
