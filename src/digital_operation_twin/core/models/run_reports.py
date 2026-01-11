from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Protocol
import pandas as pd

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