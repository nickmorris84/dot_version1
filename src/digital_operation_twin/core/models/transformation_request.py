from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Any, Callable, List
import pandas as pd
import logging

logger = logging.getLogger(__name__)

# =========================
# Request / Types
# =========================

@dataclass(frozen=True)
class TransformationRequest:
    """
    Standard request passed to every normalizer step.
    - df: current dataframe at that stage
    - cfg: full normalizer config (dict)
    """
    df: pd.DataFrame
    cfg: Dict[str, Any]

Issue = Dict[str, Any]
    
@dataclass
class TransformationResult:
    """
    Universal output for any step:
    - df: dataframe after step (validators usually return unchanged df)
    - issues: data quality findings (transforms usually return [])
    - meta: optional extra details (counts, applied mappings, etc.)
    """
    df: pd.DataFrame
    issues: List[Issue] = field(default_factory=list)
    meta: Dict[str, Any] = field(default_factory=dict)


# =========================
# Errors / Validation
# =========================

class TransformationConfigError(ValueError):
    pass


FieldMapper = Callable[[TransformationRequest], pd.DataFrame]