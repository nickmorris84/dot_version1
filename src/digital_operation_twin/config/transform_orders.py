from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple
import pandas as pd
import logging

logger = logging.getLogger(__name__)

# ============================================================
# Example: registries + orders per package
# (wire these to your actual step factories)
# ============================================================

NORMALIZER_ORDER: Tuple[str, ...] = (
    "trim_strings",
    "normalize_case",
    "remove_special_chars",
    "replace_nulls",
    "standardize_booleans",
    "clean_currency",
    "normalize_dates",
)

STANDARDIZER_ORDER: Tuple[str, ...] = (
    "rename",
    "to_snake_case",
    "drop_columns",
    "value_map",
    "cast",
    "fill_defaults",
    "require",
)

VALIDATOR_ORDER: Tuple[str, ...] = (
    "check_required_columns",
    "check_nulls",
    "check_value_range",
)