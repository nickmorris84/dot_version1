from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple
import pandas as pd
import logging

from digital_operation_twin.core.models.transformation_request import FieldMapper
from digital_operation_twin.pipelines.transforms.normaliser_steps import *
from digital_operation_twin.pipelines.transforms.standardiser_steps import *
from digital_operation_twin.pipelines.transforms.validator_steps import *


# Example registries (import your actual functions in real usage)
NORMALIZER_REGISTRY: Dict[str, Callable[[], FieldMapper]] = {
    "trim_strings": trim_strings,
    "normalize_case": normalize_case,
    "remove_special_chars": remove_special_chars,
    "replace_nulls": replace_nulls,
    "standardize_booleans": standardize_booleans,
    "clean_currency": clean_currency,
    "normalize_dates": normalize_dates,
}

STANDARDIZER_REGISTRY: Dict[str, Callable[[], FieldMapper]] = {
    "rename": rename,
    "to_snake_case": to_snake_case,
    "drop_columns": drop_columns,
    "value_map": value_map,
    "cast": cast,
    "fill_defaults": fill_defaults,
    "require": require,
}

VALIDATOR_REGISTRY: Dict[str, Callable[[], FieldMapper]] = {
    "check_required_columns": check_required_columns,
    "check_nulls": check_nulls,
    "check_value_range": check_value_range,
}