from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple
import pandas as pd
import logging
from functools import partial

from digital_operation_twin.core.models.processIO import ProcessFn
from digital_operation_twin.pipelines.gates.schema_processes import *
from digital_operation_twin.core.models.shema.default_event import EventDefault

def make_schema_step(step_name: str):
    fn = schema_validation(schema_registry=SCHEMA_REGISTRY)
    fn.__name__ = step_name
    return fn

SCHEMA_REGISTRY = {
    
    # ("cust_a", "v1"): EventCustAV1,
    # ("cust_a", "v2"): EventCustAV2,
    # ("cust_b", "v1"): EventCustBV1,
    
    ("__default__", "__default__"): EventDefault,
}

SCHEMA_VALIDATION_REGISTRY: Dict[str, Callable[[], ProcessFn]] = {
    "schema_validation_gate": lambda: make_schema_step("schema_validation_gate"),
    "schema_validation_post_transformation": lambda: make_schema_step("schema_validation_post_transformation"),
}

SCHEMA_VALIDATION_ORDER: Tuple[str, ...] = (
    "schema_validation_gate",
    "schema_validation_post_transformation",
)