from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple
import pandas as pd
import logging

from digital_operation_twin.core.models.processIO import ProcessFn
from digital_operation_twin.pipelines.gates.schema_validation import *
from digital_operation_twin.pipelines.gates.gate_processes import *
from digital_operation_twin.core.models.shema.default_event import EventDefault


API_GATE_REGISTRY: Dict[str, Callable[[], ProcessFn]] = {
    "extract_envelope": extract_envelope,
    "normalize_payload": normalize_payload,
    "idempotency": idempotency,
}

API_GATE_ORDER: Tuple[str, ...] = (
    "extract_envelope",
    "normalize_payload",
    "idempotency",
    "schema_validation",

)

SCHEMA_REGISTRY = {
    
    # ("cust_a", "v1"): EventCustAV1,
    # ("cust_a", "v2"): EventCustAV2,
    # ("cust_b", "v1"): EventCustBV1,
    
    ("__default__", "__default__"): EventDefault,
}

SCHEMA_VALIDATION_REGISTRY: Dict[str, Callable[[], ProcessFn]] = {
    # schema validation at different points
    "schema_validation_gate": schema_validation(
        schema_registry=SCHEMA_REGISTRY,
        cfg_key="schema_validation_gate",
        metric_key="schema_report_gate",
    ),
    "schema_validation_post_transform": schema_validation(
        schema_registry=SCHEMA_REGISTRY,
        cfg_key="schema_validation_post_transform",
        metric_key="schema_report_post_transform",
    ),
}

SCHEMA_VALIDATION_ORDER: Tuple[str, ...] = (
    "schema_validation_gate",
    "schema_validation_post_transform",
)

