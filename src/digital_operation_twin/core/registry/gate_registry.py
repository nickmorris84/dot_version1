from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple
import pandas as pd
import logging
from functools import partial

from digital_operation_twin.core.models.processIO import ProcessFn
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
