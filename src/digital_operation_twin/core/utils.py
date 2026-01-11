# core/utils.py
from __future__ import annotations

import hashlib
import logging
from dataclasses import is_dataclass, fields as dc_fields
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Type, Literal, Union, Sequence
import logging
from datetime import datetime, timezone
from pathlib import Path

import yaml
import os
import json
import pandas as pd

from digital_operation_twin.core.models.pipeline_state import PipelineState


# Use your centralized logger if you have one; otherwise the stdlib logger:
logger = logging.getLogger(__name__)


def records_to_df(records: List[Dict[str, Any]]) -> pd.DataFrame:
    """Helper to move from gate shape (list[dict]) to transform shape (DataFrame)."""
    return pd.DataFrame.from_records(records)

def df_to_records(df: pd.DataFrame) -> List[Dict[str, Any]]:
    """Helper to move from transform shape (DataFrame) back to list[dict]."""
    return df.to_dict("records")

def utc_now() -> datetime:
    return datetime.now(timezone.utc)

Payload = Union[Dict[str, Any], List[Dict[str, Any]]]

def load_payload(path: str) -> Payload:
    return json.loads(Path(path).read_text())

def payload_to_df(payload: Payload) -> pd.DataFrame:
    if isinstance(payload, dict):
        return pd.DataFrame([payload])
    if isinstance(payload, list):
        return pd.DataFrame(payload)
    raise TypeError("Payload must be a dict or a list[dict].")

def load_input(input_path: str, kind: str) -> pd.DataFrame:
    p = Path(input_path)
    if kind == "csv":
        return pd.read_csv(p)
    if kind in ("record", "records"):
        payload = load_payload(input_path)
        return payload_to_df(payload)
    raise ValueError("kind must be one of: csv, record, records")

def summarize_nulls(df: pd.DataFrame, top_n: int = 10) -> Dict[str, int]:
    s = df.isna().sum().sort_values(ascending=False).head(top_n)
    return {k: int(v) for k, v in s.items() if v > 0}

def write_df(df: pd.DataFrame, out_dir: Path, name: str) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{name}.csv"
    df.to_csv(path, index=False)
    return path

def load_yaml_settings(env: str | None = None, path: str =  "config/app.yaml") -> Dict[str, Any]:
    base = Path(path)
    if not base.exists():
        raise FileNotFoundError(f"Missing {path}")
    with base.open("r") as f:
        data = yaml.safe_load(f) or {}

    if env:
        overlay = Path(f"config/{env}.yaml")
        if overlay.exists():
            with overlay.open("r") as f:
                extra = yaml.safe_load(f) or {}
            for k, v in extra.items():
                if isinstance(v, dict) and isinstance(data.get(k), dict):
                    data[k].update(v)
                else:
                    data[k] = v

    token = os.getenv("CSV_API_TOKEN")
    if token:
        data.setdefault("security", {})["csv_api_token"] = token

    db_url_env = os.getenv("DATABASE_URL")
    if db_url_env:
        data.setdefault("database", {})["url"] = db_url_env

    return data


# TRANSFORMATOION UTILS

from digital_operation_twin.core.models.processIO import TransformationRequest, ProcessConfigError


def step_cfg(req: TransformationRequest, key: str) -> Any:
    """
    Retrieve the config section for a given step from the shared request.
    Ensures the pipeline config is a dict and centralizes step-level access.
    """
    if not isinstance(req.cfg, dict):
        raise ProcessConfigError(
            f"Transformation cfg must be a dict, got: {type(req.cfg).__name__}"
        )
    return req.cfg.get(key)


def require_dict(value: Any, *, step: str, key: str) -> Dict[str, Any]:
    """
    Require a config value to exist and be a dictionary.
    Used for steps that expect a structured config block.
    """
    if value is None:
        raise ProcessConfigError(f"[{step}] Missing config section '{key}'.")
    if not isinstance(value, dict):
        raise ProcessConfigError(
            f"[{step}] '{key}' must be a dict, got: {type(value).__name__}"
        )
    return value


def require_bool(value: Any, *, step: str, key: str) -> bool:
    """
    Require a config value to be a boolean feature flag.
    Prevents ambiguous truthy / falsy configuration.
    """
    if not isinstance(value, bool):
        raise ProcessConfigError(
            f"[{step}] '{key}' must be a bool, got: {type(value).__name__}"
        )
    return value


def require_keys(d: Dict[str, Any], keys: List[str], *, step: str, key: str) -> None:
    """
    Ensure all required keys are present in a step’s config dictionary.
    Fails fast with a clear, step-scoped error if any are missing.
    """
    missing = [k for k in keys if k not in d]
    if missing:
        raise ProcessConfigError(
            f"[{step}] '{key}' missing required keys: {missing}"
        )


def ensure_columns_param(
    columns: Any,
    *,
    step: str,
    key_name: str = "columns"
) -> Union[List[str], str]:
    """
    Validate the standard 'columns' parameter, accepting '*' or list[str].
    Provides a consistent column-selection contract across all steps.
    """
    if columns == "*":
        return columns
    if isinstance(columns, list) and all(isinstance(x, str) for x in columns):
        return columns
    raise ProcessConfigError(
        f"[{step}] '{key_name}' must be '*' or list[str], got: {repr(columns)}"
    )


def resolve_columns(
    df: pd.DataFrame,
    columns: Union[List[str], str]
) -> List[str]:
    """
    Resolve a validated columns parameter into concrete dataframe columns.
    Expands '*' to all object/string columns by default.
    """
    if columns == "*":
        resolved = list(df.select_dtypes(include="object").columns)
    else:
        resolved = columns

    logger.debug(f"resolve_columns resolved: {resolved}")
    return resolved


def coerce_list_of_str(
    value: Any,
    *,
    step: str,
    key_name: str
) -> List[str]:
    """
    Normalize a config value into list[str], accepting either a list or a single string.
    Simplifies step logic while keeping config ergonomic.
    """
    if isinstance(value, list) and all(isinstance(x, str) for x in value):
        return value
    if isinstance(value, str):
        return [value]
    raise ProcessConfigError(
        f"[{step}] '{key_name}' must be list[str] or str, got: {type(value).__name__}"
    )
    
    
    
def to_snake_case_label(label: str) -> str:
    label = label.strip().replace(" ", "_")
    s1 = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", label)
    s2 = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s1)
    s3 = re.sub(r"__+", "_", s2)
    return s3.lower()


def diff_columns(before: List[str], after: List[str]) -> Dict[str, Any]:
    before_set, after_set = set(before), set(after)
    return {
        "added": sorted(after_set - before_set),
        "removed": sorted(before_set - after_set),
    }


def diff_dtypes(before: pd.Series, after: pd.Series) -> Dict[str, str]:
    changed: Dict[str, str] = {}
    common = set(before.index).intersection(set(after.index))
    for col in common:
        b, a = str(before[col]), str(after[col])
        if b != a:
            changed[col] = f"{b} -> {a}"
    return changed


# ---------------------------------------------------------------------
# GATE UTILS

from digital_operation_twin.core.models.processIO import ProcessConfigError, Mode


# ---------------------------------------------------------------------
# Model helpers (pydantic v2/v1 + dataclass fallback)
# ---------------------------------------------------------------------
def _model_field_sets(model: Type[Any]) -> Tuple[Set[str], Set[str]]:
    """Return (required_fields, all_fields) for Pydantic v2/v1 models; dataclasses supported for allowlist only."""
    # pydantic v2
    if hasattr(model, "model_fields"):
        mf = getattr(model, "model_fields")
        all_fields = set(mf.keys())
        required: Set[str] = set()
        for name, finfo in mf.items():
            is_req = getattr(finfo, "is_required", None)
            if callable(is_req) and is_req():
                required.add(name)
        return required, all_fields

    # pydantic v1
    if hasattr(model, "__fields__"):
        f = getattr(model, "__fields__")
        all_fields = set(f.keys())
        required = {name for name, finfo in f.items() if getattr(finfo, "required", False)}
        return required, all_fields

    # dataclass fallback
    if is_dataclass(model):
        all_fields = {f.name for f in dc_fields(model)}
        return set(), all_fields

    return set(), set()


def _normalize_instance(obj: Any) -> Dict[str, Any]:
    """Convert an instantiated model into a dict (pydantic v2/v1, dataclass fallback)."""
    if hasattr(obj, "model_dump") and callable(getattr(obj, "model_dump")):
        return obj.model_dump()
    if hasattr(obj, "dict") and callable(getattr(obj, "dict")):
        return obj.dict()
    if is_dataclass(obj):
        return {f.name: getattr(obj, f.name) for f in dc_fields(obj)}
    return dict(obj)


def validate_one_record_against_model(
    *,
    model: Type[Any],
    payload: Dict[str, Any],
    schema_version: Optional[str],
    mode: Mode,
    supported_versions: Optional[Set[str]] = None,
) -> Tuple[str, Dict[str, Any], Dict[str, Any]]:
    """
    Returns (version_used, normalized_dict, diff). Raises GateError in enforce mode on validation failure.
    """
    sv = schema_version or "v1"
    if supported_versions is not None and sv not in supported_versions:
        raise ProcessConfigError("unsupported_version", f"Unsupported schema_version: {sv}", {"schema_version": sv})

    p = dict(payload)
    required, allowed = _model_field_sets(model)
    present = set(p.keys())

    diff = {
        "model": getattr(model, "__name__", str(model)),
        "missing_required": sorted(required - present),
        "extra_fields": sorted(present - allowed) if allowed else [],
        "present_fields": sorted(present),
    }

    if mode == "report":
        return sv, p, diff

    # enforce mode
    try:
        rec = model(**p)
    except Exception as e:
        raise ProcessConfigError(
            "schema_validation_failed",
            f"Payload failed {diff['model']} schema validation",
            {"schema_version": sv, "diff": diff, "error": str(e)},
        )

    return sv, _normalize_instance(rec), diff



########## DATA FRAME UTILS #############


from typing import Any, Dict, Optional, Sequence
import pandas as pd
import hashlib
import json
import time


def _safe_json(obj: Any) -> str:
    try:
        return json.dumps(obj, default=str, sort_keys=True)
    except Exception:
        return str(obj)


def df_fingerprint(
    df: pd.DataFrame,
    *,
    sample_rows: int = 50,
    key_cols: Optional[Sequence[str]] = None,
) -> Dict[str, Any]:
    """
    Lightweight fingerprint for debugging:
      - shape, columns, dtypes
      - SHA256 hash of a small stable sample
    """
    info: Dict[str, Any] = {
        "shape": tuple(df.shape),
        "columns": list(df.columns),
        "dtypes": {c: str(t) for c, t in df.dtypes.items()},
        "sample_rows": min(sample_rows, len(df)),
        "key_cols": list(key_cols) if key_cols else None,
    }

    sample = df
    if key_cols and all(c in df.columns for c in key_cols):
        # stable sort so sample hash is repeatable
        sample = df.sort_values(list(key_cols), kind="mergesort")

    sample = sample.head(sample_rows)
    payload = _safe_json(sample.to_dict(orient="records"))
    info["sample_hash"] = hashlib.sha256(payload.encode("utf-8")).hexdigest()
    return info


def df_change_summary(
    before: pd.DataFrame,
    after: pd.DataFrame,
    *,
    null_cols: Optional[Sequence[str]] = None,
) -> Dict[str, Any]:
    """
    Summary of what changed, without storing the full df.
    """
    before_cols = list(before.columns)
    after_cols = list(after.columns)

    added = sorted(set(after_cols) - set(before_cols))
    removed = sorted(set(before_cols) - set(after_cols))

    dtype_changes: Dict[str, Dict[str, str]] = {}
    common = set(before_cols) & set(after_cols)
    for c in common:
        bt, at = str(before.dtypes[c]), str(after.dtypes[c])
        if bt != at:
            dtype_changes[c] = {"before": bt, "after": at}

    out: Dict[str, Any] = {
        "shape_before": tuple(before.shape),
        "shape_after": tuple(after.shape),
        "rows_delta": after.shape[0] - before.shape[0],
        "cols_delta": after.shape[1] - before.shape[1],
        "cols_added": added,
        "cols_removed": removed,
        "dtype_changes": dtype_changes,
    }

    if null_cols:
        null_delta: Dict[str, Dict[str, int]] = {}
        for c in null_cols:
            if c in before.columns and c in after.columns:
                b = int(before[c].isna().sum())
                a = int(after[c].isna().sum())
                null_delta[c] = {"before_nulls": b, "after_nulls": a, "delta": a - b}
        out["null_delta"] = null_delta

    return out


def ensure_audit_root(state: Any) -> Dict[str, Any]:
    state.context.setdefault("audit", {})
    audit = state.context["audit"]
    audit.setdefault("original", {})
    audit.setdefault("segments", {})
    return audit


def capture_original_once(
    state: Any,
    df: pd.DataFrame,
    *,
    keep_df: bool = False,
    key_cols: Optional[Sequence[str]] = None,
) -> None:
    """
    Stores original snapshot only once.
    Default behavior stores a fingerprint (cheap). Optionally store full df.
    """
    audit = ensure_audit_root(state)
    orig = audit["original"]

    if "df_fingerprint" not in orig:
        orig["df_fingerprint"] = df_fingerprint(df, key_cols=key_cols)
        if keep_df:
            orig["df"] = df.copy(deep=True)


def start_segment_audit(
    state: Any,
    *,
    segment_name: str,
    df_in: pd.DataFrame,
    key_cols: Optional[Sequence[str]] = None,
) -> float:
    """
    Starts audit tracking for a segment. Returns start time to compute duration.
    """
    audit = ensure_audit_root(state)
    seg = audit["segments"].setdefault(segment_name, {})
    seg["input_fingerprint"] = df_fingerprint(df_in, key_cols=key_cols)
    return time.perf_counter()


def end_segment_audit(
    state: Any,
    *,
    segment_name: str,
    df_before: pd.DataFrame,
    df_after: pd.DataFrame,
    t0: float,
    null_cols: Optional[Sequence[str]] = None,
    key_cols: Optional[Sequence[str]] = None,
) -> None:
    """
    Finalizes segment audit: output fingerprint, change summary, duration.
    """
    audit = ensure_audit_root(state)
    seg = audit["segments"].setdefault(segment_name, {})

    seg["output_fingerprint"] = df_fingerprint(df_after, key_cols=key_cols)
    seg["change_summary"] = df_change_summary(df_before, df_after, null_cols=null_cols)
    seg["duration_ms"] = int((time.perf_counter() - t0) * 1000)

    
    
def capture_original_df_once(state: PipelineState, df: pd.DataFrame) -> None:
    """
    Store a one-time snapshot of the original dataframe for later comparison/debugging.
    Keeps state.df as the latest/current dataframe.
    """
    state.context.setdefault("original", {})
    if "df" not in state.context["original"]:
        state.context["original"]["df"] = df.copy(deep=True)
        state.context["original"]["df_shape"] = df.shape
        state.context["original"]["df_columns"] = list(df.columns)

