# core/utils.py
from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Union
import yaml
import os
import json
import pandas as pd


# Use your centralized logger if you have one; otherwise the stdlib logger:
logger = logging.getLogger(__name__)


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

from digital_operation_twin.core.models.transformation_request import TransformationRequest, TransformationConfigError


def step_cfg(req: TransformationRequest, key: str) -> Any:
    """
    Retrieve the config section for a given step from the shared request.
    Ensures the pipeline config is a dict and centralizes step-level access.
    """
    if not isinstance(req.cfg, dict):
        raise TransformationConfigError(
            f"Transformation cfg must be a dict, got: {type(req.cfg).__name__}"
        )
    return req.cfg.get(key)


def require_dict(value: Any, *, step: str, key: str) -> Dict[str, Any]:
    """
    Require a config value to exist and be a dictionary.
    Used for steps that expect a structured config block.
    """
    if value is None:
        raise TransformationConfigError(f"[{step}] Missing config section '{key}'.")
    if not isinstance(value, dict):
        raise TransformationConfigError(
            f"[{step}] '{key}' must be a dict, got: {type(value).__name__}"
        )
    return value


def require_bool(value: Any, *, step: str, key: str) -> bool:
    """
    Require a config value to be a boolean feature flag.
    Prevents ambiguous truthy / falsy configuration.
    """
    if not isinstance(value, bool):
        raise TransformationConfigError(
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
        raise TransformationConfigError(
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
    raise TransformationConfigError(
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
    raise TransformationConfigError(
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