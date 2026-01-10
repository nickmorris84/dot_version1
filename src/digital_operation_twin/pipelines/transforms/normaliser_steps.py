from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Dict, Any, List, Union, Optional
import pandas as pd
import logging
import re

from digital_operation_twin.core.models.transformation_request import TransformationRequest, FieldMapper, TransformationConfigError, TransformationResult
from digital_operation_twin.core.utils import step_cfg, ensure_columns_param, resolve_columns, require_dict, require_keys, coerce_list_of_str

logger = logging.getLogger(__name__)



# ============================================================
# NORMALIZER steps (now all: FieldMapper(req)->TransformationResult)
# ============================================================

def trim_strings() -> FieldMapper:
    """
    Config:
      trim_strings: true
    Or:
      trim_strings:
        columns: "*"
    Or:
      trim_strings:
        columns: ["name","email"]
    """
    step = "trim_strings"

    def _fn(req: TransformationRequest) -> TransformationResult:
        logger.debug(f"[{step}] START")

        enabled = step_cfg(req, step)
        if enabled is True:
            columns_param: Union[List[str], str] = "*"
        elif isinstance(enabled, dict):
            columns_param = ensure_columns_param(enabled.get("columns", "*"), step=step)
        else:
            raise TransformationConfigError(
                f"[{step}] Must be 'true' or a dict (e.g., {{columns: '*'}}). Got: {repr(enabled)}"
            )

        out = req.df.copy()
        cols_to_use = resolve_columns(out, columns_param)

        updated_cols: List[str] = []
        for col in cols_to_use:
            if col in out.columns:
                out[col] = out[col].astype(str).str.strip()
                updated_cols.append(col)

        logger.debug(f"[{step}] DONE | columns_param={columns_param!r} | updated_cols={updated_cols} | shape={out.shape}")
        return TransformationResult(df=out, issues=[], meta={"columns_param": columns_param, "updated_cols": updated_cols})

    _fn.__name__ = step
    return _fn


def normalize_case() -> FieldMapper:
    """
    Config:
      normalize_case:
        columns: ["name","email"]
        case: lower   # lower|upper|title (optional; default lower)
    """
    step = "normalize_case"

    def _fn(req: TransformationRequest) -> TransformationResult:
        logger.debug(f"[{step}] START")

        cfg = require_dict(step_cfg(req, step), step=step, key=step)
        require_keys(cfg, ["columns"], step=step, key=step)

        columns_param = ensure_columns_param(cfg.get("columns"), step=step)
        case = cfg.get("case", "lower")
        if case not in {"lower", "upper", "title"}:
            raise TransformationConfigError(
                f"[{step}] 'case' must be one of lower|upper|title, got: {repr(case)}"
            )

        out = req.df.copy()
        cols_to_use = resolve_columns(out, columns_param)

        updated_cols: List[str] = []
        for col in cols_to_use:
            if col in out.columns:
                s = out[col].astype(str)
                if case == "lower":
                    out[col] = s.str.lower()
                elif case == "upper":
                    out[col] = s.str.upper()
                else:
                    out[col] = s.str.title()
                updated_cols.append(col)

        logger.debug(
            f"[{step}] DONE | case={case!r} | columns_param={columns_param!r} | updated_cols={updated_cols} | shape={out.shape}"
        )
        return TransformationResult(
            df=out,
            issues=[],
            meta={"columns_param": columns_param, "case": case, "updated_cols": updated_cols},
        )

    _fn.__name__ = step
    return _fn


def remove_special_chars() -> FieldMapper:
    """
    Config:
      remove_special_chars:
        columns: ["name","comments"]
        # Provide ONE of:
        # pattern: "[^a-zA-Z0-9 ]"
        # allowed_chars: "\\w\\s"
    """
    step = "remove_special_chars"

    def _fn(req: TransformationRequest) -> TransformationResult:
        logger.debug(f"[{step}] START")

        cfg = require_dict(step_cfg(req, step), step=step, key=step)
        require_keys(cfg, ["columns"], step=step, key=step)

        columns_param = ensure_columns_param(cfg.get("columns"), step=step)

        pattern = cfg.get("pattern")
        allowed_chars = cfg.get("allowed_chars")

        if pattern is None and allowed_chars is None:
            raise TransformationConfigError(f"[{step}] Provide either 'pattern' or 'allowed_chars'.")

        if pattern is not None and not isinstance(pattern, str):
            raise TransformationConfigError(f"[{step}] 'pattern' must be a str, got: {type(pattern).__name__}")

        if allowed_chars is not None and not isinstance(allowed_chars, str):
            raise TransformationConfigError(f"[{step}] 'allowed_chars' must be a str, got: {type(allowed_chars).__name__}")

        if pattern is None:
            pattern = rf"[^{allowed_chars}]"

        try:
            re.compile(pattern)
        except re.error as e:
            raise TransformationConfigError(f"[{step}] Invalid regex pattern: {pattern}. Error: {e}")

        out = req.df.copy()
        cols_to_use = resolve_columns(out, columns_param)

        updated_cols: List[str] = []
        for col in cols_to_use:
            if col in out.columns:
                out[col] = out[col].astype(str).str.replace(pattern, "", regex=True)
                updated_cols.append(col)

        logger.debug(
            f"[{step}] DONE | columns_param={columns_param!r} | pattern={pattern!r} | updated_cols={updated_cols} | shape={out.shape}"
        )
        return TransformationResult(
            df=out,
            issues=[],
            meta={"columns_param": columns_param, "pattern": pattern, "updated_cols": updated_cols},
        )

    _fn.__name__ = step
    return _fn


def replace_nulls() -> FieldMapper:
    """
    Config:
      replace_nulls:
        columns: "*"
        null_values: ["", "n/a", "na", "null"]
    """
    step = "replace_nulls"

    def _fn(req: TransformationRequest) -> TransformationResult:
        logger.debug(f"[{step}] START")

        cfg = require_dict(step_cfg(req, step), step=step, key=step)
        require_keys(cfg, ["columns", "null_values"], step=step, key=step)

        columns_param = ensure_columns_param(cfg.get("columns"), step=step)
        null_values = coerce_list_of_str(cfg.get("null_values"), step=step, key_name="null_values")

        out = req.df.copy()
        cols_to_use = resolve_columns(out, columns_param)

        updated_cols: List[str] = []
        for col in cols_to_use:
            if col in out.columns:
                out[col] = out[col].replace(null_values, pd.NA)
                updated_cols.append(col)

        logger.debug(
            f"[{step}] DONE | columns_param={columns_param!r} | null_values={null_values!r} | updated_cols={updated_cols} | shape={out.shape}"
        )
        return TransformationResult(
            df=out,
            issues=[],
            meta={"columns_param": columns_param, "null_values": null_values, "updated_cols": updated_cols},
        )

    _fn.__name__ = step
    return _fn


def standardize_booleans() -> FieldMapper:
    """
    Config:
      standardize_booleans: true
    Or:
      standardize_booleans:
        columns: ["is_active"]
        true_vals: ["yes","y","true","1"]
        false_vals: ["no","n","false","0"]
    """
    step = "standardize_booleans"

    def _fn(req: TransformationRequest) -> TransformationResult:
        logger.debug(f"[{step}] START")

        cfg = step_cfg(req, step)

        if cfg is True:
            cfg_dict: Dict[str, Any] = {}
            cfg_mode = "defaults"
        else:
            cfg_dict = require_dict(cfg, step=step, key=step)
            cfg_mode = "custom"

        columns_param = ensure_columns_param(cfg_dict.get("columns", "*"), step=step)
        true_vals = coerce_list_of_str(
            cfg_dict.get("true_vals", ["yes", "y", "true", "1"]),
            step=step,
            key_name="true_vals",
        )
        false_vals = coerce_list_of_str(
            cfg_dict.get("false_vals", ["no", "n", "false", "0"]),
            step=step,
            key_name="false_vals",
        )

        true_set = {v.strip().lower() for v in true_vals}
        false_set = {v.strip().lower() for v in false_vals}

        out = req.df.copy()
        cols_to_use = resolve_columns(out, columns_param)

        updated_cols: List[str] = []
        for col in cols_to_use:
            if col in out.columns:
                s = out[col].astype(str).str.strip().str.lower()
                out[col] = s.map(lambda x: True if x in true_set else (False if x in false_set else pd.NA))
                updated_cols.append(col)

        logger.debug(
            f"[{step}] DONE | cfg_mode={cfg_mode} | columns_param={columns_param!r} | updated_cols={updated_cols} | "
            f"true_vals={sorted(true_set)!r} | false_vals={sorted(false_set)!r} | shape={out.shape}"
        )
        return TransformationResult(
            df=out,
            issues=[],
            meta={
                "cfg_mode": cfg_mode,
                "columns_param": columns_param,
                "updated_cols": updated_cols,
                "true_vals": sorted(true_set),
                "false_vals": sorted(false_set),
            },
        )

    _fn.__name__ = step
    return _fn


def clean_currency() -> FieldMapper:
    """
    Config:
      clean_currency:
        columns: ["amount","balance"]
        # optional:
        # strip_pattern: "[£$,]"
    """
    step = "clean_currency"

    def _fn(req: TransformationRequest) -> TransformationResult:
        logger.debug(f"[{step}] START")

        cfg = require_dict(step_cfg(req, step), step=step, key=step)
        require_keys(cfg, ["columns"], step=step, key=step)

        columns_param = ensure_columns_param(cfg.get("columns"), step=step)

        strip_pattern = cfg.get("strip_pattern", r"[\$,]")
        if not isinstance(strip_pattern, str):
            raise TransformationConfigError(f"[{step}] 'strip_pattern' must be str, got: {type(strip_pattern).__name__}")

        out = req.df.copy()
        cols_to_use = resolve_columns(out, columns_param)

        updated_cols: List[str] = []
        for col in cols_to_use:
            if col in out.columns:
                cleaned = (
                    out[col]
                    .astype(str)
                    .str.replace(strip_pattern, "", regex=True)
                    .str.replace(" ", "", regex=False)
                )
                out[col] = pd.to_numeric(cleaned, errors="coerce")
                updated_cols.append(col)

        logger.debug(
            f"[{step}] DONE | columns_param={columns_param!r} | strip_pattern={strip_pattern!r} | updated_cols={updated_cols} | shape={out.shape}"
        )
        return TransformationResult(
            df=out,
            issues=[],
            meta={"columns_param": columns_param, "strip_pattern": strip_pattern, "updated_cols": updated_cols},
        )

    _fn.__name__ = step
    return _fn


def normalize_dates() -> FieldMapper:
    """
    Config:
      normalize_dates:
        columns: ["application_date"]
        # optional:
        # format: "%Y-%m-%d"
    """
    step = "normalize_dates"

    def _fn(req: TransformationRequest) -> TransformationResult:
        logger.debug(f"[{step}] START")

        cfg = require_dict(step_cfg(req, step), step=step, key=step)
        require_keys(cfg, ["columns"], step=step, key=step)

        columns_param = ensure_columns_param(cfg.get("columns"), step=step)

        fmt = cfg.get("format", None)
        if fmt is not None and not isinstance(fmt, str):
            raise TransformationConfigError(f"[{step}] 'format' must be str or null, got: {type(fmt).__name__}")

        out = req.df.copy()
        cols_to_use = resolve_columns(out, columns_param)

        updated_cols: List[str] = []
        for col in cols_to_use:
            if col in out.columns:
                out[col] = pd.to_datetime(out[col], errors="coerce", format=fmt)
                updated_cols.append(col)

        logger.debug(
            f"[{step}] DONE | columns_param={columns_param!r} | format={fmt!r} | updated_cols={updated_cols} | shape={out.shape}"
        )
        return TransformationResult(
            df=out,
            issues=[],
            meta={"columns_param": columns_param, "format": fmt, "updated_cols": updated_cols},
        )

    _fn.__name__ = step
    return _fn