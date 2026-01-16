import logging
from typing import Callable, Dict, List, Any, Optional
import pandas as pd

from digital_operation_twin.core.models.processIO import TransformationRequest, ProcessFn, ProcessConfigError, ProcessResult
from digital_operation_twin.core.utils import step_cfg, ensure_columns_param, resolve_columns, require_dict, require_keys

logger = logging.getLogger(__name__)

# -------------------------
# Validators (now TranformationProcessFn -> ProcessResult)
# -------------------------

def check_required_columns() -> ProcessFn:
    """
    Config:
      check_required_columns:
        columns: ["a", "b"]
    """
    step = "check_required_columns"

    def _fn(req: TransformationRequest) -> ProcessResult:
        logger.debug(f"[{step}] START")

        cfg = require_dict(step_cfg(req, step), step=step, key=step)
        require_keys(cfg, ["columns"], step=step, key=step)

        columns_param = ensure_columns_param(cfg.get("columns"), step=step)
        cols = resolve_columns(req.df, columns_param)

        errors: List[Dict[str, Any]] = []
        missing: List[str] = []

        for col in cols:
            if col not in req.df.columns:
                missing.append(col)
                errors.append({
                    "type": "missing_column",
                    "column": col,
                    "message": f"Missing required column: {col}",
                })

        logger.debug(f"[{step}] DONE | checked={cols} | missing={missing} | issues={len(errors)}")
        return ProcessResult(
            state=req.state,
            df=req.df,
            errors=errors,
            meta={"checked": cols, "missing": missing},
        )

    _fn.__name__ = step
    return _fn


def check_nulls() -> ProcessFn:
    """
    Config:
      check_nulls:
        columns: ["a", "b"]
    """
    step = "check_nulls"

    def _fn(req: TransformationRequest) -> ProcessResult:
        logger.debug(f"[{step}] START")

        cfg = require_dict(step_cfg(req, step), step=step, key=step)
        require_keys(cfg, ["columns"], step=step, key=step)

        columns_param = ensure_columns_param(cfg.get("columns"), step=step)
        cols = resolve_columns(req.df, columns_param)

        errors: List[Dict[str, Any]] = []
        checked: List[str] = []
        missing: List[str] = []
        null_counts: Dict[str, int] = {}

        for col in cols:
            if col not in req.df.columns:
                missing.append(col)
                errors.append({
                    "type": "missing_column",
                    "column": col,
                    "message": f"Missing required column: {col}",
                })
                continue

            checked.append(col)
            null_count = int(req.df[col].isna().sum())
            null_counts[col] = null_count

            if null_count > 0:
                errors.append({
                    "type": "null_values",
                    "column": col,
                    "null_count": null_count,
                    "message": f"Column '{col}' has {null_count} nulls",
                })

        logger.debug(
            f"[{step}] DONE | columns_param={columns_param!r} | resolved={cols} | checked={checked} | "
            f"missing={missing} | issues={len(errors)}"
        )
        return ProcessResult(
            state=req.state,
            df=req.df,
            errors=errors,
            meta={"resolved": cols, "checked": checked, "missing": missing, "null_counts": null_counts},
        )

    _fn.__name__ = step
    return _fn


def check_value_range() -> ProcessFn:
    """
    Config:
      check_value_range:
        column: "amount"
        min: 0
        max: 100
    """
    step = "check_value_range"

    def _fn(req: TransformationRequest) -> ProcessResult:
        logger.debug(f"[{step}] START")

        cfg = require_dict(step_cfg(req, step), step=step, key=step)
        require_keys(cfg, ["column"], step=step, key=step)

        column = cfg.get("column")
        if not isinstance(column, str) or not column.strip():
            raise ProcessConfigError(f"[{step}] 'column' must be a non-empty string.")

        min_val = cfg.get("min", None)
        max_val = cfg.get("max", None)

        if min_val is not None and not isinstance(min_val, (int, float)):
            raise ProcessConfigError(f"[{step}] 'min' must be a number or null. Got: {type(min_val).__name__}")
        if max_val is not None and not isinstance(max_val, (int, float)):
            raise ProcessConfigError(f"[{step}] 'max' must be a number or null. Got: {type(max_val).__name__}")

        errors: List[Dict[str, Any]] = []

        if column not in req.df.columns:
            errors.append({
                "type": "missing_column",
                "column": column,
                "message": f"Missing required column: {column}",
            })
            logger.debug(f"[{step}] DONE | column={column!r} missing | issues=1")
            return ProcessResult(
                state = req.state,
                df=req.df, 
                errros=errors,
                meta={"column": column, "min": min_val, "max": max_val})

        # Range checks should operate on numeric values; do not mutate the df.
        s = pd.to_numeric(req.df[column], errors="coerce")

        below_count = 0
        above_count = 0

        if min_val is not None:
            below_count = int((s < float(min_val)).sum())
            if below_count > 0:
                errors.append({
                    "type": "value_below_min",
                    "column": column,
                    "count": below_count,
                    "min": float(min_val),
                    "message": f"Column '{column}' has {below_count} values below {min_val}",
                })

        if max_val is not None:
            above_count = int((s > float(max_val)).sum())
            if above_count > 0:
                errors.append({
                    "type": "value_above_max",
                    "column": column,
                    "count": above_count,
                    "max": float(max_val),
                    "message": f"Column '{column}' has {above_count} values above {max_val}",
                })

        logger.debug(
            f"[{step}] DONE | column={column!r} | min={min_val!r} | max={max_val!r} | "
            f"below={below_count} | above={above_count} | issues={len(errors)}"
        )
        return ProcessResult(
            state = req.state,
            df=req.df,
            errors=errors,
            meta={"column": column, "min": min_val, "max": max_val, "below": below_count, "above": above_count},
        )

    _fn.__name__ = step
    return _fn
