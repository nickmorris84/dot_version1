from __future__ import annotations

from typing import Callable, Dict, Any, List, Union
import pandas as pd
import re
import logging

from digital_operation_twin.core.models.transformation_request import TransformationRequest, FieldMapper, TransformationConfigError, TransformationResult
from digital_operation_twin.core.utils import step_cfg, ensure_columns_param, resolve_columns, require_dict, require_keys, to_snake_case_label

logger = logging.getLogger(__name__)


# ============================================================
# STANDARDIZER steps (now all: FieldMapper(req)->TransformationResult)
# ============================================================

def rename() -> FieldMapper:
    """
    Config:
      rename:
        mapping:
          OldName: new_name
          DOB: date_of_birth
    """
    step = "rename"

    def _fn(req: TransformationRequest) -> TransformationResult:
        logger.debug(f"[{step}] START")

        cfg = require_dict(step_cfg(req, step), step=step, key=step)
        require_keys(cfg, ["mapping"], step=step, key=step)

        mapping = cfg.get("mapping")
        if not isinstance(mapping, dict) or not all(isinstance(k, str) and isinstance(v, str) for k, v in mapping.items()):
            raise TransformationConfigError(f"[{step}] 'mapping' must be dict[str,str]. Got: {type(mapping).__name__}")

        before_cols = list(req.df.columns)
        out = req.df.rename(columns=mapping)
        after_cols = list(out.columns)

        applied = {src: dst for src, dst in mapping.items() if src in before_cols and src != dst}
        # Only keep those that actually changed names
        applied = {src: dst for src, dst in applied.items() if dst in after_cols and src not in after_cols}

        logger.debug(f"[{step}] DONE | applied={applied} | shape={out.shape}")
        return TransformationResult(df=out, issues=[], meta={"applied": applied})

    _fn.__name__ = step
    return _fn


def to_snake_case() -> FieldMapper:
    """
    Config:
      to_snake_case: true
    """
    step = "to_snake_case"

    def _fn(req: TransformationRequest) -> TransformationResult:
        logger.debug(f"[{step}] START")

        enabled = step_cfg(req, step)
        if enabled is not True:
            raise TransformationConfigError(f"[{step}] Must be 'true'. Got: {repr(enabled)}")

        out = req.df.copy()
        before = list(out.columns)
        after = [to_snake_case_label(c) for c in before]
        out.columns = after

        changed = {b: a for b, a in zip(before, after) if b != a}
        logger.debug(f"[{step}] DONE | changed={changed} | shape={out.shape}")
        return TransformationResult(df=out, issues=[], meta={"changed": changed})

    _fn.__name__ = step
    return _fn


def cast() -> FieldMapper:
    """
    Config:
      cast:
        dtypes:
          age: int
          amount: float
          application_date: datetime64[ns]
    Or:
      cast:
        dtypes: "*"
    """
    step = "cast"

    def _fn(req: TransformationRequest) -> TransformationResult:
        logger.debug(f"[{step}] START")

        cfg = require_dict(step_cfg(req, step), step=step, key=step)
        require_keys(cfg, ["dtypes"], step=step, key=step)

        dtypes = cfg.get("dtypes")
        if dtypes != "*" and not isinstance(dtypes, dict):
            raise TransformationConfigError(f"[{step}] 'dtypes' must be '*' or dict[str,str]. Got: {type(dtypes).__name__}")

        out = req.df.copy()

        if dtypes == "*":
            dtypes_dict: Dict[str, str] = {col: "string" for col in out.columns}
            mode = "*"
        else:
            if not all(isinstance(k, str) and isinstance(v, str) for k, v in dtypes.items()):
                raise TransformationConfigError(f"[{step}] 'dtypes' must be dict[str,str].")
            dtypes_dict = dtypes
            mode = "dict"

        attempted: List[str] = []
        casted: List[str] = []
        missing: List[str] = []

        for col, dt in dtypes_dict.items():
            attempted.append(col)
            if col not in out.columns:
                missing.append(col)
                continue
            try:
                if dt.startswith("datetime"):
                    out[col] = pd.to_datetime(out[col], errors="coerce")
                else:
                    out[col] = out[col].astype(dt)
                casted.append(col)
            except Exception as e:
                logger.error(f"[{step}] FAILED | column={col!r} dtype={dt!r} err={e}", exc_info=True)
                raise

        logger.debug(
            f"[{step}] DONE | mode={mode} | attempted={attempted} | casted={casted} | missing={missing} | shape={out.shape}"
        )
        return TransformationResult(df=out, issues=[], meta={"mode": mode, "attempted": attempted, "casted": casted, "missing": missing})

    _fn.__name__ = step
    return _fn


def value_map() -> FieldMapper:
    """
    Config:
      value_map:
        columns: ["status"]
        mapping:
          A: active
          I: inactive
    """
    step = "value_map"

    def _fn(req: TransformationRequest) -> TransformationResult:
        logger.debug(f"[{step}] START")

        cfg = require_dict(step_cfg(req, step), step=step, key=step)
        require_keys(cfg, ["columns", "mapping"], step=step, key=step)

        columns_param = ensure_columns_param(cfg.get("columns"), step=step)
        mapping = cfg.get("mapping")
        if not isinstance(mapping, dict):
            raise TransformationConfigError(f"[{step}] 'mapping' must be a dict. Got: {type(mapping).__name__}")

        out = req.df.copy()
        cols_to_use = resolve_columns(out, columns_param)

        updated: List[str] = []
        missing: List[str] = []

        for col in cols_to_use:
            if col not in out.columns:
                missing.append(col)
                continue
            out[col] = out[col].map(mapping).fillna(out[col])
            updated.append(col)

        logger.debug(
            f"[{step}] DONE | columns_param={columns_param!r} | resolved={cols_to_use} | updated={updated} | missing={missing} | mapping_size={len(mapping)} | shape={out.shape}"
        )
        return TransformationResult(df=out, issues=[], meta={"resolved": cols_to_use, "updated": updated, "missing": missing, "mapping_size": len(mapping)})

    _fn.__name__ = step
    return _fn


def require() -> FieldMapper:
    """
    Config:
      require:
        columns: ["customer_id", "application_id"]
    """
    step = "require"

    def _fn(req: TransformationRequest) -> TransformationResult:
        logger.debug(f"[{step}] START")

        cfg = require_dict(step_cfg(req, step), step=step, key=step)
        require_keys(cfg, ["columns"], step=step, key=step)

        columns_param = ensure_columns_param(cfg.get("columns"), step=step)
        cols = resolve_columns(req.df, columns_param)

        missing_or_na = [c for c in cols if c not in req.df.columns or req.df[c].isna().any()]
        if missing_or_na:
            logger.error(f"[{step}] FAILED | missing_or_na={missing_or_na}")
            raise TransformationConfigError(f"[{step}] Required columns missing/NA: {missing_or_na}")

        logger.debug(f"[{step}] DONE | required_ok={cols} | shape={req.df.shape}")
        return TransformationResult(df=req.df, issues=[], meta={"required_ok": cols})

    _fn.__name__ = step
    return _fn


def drop_columns() -> FieldMapper:
    """
    Config:
      drop_columns:
        columns: ["raw_notes", "temp_flag"]
    """
    step = "drop_columns"

    def _fn(req: TransformationRequest) -> TransformationResult:
        logger.debug(f"[{step}] START")

        cfg = require_dict(step_cfg(req, step), step=step, key=step)
        require_keys(cfg, ["columns"], step=step, key=step)

        columns = cfg.get("columns")
        if not (isinstance(columns, list) and all(isinstance(c, str) for c in columns)):
            raise TransformationConfigError(f"[{step}] 'columns' must be list[str]. Got: {type(columns).__name__}")

        existing = [c for c in columns if c in req.df.columns]
        out = req.df.drop(columns=existing, errors="ignore")

        logger.debug(f"[{step}] DONE | requested={columns} | dropped={existing} | shape={out.shape}")
        return TransformationResult(df=out, issues=[], meta={"requested": columns, "dropped": existing})

    _fn.__name__ = step
    return _fn


def fill_defaults() -> FieldMapper:
    """
    Config:
      fill_defaults:
        defaults:
          country: "UK"
          currency: "GBP"
    Or:
      fill_defaults:
        defaults: "*"
    """
    step = "fill_defaults"

    def _fn(req: TransformationRequest) -> TransformationResult:
        logger.debug(f"[{step}] START")

        cfg = require_dict(step_cfg(req, step), step=step, key=step)
        require_keys(cfg, ["defaults"], step=step, key=step)

        defaults = cfg.get("defaults")
        if defaults != "*" and not isinstance(defaults, dict):
            raise TransformationConfigError(f"[{step}] 'defaults' must be '*' or dict[str,Any]. Got: {type(defaults).__name__}")

        out = req.df.copy()

        if defaults == "*":
            d: Dict[str, Any] = {col: "" for col in out.columns}
            mode = "*"
        else:
            d = defaults
            mode = "dict"

        filled_cols: List[str] = []
        missing_cols: List[str] = []

        for col, value in d.items():
            if col in out.columns:
                out[col] = out[col].fillna(value)
                filled_cols.append(col)
            else:
                missing_cols.append(col)

        logger.debug(
            f"[{step}] DONE | mode={mode} | filled_cols={filled_cols} | missing_cols={missing_cols} | shape={out.shape}"
        )
        return TransformationResult(df=out, issues=[], meta={"mode": mode, "filled_cols": filled_cols, "missing_cols": missing_cols})

    _fn.__name__ = step
    return _fn

