from __future__ import annotations

from typing import Callable, Dict, Any, List, Union
import pandas as pd
import re
import logging

from digital_operation_twin.core.models.processIO import TransformationRequest, ProcessFn, ProcessConfigError, ProcessResult
from digital_operation_twin.core.utils import step_cfg, ensure_columns_param, resolve_columns, require_dict, require_keys, to_snake_case_label

logger = logging.getLogger(__name__)


# ============================================================
# STANDARDIZER steps (now all: TranformationProcessFn(req)->ProcessResult)
# ============================================================
def rename() -> ProcessFn:
    step = "rename"

    def _norm(s: str) -> str:
        # lower + strip + remove BOM, so " Activity_ID " matches "activity_id"
        return str(s).replace("\ufeff", "").strip().lower()

    def _fn(req: TransformationRequest) -> ProcessResult:
        logger.debug(f"[{step}] START")

        cfg = step_cfg(req, step)

        if not cfg:
            logger.debug(f"[{step}] SKIP | no config")
            return ProcessResult(df=req.df, issues=[], meta={"applied": {}, "unresolved": {}})

        cfg = require_dict(cfg, step=step, key=step)
        mapping_raw = cfg.get("mapping", cfg)

        if not isinstance(mapping_raw, dict):
            raise ProcessConfigError(
                f"[{step}] config must be dict[str,str] or {{mapping: dict[str,str]}}. "
                f"Got: {type(mapping_raw).__name__}"
            )

        # Validate types + normalize config keys (lowered)
        mapping_norm: dict[str, str] = {}
        bad_types = []
        for k, v in mapping_raw.items():
            if not isinstance(k, str) or not isinstance(v, str):
                bad_types.append((k, v, type(k).__name__, type(v).__name__))
                continue
            nk = _norm(k)
            nv = v.strip()  # keep destination case as-is (usually already snake_case)
            mapping_norm[nk] = nv

        if bad_types:
            logger.debug(f"[{step}] invalid mapping entries (sample)={bad_types[:20]}")
            raise ProcessConfigError(f"[{step}] mapping must be dict[str,str]. See logs for invalid entries.")

        if not mapping_norm:
            logger.debug(f"[{step}] SKIP | empty mapping")
            return ProcessResult(df=req.df, issues=[], meta={"applied": {}, "unresolved": {}})

        before_cols = list(req.df.columns)

        # Build lookup: normalized_col -> [actual_colnames]
        col_lookup: dict[str, list[str]] = {}
        for c in before_cols:
            col_lookup.setdefault(_norm(c), []).append(c)

        # Detect collisions where two cols normalize to the same lowered name
        collisions = {k: v for k, v in col_lookup.items() if len(v) > 1}
        if collisions:
            logger.debug(f"[{step}] WARNING | normalized column collisions detected (sample)={list(collisions.items())[:10]}")

        # Resolve mapping against actual columns
        resolved_mapping: dict[str, str] = {}
        unresolved: dict[str, str] = {}
        ambiguous: dict[str, list[str]] = {}

        for nk, dst in mapping_norm.items():
            hits = col_lookup.get(nk)
            if not hits:
                unresolved[nk] = dst
                continue
            if len(hits) > 1:
                # ambiguous: multiple columns map to same lowered key
                ambiguous[nk] = hits
                # pick first deterministically, but log loudly
                resolved_mapping[hits[0]] = dst
            else:
                resolved_mapping[hits[0]] = dst

        logger.debug(f"[{step}] mapping_raw_size={len(mapping_raw)} mapping_norm_size={len(mapping_norm)}")
        logger.debug(f"[{step}] resolved_mapping count={len(resolved_mapping)} sample={list(resolved_mapping.items())[:20]}")
        if unresolved:
            logger.debug(f"[{step}] unresolved keys (normalized) count={len(unresolved)} sample={list(unresolved.items())[:20]}")
        if ambiguous:
            logger.debug(f"[{step}] ambiguous keys (normalized) count={len(ambiguous)} sample={list(ambiguous.items())[:10]}")

        # Optional: also normalize DF columns to lowercase BEFORE renaming?
        # Usually you don't want to permanently lower columns here, because you might want snake_case later.
        # If you DO want it, uncomment:
        # req.df = req.df.rename(columns=lambda c: _norm(c))

        out = req.df.rename(columns=resolved_mapping)
        
        print(out)

        after_cols = list(out.columns)

        # Report what was actually changed (based on resolved mapping)
        applied = {}
        for src_actual, dst in resolved_mapping.items():
            # if src existed and now dst exists, count it as applied
            if src_actual in before_cols and dst in after_cols:
                # if src still exists post-rename (duplicates), still report it as applied
                applied[src_actual] = dst

        logger.debug(f"[{step}] DONE | applied_count={len(applied)} applied_sample={list(applied.items())[:20]} | shape={out.shape}")

        return ProcessResult(
            state=req.state,
            df=out,
            errors=[],
            meta={
                "applied": applied,
                "unresolved": unresolved,
                "ambiguous": ambiguous,
                "collisions": collisions,
            },
        )

    _fn.__name__ = step
    return _fn



def to_snake_case() -> ProcessFn:
    """
    Config:
      to_snake_case: true
    """
    step = "to_snake_case"

    def _fn(req: TransformationRequest) -> ProcessResult:
        logger.debug(f"[{step}] START")

        enabled = step_cfg(req, step)
        if enabled is not True:
            raise ProcessConfigError(f"[{step}] Must be 'true'. Got: {repr(enabled)}")

        out = req.df.copy()
        before = list(out.columns)
        after = [to_snake_case_label(c) for c in before]
        out.columns = after

        changed = {b: a for b, a in zip(before, after) if b != a}
        logger.debug(f"[{step}] DONE | changed={changed} | shape={out.shape}")
        return ProcessResult(
            state=req.state,
            df=out, 
            errors=[], 
            meta={"changed": changed})

    _fn.__name__ = step
    return _fn


def cast() -> ProcessFn:
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

    def _fn(req: TransformationRequest) -> ProcessResult:
        logger.debug(f"[{step}] START")

        cfg = require_dict(step_cfg(req, step), step=step, key=step)
        require_keys(cfg, ["dtypes"], step=step, key=step)

        dtypes = cfg.get("dtypes")
        if dtypes != "*" and not isinstance(dtypes, dict):
            raise ProcessConfigError(f"[{step}] 'dtypes' must be '*' or dict[str,str]. Got: {type(dtypes).__name__}")

        out = req.df.copy()

        if dtypes == "*":
            dtypes_dict: Dict[str, str] = {col: "string" for col in out.columns}
            mode = "*"
        else:
            if not all(isinstance(k, str) and isinstance(v, str) for k, v in dtypes.items()):
                raise ProcessConfigError(f"[{step}] 'dtypes' must be dict[str,str].")
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
        return ProcessResult(
            state=req.state,
            df=out, 
            errors=[], 
            meta={"mode": mode, "attempted": attempted, "casted": casted, "missing": missing})

    _fn.__name__ = step
    return _fn


def value_map() -> ProcessFn:
    """
    Config:
      value_map:
        columns: ["status"]
        mapping:
          A: active
          I: inactive
    """
    step = "value_map"

    def _fn(req: TransformationRequest) -> ProcessResult:
        logger.debug(f"[{step}] START")

        cfg = require_dict(step_cfg(req, step), step=step, key=step)
        require_keys(cfg, ["columns", "mapping"], step=step, key=step)

        columns_param = ensure_columns_param(cfg.get("columns"), step=step)
        mapping = cfg.get("mapping")
        if not isinstance(mapping, dict):
            raise ProcessConfigError(f"[{step}] 'mapping' must be a dict. Got: {type(mapping).__name__}")

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
        return ProcessResult(
            state=req.state,
            df=out, 
            errors=[],  
            meta={"resolved": cols_to_use, "updated": updated, "missing": missing, "mapping_size": len(mapping)})

    _fn.__name__ = step
    return _fn


def require() -> ProcessFn:
    """
    Config:
      require:
        columns: ["customer_id", "application_id"]
    """
    step = "require"

    def _fn(req: TransformationRequest) -> ProcessResult:
        logger.debug(f"[{step}] START")

        cfg = require_dict(step_cfg(req, step), step=step, key=step)
        require_keys(cfg, ["columns"], step=step, key=step)

        columns_param = ensure_columns_param(cfg.get("columns"), step=step)
        cols = resolve_columns(req.df, columns_param)

        missing_or_na = [c for c in cols if c not in req.df.columns or req.df[c].isna().any()]
        if missing_or_na:
            logger.error(f"[{step}] FAILED | missing_or_na={missing_or_na}")
            raise ProcessConfigError(f"[{step}] Required columns missing/NA: {missing_or_na}")

        logger.debug(f"[{step}] DONE | required_ok={cols} | shape={req.df.shape}")
        return ProcessResult(
            state=req.state,
            df=req.df, 
            errors=[],  
            meta={"required_ok": cols})

    _fn.__name__ = step
    return _fn


def drop_columns() -> ProcessFn:
    """
    Config:
      drop_columns:
        columns: ["raw_notes", "temp_flag"]
    """
    step = "drop_columns"

    def _fn(req: TransformationRequest) -> ProcessResult:
        logger.debug(f"[{step}] START")

        cfg = require_dict(step_cfg(req, step), step=step, key=step)
        require_keys(cfg, ["columns"], step=step, key=step)

        columns = cfg.get("columns")
        if not (isinstance(columns, list) and all(isinstance(c, str) for c in columns)):
            raise ProcessConfigError(f"[{step}] 'columns' must be list[str]. Got: {type(columns).__name__}")

        existing = [c for c in columns if c in req.df.columns]
        out = req.df.drop(columns=existing, errors="ignore")

        logger.debug(f"[{step}] DONE | requested={columns} | dropped={existing} | shape={out.shape}")
        return ProcessResult(
            state=req.state,
            df=out, 
            errors=[], 
            meta={"requested": columns, "dropped": existing})

    _fn.__name__ = step
    return _fn


def fill_defaults() -> ProcessFn:
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

    def _fn(req: TransformationRequest) -> ProcessResult:
        logger.debug(f"[{step}] START")

        cfg = require_dict(step_cfg(req, step), step=step, key=step)
        require_keys(cfg, ["defaults"], step=step, key=step)

        defaults = cfg.get("defaults")
        if defaults != "*" and not isinstance(defaults, dict):
            raise ProcessConfigError(f"[{step}] 'defaults' must be '*' or dict[str,Any]. Got: {type(defaults).__name__}")

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
        return ProcessResult(
            state=req.state,
            df=out, 
            errors=[],  
            meta={"mode": mode, "filled_cols": filled_cols, "missing_cols": missing_cols})

    _fn.__name__ = step
    return _fn

