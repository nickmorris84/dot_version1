from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple, Literal,Set, Type

import logging

from digital_operation_twin.core.models.processIO import ProcessRequest, ProcessConfigError, ProcessResult, ProcessFn, Mode
from digital_operation_twin.core.utils import validate_one_record_against_model

logger = logging.getLogger(__name__)




SchemaKey = Tuple[str, str]                 # (customer_id, schema_version)
SchemaRegistry = Dict[SchemaKey, Type[Any]]


def schema_validation(
    *,
    schema_registry: SchemaRegistry,
    default_key: SchemaKey = ("__default__", "__default__"),
) -> Callable[[], ProcessFn]:
    """
    Validates state.records using a schema model resolved from `schema_registry`
    based on (state.customer_id, state.schema_version), with optional default fallback.

    Expected pipeline ordering:
      extract_envelope -> normalize_payload -> schema_validation

    Config shapes:
      schema_validation: true
      schema_validation:
        mode: "report" | "enforce"          # default "report"
        attach_metric: true                 # default true (writes state.metrics["schema_report"])
        reject_on_report: false             # default false
        supported_versions: ["v1"]          # optional allowlist
        allow_default_schema: true          # default true; allows fallback to default_key

    Schema resolution order:
      1) (customer_id, schema_version)
      2) (customer_id, "__default__")
      3) ("__default__", schema_version)
      4) default_key (default: ("__default__", "__default__")) if allow_default_schema

    Behavior:
      - report: logs + metrics only; records unchanged (unless reject_on_report=true and diffs exist)
      - enforce: validates & normalizes; replaces state.records with normalized dicts; rejects on first failure
    """
    step = "schema_validation"

    def _fn(req: ProcessRequest) -> ProcessResult:
        logger.debug("[%s.%s] START", req.step_name, step)
        state = req.state

        cfg_val = req.cfg.get(step, None)

        # Disabled
        if cfg_val in (None, False):
            logger.debug("[%s.%s] SKIP (disabled)", req.step_name, step)
            return ProcessResult(state=state, meta={"skipped": True})

        # Enabled defaults
        if cfg_val is True:
            cfg: Dict[str, Any] = {}
        elif isinstance(cfg_val, dict):
            cfg = cfg_val
        else:
            raise ProcessConfigError(f"[{req.step_name}.{step}] must be true or dict, got: {repr(cfg_val)}")

        mode: Mode = cfg.get("mode", "report")
        if mode not in ("report", "enforce"):
            raise ProcessConfigError(f"[{req.step_name}.{step}] mode must be 'report'|'enforce', got: {repr(mode)}")

        attach_metric = cfg.get("attach_metric", True)
        if not isinstance(attach_metric, bool):
            raise ProcessConfigError(
                f"[{req.step_name}.{step}] attach_metric must be bool, got: {type(attach_metric).__name__}"
            )

        reject_on_report = cfg.get("reject_on_report", False)
        if not isinstance(reject_on_report, bool):
            raise ProcessConfigError(
                f"[{req.step_name}.{step}] reject_on_report must be bool, got: {type(reject_on_report).__name__}"
            )

        allow_default_schema = cfg.get("allow_default_schema", True)
        if not isinstance(allow_default_schema, bool):
            raise ProcessConfigError(
                f"[{req.step_name}.{step}] allow_default_schema must be bool, got: {type(allow_default_schema).__name__}"
            )

        sv_cfg = cfg.get("supported_versions", None)
        supported_versions: Optional[Set[str]] = None
        if sv_cfg is not None:
            if not (isinstance(sv_cfg, list) and all(isinstance(x, str) for x in sv_cfg)):
                raise ProcessConfigError(
                    f"[{req.step_name}.{step}] supported_versions must be list[str], got: {repr(sv_cfg)}"
                )
            supported_versions = set(sv_cfg)

        # ---- Resolve schema model from registry (customer_id + schema_version) ----
        customer_id = getattr(state, "customer_id", None)
        if not customer_id:
            return ProcessResult(state=state).reject(
                code="customer_id_missing",
                message="state.customer_id is required to resolve schema model",
                step=step,
            )

        schema_version = getattr(state, "schema_version", None) or "v1"

        # Resolution order:
        candidates: List[SchemaKey] = [
            (customer_id, schema_version),
            (customer_id, "__default__"),
            ("__default__", schema_version),
        ]
        if allow_default_schema:
            candidates.append(default_key)

        model: Optional[Type[Any]] = None
        used_key: Optional[SchemaKey] = None
        for k in candidates:
            m = schema_registry.get(k)
            if m is not None:
                model = m
                used_key = k
                break

        if model is None:
            available_for_customer = sorted([k for k in schema_registry.keys() if k[0] == customer_id])
            return ProcessResult(state=state).reject(
                code="schema_model_not_found",
                message=f"No schema registered for customer_id={customer_id} schema_version={schema_version}",
                step=step,
                customer_id=customer_id,
                schema_version=schema_version,
                tried=candidates,
                available_for_customer=available_for_customer,
            )

        if used_key != (customer_id, schema_version):
            logger.warning(
                "[%s.%s] Using fallback schema | requested=(%s,%s) used=%s model=%s",
                req.step_name,
                step,
                customer_id,
                schema_version,
                used_key,
                getattr(model, "__name__", str(model)),
            )

        # ---- Validate records presence ----
        records = getattr(state, "records", None)
        if not isinstance(records, list):
            return ProcessResult(state=state).reject(
                code="records_missing",
                message="state.records must be list[dict] before schema_validation runs",
                step=step,
                found_type=str(type(records)),
            )

        # Ensure metrics exist
        state.metrics = getattr(state, "metrics", {}) or {}
        if attach_metric:
            state.metrics.setdefault("schema_report", [])

        normalized_records: List[Dict[str, Any]] = []
        had_any_diff = False

        for idx, rec in enumerate(records):
            if not isinstance(rec, dict):
                return ProcessResult(state=state).reject(
                    code="invalid_record",
                    message=f"Record at index {idx} is not a dict",
                    step=step,
                    index=idx,
                    found_type=str(type(rec)),
                )

            try:
                sv_used, out, diff = validate_one_record_against_model(
                    model=model,
                    payload=rec,
                    schema_version=schema_version,
                    mode=mode,
                    supported_versions=supported_versions,
                )
            except ProcessConfigError as e:
                return ProcessResult(state=state).reject(
                    code=getattr(e, "code", "schema_validation_failed"),
                    message=str(e),
                    step=step,
                    index=idx,
                    **(getattr(e, "details", {}) or {}),
                )

            missing = diff.get("missing_required") or []
            extras = diff.get("extra_fields") or []

            if missing or extras:
                had_any_diff = True
                logger.info(
                    "[%s.%s] REPORT idx=%s missing_required=%s extra_fields=%s",
                    req.step_name,
                    step,
                    idx,
                    missing,
                    extras,
                    extra={
                        "step": step,
                        "event_id": getattr(state, "event_id", None),
                        "schema_version": sv_used,
                        "index": idx,
                    },
                )
            else:
                logger.debug(
                    "[%s.%s] REPORT idx=%s ok",
                    req.step_name,
                    step,
                    idx,
                    extra={
                        "step": step,
                        "event_id": getattr(state, "event_id", None),
                        "schema_version": sv_used,
                        "index": idx,
                    },
                )

            if attach_metric:
                state.metrics["schema_report"].append(
                    {
                        "index": idx,
                        "schema_version": sv_used,
                        "customer_id": customer_id,
                        "schema_key": used_key,
                        "model": getattr(model, "__name__", str(model)),
                        **diff,
                    }
                )

            normalized_records.append(out if mode == "enforce" else rec)

        # Optional: treat report diffs as rejection
        if mode == "report" and reject_on_report and had_any_diff:
            logger.debug("[%s.%s] DONE -> reject_on_report (diffs found)", req.step_name, step)
            return ProcessResult(state=state).reject(
                code="schema_report_failed",
                message="Schema report found missing/extra fields",
                step=step,
                customer_id=customer_id,
                schema_version=schema_version,
            )

        # Commit enforce normalization
        if mode == "enforce":
            state.records = normalized_records

        logger.debug(
            "[%s.%s] DONE mode=%s records=%s had_any_diff=%s model=%s schema_key=%s",
            req.step_name,
            step,
            mode,
            len(records),
            had_any_diff,
            getattr(model, "__name__", str(model)),
            used_key,
        )

        return ProcessResult(
            state=state,
            meta={
                "mode": mode,
                "customer_id": customer_id,
                "schema_version": schema_version,
                "schema_key": used_key,
                "model": getattr(model, "__name__", str(model)),
                "records": len(records),
                "had_any_diff": had_any_diff,
            },
        )

    _fn.__name__ = step
    return _fn
