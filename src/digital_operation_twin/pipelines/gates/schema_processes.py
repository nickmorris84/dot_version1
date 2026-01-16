from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple, Literal,Set, Type

import logging


from digital_operation_twin.core.models.processIO import ProcessConfigError, ProcessResult, ProcessFn, Mode
from digital_operation_twin.core.utils import validate_one_record_against_model, records_to_df

logger = logging.getLogger(__name__)

SchemaKey = Tuple[str, str]                 # (customer_id, schema_version)
SchemaRegistry = Dict[SchemaKey, Type[Any]]


def schema_validation(
    *,
    schema_registry: SchemaRegistry,
    default_key: SchemaKey = ("__default__", "__default__"),
) -> Callable[[], ProcessFn]:
    step = "schema_validation"  # will be overridden by __name__ wrappers like schema_validation_gate

    def _fn(req: Any) -> ProcessResult:
        invoked_step = getattr(_fn, "__name__", step)
        logger.debug("[%s.%s] START", req.step_type, invoked_step)

        state = req.state
        cfg_val = req.cfg.get(invoked_step, None)

        # Disabled
        if cfg_val in (None, False):
            logger.debug("[%s.%s] SKIP (disabled)", req.step_type, invoked_step)
            return ProcessResult(state=state, meta={"skipped": True})

        # Enabled defaults
        if cfg_val is True:
            cfg: Dict[str, Any] = {}
        elif isinstance(cfg_val, dict):
            cfg = cfg_val
        else:
            raise ProcessConfigError(f"[{req.step_type}.{invoked_step}] must be true or dict, got: {repr(cfg_val)}")

        # ---- NEW: input_source controls whether we validate records or df ----
        input_source = cfg.get("input_source", "auto")  # records | df | auto
        if input_source not in ("records", "df", "auto"):
            raise ProcessConfigError(
                f"[{req.step_type}.{invoked_step}] input_source must be 'records'|'df'|'auto', got: {repr(input_source)}"
            )

        mode: Mode = cfg.get("mode", "report")
        if mode not in ("report", "enforce"):
            raise ProcessConfigError(f"[{req.step_type}.{invoked_step}] mode must be 'report'|'enforce', got: {repr(mode)}")

        attach_metric = cfg.get("attach_metric", True)
        reject_on_report = cfg.get("reject_on_report", False)

        sv_cfg = cfg.get("supported_versions", None)
        supported_versions: Optional[Set[str]] = None
        if sv_cfg is not None:
            if not (isinstance(sv_cfg, list) and all(isinstance(x, str) for x in sv_cfg)):
                raise ProcessConfigError(
                    f"[{req.step_type}.{invoked_step}] supported_versions must be list[str], got: {repr(sv_cfg)}"
                )
            supported_versions = set(sv_cfg)

        # -------- Resolve schema model (default fallback only when customer_id missing) --------
        customer_id = getattr(state, "customer_id", None)
        schema_version = getattr(state, "schema_version", None)

        def _pick_version(versions: List[str]) -> Optional[str]:
            if not versions:
                return None
            parsed = []
            for v in versions:
                if v.startswith("v") and v[1:].isdigit():
                    parsed.append((int(v[1:]), v))
            return max(parsed)[1] if parsed else sorted(versions)[-1]

        if not customer_id:
            # fallback to default ONLY when customer_id missing
            model = schema_registry.get(default_key)
            if model is None:
                return ProcessResult(state=state).reject(
                    code="default_schema_missing",
                    message=f"state.customer_id missing and default schema not registered for key={default_key}",
                    step=invoked_step,
                    default_key=default_key,
                )
            used_key = default_key
            eff_customer = default_key[0]
            eff_version = default_key[1]
        else:
            # customer present -> must resolve exact; no fallback
            customer_keys = [k for k in schema_registry.keys() if k[0] == customer_id]
            customer_versions = [k[1] for k in customer_keys]
            if supported_versions is not None:
                customer_versions = [v for v in customer_versions if v in supported_versions]

            if not customer_versions:
                return ProcessResult(state=state).reject(
                    code="schema_model_not_found",
                    message=f"No schemas registered for customer_id={customer_id}",
                    step=invoked_step,
                    customer_id=customer_id,
                    available_for_customer=sorted(customer_keys),
                )

            if not schema_version:
                schema_version = _pick_version(sorted(set(customer_versions)))

            if supported_versions is not None and schema_version not in supported_versions:
                return ProcessResult(state=state).reject(
                    code="schema_version_not_supported",
                    message=f"schema_version={schema_version} not in supported_versions",
                    step=invoked_step,
                    customer_id=customer_id,
                    schema_version=schema_version,
                    supported_versions=sorted(list(supported_versions)),
                )

            used_key = (customer_id, schema_version)
            model = schema_registry.get(used_key)
            if model is None:
                return ProcessResult(state=state).reject(
                    code="schema_model_not_found",
                    message=f"No schema registered for customer_id={customer_id} schema_version={schema_version}",
                    step=invoked_step,
                    customer_id=customer_id,
                    schema_version=schema_version,
                    tried=[used_key],
                    available_for_customer=sorted(customer_keys),
                )

            eff_customer = customer_id
            eff_version = schema_version

        # -------- Choose input data based on input_source --------
        records = getattr(state, "records", None)
        df = getattr(req, "df", None)
        df = df if df is not None else getattr(state, "df", None)


        source_used: Optional[str] = None
        if input_source == "records":
            if not isinstance(records, list):
                return ProcessResult(state=state).reject(
                    code="records_missing",
                    message="input_source='records' but state.records is missing/not a list",
                    step=invoked_step,
                    found_type=str(type(records)),
                )
            source_used = "records"

        elif input_source == "df":
            if df is None or not hasattr(df, "to_dict"):
                return ProcessResult(state=state).reject(
                    code="df_missing",
                    message="input_source='df' but no df available on req.df/state.df (or df has no to_dict)",
                    step=invoked_step,
                    df_type=str(type(df)) if df is not None else None,
                )
            records = df.to_dict(orient="records")
            source_used = "df"

        else:  # auto
            if isinstance(records, list):
                source_used = "records"
            elif df is not None and hasattr(df, "to_dict"):
                records = df.to_dict(orient="records")
                source_used = "df"
            else:
                return ProcessResult(state=state).reject(
                    code="no_input",
                    message="input_source='auto' but neither records nor df available",
                    step=invoked_step,
                )

        logger.debug("[%s.%s] input_source=%s source_used=%s", req.step_type, invoked_step, input_source, source_used)

        # Ensure metrics exist
        state.metrics = getattr(state, "metrics", {}) or {}
        if attach_metric:
            state.metrics.setdefault("schema_report", [])

        normalized_records: List[Dict[str, Any]] = []
        had_any_diff = False
        
        # Decide what version string (if any) should be passed to the helper
        # - default schema path: don't pass a version unless state actually has one
        # - customer schema path: pass the resolved customer version
        if used_key == default_key:
            version_for_validation = getattr(state, "schema_version", None)  # often None
        else:
            version_for_validation = eff_version  # exact resolved version, e.g. "v1"


        for idx, rec in enumerate(records):
            if not isinstance(rec, dict):
                return ProcessResult(state=state).reject(
                    code="invalid_record",
                    message=f"Record at index {idx} is not a dict",
                    step=invoked_step,
                    index=idx,
                    found_type=str(type(rec)),
                )

            try:

                sv_used, out, diff = validate_one_record_against_model(
                    model=model,
                    payload=rec,
                    schema_version=version_for_validation,
                    mode=mode,
                    supported_versions=supported_versions if version_for_validation is not None else None,
                )
                
                if used_key == default_key and schema_version is None:
                    sv_used = "registry_default"

            except ProcessConfigError as e:
                # In REPORT mode, do not reject; convert to a report row and continue
                if mode == "report":
                    msg = str(e)
                    # best-effort parse: if details exist, use them; else store the message
                    diff = getattr(e, "details", {}) or {"error": msg}
                    sv_used = schema_version
                    out = rec  # keep original
                else:
                    # ENFORCE mode keeps strict behavior
                    return ProcessResult(state=state).reject(
                        code=getattr(e, "code", "schema_validation_failed"),
                        message=str(e),
                        step=invoked_step,
                        index=idx,
                        customer_id=eff_customer,
                        schema_version=schema_version,
                        schema_key=used_key,
                        model=getattr(model, "__name__", str(model)),
                        **(getattr(e, "details", {}) or {}),
                    )


            missing = diff.get("missing_required") or []
            extras = diff.get("extra_fields") or []
            if missing or extras:
                had_any_diff = True

            if attach_metric:
                state.metrics["schema_report"].append(
                    {
                        "index": idx,
                        "customer_id": eff_customer,
                        "schema_version": sv_used,
                        "schema_key": used_key,
                        "model": getattr(model, "__name__", str(model)),
                        "source_used": source_used,
                        **diff,
                    }
                )

            normalized_records.append(out if mode == "enforce" else rec)

        if mode == "report" and reject_on_report and had_any_diff:
            return ProcessResult(state=state).reject(
                code="schema_report_failed",
                message="Schema report found missing/extra fields",
                step=invoked_step,
                customer_id=eff_customer,
                schema_version=eff_version,
                schema_key=used_key,
                model=getattr(model, "__name__", str(model)),
                source_used=source_used,
            )

        if mode == "enforce":
            state.records = normalized_records
            # Optional: if validated from df, also update state.df
            if source_used == "df":
                try:
                    import pandas as pd
                    state.df = pd.DataFrame(normalized_records)
                except Exception as e:
                    logger.warning("[%s.%s] enforce: could not rebuild state.df from normalized records: %s",
                                   req.step_type, invoked_step, e)

        logger.debug("[%s.%s] DONE mode=%s source_used=%s records=%s had_any_diff=%s schema_key=%s",
                     req.step_type, invoked_step, mode, source_used, len(records), had_any_diff, used_key)

        return ProcessResult(
            df=records_to_df(records=records),
            state=state,
            meta={
                "mode": mode,
                "input_source": input_source,
                "source_used": source_used,
                "schema_key": used_key,
                "model": getattr(model, "__name__", str(model)),
                "records": len(records),
                "had_any_diff": had_any_diff,
            },
        )

    return _fn
