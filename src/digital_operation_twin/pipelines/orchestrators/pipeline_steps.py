from __future__ import annotations

from typing import Any, Callable, Dict, Sequence, Optional
import logging
import pandas as pd

from digital_operation_twin.core.models.pipeline_state import PipelineState
from digital_operation_twin.core.models.processIO import ProcessResult
from digital_operation_twin.pipelines.orchestrators.process_runner import ProcessRunner, GATE_ADAPTER, TRANSFORM_ADAPTER
from digital_operation_twin.core.utils import capture_original_df_once, start_segment_audit, capture_original_once, end_segment_audit, records_to_df, df_to_records

logger = logging.getLogger(__name__)


# ============================================================
# Gate Step
# ============================================================

class GateStep:
    """
    Async orchestration step that runs the "gate" segment (extract/normalize/idempotency/schema).
    Under the hood it uses ProcessRunner (sync) with the GATE_ADAPTER.
    """
    name = "gate"

    def __init__(
        self,
        *,
        cfg: Dict[str, Any],
        registry: Dict[str, Callable[[], Callable[..., ProcessResult]]],
        order: Sequence[str],
        strict: bool = True,
        verbose: bool = False,
    ) -> None:
        # ✅ Builder removed: instantiate ProcessRunner directly.
        self.runner = ProcessRunner(
            segment_name=self.name,
            cfg=cfg,
            registry=registry,
            default_order=order,
            adapter=GATE_ADAPTER,
            strict_cfg=strict,
            verbose=verbose,
        )

    async def run(self, state: PipelineState) -> PipelineState:
        msg = state.context.get("msg")
        if msg is None:
            state.status = "rejected"
            state.errors.append({"code": "msg_missing", "message": "[gate] state.context['msg'] is missing"})
            return state

        logger.info("[GateStep] RUN start | event_id=%s", getattr(state, "event_id", None))

        out: ProcessResult = self.runner.run(state=state, inputs={"msg": msg})

        # state is canonical
        state = out.state
        state.errors.extend(out.errors)
        state.metrics.update(out.metrics)
        state.context.setdefault("gate_meta", {}).update(out.meta)

        if out.status == "rejected":
            state.status = "rejected"
        elif out.status == "accepted":
            state.status = "accepted"
        else:
            state.status = state.status or "running"

        logger.info("[GateStep] RUN done | status=%s errors=%s", state.status, len(state.errors))
        return state


# ============================================================
# Transform Segment Step
# ============================================================

class TransformStep:
    """
    Async orchestration step that runs one transform segment (normalizer/standardizer/validator).

    - Reads df either from state.df or from state.records -> DataFrame
    - Runs ProcessRunner with TRANSFORM_ADAPTER
    - Writes latest output to state.df
    - Optionally writes back to state.records
    - Adds audit trail (fingerprints + summaries) under state.context["audit"]
    """
    def __init__(
        self,
        *,
        name: str,
        cfg: Dict[str, Any],
        registry: Dict[str, Callable[[], Callable[..., ProcessResult]]],
        order: Sequence[str],
        strict: bool = True,
        verbose: bool = False,
        input_from: str = "records",   # "records" or "df"
        output_to: str = "df",         # "df" or "records"
        capture_original: bool = True,
        keep_original_df: bool = False,                 # NEW: full df snapshot is optional
        audit_key_cols: Optional[Sequence[str]] = None, # NEW: stable fingerprint sorting
        audit_null_cols: Optional[Sequence[str]] = None # NEW: null delta tracking for key fields
    ) -> None:
        self.name = name
        self.input_from = input_from
        self.output_to = output_to
        self.capture_original = capture_original
        self.keep_original_df = keep_original_df
        self.audit_key_cols = audit_key_cols
        self.audit_null_cols = audit_null_cols

        self.runner = ProcessRunner(
            segment_name=self.name,
            cfg=cfg,
            registry=registry,
            default_order=order,
            adapter=TRANSFORM_ADAPTER,
            strict_cfg=strict,
            verbose=verbose,
        )

    async def run(self, state: PipelineState) -> PipelineState:
        logger.info("[%s] RUN start | input_from=%s output_to=%s", self.name, self.input_from, self.output_to)

        # Prepare df input
        if self.input_from == "df":
            if state.df is None:
                state.status = "rejected"
                state.errors.append({"code": "df_missing", "message": f"[{self.name}] state.df is None"})
                logger.warning("[%s] RUN rejected: df_missing", self.name)
                return state
            df_in = state.df
        else:
            if not isinstance(getattr(state, "records", None), list) or not state.records:
                state.status = "rejected"
                state.errors.append({"code": "records_missing", "message": f"[{self.name}] state.records missing/empty"})
                logger.warning("[%s] RUN rejected: records_missing", self.name)
                return state
            df_in = records_to_df(state.records)

        logger.debug("[%s] df_in shape=%s", self.name, getattr(df_in, "shape", None))

        # --- audit: original + segment start
        if self.capture_original:
            capture_original_once(
                state,
                df_in,
                keep_df=self.keep_original_df,
                key_cols=self.audit_key_cols,
            )

        df_before = df_in  # keep reference to compute change summary against input
        t0 = start_segment_audit(state, segment_name=self.name, df_in=df_in, key_cols=self.audit_key_cols)

        # Run segment
        res: ProcessResult = self.runner.run(state=state, inputs={"df": df_in})

        # state is canonical
        state = res.state
        state.errors.extend(res.errors)
        state.metrics.update(res.metrics)
        state.context.setdefault(f"{self.name}_meta", {}).update(res.meta)

        # Ensure output df exists
        if res.df is None:
            state.status = "rejected"
            state.errors.append({"code": "df_output_missing", "message": f"[{self.name}] transform returned df=None"})
            logger.warning("[%s] RUN rejected: df_output_missing", self.name)
            return state

        # Latest df lives here
        state.df = res.df

        # --- audit: segment end
        end_segment_audit(
            state,
            segment_name=self.name,
            df_before=df_before,
            df_after=state.df,
            t0=t0,
            null_cols=self.audit_null_cols,
            key_cols=self.audit_key_cols,
        )

        # Optional convert back
        if self.output_to == "records":
            state.records = df_to_records(state.df)

        logger.info(
            "[%s] RUN done | df_shape=%s errors_total=%s",
            self.name,
            getattr(state.df, "shape", None),
            len(state.errors),
        )
        return state


class PersistStep:
    """
    Sink step: writes the canonical dataframe to a repository.
    This is an orchestration PipelineStep (async), not a transform/gate step.
    """
    name = "persist"

    def __init__(self, repo: Any, *, mode: str = "best_effort") -> None:
        # mode: "best_effort" (default) or "enforce" (fail pipeline on error)
        self.repo = repo
        self.mode = mode

    async def run(self, state: PipelineState) -> PipelineState:
        event_id = getattr(state, "event_id", None)

        if state.df is None:
            msg = "No DataFrame to persist."
            logger.error("Persist failed: no DataFrame", extra={"step": self.name, "event_id": event_id})
            state.errors.append({"code": "no_df", "message": msg, "step": self.name})
            state.status = "failed" if self.mode == "enforce" else state.status
            return state

        records = state.df.to_dict(orient="records")
        count = len(records)

        logger.info("Persist start", extra={"step": self.name, "event_id": event_id, "records": count})

        # Optional: attach audit metadata to metrics (small only)
        audit = (state.context or {}).get("audit")
        if audit:
            state.metrics.setdefault("audit", {})
            # keep small metadata only
            state.metrics["audit"]["original_fp"] = audit.get("original", {}).get("df_fingerprint")
            state.metrics["audit"]["segments"] = {
                k: {
                    "duration_ms": v.get("duration_ms"),
                    "change_summary": v.get("change_summary"),
                }
                for k, v in (audit.get("segments") or {}).items()
            }

        try:
            # Prefer bulk/batch if available
            if hasattr(self.repo, "write_many"):
                await self.repo.write_many(records)
            elif hasattr(self.repo, "write_application_records"):
                await self.repo.write_application_records(records)
            elif hasattr(self.repo, "write_application_record"):
                for r in records:
                    await self.repo.write_application_record(r)
            elif hasattr(self.repo, "write"):
                for r in records:
                    await self.repo.write(r)
            else:
                logger.warning(
                    "Repo has no write method; skipping persistence",
                    extra={"step": self.name, "event_id": event_id},
                )
                state.metrics.setdefault("persist", {})
                state.metrics["persist"].update({"attempted": count, "persisted": 0, "skipped": True})
                return state

            state.metrics.setdefault("persist", {})
            state.metrics["persist"].update({"attempted": count, "persisted": count, "skipped": False})
            logger.info("Persist done", extra={"step": self.name, "event_id": event_id, "persisted": count})
            return state

        except Exception as e:
            logger.exception(
                "Persist failed",
                extra={"step": self.name, "event_id": event_id, "records": count},
            )
            state.errors.append(
                {"code": "persist_failed", "message": str(e), "step": self.name, "records": count}
            )

            if self.mode == "enforce":
                state.status = "failed"
            # best_effort: keep state.status as-is so pipeline can still complete
            return state
        
        
class ToDataFrameStep:
    name = "to_dataframe"

    async def run(self, state: PipelineState) -> PipelineState:
        if not state.records:
            state.status = "rejected"
            state.errors.append({"code": "no_records", "message": "No records to convert."})
            logger.warning(
                "ToDataFrame rejected: no records",
                extra={"step": self.name, "event_id": state.event_id},
            )
            return state

        state.df = pd.DataFrame(state.records)
        logger.info(
            "Converted records to DataFrame",
            extra={
                "step": self.name,
                "event_id": state.event_id,
                "rows": int(len(state.df)),
                "cols": int(len(state.df.columns)),
            },
        )
        return state
