from __future__ import annotations

import logging
import json
import os
from typing import Any, Dict, List, Optional
import pandas as pd

from digital_operation_twin.core.models.api_gate import APIModel
from digital_operation_twin.pipelines.transforms.schema_validation import schema_validation, DQError
from digital_operation_twin.core.models.pipeline_state import PipelineState

logger = logging.getLogger(__name__)

_LOG_DATA = os.getenv("DOT_LOG_DATA", "0").lower() in {"1","true","yes","y"}
_MAX_CHARS = int(os.getenv("DOT_LOG_DATA_MAX_CHARS", "2000"))


def _preview(obj: Any) -> str:
    try:
        s = json.dumps(obj, default=str)
    except Exception:
        s = str(obj)
    return s if len(s) <= _MAX_CHARS else s[:_MAX_CHARS] + "...(truncated)"



class GateStep:
    """
    Fast gate: normalize payload to list-of-dicts + optional schema/DQ validation.
    No heavy transforms here.
    """
    name = "gate"

    def __init__(self, *, enable_validation: bool = True, validation_mode: str = "report") -> None:
        # validation_mode: "report" (log-only) or "enforce" (reject early)
        self.enable_validation = enable_validation
        self.validation_mode = validation_mode

    async def run(self, state: PipelineState) -> PipelineState:
        msg: APIModel = state.context["msg"]

        env = msg.envelope
        state.event_id = env.event_id
        state.schema_version = (env.attributes or {}).get("schema_version", state.schema_version)

        logger.info(
            "Gate start",
            extra={
                "step": self.name,
                "event_id": state.event_id,
                "schema_version": state.schema_version,
                "source": getattr(env, "source", None),
                "event_type": getattr(env, "event_type", None),
            },
        )

        payload = msg.payload

        # Normalize payload shape to list[dict]
        records: List[Dict[str, Any]]
        if isinstance(payload, list):
            records = payload
        elif isinstance(payload, dict):
            records = [payload]
        else:
            # Payload might be a Pydantic model (Payload); allow conversion
            if hasattr(payload, "model_dump"):
                records = [payload.model_dump()]
            elif hasattr(payload, "dict"):
                records = [payload.dict()]
            else:
                state.status = "rejected"
                state.errors.append(
                    {"code": "invalid_payload", "message": f"Unsupported payload type: {type(payload)}"}
                )
                logger.warning(
                    "Gate rejected: invalid payload type",
                    extra={"step": self.name, "event_id": state.event_id, "payload_type": str(type(payload))},
                )
                return state

        # Now it is safe to log len(records)
        logger.info(
            "Gate normalized payload",
            extra={"step": self.name, "event_id": state.event_id, "received": len(records)},
        )

        if _LOG_DATA:
            logger.debug(
                "Gate received payload_type=%s record_count=%s",
                str(type(payload)),
                len(records),
                extra={"step": self.name, "event_id": state.event_id},
            )

        if not records:
            state.status = "rejected"
            state.errors.append({"code": "empty_payload", "message": "No records provided."})
            logger.warning(
                "Gate rejected: empty payload",
                extra={"step": self.name, "event_id": state.event_id},
            )
            return state

        # Optional schema/DQ validation per record
        if self.enable_validation:
            state.metrics.setdefault("schema_report", [])
            normalized_records: List[Dict[str, Any]] = []

            for idx, rec in enumerate(records):
                try:
                    # report mode logs diffs, enforce mode validates/normalizes and can reject
                    _, out, diff = schema_validation(rec, state.schema_version, mode=self.validation_mode)

                    missing = diff.get("missing_required", [])
                    extras = diff.get("extra_fields", [])

                    for idx, rec in enumerate(records):
                        _, _, diff = schema_validation(rec, state.schema_version, mode="report")

                        missing = diff.get("missing_required") or []
                        extras = diff.get("extra_fields") or []

                        if missing or extras:
                            logger.info(
                                f"Gate schema report idx={idx} missing_required={missing} extra_fields={extras}",
                                extra={"step": self.name, "event_id": state.event_id, "schema_version": state.schema_version, "index": idx},
                            )
                        else:
                            logger.debug(
                                f"Gate schema report idx={idx} ok",
                                extra={"step": self.name, "event_id": state.event_id, "schema_version": state.schema_version, "index": idx},
                            )


                    state.metrics["schema_report"].append({"index": idx, **diff})

                    # Keep original record if report-only; use normalized if enforce
                    normalized_records.append(out if self.validation_mode == "enforce" else rec)

                except DQError as e:
                    state.status = "rejected"
                    state.errors.append({"index": idx, "code": e.code, "message": str(e), "details": e.details})
                    logger.warning(
                        "Gate rejected: schema/DQ validation failed",
                        extra={
                            "step": self.name,
                            "event_id": state.event_id,
                            "schema_version": state.schema_version,
                            "index": idx,
                            "dq_code": getattr(e, "code", "dq_error"),
                            "details": getattr(e, "details", {}),
                        },
                    )
                    return state

            state.records = normalized_records

            if _LOG_DATA and state.records:
                logger.debug(
                    "Gate records sample=%s",
                    _preview(state.records[0]),
                    extra={"step": self.name, "event_id": state.event_id},
                )

            logger.info(
                "Gate schema check complete",
                extra={"step": self.name, "event_id": state.event_id, "records": len(state.records)},
            )
        else:
            state.records = records
            logger.info(
                "Gate validation disabled",
                extra={"step": self.name, "event_id": state.event_id, "records": len(state.records)},
            )

        state.status = "accepted"
        state.metrics["received"] = len(state.records)

        logger.info(
            "Gate accepted",
            extra={"step": self.name, "event_id": state.event_id, "received": len(state.records)},
        )
        return state


class FinalSchemaValidationStep:
    """
    Enforce Event schema right before persistence.
    If anything still doesn't satisfy Event required fields -> reject & short-circuit.
    """
    name = "final_schema_validation"

    def __init__(self, *, schema_version: Optional[str] = None) -> None:
        self.schema_version = schema_version  # allow override; else uses state.schema_version

    async def run(self, state: PipelineState) -> PipelineState:
        if state.df is None:
            state.status = "rejected"
            state.errors.append({"code": "no_df", "message": "No DataFrame provided to final schema validation."})
            logger.warning("Final schema validation rejected: no df", extra={"step": self.name, "event_id": state.event_id})
            return state

        if not isinstance(state.df, pd.DataFrame):
            state.status = "rejected"
            state.errors.append({"code": "invalid_df", "message": f"Expected DataFrame, got {type(state.df).__name__}"})
            logger.warning(
                "Final schema validation rejected: invalid df type",
                extra={"step": self.name, "event_id": state.event_id, "df_type": type(state.df).__name__},
            )
            return state

        sv = self.schema_version or state.schema_version
        records = state.df.to_dict(orient="records")

        for idx, rec in enumerate(records):
            try:
                _, normalized, diff = schema_validation(rec, sv, mode="enforce")
            except DQError as e:
                state.status = "rejected"
                state.errors.append({"index": idx, "code": e.code, "message": str(e), "details": e.details})
                logger.warning(
                    "Final schema validation failed",
                    extra={
                        "step": self.name,
                        "event_id": state.event_id,
                        "index": idx,
                        "dq_code": e.code,
                        "missing_required": (e.details or {}).get("missing_required", []),
                        "extra_fields": (e.details or {}).get("extra_fields", []),
                    },
                )
                return state

            # Optional: you *can* replace df row with normalized fields here if you want canonical output.
            # For now, we just enforce.
            if diff.get("extra_fields"):
                logger.info(
                    "Final schema validation: extras present (allowed)",
                    extra={"step": self.name, "event_id": state.event_id, "index": idx, "extra_fields": diff["extra_fields"]},
                )

        logger.info(
            "Final schema validation ok",
            extra={"step": self.name, "event_id": state.event_id, "rows": len(records)},
        )
        return state


class IdempotencyStep:
    name = "idempotency"

    def __init__(self, store) -> None:
        # Allow passing either an instance or the class (common mistake)
        if store is None:
            raise ValueError("IdempotencyStep requires a store instance")
        if isinstance(store, type):
            store = store()  # instantiate if a class was passed by accident
        self.store = store

    async def run(self, state: PipelineState) -> PipelineState:
        env = state.context["msg"].envelope
        dedupe_key = env.dedupe_key or env.event_id

        logger.info(
            "Idempotency check",
            extra={"step": self.name, "event_id": state.event_id, "dedupe_key": dedupe_key},
        )

        if not dedupe_key:
            logger.info(
                "Idempotency skipped: no dedupe_key",
                extra={"step": self.name, "event_id": state.event_id},
            )
            return state

        try:
            # Use keyword arg to reduce signature mismatch risk
            is_dup = self.store.seen(key=dedupe_key) if "key" in getattr(self.store.seen, "__code__", ()).co_varnames else self.store.seen(dedupe_key)

            if is_dup:
                state.status = "duplicate"
                logger.info(
                    "Duplicate detected",
                    extra={"step": self.name, "event_id": state.event_id, "dedupe_key": dedupe_key},
                )
                return state

            # Some stores auto-mark inside seen(); only call mark if it exists
            if hasattr(self.store, "mark"):
                self.store.mark(dedupe_key)

            logger.info(
                "Idempotency passed",
                extra={"step": self.name, "event_id": state.event_id, "dedupe_key": dedupe_key},
            )
            return state

        except Exception:
            logger.exception(
                "Idempotency store error; continuing without dedupe",
                extra={"step": self.name, "event_id": state.event_id, "dedupe_key": dedupe_key},
            )
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


class NormalizerStep:
    name = "normalize"

    def __init__(self, normalizer_cfg: Dict[str, Any]):
        from digital_operation_twin.pipelines.transforms.normaliser import NormalizerPipeline

        self.pipeline = NormalizerPipeline(normalizer_cfg)

    async def run(self, state: PipelineState) -> PipelineState:
        assert state.df is not None
        logger.info(
            "Normalizer start",
            extra={"step": self.name, "event_id": state.event_id, "rows": int(len(state.df))},
        )
        state.df = self.pipeline.run(state.df)
        logger.info(
            "Normalizer done",
            extra={"step": self.name, "event_id": state.event_id, "rows": int(len(state.df))},
        )
        return state


class StandardiserStep:
    name = "standardise"

    def __init__(self, standardiser_cfg: Dict[str, Any]):
        from digital_operation_twin.pipelines.transforms.standardiser import StandardiserPipeline

        self.pipeline = StandardiserPipeline(standardiser_cfg)

    async def run(self, state: PipelineState) -> PipelineState:
        assert state.df is not None
        logger.info(
            "Standardiser start",
            extra={"step": self.name, "event_id": state.event_id, "rows": int(len(state.df))},
        )
        state.df = self.pipeline.run(state.df)
        logger.info(
            "Standardiser done",
            extra={"step": self.name, "event_id": state.event_id, "rows": int(len(state.df))},
        )
        return state


class DataQualityStep:
    name = "data_quality"

    def __init__(self, dq_cfg: Dict[str, Any]):
        from digital_operation_twin.pipelines.transforms.quality import DataQualityService
        self.service = DataQualityService(dq_cfg)

    async def run(self, state: PipelineState) -> PipelineState:
        if state.df is None:
            state.status = "rejected"
            state.errors.append({"code": "no_df", "message": "No DataFrame provided to DataQuality."})
            logger.warning("DataQuality rejected: no df", extra={"step": self.name, "event_id": state.event_id})
            return state

        if not isinstance(state.df, pd.DataFrame):
            raise TypeError(f"DataQualityStep expected DataFrame, got {type(state.df).__name__}")

        logger.info(
            "DataQuality start",
            extra={"step": self.name, "event_id": state.event_id, "rows": int(len(state.df))},
        )

        try:
            state.df = self.service.run(state.df)
        except Exception as e:
            # Decide your behavior: reject gracefully (below) instead of blowing up the whole pipeline
            state.status = "rejected"
            state.errors.append({"code": "data_quality_failed", "message": str(e)})
            logger.warning(
                "DataQuality rejected",
                extra={"step": self.name, "event_id": state.event_id, "reason": str(e)},
            )
            return state

        logger.info(
            "After DataQuality",
            extra={"step": self.name, "event_id": state.event_id, "df_type": type(state.df).__name__},
        )

        logger.info(
            "DataQuality done",
            extra={"step": self.name, "event_id": state.event_id, "rows": int(len(state.df))},
        )

        # enforce contract for downstream steps
        if not isinstance(state.df, pd.DataFrame):
            raise TypeError(f"DataQualityStep must return DataFrame, got {type(state.df).__name__}")

        return state


class PersistStep:
    """
    Best-effort persistence: convert df back to records and write.
    Replace with your Repository implementation (bulk insert etc.) later.
    """
    name = "persist"

    def __init__(self, repo: Any):
        self.repo = repo

    async def run(self, state: PipelineState) -> PipelineState:
        if state.df is None:
            state.status = "failed"
            state.errors.append({"code": "no_df", "message": "No DataFrame to persist."})
            logger.error(
                "Persist failed: no DataFrame",
                extra={"step": self.name, "event_id": state.event_id},
            )
            return state

        records = state.df.to_dict(orient="records")
        count = len(records)

        logger.info(
            "Persist start",
            extra={"step": self.name, "event_id": state.event_id, "records": count},
        )

        try:
            if hasattr(self.repo, "write_application_record"):
                for r in records:
                    await self.repo.write_application_record(r)
            elif hasattr(self.repo, "write"):
                for r in records:
                    await self.repo.write(r)
            else:
                logger.warning(
                    "Repo has no write method; skipping persistence",
                    extra={"step": self.name, "event_id": state.event_id},
                )

            state.metrics["processed"] = count
            logger.info(
                "Persist done",
                extra={"step": self.name, "event_id": state.event_id, "processed": count},
            )
            return state

        except Exception:
            logger.exception(
                "Persist failed",
                extra={"step": self.name, "event_id": state.event_id, "records": count},
            )
            state.status = "failed"
            state.errors.append({"code": "persist_failed", "message": "Persistence step failed."})
            return state
