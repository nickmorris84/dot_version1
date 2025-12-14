from __future__ import annotations

import logging
from typing import Any, Dict, List
import pandas as pd

from digital_operation_twin.core.models.api_gate import APIModel
from digital_operation_twin.pipelines.transforms.schema_validation import schema_validation, DQError
from digital_operation_twin.core.models.pipeline_state import PipelineState

logger = logging.getLogger(__name__)


class GateStep:
    """
    Fast gate: normalize payload to list-of-dicts + optional schema/DQ validation.
    No heavy transforms here.
    """
    name = "gate"

    def __init__(self, *, enable_validation: bool = True) -> None:
        self.enable_validation = enable_validation

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

        if not records:
            state.status = "rejected"
            state.errors.append({"code": "empty_payload", "message": "No records provided."})
            logger.warning(
                "Gate rejected: empty payload",
                extra={"step": self.name, "event_id": state.event_id},
            )
            return state

        logger.info(
            "Gate normalized payload",
            extra={"step": self.name, "event_id": state.event_id, "received": len(records)},
        )

        # Optional schema/DQ validation per record (fast-ish)
        if self.enable_validation:
            normalized: List[Dict[str, Any]] = []
            for idx, rec in enumerate(records):
                try:
                    _, out = schema_validation(rec, state.schema_version)
                    normalized.append(out)
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
                        },
                    )
                    return state

            state.records = normalized
            logger.info(
                "Gate schema validation ok",
                extra={"step": self.name, "event_id": state.event_id, "validated": len(state.records)},
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


class IdempotencyStep:
    name = "idempotency"

    def __init__(self, store) -> None:
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
            if self.store.seen(dedupe_key):
                state.status = "duplicate"
                logger.info(
                    "Duplicate detected",
                    extra={"step": self.name, "event_id": state.event_id, "dedupe_key": dedupe_key},
                )
                return state

            # mark only AFTER acceptance
            self.store.mark(dedupe_key)
            logger.info(
                "Idempotency marked",
                extra={"step": self.name, "event_id": state.event_id, "dedupe_key": dedupe_key},
            )
            return state

        except Exception:
            # Idempotency should be best-effort: log, but allow processing to continue.
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
        assert state.df is not None
        logger.info(
            "DataQuality start",
            extra={"step": self.name, "event_id": state.event_id, "rows": int(len(state.df))},
        )
        state.df = self.service.run(state.df)
        logger.info(
            "DataQuality done",
            extra={"step": self.name, "event_id": state.event_id, "rows": int(len(state.df))},
        )
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
