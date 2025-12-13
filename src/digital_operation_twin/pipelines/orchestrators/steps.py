from __future__ import annotations

import logging
from typing import Any, Dict, List
import pandas as pd

from digital_operation_twin.core.models.api import APIModel
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

        # payload may be dict or list[dict] (per your updated ingestion_models)
        payload = msg.payload

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
                state.errors.append({"code": "invalid_payload", "message": f"Unsupported payload type: {type(payload)}"})
                return state

        if not records:
            state.status = "rejected"
            state.errors.append({"code": "empty_payload", "message": "No records provided."})
            return state

        # Optional DQ validation per record (fast-ish)
        if self.enable_validation:
            normalized: List[Dict[str, Any]] = []
            for idx, rec in enumerate(records):
                try:
                    _, out = schema_validation(rec, state.schema_version)
                    normalized.append(out)
                except DQError as e:
                    state.status = "rejected"
                    state.errors.append({"index": idx, "code": e.code, "message": str(e), "details": e.details})
                    return state
            state.records = normalized
        else:
            state.records = records

        state.status = "accepted"
        state.metrics["received"] = len(state.records)
        return state


class IdempotencyStep:
    name = "idempotency"

    def __init__(self, store):
        self.store = store

    async def run(self, state: PipelineState) -> PipelineState:
        env = state.context["msg"].envelope
        dedupe_key = env.dedupe_key or env.event_id

        if not dedupe_key:
            return state  # nothing to dedupe on

        if self.store.seen(dedupe_key):
            state.status = "duplicate"
            return state

        # mark only AFTER acceptance
        self.store.mark(dedupe_key)
        return state
    

class ToDataFrameStep:
    name = "to_dataframe"

    async def run(self, state: PipelineState) -> PipelineState:
        if not state.records:
            state.status = "rejected"
            state.errors.append({"code": "no_records", "message": "No records to convert."})
            return state
        state.df = pd.DataFrame(state.records)
        return state


class NormalizerStep:
    name = "normalize"

    def __init__(self, normalizer_cfg: Dict[str, Any]):
        from digital_operation_twin.pipelines.transforms.normaliser import NormalizerPipeline
        self.pipeline = NormalizerPipeline(normalizer_cfg)

    async def run(self, state: PipelineState) -> PipelineState:
        assert state.df is not None
        state.df = self.pipeline.run(state.df)
        return state


class StandardiserStep:
    name = "standardise"

    def __init__(self, standardiser_cfg: Dict[str, Any]):
        from digital_operation_twin.pipelines.transforms.standardiser import StandardiserPipeline
        self.pipeline = StandardiserPipeline(standardiser_cfg)

    async def run(self, state: PipelineState) -> PipelineState:
        assert state.df is not None
        state.df = self.pipeline.run(state.df)
        return state


class DataQualityStep:
    name = "data_quality"

    def __init__(self, dq_cfg: Dict[str, Any]):
        from digital_operation_twin.pipelines.transforms.quality import DataQualityService
        self.service = DataQualityService(dq_cfg)

    async def run(self, state: PipelineState) -> PipelineState:
        assert state.df is not None
        state.df = self.service.run(df=state.df)
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
            return state

        records = state.df.to_dict(orient="records")
        # Example: row-by-row; later swap to bulk insert
        if hasattr(self.repo, "write_application_record"):
            for r in records:
                await self.repo.write_application_record(r)
        elif hasattr(self.repo, "write"):
            for r in records:
                await self.repo.write(r)

        state.metrics["processed"] = len(records)
        return state
