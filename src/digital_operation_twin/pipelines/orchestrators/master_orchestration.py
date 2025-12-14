from __future__ import annotations

import logging
from typing import Any, Dict

from digital_operation_twin.core.models.api_gate import APIModel
from digital_operation_twin.core.io.repo import Repository
from digital_operation_twin.core.io.idempotency import IdempotencyStore

from digital_operation_twin.core.models.pipeline_state import PipelineState
from .runner import PipelineRunner
from .steps import (
    GateStep,
    IdempotencyStep,
    ToDataFrameStep,
    NormalizerStep,
    StandardiserStep,
    DataQualityStep,
    PersistStep,
)

logger = logging.getLogger(__name__)


class MasterOrchestrator:
    """
    Two-phase orchestration:
      1) gate_runner: fast checks, returns accepted/rejected/duplicate
      2) processing_runner: heavy DF pipeline, intended to run async
    """

    def __init__(self, cfg: Dict[str, Any], repo: Any | None = None):
        self.cfg = cfg
        self.repo = repo or Repository()

        data_cfg = cfg.get("data_config", {})
        self.normaliser_cfg = data_cfg.get("normaliser", {})
        self.standardiser_cfg = data_cfg.get("standardiser", {})
        self.dq_cfg = data_cfg.get("data_quality", {})

        self.gate_runner = PipelineRunner(
            steps=[
                GateStep(enable_validation=False),
                IdempotencyStep(IdempotencyStore)],
            
            name="gate",
        )

        self.processing_runner = PipelineRunner(
            steps=[
                ToDataFrameStep(),
                NormalizerStep(self.normaliser_cfg),
                StandardiserStep(self.standardiser_cfg),
                DataQualityStep(self.dq_cfg),
                PersistStep(self.repo),
            ],
            name="event cleaning",
        )

    async def gate(self, msg: APIModel) -> PipelineState:
        state = PipelineState(event_id=msg.envelope.event_id, context={"msg": msg}, cfg=self.cfg)
        return await self.gate_runner.run(state)

    async def process(self, accepted_state: PipelineState) -> PipelineState:
        # accepted_state.records must exist here
        accepted_state.status = "running"
        return await self.processing_runner.run(accepted_state)
