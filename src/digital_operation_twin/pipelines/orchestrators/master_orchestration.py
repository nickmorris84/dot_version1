from __future__ import annotations

import logging
from typing import Any, Dict

from digital_operation_twin.core.models.api_gate import APIModel
from digital_operation_twin.core.io.repo import Repository
from digital_operation_twin.core.io.idempotency import IdempotencyStore

from digital_operation_twin.core.registry.gate_registry import *
from digital_operation_twin.core.registry.transform_registry import *

from digital_operation_twin.core.models.pipeline_state import PipelineState
from .pipeline_runner import PipelineRunner
from .pipeline_steps import (
    GateStep,
    TransformStep,
    ToDataFrameStep,
    PersistStep,
)

logger = logging.getLogger(__name__)

class MasterOrchestrator:
    """
    Orchestration flow:

    gate_runner:
      - config-driven gate processes (extract/normalize/idempotency/schema report)

    processing_runner:
      - ToDataFrame
      - normalizer segment (config-driven)
      - standardiser segment (config-driven)
      - data_quality segment (config-driven)
      - schema validation again (enforce)
      - persist
    """

    def __init__(self, cfg: Dict[str, Any], repo: Any | None = None):
        self.cfg = cfg
        self.repo = repo or Repository()

        data_cfg = cfg.get("data_config", {})

        # Segment configs
        self.gate_cfg = data_cfg.get("gate", {})  # NEW: allow gate to be configured
        self.normaliser_cfg = data_cfg.get("normaliser", {})
        self.standardiser_cfg = data_cfg.get("standardiser", {})
        self.data_quality_cfg = data_cfg.get("data_quality", {})

        # Schema configs (same function, different mode)
        self.schema_report_cfg = data_cfg.get("schema_report", {"schema_validation": {"mode": "report"}})
        self.schema_enforce_cfg = data_cfg.get("schema_enforce", {"schema_validation": {"mode": "enforce"}})

        # Shared stores
        self.idempotency_store = IdempotencyStore()

        # -------------------------
        # Gate runner (registry-driven)
        # -------------------------
        self.gate_runner = PipelineRunner(
            steps=[
                GateStep(
                    name="gate",
                    cfg=self._merge_gate_cfg(),
                    registry=API_GATE_REGISTRY,
                    order=API_GATE_ORDER,
                    strict=True,
                    verbose=cfg.get("verbose_gate", False),
                ),
            ],
            name="api_gate",
        )

        # -------------------------
        # Processing runner
        # -------------------------
        self.processing_runner = PipelineRunner(
            steps=[
                ToDataFrameStep(),

                TransformStep(
                    name="normaliser",
                    cfg=self.normaliser_cfg,
                    registry=NORMALIZER_REGISTRY,
                    order=NORMALIZER_ORDER,
                    strict=True,
                    verbose=cfg.get("verbose_transforms", False),
                    input_from="records",
                    output_to="df",
                ),

                TransformStep(
                    name="standardiser",
                    cfg=self.standardiser_cfg,
                    registry=STANDARDIZER_REGISTRY,
                    order=STANDARDIZER_ORDER,
                    strict=True,
                    verbose=cfg.get("verbose_transforms", False),
                    input_from="df",
                    output_to="df",
                ),

                TransformStep(
                    name="validator",
                    cfg=self.data_quality_cfg,
                    registry=VALIDATOR_REGISTRY,
                    order=VALIDATOR_ORDER,
                    strict=True,
                    verbose=cfg.get("verbose_transforms", False),
                    input_from="df",
                    output_to="df",
                ),

                # final schema enforcement (reject if wrong)
                GateStep(
                    name="final_schema_validation",
                    cfg=self.schema_enforce_cfg,
                    registry={"schema_validation": schema_validation},
                    order=["schema_validation"],
                    strict=True,
                    verbose=cfg.get("verbose_gate", False),
                ),

                PersistStep(self.repo),
            ],
            name="event_processing",
        )

    def _merge_gate_cfg(self) -> Dict[str, Any]:
        """
        Gate cfg needs access to idempotency store + schema report mode.

        If you choose not to inject store via cfg, you can store it on state/context instead.
        """
        out = dict(self.gate_cfg or {})
        out.setdefault("schema_validation", {"mode": "report"})
        # you can inject store info for idempotency_check process
        out.setdefault("idempotency_check", {"enabled": True})
        return out

    async def gate(self, msg: APIModel) -> PipelineState:
        state = PipelineState(event_id=msg.envelope.event_id, context={"msg": msg}, cfg=self.cfg)
        return await self.gate_runner.run(state)

    async def process(self, accepted_state: PipelineState) -> PipelineState:
        accepted_state.status = "running"
        return await self.processing_runner.run(accepted_state)