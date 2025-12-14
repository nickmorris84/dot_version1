from __future__ import annotations

from typing import Any, Dict

import pytest
import pandas as pd

from digital_operation_twin.core.models.api_gate import APIModel
from digital_operation_twin.core.models.pipeline_state import PipelineState
from digital_operation_twin.pipelines.orchestrators.steps import GateStep, ToDataFrameStep, NormalizerStep, StandardiserStep


def _msg(payload: Any) -> APIModel:
    body: Dict[str, Any] = {
        "envelope": {"event_id": "t-unit-step-1", "event_type": "x", "source": "unit", "attributes": {"schema_version": "v1"}},
        "payload": payload,
    }
    return APIModel(**body)


@pytest.mark.asyncio
async def test_gate_step_accepts_single_record() -> None:
    msg = _msg({"id": 1, "name": "one"})
    state = PipelineState(event_id=msg.envelope.event_id, context={"msg": msg}, cfg={"data_config": {}})
    step = GateStep(enable_validation=False)
    out = await step.run(state)
    assert out.status == "accepted"
    assert len(out.records) == 1
    assert out.records[0]["id"] == 1


@pytest.mark.asyncio
async def test_gate_step_accepts_list_payload() -> None:
    msg = _msg([{"id": 1}, {"id": 2}])
    state = PipelineState(event_id=msg.envelope.event_id, context={"msg": msg}, cfg={"data_config": {}})
    step = GateStep(enable_validation=False)
    out = await step.run(state)
    assert out.status == "accepted"
    assert len(out.records) == 2


@pytest.mark.asyncio
async def test_to_dataframe_step_builds_df() -> None:
    msg = _msg([{"id": 1, "name": "a"}, {"id": 2, "name": "b"}])
    state = PipelineState(event_id=msg.envelope.event_id, context={"msg": msg}, cfg={"data_config": {}}, records=[{"id": 1, "name": "a"}, {"id": 2, "name": "b"}])
    out = await ToDataFrameStep().run(state)
    assert out.df is not None
    assert list(out.df.columns) == ["id", "name"]
    assert out.df.shape == (2, 2)


@pytest.mark.asyncio
async def test_normaliser_and_standardiser_noop_with_empty_cfg() -> None:
    df = pd.DataFrame([{"id": 1, "name": "a"}])
    state = PipelineState(event_id="t-unit-step-2", df=df, cfg={"data_config": {}})
    out = await NormalizerStep({}).run(state)
    out = await StandardiserStep({}).run(out)
    assert out.df is not None
    assert out.df.shape == (1, 2)
