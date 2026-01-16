from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Dict, Generic, List, Optional, Sequence, TypeVar

import logging
import time
from datetime import time as dt_time

from digital_operation_twin.core.models.pipeline_state import PipelineState
from digital_operation_twin.core.models.processIOAdaptor import ProcessIOAdapter
from digital_operation_twin.core.models.processIO import ProcessResult, ProcessFn, Registry

logger = logging.getLogger(__name__)


class ProcessConfigError(ValueError):
    pass


class ProcessRunner:
    ORDER_KEY = "__order__"

    def __init__(
        self,
        *,
        segment_name: str,
        cfg: Dict[str, Any],
        registry: Registry,
        default_order: Sequence[str],
        adapter: ProcessIOAdapter[Any],
        strict_cfg: bool = True,
        verbose: bool = False,
    ) -> None:
        if not isinstance(cfg, dict):
            raise ProcessConfigError(f"[{segment_name}] cfg must be dict, got {type(cfg).__name__}")
        if not registry:
            raise ProcessConfigError(f"[{segment_name}] registry must be non-empty")
        if not default_order:
            raise ProcessConfigError(f"[{segment_name}] default_order must be non-empty")

        self.segment_name = segment_name
        self.cfg = cfg
        self.registry = registry
        self.default_order = list(default_order)
        self.adapter = adapter
        self.strict_cfg = strict_cfg
        self.verbose = verbose

        self.steps: List[ProcessFn[Any]] = self._build_steps()
        logger.info("[%s] runner initialized | steps=%s", self.segment_name, self.selected_step_names())

    def _resolve_order(self) -> List[str]:
        override = self.cfg.get(self.ORDER_KEY)
        if override is None:
            return list(self.default_order)

        if not isinstance(override, list) or not all(isinstance(x, str) for x in override):
            raise ProcessConfigError(f"[{self.segment_name}] {self.ORDER_KEY} must be list[str], got: {repr(override)}")

        unknown = [x for x in override if x not in self.registry]
        if unknown:
            raise ProcessConfigError(f"[{self.segment_name}] {self.ORDER_KEY} has unknown steps: {unknown}")

        return override

    def _build_steps(self) -> List[ProcessFn[Any]]:
        known = set(self.registry.keys())

        if self.strict_cfg:
            unknown_cfg = [k for k in self.cfg.keys() if k not in known and k != self.ORDER_KEY]
            if unknown_cfg:
                raise ProcessConfigError(f"[{self.segment_name}] Unknown config keys: {unknown_cfg}")

        enabled = {k for k, v in self.cfg.items() if k in known and v not in (None, False)}
        order = self._resolve_order()

        ordered = [x for x in order if x in enabled]
        ordered.extend(sorted(enabled - set(ordered)))

        return [self.registry[name]() for name in ordered]

    def selected_step_names(self) -> List[str]:
        return [getattr(s, "__name__", s.__class__.__name__) for s in self.steps]

    def _merge(self, acc: ProcessResult, out: ProcessResult, *, step: str, duration_ms: int) -> ProcessResult:
        # state is the source of truth
        acc.state = out.state

        # convenience mirrors (optional)
        acc.df = out.df if out.df is not None else acc.df
        acc.records = out.records if out.records is not None else acc.records

        # errors + metrics
        acc.errors.extend(out.errors or [])
        acc.metrics.update(out.metrics or {})

        # propagate terminal status
        if out.status and out.status != "continue":
            acc.status = out.status

        # per-step observability
        acc.meta.setdefault("__steps__", {})
        acc.data.setdefault("__steps__", {})
        acc.meta["__steps__"][step] = {**(out.meta or {}), "duration_ms": duration_ms}
        acc.data["__steps__"][step] = out.data or {}

        return acc

    def run(self, *, state: PipelineState, inputs: Optional[Dict[str, Any]] = None) -> ProcessResult:
        inputs = inputs or {}

        acc = ProcessResult(state=state)

        for idx, step_fn in enumerate(self.steps, 1):
            step_name = getattr(step_fn, "__name__", step_fn.__class__.__name__)
            logger.debug("[%s] START step=%s (%s/%s)", self.segment_name, step_name, idx, len(self.steps))
            
            req = self.adapter.build_request(
                state=acc.state,
                cfg=self.cfg,
                segment_name=self.segment_name,
                inputs=inputs,
            )

            t0 = time.perf_counter()
            try:
                out = step_fn(req)
                inputs['df'] = out.df
            except Exception as e:
                logger.exception("[%s] FAILED step=%s", self.segment_name, step_name)
                return acc.reject(code="step_failed", message=f"{step_name} failed: {e}", step=step_name)

            duration_ms = int((time.perf_counter() - t0) * 1000)
            acc = self._merge(acc, out, step=step_name, duration_ms=duration_ms)
            
            

            if self.verbose:
                logger.debug("[%s] DONE step=%s status=%s errors_total=%s duration_ms=%s", self.segment_name, step_name, acc.status, len(acc.errors), duration_ms)
            else:
                logger.info("[%s] DONE step=%s (%sms)", self.segment_name, step_name, duration_ms)

            if acc.status in {"accepted", "rejected", "duplicate"}:
                logger.info("[%s] STOP early status=%s at step=%s", self.segment_name, acc.status, step_name)
                return acc

        return acc
