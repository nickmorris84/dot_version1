from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional, Sequence
import pandas as pd
import logging

from digital_operation_twin.core.models.transformation_processIO import TransformationRequest, TranformationProcessFn, ProcessConfigError, ProcessResult, Issue

logger = logging.getLogger(__name__)


# ============================================================
# Runner class (single, reusable for normalizer/standardizer/validator)
# ============================================================

class TransformationRunner:
    """
    Configure once with:
      - cfg (dict): step configs enabling each step
      - registry (dict): step_name -> factory() returning TranformationProcessFn
      - order (sequence): default execution order

    Optional config override:
      __order__: [step_a, step_b, ...]
    """

    ORDER_KEY = "__order__"

    def __init__(
        self,
        *,
        cfg: Dict[str, Any],
        registry: Dict[str, Callable[[], TranformationProcessFn]],
        order: Sequence[str],
        strict: bool = True,
        verbose: bool = False,
    ):
        if not isinstance(cfg, dict):
            raise ProcessConfigError(f"cfg must be dict, got {type(cfg).__name__}")
        if not registry:
            raise ProcessConfigError("registry must be non-empty")
        if not order:
            raise ProcessConfigError("order must be non-empty")

        self.cfg = cfg
        self.registry = registry
        self.default_order = list(order)
        self.strict = strict
        self.verbose = verbose

        self.steps: List[TranformationProcessFn] = self._build_steps()

        logger.info(
            "[TransformationRunner] Initialized | enabled_steps=%s",
            self.selected_step_names(),
        )

    # ------------------------------------------------------------------
    # Step resolution
    # ------------------------------------------------------------------

    def _resolve_order(self) -> List[str]:
        """
        Determine execution order:
        1) cfg['__order__'] if present
        2) fallback to default order
        """
        override = self.cfg.get(self.ORDER_KEY)

        if override is None:
            return list(self.default_order)

        if not isinstance(override, list) or not all(isinstance(x, str) for x in override):
            raise ProcessConfigError(
                f"{self.ORDER_KEY} must be list[str], got {repr(override)}"
            )

        unknown = [s for s in override if s not in self.registry]
        if unknown:
            raise ProcessConfigError(
                f"{self.ORDER_KEY} contains unknown steps: {unknown}"
            )

        return override

    def _build_steps(self) -> List[TranformationProcessFn]:
        known_steps = set(self.registry.keys())

        # Validate unknown cfg keys (excluding reserved keys)
        if self.strict:
            unknown_cfg_keys = [
                k for k in self.cfg.keys()
                if k not in known_steps and k != self.ORDER_KEY
            ]
            if unknown_cfg_keys:
                raise ProcessConfigError(
                    f"Unknown config keys: {unknown_cfg_keys}. Known steps: {sorted(known_steps)}"
                )

        # Enabled steps = present and not False / None
        enabled = {
            name for name, val in self.cfg.items()
            if name in known_steps and val not in (None, False)
        }

        resolved_order = self._resolve_order()

        # Start with explicitly ordered enabled steps
        ordered_steps = [s for s in resolved_order if s in enabled]

        # Append any enabled steps not mentioned in override
        missing = sorted(enabled - set(ordered_steps))
        ordered_steps.extend(missing)

        steps: List[TranformationProcessFn] = []
        for name in ordered_steps:
            factory = self.registry.get(name)
            if not factory:
                raise ProcessConfigError(f"Step '{name}' not registered")
            steps.append(factory())

        return steps

    # ------------------------------------------------------------------
    # Execution
    # ------------------------------------------------------------------

    def run(self, df: pd.DataFrame) -> ProcessResult:
        if not isinstance(df, pd.DataFrame):
            raise TypeError(f"df must be DataFrame, got {type(df).__name__}")

        out = df.copy()
        all_issues: List[Issue] = []
        all_meta: Dict[str, Any] = {}

        for idx, step in enumerate(self.steps, 1):
            name = getattr(step, "__name__", step.__class__.__name__)
            before = out.shape

            logger.debug("[Runner] START %s (%s/%s)", name, idx, len(self.steps))
            try:
                result = step(TransformationRequest(df=out, cfg=self.cfg))
            except Exception as e:
                logger.error("[Runner] FAILED step=%s err=%s", name, e, exc_info=True)
                raise

            out = result.df
            all_issues.extend(result.issues)
            all_meta[name] = result.meta

            after = out.shape
            if self.verbose:
                logger.debug("[Runner] DONE %s %s→%s issues+%s", name, before, after, len(result.issues))
            else:
                logger.info("[Runner] DONE %s issues+%s", name, len(result.issues))

        logger.info(
            "[Runner] Complete | final_shape=%s | total_issues=%s",
            out.shape,
            len(all_issues),
        )
        return ProcessResult(df=out, issues=all_issues, meta=all_meta)

    # ------------------------------------------------------------------
    # Introspection / lifecycle
    # ------------------------------------------------------------------

    def selected_step_names(self) -> List[str]:
        return [getattr(s, "__name__", s.__class__.__name__) for s in self.steps]

    def rebuild(self, *, cfg: Optional[Dict[str, Any]] = None) -> None:
        """
        Rebuild steps if config changes.
        """
        if cfg is not None:
            if not isinstance(cfg, dict):
                raise ProcessConfigError("cfg must be dict")
            self.cfg = cfg

        self.steps = self._build_steps()
        logger.info("[TransformationRunner] Rebuilt | enabled_steps=%s", self.selected_step_names())


# ============================================================
# Example usage
# ============================================================
# runner = TransformationRunner(
#     cfg=normalizer_cfg,
#     registry=NORMALIZER_REGISTRY,
#     order=NORMALIZER_ORDER,
#     strict=True,
#     verbose=True,
# )
# result = runner.run(df)
# df_out = result.df
# issues = result.issues
# meta = result.meta


# ============================================================
# Usage examples
# ============================================================
# normalizer_result = run_steps(
#     df=df,
#     cfg=normalizer_cfg,
#     registry=NORMALIZER_REGISTRY,
#     order=NORMALIZER_ORDER,
#     strict=True,
#     verbose=True,
# )
#
# standardizer_result = run_steps(
#     df=df,
#     cfg=standardizer_cfg,
#     registry=STANDARDIZER_REGISTRY,
#     order=STANDARDIZER_ORDER,
# )
#
# validator_result = run_steps(
#     df=df,
#     cfg=validator_cfg,
#     registry=VALIDATOR_REGISTRY,
#     order=VALIDATOR_ORDER,
# )
# issues = validator_result.issues
