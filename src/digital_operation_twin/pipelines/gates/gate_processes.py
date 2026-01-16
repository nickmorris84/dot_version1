from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple, Literal,Set, Type

import logging

from digital_operation_twin.core.models.processIO import ApiGateRequest, ProcessConfigError, ProcessResult, ProcessFn, Mode
from digital_operation_twin.core.utils import validate_one_record_against_model

logger = logging.getLogger(__name__)


def extract_envelope() -> Callable[[], ProcessFn]:
    step = "extract_envelope"

    def _fn(req: ApiGateRequest) -> ProcessResult:
        logger.debug("[%s.%s] START", req.step_name, step)
        state = req.state
        env = req.msg.envelope

        state.event_id = env.event_id
        state.schema_version = (env.attributes or {}).get("schema_version", state.schema_version)

        logger.debug("[%s.%s] DONE event_id=%s schema_version=%s", req.step_name, step, state.event_id, state.schema_version)
        return ProcessResult(state=state, meta={"event_id": state.event_id, "schema_version": state.schema_version})

    _fn.__name__ = step
    return _fn


def normalize_payload() -> Callable[[], ProcessFn]:
    step = "normalize_payload"

    def _fn(req: ApiGateRequest) -> ProcessResult:
        logger.debug("[%s.%s] START", req.step_name, step)
        state = req.state
        payload = req.msg.payload

        if isinstance(payload, list):
            records = payload
        elif isinstance(payload, dict):
            records = [payload]
        elif hasattr(payload, "model_dump"):
            records = [payload.model_dump()]
        elif hasattr(payload, "dict"):
            records = [payload.dict()]
        else:
            return ProcessResult(state=state).reject(
                code="invalid_payload",
                message=f"Unsupported payload type: {type(payload)}",
                payload_type=str(type(payload)),
            )

        if not records:
            return ProcessResult(state=state).reject(code="empty_payload", message="No records provided.")

        state.records = records
        logger.debug("[%s.%s] DONE records=%s", req.step_name, step, len(records))
        return ProcessResult(state=state, meta={"received": len(records)})

    _fn.__name__ = step
    return _fn


def idempotency(*, store: Any) -> Callable[[], ProcessFn]:
    """
    Gate process: idempotency (dedupe)

    Looks for a dedupe key in the envelope:
      - env.dedupe_key (preferred)
      - env.event_id (fallback)

    Config:
      idempotency: true
      idempotency:
        mode: "reject" | "accept" | "continue"     # default "reject"
        fail_open: true                             # default true (continue if store fails)
        mark_on_first_seen: true                     # default true (call mark if available)
        key_field: "dedupe_key"                      # default "dedupe_key" (envelope attribute)
        fallback_field: "event_id"                   # default "event_id"

    Mode behaviour:
      - reject: duplicates -> ProcessResult.reject(...)
      - accept: duplicates -> ProcessResult.accept()   (stops gate as accepted)
      - continue: duplicates -> continue but set meta flag
    """
    step = "idempotency"

    if store is None:
        raise ValueError("idempotency requires a store instance")

    # Allow passing either an instance or a class
    if isinstance(store, type):
        store = store()

    def _fn(req: ApiGateRequest) -> ProcessResult:
        logger.debug("[%s.%s] START", req.step_name, step)
        state = req.state

        cfg_val = req.cfg.get(step, None)

        # Disabled
        if cfg_val in (None, False):
            logger.debug("[%s.%s] SKIP (disabled)", req.step_name, step)
            return ProcessResult(state=state, meta={"skipped": True})

        # Enabled defaults
        if cfg_val is True:
            cfg: Dict[str, Any] = {}
        elif isinstance(cfg_val, dict):
            cfg = cfg_val
        else:
            raise ProcessConfigError(f"[{req.step_name}.{step}] must be true or dict, got: {repr(cfg_val)}")

        mode = cfg.get("mode", "reject")
        if mode not in {"reject", "accept", "continue"}:
            raise ProcessConfigError(f"[{req.step_name}.{step}] mode must be reject|accept|continue, got: {repr(mode)}")

        fail_open = cfg.get("fail_open", True)
        if not isinstance(fail_open, bool):
            raise ProcessConfigError(f"[{req.step_name}.{step}] fail_open must be bool, got: {type(fail_open).__name__}")

        mark_on_first_seen = cfg.get("mark_on_first_seen", True)
        if not isinstance(mark_on_first_seen, bool):
            raise ProcessConfigError(
                f"[{req.step_name}.{step}] mark_on_first_seen must be bool, got: {type(mark_on_first_seen).__name__}"
            )

        key_field = cfg.get("key_field", "dedupe_key")
        fallback_field = cfg.get("fallback_field", "event_id")
        if not isinstance(key_field, str) or not isinstance(fallback_field, str):
            raise ProcessConfigError(f"[{req.step_name}.{step}] key_field and fallback_field must be strings")

        # Envelope read (same pattern as your other gate steps)
        env = req.msg.envelope
        dedupe_key = getattr(env, key_field, None) or getattr(env, fallback_field, None)

        logger.info(
            "[%s.%s] Idempotency check dedupe_key=%s",
            req.step_name,
            step,
            dedupe_key,
            extra={"step": step, "event_id": getattr(state, "event_id", None), "dedupe_key": dedupe_key},
        )

        if not dedupe_key:
            logger.info(
                "[%s.%s] Idempotency skipped (no key)",
                req.step_name,
                step,
                extra={"step": step, "event_id": getattr(state, "event_id", None)},
            )
            logger.debug("[%s.%s] DONE skipped", req.step_name, step)
            return ProcessResult(state=state, meta={"skipped": True, "reason": "no_dedupe_key"})

        try:
            # Prefer keyword arg if supported; otherwise positional
            seen_fn = getattr(store, "seen", None)
            if not callable(seen_fn):
                raise ProcessConfigError(f"[{req.step_name}.{step}] store must implement seen(key)")

            varnames = getattr(getattr(seen_fn, "__code__", None), "co_varnames", ())
            is_dup = seen_fn(key=dedupe_key) if "key" in varnames else seen_fn(dedupe_key)

            if is_dup:
                logger.info(
                    "[%s.%s] Duplicate detected",
                    req.step_name,
                    step,
                    extra={"step": step, "event_id": getattr(state, "event_id", None), "dedupe_key": dedupe_key},
                )

                if mode == "reject":
                    return ProcessResult(state=state).reject(
                        code="duplicate_event",
                        message="Duplicate event detected",
                        step=step,
                        dedupe_key=dedupe_key,
                    )

                if mode == "accept":
                    # Stop gate early as accepted
                    return ProcessResult(state=state, meta={"duplicate": True, "dedupe_key": dedupe_key}).accept()

                # mode == "continue"
                logger.debug("[%s.%s] DONE duplicate (continue)", req.step_name, step)
                return ProcessResult(state=state, meta={"duplicate": True, "dedupe_key": dedupe_key})

            # Not a dup: optionally mark
            if mark_on_first_seen and hasattr(store, "mark") and callable(getattr(store, "mark")):
                store.mark(dedupe_key)

            logger.info(
                "[%s.%s] Idempotency passed",
                req.step_name,
                step,
                extra={"step": step, "event_id": getattr(state, "event_id", None), "dedupe_key": dedupe_key},
            )

            logger.debug("[%s.%s] DONE", req.step_name, step)
            return ProcessResult(state=state, meta={"duplicate": False, "dedupe_key": dedupe_key})

        except Exception as e:
            logger.exception(
                "[%s.%s] Idempotency store error",
                req.step_name,
                step,
                extra={"step": step, "event_id": getattr(state, "event_id", None), "dedupe_key": dedupe_key},
            )
            if fail_open:
                logger.debug("[%s.%s] DONE fail_open=True (continue)", req.step_name, step)
                return ProcessResult(state=state, meta={"error": str(e), "fail_open": True, "dedupe_key": dedupe_key})

            return ProcessResult(state=state).reject(
                code="idempotency_store_error",
                message="Idempotency store error",
                step=step,
                dedupe_key=dedupe_key,
                error=str(e),
            )

    _fn.__name__ = step
    return _fn
