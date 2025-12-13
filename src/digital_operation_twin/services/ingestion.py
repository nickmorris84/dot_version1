from __future__ import annotations

import os
from typing import Any, Dict, List, Optional
from logging import getLogger

# Core ingestion contracts and infrastructure
from src.digital_operation_twin.core.models.api import APIModel
from digital_operation_twin.core.io.idempotency import IdempotencyStore
from src.digital_operation_twin.core.io.repo import Repository

# Data quality / validation pipeline
from src.digital_operation_twin.pipelines.transforms.schema_validation import (
    schema_validation,
    DQError,
)

logger = getLogger(__name__)

# Feature flag: allows validation to be switched on/off without code changes
ENABLE_SCHEMA_VALIDATION = os.getenv("ENABLE_SCHEMA_VALIDATION", "false").lower() == "true"


class IngestionService:
    """
    Core ingestion orchestration service.

    Responsibilities:
    - Accept a single ingestion contract (IngestMessage)
    - Normalize payload into batch form (list of records)
    - Apply idempotency protection
    - Optionally validate/normalize records
    - Persist accepted records
    - Return a structured result for REST or Pub/Sub callers

    This service is intentionally:
    - transport-agnostic (REST, Pub/Sub, CSV, batch jobs)
    - safe to run during refactors (best-effort persistence & validation)
    """

    def __init__(
        self,
        repo: Optional[Any] = None,
        idempotency: Optional[Any] = None,
        default_schema_version: str = "v1",
    ) -> None:
        """
        Constructor supports dependency injection for testing or alternate backends.

        Args:
            repo: persistence layer (Repository). Defaults to concrete Repository.
            idempotency: idempotency store. Defaults to IdempotencyStore.
            default_schema_version: fallback schema version for validation.
        """
        self.repo = repo or Repository()
        self.idempotency = idempotency or IdempotencyStore()
        self.default_schema_version = default_schema_version

    async def process(self, msg: APIModel) -> Dict[str, Any]:
        """
        Main ingestion entrypoint.

        Called by:
        - REST router
        - Pub/Sub router
        - Any future ingestion transport

        Args:
            msg: IngestMessage containing envelope metadata + payload

        Returns:
            Structured result describing ingestion outcome
        """
        # --- Extract envelope and payload defensively
        env = getattr(msg, "envelope", None)
        payload = getattr(msg, "payload", None)

        event_id = getattr(env, "event_id", None)

        # Dedupe key priority: explicit dedupe_key > event_id
        dedupe_key = getattr(env, "dedupe_key", None) or event_id

        # --- Idempotency guard (fail-fast)
        if self.idempotency and dedupe_key:
            try:
                if self.idempotency.seen(dedupe_key):
                    return {
                        "status": "duplicate",
                        "event_id": event_id,
                        "dedupe_key": dedupe_key,
                    }
            except Exception:
                # Idempotency failure should not crash ingestion
                logger.exception("Idempotency check failed; continuing without dedupe.")

        # --- Determine schema version for downstream validation
        schema_version = self._get_schema_version(env)

        # --- Normalize payload to a list (single-record and batch share the same path)
        records = self._coerce_payload_to_records(payload)

        # Reject empty payloads explicitly
        if not records:
            return {
                "status": "rejected",
                "event_id": event_id,
                "schema_version": schema_version,
                "received": 0,
                "processed": 0,
                "failed": 0,
                "error_code": "empty_payload",
                "message": "No records provided in payload.",
            }

        processed = 0
        failed = 0
        errors_sample: List[Dict[str, Any]] = []

        # --- Core per-record processing loop
        for idx, rec in enumerate(records):
            try:
                # Ensure downstream pipeline only sees dict-like records
                rec_dict = self._ensure_dict(rec)

                # Optional validation / normalization
                if ENABLE_SCHEMA_VALIDATION:
                    _, rec_dict = schema_validation(rec_dict, schema_version)

                # Persist record (best-effort)
                await self._persist(rec_dict)

                processed += 1

            except DQError as e:
                # Expected data-quality failures (do not crash batch)
                failed += 1
                if len(errors_sample) < 10:
                    errors_sample.append(
                        {
                            "index": idx,
                            "error_code": getattr(e, "code", "dq_error"),
                            "message": str(e),
                            "details": getattr(e, "details", {}),
                        }
                    )

            except Exception as e:
                # Unexpected system/runtime failures
                failed += 1
                logger.exception("Record processing failed (index=%s)", idx)
                if len(errors_sample) < 10:
                    errors_sample.append(
                        {
                            "index": idx,
                            "error_code": "processing_error",
                            "message": str(e),
                            "details": {},
                        }
                    )

        # --- Mark idempotency after processing (best-effort)
        if self.idempotency and dedupe_key:
            try:
                if hasattr(self.idempotency, "mark"):
                    self.idempotency.mark(dedupe_key)
                elif hasattr(self.idempotency, "set"):
                    self.idempotency.set(dedupe_key)
            except Exception:
                logger.exception("Idempotency mark failed.")

        # --- Shape final response
        if processed == 0:
            return {
                "status": "rejected",
                "event_id": event_id,
                "schema_version": schema_version,
                "received": len(records),
                "processed": processed,
                "failed": failed,
                "errors_sample": errors_sample,
            }

        if failed > 0:
            return {
                "status": "partial_ok",
                "event_id": event_id,
                "schema_version": schema_version,
                "received": len(records),
                "processed": processed,
                "failed": failed,
                "errors_sample": errors_sample,
            }

        return {
            "status": "ok",
            "event_id": event_id,
            "schema_version": schema_version,
            "received": len(records),
            "processed": processed,
            "failed": failed,
            "data": self.repo,
        }

    # ------------------------------------------------------------------
    # Helper methods
    # ------------------------------------------------------------------

    def _get_schema_version(self, env: Any) -> str:
        """
        Extract schema version from envelope attributes.
        Allows versioned validation / transforms later.
        """
        attrs = getattr(env, "attributes", None) or {}
        if isinstance(attrs, dict):
            sv = attrs.get("schema_version")
            if sv:
                return str(sv)
        return self.default_schema_version

    def _coerce_payload_to_records(self, payload: Any) -> List[Any]:
        """
        Normalize payload into list form.

        Supported input:
        - list[...]          → batch
        - dict               → single record
        - pydantic model     → single record via .dict()
        """
        if payload is None:
            return []

        if isinstance(payload, list):
            return payload

        if isinstance(payload, dict):
            return [payload]

        if hasattr(payload, "dict"):
            try:
                return [payload.dict()]
            except Exception:
                pass

        # Last resort: treat as single item; _ensure_dict will reject if invalid
        return [payload]

    def _ensure_dict(self, obj: Any) -> Dict[str, Any]:
        """
        Guardrail to ensure pipeline only receives dictionaries.
        """
        if isinstance(obj, dict):
            return obj
        if hasattr(obj, "dict"):
            return obj.dict()
        raise TypeError(f"Record must be a dict-like object, got: {type(obj)}")

    async def _persist(self, record: Dict[str, Any]) -> None:
        """
        Best-effort persistence abstraction.

        - Allows repo implementation to evolve without touching service
        - No-op if repo not configured (during early dev / tests)
        """
        if not self.repo:
            logger.info(
                "Repo not configured; skipping persistence for record keys=%s",
                list(record.keys()),
            )
            return

        if hasattr(self.repo, "write_application_record"):
            await self.repo.write_application_record(record)
            return

        if hasattr(self.repo, "write"):
            await self.repo.write(record)
            return

        logger.warning("Repo has no known write method; skipping persistence.")
