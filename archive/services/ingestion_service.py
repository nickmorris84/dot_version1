from __future__ import annotations
import os
from typing import Dict, Any
from ..core.io.ingestion_models import IngestMessage
from ..core.io.idempotency import IdempotencyStore
from ..core.quality.ingestion_dq import normalize_and_validate, DQError
from .initialise_events_service import EventInput
from src.digital_operation_twin.core.logger import get_logger
from ..services.initialise_events_service import InitialiseEventsService
from ..core.io.repo import Repository

# Toggle validation (can be set via environment variable)
ENABLE_VALIDATION = os.getenv("ENABLE_VALIDATION", "false").lower() == "true"

logger = get_logger(__name__)

class IngestionService:
    def __init__(self, repo: Repository | None = None, idempotency: IdempotencyStore | None = None):
        self.repo = repo or Repository()
        self.idempotency = idempotency or IdempotencyStore()

    async def process(self, msg: IngestMessage) -> Dict[str, Any]:
        env = msg.envelope
        raw = msg.payload.model_dump()  # raw, dynamic input
        
        logger.info(f"[info] columns={ds.df.columns}")

        # 1) Idempotency check
        key = env.dedupe_key or env.event_id
        if self.idempotency.seen(key):
            return {
                "status": "duplicate",
                "event_id": env.event_id
            }
            
        # 2) Create Events: normalize, standardize, validate, DQ, classify   
        try:
            bronze_data = InitialiseEventsService(customer_id = "customer_a", source = raw)
        except Exception as e:
            return {
                "status": "rejected",
                "event_id": env.event_id,
                "error_code": e.code,
                "message": str(e),
                "details": e.details,
                # "schema_version": schema_version
            }



