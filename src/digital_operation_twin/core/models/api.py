from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional, Union
import uuid

from pydantic import BaseModel, Field

# -----------------------------
# Edge models (lenient / transport contract)
# -----------------------------

class Envelope(BaseModel):
    """
    Transport metadata. Keep lenient to allow evolution without breaking producers.
    """
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    event_type: str = Field(default="record.ingest")
    source: Optional[str] = None
    produced_at: datetime = Field(default_factory=datetime.utcnow)
    dedupe_key: Optional[str] = None
    attributes: Dict[str, Any] = Field(default_factory=dict)

    model_config = {"extra": "allow"}  # tolerate extra keys at the edge


class Payload(BaseModel):
    """
    Dynamic payload container (single record). Accepts any keys.
    Strict validation happens later in the DQ/domain layer.
    """
    model_config = {"extra": "allow"}


# Payload can be:
# - a Payload model (single record)
# - a raw dict (single record)
# - a list of dicts (batch)
Record = Dict[str, Any]
PayloadType = Union[Payload, Record, List[Record]]


class APIModel(BaseModel):
    """
    Main ingestion message contract used by REST + Pub/Sub.
    Supports single record or batch payload.
    """
    envelope: Envelope
    payload: PayloadType

    model_config = {"extra": "allow"}


# -----------------------------
# Strict domain models (optional, used in DQ layer)
# -----------------------------

class ApplicationRecordV1(BaseModel):
    application_id: str
    status: str
    started_at: datetime
    ended_at: Optional[datetime] = None
    tat_seconds: Optional[int] = None
    product_id: Optional[str] = None
    entity_id: Optional[str] = None
    customer_id: Optional[str] = None
    customer_type: Optional[str] = None

    model_config = {"extra": "forbid"}  # strict once we decide version
