from __future__ import annotations
from typing import Any, Dict, Optional
from datetime import datetime
from pydantic import BaseModel, Field, validator
import uuid

# --------- Edge models (lenient) ---------
class Envelope(BaseModel):
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    event_type: str = Field(default="record.ingest")
    source: Optional[str] = None
    produced_at: datetime = Field(default_factory=datetime.utcnow)
    dedupe_key: Optional[str] = None
    attributes: Dict[str, Any] = {}

    class Config:
        extra = "allow"   # tolerate extra keys at the edge

class Payload(BaseModel):
    """
    Dynamic payload container. Accepts any keys.
    You will validate/normalize in DQ logic by schema_version.
    """
    class Config:
        extra = "allow"

class IngestMessage(BaseModel):
    envelope: Envelope
    payload: Payload

# --------- Strict domain models (optional, used in DQ layer) ---------
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

    class Config:
        extra = "forbid"  # strict once we decide version
