from __future__ import annotations

import io
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
from digital_operation_twin.core.utils import utc_now

import pandas as pd


class CsvIngestionService:
    """
    Converts CSV sources into the API ingest shape:
      { "envelope": {...}, "payload": [ {..}, {..} ] }

    Supports:
      - bytes (from UploadFile)
      - file path (str/Path)
      - pandas DataFrame
    """

    def __init__(self, *, default_event_type: str = "record.ingest", default_source: str = "csv") -> None:
        self.default_event_type = default_event_type
        self.default_source = default_source

    def load_to_df(
        self,
        source: Union[bytes, str, Path, pd.DataFrame],
        *,
        read_csv_kwargs: Optional[Dict[str, Any]] = None,
    ) -> pd.DataFrame:
        read_csv_kwargs = read_csv_kwargs or {}

        if isinstance(source, pd.DataFrame):
            return source.copy()

        if isinstance(source, (str, Path)):
            return pd.read_csv(str(source), **read_csv_kwargs)

        if isinstance(source, (bytes, bytearray)):
            bio = io.BytesIO(source)
            return pd.read_csv(bio, **read_csv_kwargs)

        raise TypeError(f"Unsupported CSV source type: {type(source)}")

    def df_to_records(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        # Convert NaN to None so JSON / pydantic doesn’t choke
        clean = df.where(pd.notna(df), None)
        return clean.to_dict(orient="records")

    def build_api_body(
        self,
        *,
        df: pd.DataFrame,
        event_id: Optional[str] = None,
        event_type: Optional[str] = None,
        source: Optional[str] = None,
        attributes: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        eid = event_id or str(uuid.uuid4())
        return {
            "envelope": {
                "event_id": eid,
                "event_type": event_type or self.default_event_type,
                "source": source or self.default_source,
                "produced_at": utc_now().isoformat(),
                "attributes": attributes or {},
            },
            "payload": self.df_to_records(df),
        }