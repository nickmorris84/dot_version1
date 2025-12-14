from __future__ import annotations
from typing import Dict, Any, List

class Repository:
    """
    Abstraction over your sinks.
    Swap with BigQuery/Snowflake/Postgres/S3 writer, etc.
    """
    def __init__(self):
        self._records: List[Dict[str, Any]] = []

    async def write_application_record(self, record: Dict[str, Any]) -> None:
        # Replace with real implementation:
        # - Postgres/SQLAlchemy async upsert
        # - BigQuery load job
        # - S3 parquet batch writer, etc.
        self._records.append(record)

    # for testing/demo
    def all(self) -> list[Dict[str, Any]]:
        return list(self._records)