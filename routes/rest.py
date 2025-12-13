from __future__ import annotations
from fastapi import FastAPI, Header, HTTPException, UploadFile, File, Form
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import Optional, Dict, Any
import pandas as pd
import io
import logging

from core.io.ingestion_models import IngestMessage, Envelope
from services.ingestion_service import IngestionService

logger = logging.getLogger(__name__)

def build_app(service: IngestionService, expected_api_key: Optional[str] = None) -> FastAPI:
    app = FastAPI(title="Ingestion API", version="1.0.0")

    @app.get("/healthz")
    async def healthz():
        return {"status": "healthy"}

    @app.post("/ingest")
    async def ingest(msg: IngestMessage, x_api_key: Optional[str] = Header(default=None)):
        if expected_api_key and x_api_key != expected_api_key:
            raise HTTPException(status_code=401, detail="Invalid API key")
        result = await service.process(msg)
        status = 200 if result.get("status") in ("ok", "duplicate") else 422
        return JSONResponse(result, status_code=status)

    @app.post("/ingest_csv")
    async def ingest_csv(
        file: UploadFile = File(...),
        run_mode: str = Form("sync"),
        save: str = Form("false"),
        x_api_key: Optional[str] = Header(default=None),
    ):
        # Basic auth
        if expected_api_key and x_api_key != expected_api_key:
            raise HTTPException(status_code=401, detail="Invalid API key")

        # Read CSV into DataFrame
        try:
            content = await file.read()
            df = pd.read_csv(io.BytesIO(content))
        except Exception as e:
            logger.exception("Failed to read CSV")
            raise HTTPException(status_code=400, detail=f"Failed to read CSV: {e}")

        if df.empty:
            raise HTTPException(status_code=400, detail="Uploaded CSV is empty or unreadable.")

        results = []
        try:
            for row in df.to_dict(orient="records"):
                msg = IngestMessage(
                    envelope=Envelope(source="csv_upload"),
                    payload=row
                )
                result = await service.process(msg)
                results.append(result)
        except Exception as e:
            logger.exception("Pipeline failed")
            raise HTTPException(status_code=500, detail=f"Pipeline error: {e}")

        return {
            "ok": True,
            "rows": len(df),
            "columns": len(df.columns),
            "sample_columns": df.columns.tolist()[:10],
            "results": results,
        }

    return app