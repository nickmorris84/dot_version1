# main.py
from __future__ import annotations

import os
from dotenv import load_dotenv
import uvicorn

from digital_operation_twin.core.logger import get_logger
from digital_operation_twin.core.utils import load_yaml_settings
from digital_operation_twin.services.ingestion_service import IngestionService
from digital_operation_twin.api.routers.rest import build_app
from digital_operation_twin.api.routers.pubsub_gcp import pubsub_router

# ------------------------
# Load environment config
# ------------------------
load_dotenv()  # load from .env file if present

ENV = os.getenv("APP_ENV", "dev")
MODE = os.getenv("MODE", "rest")            # rest | worker | csv | ...
API_KEY = os.getenv("API_KEY")
HOST = os.getenv("HOST", "0.0.0.0")
PORT = int(os.getenv("PORT", 8000))

# ------------------------
# Initialize logger + config
# ------------------------
config = load_yaml_settings()
logger = get_logger(__name__)
logger.info("Starting app (ENV=%s, MODE=%s)", ENV, MODE)

# ------------------------
# Create service instance
# ------------------------
service = IngestionService()

# ------------------------
# REST mode
# ------------------------
def create_api_app():
    app = build_app(service, expected_api_key=API_KEY)
    app.include_router(pubsub_router(service))
    return app

# ------------------------
# Entry point
# ------------------------
if __name__ == "__main__":
    if MODE == "rest":
        app = create_api_app()

        # IMPORTANT: reload only works when using CLI or import string
        # Use this for prod/local when running directly
        uvicorn.run(app, host=HOST, port=PORT, reload=False)

    # elif MODE == "csv":
    #     # Example: CLI or batch file ingestion
    #     from cli.ingest_file import run_csv_ingestion
    #     run_csv_ingestion(service)

    elif MODE == "worker":
        logger.info("Worker mode not yet implemented.")

    else:
        logger.error("Unknown MODE: %s", MODE)

