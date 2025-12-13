from __future__ import annotations

from fastapi import FastAPI
from digital_operation_twin.api.routers.rest import router as rest_router
from digital_operation_twin.api.routers.pubsub_gcp import router as pubsub_router
from digital_operation_twin.core.logger import configure_logger
from digital_operation_twin.core.middleware import CorrelationIdMiddleware


def create_app() -> FastAPI:
    configure_logger() 

    app = FastAPI(title="Digital Operation Twin", version="0.1.0")

    # Adds X-Correlation-ID generation + request logging context
    app.add_middleware(CorrelationIdMiddleware)

    # Transports
    app.include_router(rest_router)
    app.include_router(pubsub_router)

    return app


app = create_app()
