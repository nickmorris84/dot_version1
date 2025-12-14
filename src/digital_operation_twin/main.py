from __future__ import annotations

import os
from fastapi import FastAPI

from digital_operation_twin.api.routers.rest import router as rest_router
from digital_operation_twin.api.routers.pubsub_gcp import router as pubsub_router
from digital_operation_twin.core.logger import configure_logger
from digital_operation_twin.core.middleware import CorrelationIdMiddleware
from digital_operation_twin.config.loader import load_config_store, get_runtime_env


def create_app() -> FastAPI:
    configure_logger()

    app = FastAPI(title="Digital Operation Twin", version="0.1.0")

    # Config: load ONCE at startup, then inject via dependencies.
    config_dir = os.getenv("CONFIG_PATH", "./config")
    env = get_runtime_env()
    app.state.config_store = load_config_store(config_dir=config_dir, env=env)

    # Adds X-Correlation-ID generation + request logging context
    app.add_middleware(CorrelationIdMiddleware)

    # Transports
    app.include_router(rest_router)
    app.include_router(pubsub_router)

    return app


app = create_app()
