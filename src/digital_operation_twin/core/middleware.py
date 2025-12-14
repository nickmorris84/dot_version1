from __future__ import annotations

import time
import uuid
import logging

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

from digital_operation_twin.core.request_context import correlation_id_var, customer_id_var

logger = logging.getLogger(__name__)


class CorrelationIdMiddleware(BaseHTTPMiddleware):
    """Adds/propagates correlation id and stores request context for logging."""

    async def dispatch(self, request: Request, call_next):
        t0 = time.perf_counter()

        cid = request.headers.get("x-correlation-id") or str(uuid.uuid4())
        cust = request.headers.get("x-customer-id") or request.query_params.get("customer_id")

        token_cid = correlation_id_var.set(cid)
        token_cust = customer_id_var.set(cust)

        try:
            logger.debug("request start %s %s", request.method, request.url.path)
            response: Response = await call_next(request)
            return response
        finally:
            elapsed_ms = int((time.perf_counter() - t0) * 1000)
            # Best-effort response header: if response exists, starlette will still send it
            try:
                response.headers["x-correlation-id"] = cid  # type: ignore[name-defined]
            except Exception:
                pass
            logger.info(
                "request end %s %s (%sms)",
                request.method,
                request.url.path,
                elapsed_ms,
            )
            correlation_id_var.reset(token_cid)
            customer_id_var.reset(token_cust)
