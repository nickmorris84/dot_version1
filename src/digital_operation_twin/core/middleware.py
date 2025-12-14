from __future__ import annotations

import uuid
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

from ..core.request_context import correlation_id_var


class CorrelationIdMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        cid = request.headers.get("x-correlation-id") or str(uuid.uuid4())
        token = correlation_id_var.set(cid)
        try:
            response: Response = await call_next(request)
            response.headers["x-correlation-id"] = cid
            return response
        finally:
            correlation_id_var.reset(token)
