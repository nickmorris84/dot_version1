from __future__ import annotations

import contextvars

# Holds the correlation ID for the current request / message
correlation_id_var: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "correlation_id",
    default=None,
)