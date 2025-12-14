from __future__ import annotations

import contextvars
from dataclasses import dataclass
from typing import Optional


# Backwards-compatible alias used by older code
correlation_id_var: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar(
    "correlation_id",
    default=None,
)

# Extra context for richer logs
customer_id_var: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar(
    "customer_id",
    default=None,
)

event_id_var: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar(
    "event_id",
    default=None,
)


@dataclass(frozen=True)
class LogContext:
    correlation_id: Optional[str] = None
    customer_id: Optional[str] = None
    event_id: Optional[str] = None


def get_log_context() -> LogContext:
    return LogContext(
        correlation_id=correlation_id_var.get(),
        customer_id=customer_id_var.get(),
        event_id=event_id_var.get(),
    )
