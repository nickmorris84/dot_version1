"""Logging configuration for Digital Operation Twin.

This configures the *root* logger once, so module-level loggers (getLogger(__name__))
inherit handlers and formatting.

Env vars:
- LOG_LEVEL: DEBUG|INFO|WARNING|ERROR (default INFO)
- DOT_LOG_DIR: directory for log files (default ./logs)
- DOT_LOG_FILE: filename (default dot.log)
"""

from __future__ import annotations

import logging
import os
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Optional

from digital_operation_twin.core.request_context import get_log_context


class ContextFilter(logging.Filter):
    """Inject correlation/customer/event ids into every log record."""

    def filter(self, record: logging.LogRecord) -> bool:  # noqa: D401
        ctx = get_log_context()
        # Don't overwrite if a caller explicitly provided extra={}
        if not hasattr(record, "correlation_id"):
            record.correlation_id = ctx.correlation_id or "-"
        if not hasattr(record, "customer_id"):
            record.customer_id = ctx.customer_id or "-"
        if not hasattr(record, "event_id"):
            record.event_id = ctx.event_id or "-"
        return True


def _parse_level(level: str | None) -> int:
    if not level:
        return logging.INFO
    level = level.strip().upper()
    return getattr(logging, level, logging.INFO)


def configure_logger(log_dir: Optional[str] = None, level: int | None = None) -> logging.Logger:
    """Configure root logger (idempotent)."""
    resolved_level = level if level is not None else _parse_level(os.getenv("LOG_LEVEL"))
    resolved_dir = Path(log_dir or os.getenv("DOT_LOG_DIR", "./data/logs"))
    log_file = os.getenv("DOT_LOG_FILE", "dot.log")

    resolved_dir.mkdir(parents=True, exist_ok=True)
    file_path = resolved_dir / log_file

    root = logging.getLogger()
    root.setLevel(resolved_level)

    # Avoid duplicate handlers if called multiple times (uvicorn reload, tests, etc.)
    if getattr(root, "_dot_configured", False):
        return logging.getLogger("digital_operation_twin")

    fmt = logging.Formatter(
        "%(asctime)s %(levelname)s %(name)s "
        # "[cid=%(correlation_id)s cust=%(customer_id)s event=%(event_id)s] "
        "%(message)s"
    )

    # File (rotating)
    fh = RotatingFileHandler(
        filename=str(file_path),
        maxBytes=10 * 1024 * 1024,  # 10MB
        backupCount=5,
        encoding="utf-8",
    )
    fh.setFormatter(fmt)
    fh.addFilter(ContextFilter())

    # Console
    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    ch.addFilter(ContextFilter())

    root.addHandler(fh)
    root.addHandler(ch)

    # Tame noisy libs
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)

    setattr(root, "_dot_configured", True)
    return logging.getLogger("digital_operation_twin")
