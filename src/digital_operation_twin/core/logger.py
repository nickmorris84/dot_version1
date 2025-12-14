"""Logging configuration.

Why this exists:
- Most modules use `logging.getLogger(__name__)`.
- If you only attach handlers to a single named logger (e.g. "dot"), those
  module loggers won't emit anything unless they propagate to a configured
  root logger.

So we configure the *root* logger once, and everything inherits it.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path


def configure_logger(log_dir: str | None = None, level: int = logging.INFO) -> logging.Logger:
    """Configure root logging (file + stdout) and return an app logger.

    - Safe to call multiple times.
    - Uses LOG_DIR if set; defaults to ./data/logs.
    """

    root = logging.getLogger()
    if root.handlers:
        # Already configured.
        return logging.getLogger("digital_operation_twin")

    log_dir = log_dir or os.getenv("LOG_DIR", "data/logs")
    Path(log_dir).mkdir(parents=True, exist_ok=True)

    root.setLevel(level)

    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")

    file_path = os.path.join(log_dir, "process_log.log")
    fh = logging.FileHandler(file_path)
    fh.setFormatter(fmt)

    ch = logging.StreamHandler()
    ch.setFormatter(fmt)

    root.addHandler(fh)
    root.addHandler(ch)

    # Reduce noisy libs if desired
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)

    return logging.getLogger("digital_operation_twin")