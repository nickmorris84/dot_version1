# core/utils.py
from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Tuple
import yaml
import os
from typing import Any, Dict


# Use your centralized logger if you have one; otherwise the stdlib logger:
logger = logging.getLogger("csv_api")


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def load_yaml_settings(env: str | None = None, path: str =  "config/app.yaml") -> Dict[str, Any]:
    base = Path(path)
    if not base.exists():
        raise FileNotFoundError(f"Missing {path}")
    with base.open("r") as f:
        data = yaml.safe_load(f) or {}

    if env:
        overlay = Path(f"config/{env}.yaml")
        if overlay.exists():
            with overlay.open("r") as f:
                extra = yaml.safe_load(f) or {}
            for k, v in extra.items():
                if isinstance(v, dict) and isinstance(data.get(k), dict):
                    data[k].update(v)
                else:
                    data[k] = v

    token = os.getenv("CSV_API_TOKEN")
    if token:
        data.setdefault("security", {})["csv_api_token"] = token

    db_url_env = os.getenv("DATABASE_URL")
    if db_url_env:
        data.setdefault("database", {})["url"] = db_url_env

    return data