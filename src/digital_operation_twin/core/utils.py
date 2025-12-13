# core/utils.py
from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import Tuple
import yaml
import os
from typing import Any, Dict

from flask import jsonify, current_app
from werkzeug.utils import secure_filename


# Use your centralized logger if you have one; otherwise the stdlib logger:
logger = logging.getLogger("csv_api")


def _auth_ok(req) -> bool:
    """Check Bearer token from app config."""
    token = current_app.config.get("CSV_API_TOKEN")
    if token is None:
        return True  # auth disabled
    header = req.headers.get("Authorization", "")
    parts = header.split()
    return len(parts) == 2 and parts[0].lower() == "bearer" and parts[1] == token


def _bad_request(message: str, *, status: int = 400):
    return jsonify({"ok": False, "error": message}), status


def _validate_upload(file_storage) -> Tuple[bool, str]:
    if file_storage is None or file_storage.filename == "":
        return False, "No file part or empty filename. Use field name 'file'."

    # Best-effort Content-Type check
    ctype = (file_storage.mimetype or "").lower()
    allowed = set(current_app.config.get("ALLOWED_MIME_TYPES", []))
    if allowed and ctype not in allowed:
        logger.warning("Unexpected Content-Type: %s", ctype)
        # don't hard-fail; many clients send text/plain

    # Extension check
    if not file_storage.filename.lower().endswith(".csv"):
        return False, "Only .csv files are accepted."

    return True, ""


def _save_file(file_storage) -> Path:
    """Save to UPLOAD_DIR/<timestamp>__<secure_name> and return the path."""
    upload_dir = Path(current_app.config.get("UPLOAD_DIR", "./uploads"))
    upload_dir.mkdir(parents=True, exist_ok=True)

    ts = datetime.utcnow().strftime("%Y%m%d-%H%M%S-%f")
    clean = secure_filename(file_storage.filename)
    out_path = upload_dir / f"{ts}__{clean}"
    file_storage.save(out_path)
    return out_path


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