import yaml
import os
from pathlib import Path
from typing import Any, Dict

def _expand_env(obj: Any) -> Any:
    """Expand ${VAR} in YAML strings recursively."""
    if isinstance(obj, str):
        return os.path.expandvars(obj)
    if isinstance(obj, list):
        return [_expand_env(x) for x in obj]
    if isinstance(obj, dict):
        return {k: _expand_env(v) for k, v in obj.items()}
    return obj

def load_yaml(path: str | Path) -> Dict[str, Any]:
    with open(path, "r") as f:
        return _expand_env(yaml.safe_load(f) or {})

def deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    """Recursively merge override into base."""
    merged = base.copy()
    for k, v in override.items():
        if k in merged and isinstance(merged[k], dict) and isinstance(v, dict):
            merged[k] = deep_merge(merged[k], v)
        else:
            merged[k] = v
    return merged
