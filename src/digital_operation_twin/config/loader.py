from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any
import os
import yaml

from digital_operation_twin.config.schema import Settings


def _expand_env(obj: Any) -> Any:
    """Expand ${VAR} in YAML strings recursively."""
    if isinstance(obj, str):
        return os.path.expandvars(obj)
    if isinstance(obj, list):
        return [_expand_env(x) for x in obj]
    if isinstance(obj, dict):
        return {k: _expand_env(v) for k, v in obj.items()}
    return obj


def read_yaml(path: Path) -> dict:
    if not path.exists():
        return {}
    return _expand_env(yaml.safe_load(path.read_text()) or {})


def deep_merge(base: dict, override: dict) -> dict:
    merged = dict(base)
    for k, v in (override or {}).items():
        if isinstance(v, dict) and isinstance(merged.get(k), dict):
            merged[k] = deep_merge(merged[k], v)
        else:
            merged[k] = v
    return merged


@dataclass(frozen=True)
class ConfigStore:
    """Holds raw config layers and can produce validated Settings per customer."""

    base: dict
    env: dict
    customers: dict[str, dict]

    def settings_for(self, customer_id: str) -> Settings:
        customer = self.customers.get(customer_id, {})
        merged = deep_merge(deep_merge(self.base, self.env), customer)
        return Settings.model_validate(merged)


def get_runtime_env() -> str:
    return os.getenv("APP_ENV", "dev")


def load_config_store(config_dir: str, env: str) -> ConfigStore:
    """Load base + env + customer configs from a repo-root config directory.

    Expected layout:
      config/
        base.yaml
        env/{dev,uat,prod}.yaml
        customers/*.yaml

    Backwards compatible:
      - if env/{env}.yaml missing, will fall back to app.yaml at config root.
    """
    root = Path(config_dir)

    base = read_yaml(root / "base.yaml")

    env_cfg = read_yaml(root / "env" / f"{env}.yaml")
    if not env_cfg:
        # Backward compat: old layout used app.yaml
        env_cfg = read_yaml(root / "app.yaml")

    customers_dir = root / "customers"
    customers: dict[str, dict] = {}
    if customers_dir.exists():
        for p in customers_dir.glob("*.yaml"):
            customers[p.stem] = read_yaml(p)
    else:
        # Backward compat: customer yaml at root (e.g. customer_a.yaml)
        for p in root.glob("customer_*.yaml"):
            customers[p.stem] = read_yaml(p)

    return ConfigStore(base=base, env=env_cfg, customers=customers)
