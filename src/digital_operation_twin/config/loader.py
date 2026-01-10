from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any
import os
import yaml
import logging

from digital_operation_twin.config.schema import Settings

logger = logging.getLogger(__name__)


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
    """
    Read YAML and expand env vars.
    Logs what it loads (and warns if missing/empty).
    """
    if not path.exists():
        logger.warning("Config file missing: %s", str(path))
        return {}

    raw_text = path.read_text()
    if not raw_text.strip():
        logger.warning("Config file empty: %s", str(path))
        return {}

    data = yaml.safe_load(raw_text) or {}
    if not isinstance(data, dict):
        logger.warning("Config file not a dict: %s (type=%s)", str(path), type(data).__name__)
        return {}

    data = _expand_env(data)

    logger.info("Loaded config: %s (top_keys=%s)", str(path), list(data.keys()))
    return data


def deep_merge(base: dict, override: dict) -> dict:
    """
    Merge override into base recursively (override wins).
    """
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
        logger.info("Building settings for customer=%s", customer_id)

        customer = self.customers.get(customer_id, {})
        if not customer:
            logger.warning("No customer-specific config found for customer=%s", customer_id)

        merged = deep_merge(deep_merge(self.base, self.env), customer)

        # Prefer nested config (your actual structure), fallback to top-level for legacy layouts
        data_cfg = merged.get("data_config") or {}
        std = (
            data_cfg.get("standardiser")
            or data_cfg.get("standardizer")
            or merged.get("standardiser")
            or merged.get("standardizer")
        )

        if not std:
            logger.info(
                "No standardiser config found (expected under data_config.standardiser); customer=%s",
                customer_id,
            )
        else:
            std_keys = list(std.keys()) if isinstance(std, dict) else [type(std).__name__]
            logger.info("Standardiser config present (keys=%s)", std_keys)

            # Support both styles:
            #  1) your current "flat" config: {to_snake_case, rename, cast, ...}
            #  2) an explicit "steps" list: {steps: [{name: ...}, ...]}
            steps = std.get("steps") if isinstance(std, dict) else None
            if isinstance(steps, list):
                step_names = []
                for s in steps:
                    if isinstance(s, dict):
                        step_names.append(s.get("name") or s.get("step") or "<unnamed>")
                    else:
                        step_names.append(str(s))
                logger.info("Standardiser steps=%s", step_names)
            elif isinstance(std, dict):
                # "flat" style: infer which transforms are configured
                inferred = [k for k in ("to_snake_case", "drop_columns", "fill_defaults", "rename", "cast", "value_map", "require") if k in std]
                if inferred:
                    logger.info("Standardiser inferred steps=%s", inferred)

        return Settings.model_validate(merged)


def get_runtime_env() -> str:
    env = os.getenv("APP_ENV", "dev")
    logger.info("Runtime env resolved to APP_ENV=%s", env)
    return env


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

    logger.info("Loading config store: dir=%s env=%s", str(root), env)

    base = read_yaml(root / "base.yaml")

    env_cfg = read_yaml(root / "env" / f"{env}.yaml")
    if not env_cfg:
        logger.warning("Env config missing/empty for env=%s; falling back to app.yaml", env)
        env_cfg = read_yaml(root / "app.yaml")

    customers_dir = root / "customers"
    customers: dict[str, dict] = {}

    if customers_dir.exists():
        for p in customers_dir.glob("*.yaml"):
            customers[p.stem] = read_yaml(p)
            logger.info("Loaded customer config: %s", p.stem)
    else:
        # Backward compat: customer yaml at root (e.g. customer_a.yaml)
        for p in root.glob("customer_*.yaml"):
            customers[p.stem] = read_yaml(p)
            logger.info("Loaded legacy customer config: %s", p.stem)

    logger.info("Config store ready (customers=%s)", list(customers.keys()))
    return ConfigStore(base=base, env=env_cfg, customers=customers)
