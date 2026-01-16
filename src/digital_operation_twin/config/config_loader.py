from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Mapping
import os
import yaml
import logging

from digital_operation_twin.config.schema import Settings

logger = logging.getLogger(__name__)


def _expand_env(obj: Any) -> Any:
    if isinstance(obj, str):
        return os.path.expandvars(obj)
    if isinstance(obj, list):
        return [_expand_env(x) for x in obj]
    if isinstance(obj, dict):
        return {k: _expand_env(v) for k, v in obj.items()}
    return obj


def _read_yaml(path: Path) -> dict:
    if not path.exists():
        return {}
    raw = path.read_text()
    if not raw.strip():
        return {}
    data = yaml.safe_load(raw) or {}
    if not isinstance(data, dict):
        logger.warning("YAML not a dict: %s (type=%s)", str(path), type(data).__name__)
        return {}
    return _expand_env(data)


def _deep_merge(*layers: Mapping[str, Any]) -> dict:
    out: dict[str, Any] = {}
    for layer in layers:
        if not layer:
            continue
        for k, v in layer.items():
            if isinstance(v, dict) and isinstance(out.get(k), dict):
                out[k] = _deep_merge(out[k], v)  # type: ignore[arg-type]
            else:
                out[k] = v
    return out


def _iter_paths(d: Any, prefix: tuple[str, ...] = ()) -> Iterable[tuple[str, ...]]:
    if isinstance(d, dict):
        for k, v in d.items():
            p = prefix + (str(k),)
            yield p
            yield from _iter_paths(v, p)
    elif isinstance(d, list):
        for i, v in enumerate(d):
            p = prefix + (str(i),)
            yield p
            yield from _iter_paths(v, p)


def _tree_keys(d: Any) -> Any:
    if isinstance(d, dict):
        return {k: _tree_keys(v) for k, v in d.items()}
    if isinstance(d, list):
        return ["<list>"]
    return "<value>"


def _split_path(path: str | Iterable[str]) -> list[str]:
    if isinstance(path, str):
        path = path.replace("[", ".").replace("]", "")
        return [p for p in path.split(".") if p]
    return [str(p) for p in path]


def _get_in(d: Any, path: Iterable[str], default: Any = None) -> Any:
    cur = d
    for part in path:
        if isinstance(cur, dict) and part in cur:
            cur = cur[part]
        elif isinstance(cur, list):
            try:
                idx = int(part)
            except ValueError:
                return default
            if 0 <= idx < len(cur):
                cur = cur[idx]
            else:
                return default
        else:
            return default
    return cur


@dataclass
class ConfigLoader:
    """
    Usage:
      cfg = ConfigLoader("config", customer_id="acme")   # env from APP_ENV (default dev)
      cfg = ConfigLoader("config", env="uat", customer_id="acme")

    Layout:
      config/
        base.yaml
        env/{dev,uat,prod}.yaml   (or app.yaml fallback)
        customers/{customer}.yaml (or legacy customer_{customer}.yaml)
    """
    config_dir: str | Path
    customer_id: str
    env: str = field(default_factory=lambda: os.getenv("APP_ENV", "dev"))

    root: Path = field(init=False)
    base_cfg: dict = field(init=False, default_factory=dict)
    env_cfg: dict = field(init=False, default_factory=dict)
    customer_cfg: dict = field(init=False, default_factory=dict)

    def __post_init__(self) -> None:
        self.root = Path(self.config_dir)

        self.base_cfg = _read_yaml(self.root / "base.yaml")

        self.env_cfg = _read_yaml(self.root / "env" / f"{self.env}.yaml")
        if not self.env_cfg:
            self.env_cfg = _read_yaml(self.root / "app.yaml")

        self.customer_cfg = _read_yaml(self.root / "customers" / f"{self.customer_id}.yaml")
        if not self.customer_cfg:
            legacy = self.root / f"customer_{self.customer_id}.yaml"
            if legacy.exists():
                self.customer_cfg = _read_yaml(legacy)

        logger.info("ConfigLoader: root=%s env=%s customer=%s", self.root, self.env, self.customer_id)
        logger.info(
            "Config layers present: base=%s env=%s customer=%s",
            bool(self.base_cfg), bool(self.env_cfg), bool(self.customer_cfg)
        )
        if not self.customer_cfg:
            logger.warning("No customer config found for customer=%s", self.customer_id)

    # --- core API ---
    def merged(self) -> dict:
        return _deep_merge(self.base_cfg, self.env_cfg, self.customer_cfg)

    def settings(self) -> Settings:
        return Settings.model_validate(self.merged())

    def with_customer(self, customer_id: str) -> "ConfigLoader":
        return ConfigLoader(self.root, env=self.env, customer_id=customer_id)

    # --- introspection ---
    def keys_tree(self) -> dict:
        return _tree_keys(self.merged())

    def keys_flat(self) -> list[str]:
        return sorted({".".join(p) for p in _iter_paths(self.merged())})

    def get(self, path: str | Iterable[str], default: Any = None) -> Any:
        return _get_in(self.merged(), _split_path(path), default)

    def values_for_paths(self, paths: Iterable[str | Iterable[str]], default: Any = None) -> dict[str, Any]:
        out: dict[str, Any] = {}
        for p in paths:
            key = p if isinstance(p, str) else ".".join(map(str, p))
            out[key] = self.get(p, default=default)
        return out
