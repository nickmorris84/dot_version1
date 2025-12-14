from __future__ import annotations

from functools import lru_cache
from typing import Any, Dict, Optional

from fastapi import Depends, Header, HTTPException, Request

from digital_operation_twin.config.schema import Settings
from digital_operation_twin.pipelines.orchestrators.master_orchestration import MasterOrchestrator


# -----------------------------
# Core: access ConfigStore
# -----------------------------
def get_config_store(request: Request):
    store = getattr(request.app.state, "config_store", None)
    if store is None:
        raise RuntimeError("Config store not initialised. Load it in main.py/create_app().")
    return store


# -----------------------------
# REST: customer via header
# -----------------------------
def get_rest_customer_id(
    x_customer_id: Optional[str] = Header(default=None, alias="X-Customer-Id"),
) -> str:
    if not x_customer_id:
        raise HTTPException(status_code=400, detail="Missing X-Customer-Id header")
    return x_customer_id


# -----------------------------
# Pub/Sub: body + customer from attributes
# -----------------------------
async def get_pubsub_body(request: Request) -> Dict[str, Any]:
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON body")
    request.state.pubsub_body = body
    return body


def get_pubsub_customer_id(body: Dict[str, Any] = Depends(get_pubsub_body)) -> str:
    message = body.get("message") or {}
    attrs = message.get("attributes") or {}
    customer_id = attrs.get("customer_id") or attrs.get("customerId")
    if not customer_id:
        raise HTTPException(status_code=400, detail="Missing customer_id in Pub/Sub message attributes")
    return customer_id


# -----------------------------
# Settings resolution (typed + validated)
# -----------------------------
def get_settings_for_customer(request: Request, customer_id: str) -> Settings:
    store = get_config_store(request)
    if customer_id not in store.customers:
        raise HTTPException(status_code=404, detail=f"Unknown customer: {customer_id}")
    return store.settings_for(customer_id)


def get_settings_rest(request: Request, customer_id: str = Depends(get_rest_customer_id)) -> Settings:
    return get_settings_for_customer(request, customer_id)


def get_settings_pubsub(request: Request, customer_id: str = Depends(get_pubsub_customer_id)) -> Settings:
    return get_settings_for_customer(request, customer_id)


# -----------------------------
# Orchestrator creation (cached per customer)
# -----------------------------
def get_customer_id_from_settings(settings: Settings) -> str:
    return settings.customers.id


@lru_cache(maxsize=128)
def _orch_from_customer_and_hash(customer_id: str, settings_hash: int) -> MasterOrchestrator:
    """
    Cache orchestrators by (customer_id, settings_hash).
    settings_hash lets you invalidate cache on config changes between restarts.
    """
    # NOTE: MasterOrchestrator expects a dict-like config
    # The caller will pass actual Settings separately; we keep this pure cache key.
    raise RuntimeError("This function should be called via get_orchestrator_* only.")


def get_orchestrator_from_settings(settings: Settings) -> MasterOrchestrator:
    # Stable-ish hash for cache key
    dumped = settings.model_dump(mode="python")
    settings_hash = hash(str(dumped))  # good enough for runtime cache; restarts reload anyway

    # Local mini-cache using lru_cache trick
    key = (settings.customer_id, settings_hash)

    # manual cache store on function attribute (keeps it simple)
    cache = getattr(get_orchestrator_from_settings, "_cache", {})
    if key in cache:
        return cache[key]

    orch = MasterOrchestrator(dumped)
    cache[key] = orch
    setattr(get_orchestrator_from_settings, "_cache", cache)
    return orch


def get_orchestrator_rest(settings: Settings = Depends(get_settings_rest)) -> MasterOrchestrator:
    return get_orchestrator_from_settings(settings)


def get_orchestrator_pubsub(settings: Settings = Depends(get_settings_pubsub)) -> MasterOrchestrator:
    return get_orchestrator_from_settings(settings)


# Optional: if any older code imports get_settings, make it explicit REST.
get_settings = get_settings_rest
