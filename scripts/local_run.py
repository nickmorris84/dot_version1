import asyncio
import json
import time
import logging
from pathlib import Path
import sys
from concurrent.futures import ThreadPoolExecutor

import pandas as pd

# --- Ensure imports work from anywhere (notebook/script) ---
REPO_ROOT = Path("/Users/nickmorris/DOT version3.0 2")
sys.path.insert(0, str(REPO_ROOT / "src"))

from digital_operation_twin.core.models.pipeline_state import PipelineState
from digital_operation_twin.core.logger import capture_logs_for_step, detach
from digital_operation_twin.core.utils import write_df, summarize_nulls
from digital_operation_twin.config.loader import load_config_store, get_runtime_env

from digital_operation_twin.pipelines.orchestrators.steps import (
    ToDataFrameStep,
    NormalizerStep,
    StandardiserStep,
    DataQualityStep,
    FinalSchemaValidationStep,
)

def cfg_get(obj, key, default=None):
    if obj is None:
        return default
    # dict-style
    if isinstance(obj, dict):
        return obj.get(key, default)
    # ConfigStore-style: try attribute
    if hasattr(obj, key):
        return getattr(obj, key)
    # ConfigStore-style: try __getitem__
    try:
        return obj[key]
    except Exception:
        return default

# -----------------------------
# CONFIG
# -----------------------------
CSV_PATH = REPO_ROOT / "data" / "inputs" / "credit_card_process_activities.csv"
OUT_DIR = REPO_ROOT / "data" / "outputs" / "local_step_test"
CUSTOMER_ID = "customer_a"

OUT_DIR.mkdir(parents=True, exist_ok=True)

# -----------------------------
# LOAD INPUT -> records
# -----------------------------
df = pd.read_csv(CSV_PATH)
records = df.where(pd.notna(df), None).to_dict(orient="records")
print(f"Loaded {len(records)} records from {CSV_PATH}")

# -----------------------------
# LOAD SETTINGS
# -----------------------------
config_dir = REPO_ROOT / "config"
env = get_runtime_env()
settings = load_config_store(config_dir=str(config_dir), env=env)

cust_settings = settings.settings_for(CUSTOMER_ID)          # Settings model
merged = cust_settings.model_dump()                         # plain dict for easy access
data_cfg = merged.get("data_config", {}) or {}
data_cfg

normaliser_cfg = data_cfg.get("normaliser", {}) or {}
standardiser_cfg = data_cfg.get("standardiser") or data_cfg.get("standardizer") or {}
data_quality_cfg = data_cfg.get("data_quality", {}) or {}
standardiser_cfg

# -----------------------------
# BUILD STATE (robust: don't assume PipelineState accepts records=)
# -----------------------------
state = PipelineState(df=None, records=records, event_id = 1, customer_id=CUSTOMER_ID)
state.schema_version = "v1"
state.reports = []

# -----------------------------
# BUILD STEPS
# -----------------------------
SELECTED_STEPS = [
    "to_dataframe",
    "normaliser",
    "standardiser",
    # "data_quality",
    # "final_schema_validation",
]

STEP_REGISTRY = {
    "to_dataframe": {
        "build": lambda: ToDataFrameStep(),
        "cfg":   lambda: None,
    },
    "normaliser": {
        "build": lambda: NormalizerStep(normaliser_cfg),
        "cfg":   lambda: normaliser_cfg,
    },
    "standardiser": {
        "build": lambda: StandardiserStep(standardiser_cfg),
        "cfg":   lambda: standardiser_cfg,
    },
    "data_quality": {
        "build": lambda: DataQualityStep(dq_cfg),
        "cfg":   lambda: data_quality_cfg,
    },
    "final_schema_validation": {
        "build": lambda: FinalSchemaValidationStep(),
        "cfg":   lambda: {"schema_version": getattr(state, "schema_version", None)},
    },
}


def safe_cfg_snapshot(cfg, *, max_items=100):
    if cfg is None:
        return None

    def sanitize(value):
        # mask common secret patterns
        if isinstance(value, str):
            lowered = value.lower()
            if any(k in lowered for k in ("password", "token", "secret", "key")):
                return "***masked***"
            return value

        if isinstance(value, (int, float, bool)) or value is None:
            return value

        if isinstance(value, list):
            return [sanitize(v) for v in value[:max_items]]

        if isinstance(value, dict):
            return {k: sanitize(v) for k, v in value.items()}

        # fallback for objects / enums / pydantic models
        return str(value)

    if isinstance(cfg, dict):
        return {
            "type": "dict",
            "key_count": len(cfg),
            "config": sanitize(cfg),
        }

    return {
        "type": type(cfg).__name__,
        "value": sanitize(cfg),
    }



steps = [STEP_REGISTRY[name]["build"]() for name in SELECTED_STEPS]

state.selected_steps = SELECTED_STEPS
state.selected_step_configs = {
    name: safe_cfg_snapshot(STEP_REGISTRY[name]["cfg"]())
    for name in SELECTED_STEPS
}


state.selected_step_configs

# -----------------------------
# RUNNER (async, but we will execute it safely for Jupyter)
# -----------------------------
root_logger = logging.getLogger()
root_logger.setLevel(logging.DEBUG)
if not root_logger.handlers:
    logging.basicConfig(level=logging.DEBUG, format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s")

async def run_steps_with_logs(state, steps, out_dir: Path):
    # Save "input" snapshot if df already exists (it won't before ToDataFrameStep)
    if getattr(state, "df", None) is not None:
        write_df(state.df, out_dir, "00_input")

    for i, step in enumerate(steps, start=1):
        step_name = getattr(step, "name", step.__class__.__name__)
        print(f"\n=== STEP {i}: {step_name} ===")

        before_df = getattr(state, "df", None)
        cols_before = list(before_df.columns) if before_df is not None else []
        rows_before = int(len(before_df)) if before_df is not None else 0

        t0 = time.time()

        # capture logs for this step
        handler, attached = capture_logs_for_step([""], level=logging.DEBUG)

        try:
            state = await step.run(state)
            ok = True
            err = None
        except Exception as e:
            ok = False
            err = f"{type(e).__name__}: {e}"
        finally:
            detach(handler, attached)

        duration_ms = int((time.time() - t0) * 1000)

        after_df = getattr(state, "df", None)
        cols_after = list(after_df.columns) if after_df is not None else []
        rows_after = int(len(after_df)) if after_df is not None else 0

        added = sorted(list(set(cols_after) - set(cols_before)))
        removed = sorted(list(set(cols_before) - set(cols_after)))
        nulls_top = summarize_nulls(after_df, top_n=10) if after_df is not None else []

        # write df snapshot
        output_path = None
        if after_df is not None:
            output_path = str(write_df(after_df, out_dir, f"{i:02d}_{step_name}"))

        # write step log file
        log_path = out_dir / f"{i:02d}_{step_name}.log"
        log_path.write_text("\n".join(handler.records))

        # append a simple report record
        rep = {
            "step_name": step_name,
            "ok": ok,
            "duration_ms": duration_ms,
            "rows_before": rows_before,
            "rows_after": rows_after,
            "added_cols": added,
            "removed_cols": removed,
            "nulls_top": nulls_top,
            "output_path": output_path,
            "log_path": str(log_path),
            "error": err,
        }
        state.reports.append(rep)

        print(f"Rows: {rows_before} → {rows_after} ({duration_ms} ms)")
        print(f"Added cols: {added}")

        # stop early on failure/rejection
        if (not ok) or (getattr(state, "status", None) in {"rejected", "failed", "duplicate"}):
            print("STOPPED:", getattr(state, "status", None), err)
            break

    return state

def run_async_in_thread(coro):
    # Jupyter-safe: run a fresh event loop in a background thread
    with ThreadPoolExecutor(max_workers=1) as ex:
        fut = ex.submit(lambda: asyncio.run(coro))
        return fut.result()

# -----------------------------
# EXECUTE (no await, no loop.run_until_complete)
# -----------------------------
final_state = run_async_in_thread(run_steps_with_logs(state, steps, OUT_DIR))

# -----------------------------
# WRITE SUMMARY
# -----------------------------
summary_path = OUT_DIR / "run_summary.json"
summary_path.write_text(json.dumps(final_state.reports, indent=2, default=str))

print("\nDone.")
print("Summary:", summary_path)
print("\nStep logs:")
for r in final_state.reports:
    print(f"- {r['step_name']}: {r['log_path']}")
    
final_state