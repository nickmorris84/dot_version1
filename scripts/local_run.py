%load_ext autoreload
%autoreload 2

import asyncio
import json
import time
import logging
from pathlib import Path
import sys
from concurrent.futures import ThreadPoolExecutor
from pprint import pprint

import pandas as pd

# --- Ensure imports work from anywhere (notebook/script) ---
REPO_ROOT = Path("/Users/nickmorris/DOT version3.0 2")
sys.path.insert(0, str(REPO_ROOT / "src"))

from digital_operation_twin.core.models.pipeline_state import PipelineState
from digital_operation_twin.core.models.processIOAdaptor import GATE_ADAPTER, TRANSFORM_ADAPTER
from digital_operation_twin.core.registry.gate_registry import API_GATE_REGISTRY, API_GATE_ORDER
from digital_operation_twin.core.registry.schema_registry import SCHEMA_VALIDATION_REGISTRY, SCHEMA_REGISTRY, SCHEMA_VALIDATION_ORDER
from digital_operation_twin.core.registry.transform_registry import VALIDATOR_REGISTRY, NORMALIZER_REGISTRY, VALIDATOR_ORDER, NORMALIZER_ORDER, STANDARDIZER_REGISTRY, STANDARDIZER_ORDER
from digital_operation_twin.core.logger import capture_logs_for_step, detach
from digital_operation_twin.core.utils import df_to_records, pdic, write_df, summarize_nulls, get_runtime_env
from digital_operation_twin.config.config_loader import ConfigLoader
from digital_operation_twin.pipelines.orchestrators.process_runner import ProcessRunner

# -----------------------------
# CONFIG
# -----------------------------
CSV_PATH = REPO_ROOT / "data" / "inputs" / "credit_card_process_activities.csv"
OUT_DIR = REPO_ROOT / "data" / "outputs" / "local_step_test"
OUT_DIR.mkdir(parents=True, exist_ok=True)
CUSTOMER_ID = "customer_a"
config_dir = REPO_ROOT / "config"
env = get_runtime_env()

cfg = ConfigLoader(config_dir=config_dir, env=env, customer_id=CUSTOMER_ID)
cfg.settings().model_dump()

pdic(cfg.get("data_config.standardiser.rename"))
pdic(cfg.keys_tree())          # keys-only tree
pdic(cfg.keys_flat()[:50])     # first 50 paths
settings = cfg.settings()       # validated Settings
settings.data_config
data_cfg = settings.data_config
data_cfg.model_dump()

# -----------------------------
# LOAD INPUT -> records
# -----------------------------
df = pd.read_csv(CSV_PATH)
records = df.where(pd.notna(df), None).to_dict(orient="records")
print(f"Loaded {len(records)} records from {CSV_PATH}")

logging.basicConfig(level=logging.DEBUG)

state = PipelineState(
    event_id=1,
    records=records,
    cfg=data_cfg,
    df = df
)

schema_step = ProcessRunner(
    segment_name='schema_validation_gate',
    cfg={"schema_validation_gate": data_cfg.schema.schema_validation_gate.model_dump()},
    registry=SCHEMA_VALIDATION_REGISTRY,
    default_order=("schema_validation_gate",),
    adapter=TRANSFORM_ADAPTER
)

results1 = schema_step.run(state = state, inputs = {"df":state.df})

report = results1.state.metrics.get("schema_report", [])
report[:3]


normalizer_step = ProcessRunner(
    segment_name='normaliser',
    cfg=data_cfg.normalizer.model_dump(),
    registry=NORMALIZER_REGISTRY, 
    default_order=NORMALIZER_ORDER,
    adapter=TRANSFORM_ADAPTER
)

results2 = normalizer_step.run(state = state, inputs = {"df":results1.df})
results2.status
results2.df


standardizer_step = ProcessRunner(
    segment_name='standardizer',
    cfg=data_cfg.standardizer.model_dump(),
    registry=STANDARDIZER_REGISTRY, 
    default_order=STANDARDIZER_ORDER,
    adapter=TRANSFORM_ADAPTER
)

results3 = standardizer_step.run(state = state, inputs = {"df":results2.df})
results3.df

valitor_step = ProcessRunner(
    segment_name='validator',
    cfg=data_cfg.validator.model_dump(),
    registry=VALIDATOR_REGISTRY, 
    default_order=VALIDATOR_ORDER,
    adapter=TRANSFORM_ADAPTER
)

results4 = valitor_step.run(state = state, inputs = {"df":results3.df})
results4.errors
results4.df.columns

results4.records = df_to_records(results4.df)

results4.records

schema_step2 = ProcessRunner(
    segment_name='schema_validation_post_transformation',
    cfg={"schema_validation_post_transformation": data_cfg.schema.schema_validation_post_transformation.model_dump()},
    registry=SCHEMA_VALIDATION_REGISTRY,
    default_order=("schema_validation_post_transformation",),
    adapter=TRANSFORM_ADAPTER
)

results5 = schema_step2.run(state = state, inputs = {"df":results4.df})

report5 = results5.state.metrics.get("schema_report", [])
report5[:3]
results5.df



df
results.state.df
results.df
results.errors


from digital_operation_twin.config.schema import Settings  # adjust import to your actual Settings module

print("Settings class:", Settings, "from:", Settings.__module__)
print("Settings fields:", list(Settings.model_fields.keys()))

DC = Settings.model_fields["data_config"].annotation
print("data_config type:", DC, "from:", getattr(DC, "__module__", None))

print("data_config fields:", list(DC.model_fields.keys()))
print("has schema_enforce?", "schema_enforce" in DC.model_fields)



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