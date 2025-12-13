from __future__ import annotations
from helpers import simplify_dtype
def test_stage_schema_columns(current_outputs, golden_datasets, tolerances):
    stage, _ = current_outputs
    g_stage, _ = golden_datasets
    cur_cols = list(stage.columns); gold_cols = list(g_stage.columns)
    if tolerances["allow_new_columns"]:
        missing = [c for c in gold_cols if c not in cur_cols]
        assert not missing, f"Missing required columns in stage_master: {missing}"
    else:
        assert set(cur_cols) == set(gold_cols), f"Stage columns drifted.\ncurrent={cur_cols}\n golden={gold_cols}"
def test_app_schema_columns(current_outputs, golden_datasets, tolerances):
    _, app = current_outputs
    _, g_app = golden_datasets
    cur_cols = list(app.columns); gold_cols = list(g_app.columns)
    if tolerances["allow_new_columns"]:
        missing = [c for c in gold_cols if c not in cur_cols]
        assert not missing, f"Missing required columns in application_master: {missing}"
    else:
        assert set(cur_cols) == set(gold_cols), f"Application columns drifted.\ncurrent={cur_cols}\n golden={gold_cols}"
def test_semantic_dtypes(current_outputs):
    stage, app = current_outputs
    for c in ["Stage_Start","Stage_End"]:
        if c in stage.columns: assert "datetime" in simplify_dtype(stage[c].dtype)
    for c in ["Application_Start","Application_End"]:
        if c in app.columns: assert "datetime" in simplify_dtype(app[c].dtype)
    if "TAT_Minutes" in stage.columns:
        assert simplify_dtype(stage["TAT_Minutes"].dtype) in ("float","int")
    if "Total_TAT_Minutes" in app.columns:
        assert simplify_dtype(app["Total_TAT_Minutes"].dtype) in ("float","int")
