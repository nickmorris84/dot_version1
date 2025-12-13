from __future__ import annotations
import pandas as pd
from helpers import total_variation_distance, rel_or_abs_ok
def _mean_safe(s: pd.Series) -> float:
    try:
        return float(pd.to_numeric(s, errors="coerce").dropna().mean())
    except Exception:
        return float("nan")
def _rowcount_ok(cur: int, gold: int, pct_tol: float) -> bool:
    if gold == 0: return True
    return abs(cur - gold) <= pct_tol * gold
def test_rowcount_drift(current_outputs, golden_datasets, tolerances):
    stage, app = current_outputs
    g_stage, g_app = golden_datasets
    assert _rowcount_ok(len(stage), len(g_stage), tolerances["rowcount_pct"]),         f"Stage rowcount drift: current={len(stage)} golden={len(g_stage)}"
    assert _rowcount_ok(len(app), len(g_app), tolerances["rowcount_pct"]),         f"Application rowcount drift: current={len(app)} golden={len(g_app)}"
def test_numeric_means(current_outputs, golden_datasets, tolerances):
    stage, app = current_outputs
    g_stage, g_app = golden_datasets
    if "TAT_Minutes" in stage.columns and "TAT_Minutes" in g_stage.columns:
        cur = _mean_safe(stage["TAT_Minutes"]); gold = _mean_safe(g_stage["TAT_Minutes"])
        assert rel_or_abs_ok(cur, gold, tolerances["mean_minutes_rel"], tolerances["mean_minutes_abs"]),             f"TAT_Minutes mean drift: current={cur:.2f}, golden={gold:.2f}"
    if "Total_TAT_Minutes" in app.columns and "Total_TAT_Minutes" in g_app.columns:
        cur = _mean_safe(app["Total_TAT_Minutes"]); gold = _mean_safe(g_app["Total_TAT_Minutes"])
        assert rel_or_abs_ok(cur, gold, tolerances["mean_minutes_rel"], tolerances["mean_minutes_abs"]),             f"Total_TAT_Minutes mean drift: current={cur:.2f}, golden={gold:.2f}"
def test_categorical_distributions(current_outputs, golden_datasets, tolerances):
    _, app = current_outputs
    _, g_app = golden_datasets
    for col in ["Final_Risk_Grade", "Channel"]:
        if col in app.columns and col in g_app.columns:
            cur = app[col].fillna("__NA__").astype(str).value_counts(normalize=True)
            gold = g_app[col].fillna("__NA__").astype(str).value_counts(normalize=True)
            tvd = total_variation_distance(cur, gold)
            assert tvd <= tolerances["dist_tvd"], f"{col} distribution drift TVD={tvd:.3f} > {tolerances['dist_tvd']}"
