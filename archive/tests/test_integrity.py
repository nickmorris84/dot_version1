from __future__ import annotations
import pandas as pd
def test_keys_and_integrity(current_outputs):
    stage, app = current_outputs
    if {"Application_ID","Stage"}.issubset(stage.columns):
        assert not stage.duplicated(subset=["Application_ID","Stage"]).any(),             "Stage Master has duplicate (Application_ID, Stage) rows."
    if "Application_ID" in app.columns:
        assert not app.duplicated(subset=["Application_ID"]).any(),             "Application Master has duplicate Application_ID rows."
    if {"Stage_Start","Stage_End"}.issubset(stage.columns):
        invalid = (stage["Stage_Start"] > stage["Stage_End"]).sum()
        assert invalid == 0, f"Stage has {invalid} rows where Stage_Start > Stage_End"
    if "Age_Days" in stage.columns:
        assert (stage["Age_Days"].dropna() >= 0).all(), "Negative Age_Days in Stage Master"
    if "TAT_Minutes" in stage.columns:
        assert (pd.to_numeric(stage["TAT_Minutes"], errors="coerce").dropna() >= 0).all(), "Negative TAT_Minutes"
    if "Total_TAT_Minutes" in app.columns:
        assert (pd.to_numeric(app["Total_TAT_Minutes"], errors="coerce").dropna() >= 0).all(), "Negative Total_TAT_Minutes"
