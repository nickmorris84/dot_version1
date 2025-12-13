import pandas as pd
from ....src.digital_operation_twin.core.logger import get_logger
logger = get_logger(__name__)

def run_data_quality_checks(df):
    issues = {}
    nulls = df.isnull().sum()
    issues["nulls"] = {c:int(v) for c,v in nulls.items() if v>0}
    dups = df.duplicated().sum()
    if dups>0: issues["duplicates"]=int(dups)
    if {"Stage_Start_Timestamp","Stage_End_Timestamp"}.issubset(df.columns):
        bad = (df["Stage_Start_Timestamp"]>df["Stage_End_Timestamp"]).sum()
        if bad>0: issues["invalid_timestamps"]=int(bad)
    if issues: logger.warning(f"[DQ] Issues detected: {issues}")
    else: logger.info("[DQ] Passed.")
    return issues
