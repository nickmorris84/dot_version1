# dq/dataset_steps.py
import pandas as pd
from ..registries import register, DQ_DATASET

@register(DQ_DATASET, "drop_empty_journey")
def drop_empty_journey(df: pd.DataFrame) -> pd.DataFrame:
    if "journey_id" in df:
        mask = df["journey_id"].astype(str).str.strip() != ""
        df = df[mask]
    return df

@register(DQ_DATASET, "trim_strings")
def trim_strings(df: pd.DataFrame) -> pd.DataFrame:
    for c in df.select_dtypes(include="object"):
        df[c] = df[c].astype(str).str.strip()
    return df
