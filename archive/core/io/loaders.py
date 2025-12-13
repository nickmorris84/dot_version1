import os, pandas as pd
from ....src.digital_operation_twin.core.logger import get_logger
logger = get_logger(__name__)
def read_csv_local(path: str):
    if not os.path.exists(path): raise FileNotFoundError(path)
    df = pd.read_csv(path)
    logger.info(f"[INGEST] Loaded {path} rows={len(df)} cols={df.shape[1]}")
    for col in ["Activity_Timestamp","Stage_Start_Timestamp","Stage_End_Timestamp"]:
        if col in df.columns: df[col] = pd.to_datetime(df[col], errors="coerce")
    return df
