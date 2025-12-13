import os
import pytest
import pandas as pd
from config import config
from core.transforms.ingest import ingest_from_csv
from core.transforms.process_master import ProcessMasterBuilder
@pytest.fixture(scope="session")
def current_outputs():
    df = ingest_from_csv(config.RAW_DATA_PATH)
    b = ProcessMasterBuilder(df)
    return b.stage_master, b.application_master
@pytest.fixture(scope="session")
def golden_datasets():
    stage_path = config.GOLDEN_STAGE_PATH
    app_path = config.GOLDEN_APP_PATH
    if not (os.path.exists(stage_path) and os.path.exists(app_path)):
        pytest.skip("Golden datasets not found in repo. Copy them to data/golden/ first.")
    g_stage = pd.read_csv(stage_path, parse_dates=["Stage_Start", "Stage_End"], keep_default_na=False)
    g_app = pd.read_csv(app_path, parse_dates=["Application_Start", "Application_End"], keep_default_na=False)
    return g_stage, g_app
@pytest.fixture(scope="session")
def tolerances():
    return {
        "rowcount_pct": float(os.getenv("TOL_ROWCOUNT_PCT", "0.10")),
        "mean_minutes_abs": float(os.getenv("TOL_MEAN_ABS", "10.0")),
        "mean_minutes_rel": float(os.getenv("TOL_MEAN_REL", "0.15")),
        "dist_tvd": float(os.getenv("TOL_DIST_TVD", "0.10")),
        "allow_new_columns": os.getenv("ALLOW_NEW_COLUMNS", "0") == "1"
    }
