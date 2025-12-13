"""
Events pipeline:
1) Ingest raw CSV
2) DQ checks (log-only)
3) Classify to canonical shape
4) Process per-journey with ActivityProcessor (DB persistence)
5) Rebuild legacy CSV outputs (unchanged paths)
"""
import pandas as pd
from config import config
from src.digital_operation_twin.core.logger import get_logger
from core.transforms.ingest import ingest_from_csv
from core.quality.dq_checks import run_data_quality_checks
from core.io.database_init import init_db
from core.classes.activity_classifier import ActivityClassifier
from core.classes.activity_processor import ActivityProcessor
from core.transforms.process_master import ProcessMasterBuilder
from core.transforms.validate import validate_outputs
from core.io.writers import write_csv

logger = get_logger(__name__)

def run(db_url: str = "sqlite:///data/app.db"):
    logger.info("[RUN] events_project01 starting…")
    init_db(db_url, echo=False)
    raw = ingest_from_csv(config.RAW_DATA_PATH)
    logger.info(f"[INGEST] raw rows={len(raw)} cols={raw.shape[1]}")
    run_data_quality_checks(raw)
    clf = ActivityClassifier()
    events = clf.transform(raw)
    journeys = events["journey_id"].nunique() if "journey_id" in events.columns else 0
    logger.info(f"[CLASSIFY] events={len(events)} journeys={journeys}")
    if "journey_id" not in events.columns:
        raise ValueError("journey_id missing after classification.")
    for j_id, group in events.groupby("journey_id"):
        logger.debug(f"[PROCESS] journey={j_id} batch_size={len(group)}")
        ActivityProcessor(str(j_id), group).process()
    b = ProcessMasterBuilder(raw)
    validate_outputs(b.stage_master,b.application_master,config.GOLDEN_STAGE_PATH,config.GOLDEN_APP_PATH)
    write_csv(b.stage_master, config.OUTPUT_STAGE_PATH)
    write_csv(b.application_master, config.OUTPUT_APP_PATH)
    print("\nStage Master sample:\n", b.stage_master.head(5))
    print("\nApplication Master sample:\n", b.application_master.head(5))
    logger.info("[RUN] events_project01 finished.")
if __name__ == "__main__":
    run()
