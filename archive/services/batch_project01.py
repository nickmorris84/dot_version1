from config import config
from core.transforms.ingest import ingest_from_csv
from core.quality.dq_checks import run_data_quality_checks
from core.transforms.process_master import ProcessMasterBuilder
from core.transforms.validate import validate_outputs
from core.io.writers import write_csv
from src.digital_operation_twin.core.logger import get_logger
logger=get_logger(__name__)
def run():
    logger.info("[RUN] batch_project01 starting…")
    df=ingest_from_csv(config.RAW_DATA_PATH)
    run_data_quality_checks(df)
    b=ProcessMasterBuilder(df)
    validate_outputs(b.stage_master,b.application_master,config.GOLDEN_STAGE_PATH,config.GOLDEN_APP_PATH)
    write_csv(b.stage_master,config.OUTPUT_STAGE_PATH)
    write_csv(b.application_master,config.OUTPUT_APP_PATH)
    print("\nStage Master sample:\n",b.stage_master.head(5))
    print("\nApp Master sample:\n",b.application_master.head(5))
    logger.info("[RUN] batch_project01 finished.")
if __name__=="__main__": run()
