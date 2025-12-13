"""
Streaming outline. Hook into Kafka or any stream,
call process_microbatch(records) with list of dicts.
"""
import pandas as pd
from core.transforms.process_master import ProcessMasterBuilder
from core.io.writers import write_csv
from src.digital_operation_twin.core.logger import get_logger
logger=get_logger(__name__)
def process_microbatch(records):
    if not records: return
    df=pd.DataFrame(records)
    b=ProcessMasterBuilder(df)
    write_csv(b.stage_master,"data/stream_stage_master.csv")
    write_csv(b.application_master,"data/stream_application_master.csv")
    logger.info(f"[STREAM] Processed microbatch={len(df)}")
