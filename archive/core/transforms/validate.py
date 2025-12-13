import os, pandas as pd
from ....src.digital_operation_twin.core.logger import get_logger
logger = get_logger(__name__)
def validate_outputs(stage,app,g_stage,g_app):
    if not (os.path.exists(g_stage) and os.path.exists(g_app)):
        logger.warning("[VALIDATE] Golden files not found; skipping.")
        return
    gs=pd.read_csv(g_stage,parse_dates=["Stage_Start","Stage_End"])
    ga=pd.read_csv(g_app,parse_dates=["Application_Start","Application_End"])
    if not stage.empty and not app.empty:
        diff_s=stage.compare(gs,keep_shape=True,keep_equal=False)
        diff_a=app.compare(ga,keep_shape=True,keep_equal=False)
        if not diff_s.empty or not diff_a.empty:
            logger.warning("[VALIDATE] Differences detected vs golden.")
        else:
            logger.info("[VALIDATE] Passed.")
