"""
ActivityClassifier: normalize raw events to a canonical shape.
"""
import pandas as pd

class ActivityClassifier:
    def __init__(self): pass
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        # journey_id from Application_ID if not present
        if "journey_id" not in out.columns:
            if "Application_ID" in out.columns:
                out["journey_id"] = out["Application_ID"].astype(str)
            else:
                raise ValueError("Missing 'journey_id' or 'Application_ID'.")
        # sub/step
        out["sub_process"] = out["Sub_Process"] if "Sub_Process" in out.columns else None
        out["step_name"] = out["Stage"] if "Stage" in out.columns else None
        # timestamps
        out["event_ts"] = out["Activity_Timestamp"] if "Activity_Timestamp" in out.columns else pd.NaT
        out["stage_start_ts"] = out["Stage_Start_Timestamp"] if "Stage_Start_Timestamp" in out.columns else pd.NaT
        out["stage_end_ts"] = out["Stage_End_Timestamp"] if "Stage_End_Timestamp" in out.columns else pd.NaT
        # status/performed
        out["status_after"] = out["Status_After_Activity"] if "Status_After_Activity" in out.columns else None
        out["performed_by_std"] = out["Performed_By"] if "Performed_By" in out.columns else None
        # other attrs
        out["risk_grade_std"] = out["Risk_Grade"] if "Risk_Grade" in out.columns else None
        out["uw_decision_std"] = out["UW_Decision"] if "UW_Decision" in out.columns else None
        out["issue_flag"] = out["Issue_Flag"] if "Issue_Flag" in out.columns else None
        out["issue_code"] = out["Issue_Code"] if "Issue_Code" in out.columns else None
        # ensure datetimes
        for c in ["event_ts","stage_start_ts","stage_end_ts"]:
            if c in out.columns: out[c] = pd.to_datetime(out[c], errors="coerce")
        return out
