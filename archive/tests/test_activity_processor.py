import pandas as pd
from core.io.database_init import init_db, get_session
from core.classes.activity_processor import ActivityProcessor
from core.io.database_models import Journey, Step, EventFact
def test_activity_processor_runs(tmp_path):
    init_db(f"sqlite:///{tmp_path}/test.db")
    df = pd.DataFrame([{
        "journey_id":"J1","step_name":"StageA",
        "event_ts":"2024-01-01","stage_start_ts":"2024-01-01","stage_end_ts":"2024-01-02",
        "status_after":"Done","performed_by_std":"tester",
        "risk_grade_std":"Low","uw_decision_std":"Approve"
    }])
    for c in ["event_ts","stage_start_ts","stage_end_ts"]:
        df[c] = pd.to_datetime(df[c])
    ActivityProcessor("J1", df).process()
    s = get_session()
    assert s.get(Journey, "J1") is not None
    assert s.query(Step).count() == 1
    assert s.query(EventFact).count() == 1
    s.close()
