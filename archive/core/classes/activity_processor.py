"""
ActivityProcessor: processes one journey's events with a Step 1..4 flow.
"""
from __future__ import annotations
import pandas as pd
from datetime import datetime
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from ....src.digital_operation_twin.core.logger import get_logger
from ..io.database_init import get_session
from ..io.database_models import Journey, Step, SubProcess, EventFact

logger = get_logger(__name__)

class ActivityProcessor:
    def __init__(self, journey_id: str, events: pd.DataFrame):
        self.journey_id = str(journey_id)
        self.events = events.sort_values(by=["event_ts", "stage_end_ts"], na_position="last").reset_index(drop=True)

    def _ensure_journey(self, session) -> Journey:
        j = session.get(Journey, self.journey_id)
        if not j:
            j = Journey(journey_id=self.journey_id)
            session.add(j); session.flush()
            logger.info(f"[JOURNEY] created: {self.journey_id}")
        return j

    def _ensure_step(self, session, step_name: str) -> Step:
        step_name = step_name or "__UNKNOWN_STEP__"
        stmt = select(Step).where(Step.journey_id == self.journey_id, Step.step_name == step_name)
        s = session.execute(stmt).scalar_one_or_none()
        if not s:
            s = Step(journey_id=self.journey_id, step_name=step_name)
            session.add(s); session.flush()
            logger.debug(f"[STEP] created: ({self.journey_id}, {step_name})")
        return s

    def _apply_issues(self, step: Step, row: pd.Series):
        issue_hit = False
        if "issue_flag" in row and pd.notna(row["issue_flag"]) and str(row["issue_flag"]).strip(): issue_hit = True
        if "issue_code" in row and pd.notna(row["issue_code"]) and str(row["issue_code"]).strip(): issue_hit = True
        if issue_hit: step.issues_count = (step.issues_count or 0) + 1

    def _apply_status(self, journey: Journey, step: Step, row: pd.Series):
        status = row.get("status_after", None)
        if pd.notna(status):
            step.status = str(status); journey.status = str(status)

    def _apply_timing(self, journey: Journey, step: Step, row: pd.Series):
        s_start = row.get("stage_start_ts"); e_ts = row.get("event_ts"); s_end = row.get("stage_end_ts")
        if pd.notna(s_start):
            step.start_time = min([t for t in [step.start_time, s_start] if t is not None], default=s_start)
        elif pd.notna(e_ts) and step.start_time is None:
            step.start_time = e_ts
        if pd.notna(s_end):
            step.end_time = max([t for t in [step.end_time, s_end] if t is not None], default=s_end)
        if step.start_time and step.end_time and step.end_time >= step.start_time:
            step.tat_minutes = (step.end_time - step.start_time).total_seconds() / 60.0
        if pd.notna(e_ts):
            if journey.start_time is None or e_ts < journey.start_time: journey.start_time = e_ts
            if journey.end_time is None or e_ts > journey.end_time: journey.end_time = e_ts
        if journey.start_time:
            journey.age_days = (datetime.utcnow() - journey.start_time).days

    def _record_event_fact(self, session, row: pd.Series):
        ef = EventFact(
            journey_id=self.journey_id,
            sub_process=row.get("sub_process"),
            step_name=row.get("step_name"),
            event_ts=row.get("event_ts"),
            stage_start_ts=row.get("stage_start_ts"),
            stage_end_ts=row.get("stage_end_ts"),
            status_after=row.get("status_after"),
            performed_by=row.get("performed_by_std"),
            risk_grade=row.get("risk_grade_std"),
            uw_decision=row.get("uw_decision_std"),
            issue_flag=row.get("issue_flag"),
            issue_code=row.get("issue_code"),
        )
        session.add(ef)

    def process(self):
        session = get_session()
        try:
            journey = self._ensure_journey(session)
            for _, row in self.events.iterrows():
                step = self._ensure_step(session, row.get("step_name"))
                self._apply_issues(step, row)
                self._apply_status(journey, step, row)
                self._apply_timing(journey, step, row)
                self._record_event_fact(session, row)
            if journey.start_time and journey.end_time and journey.end_time >= journey.start_time:
                journey.tat_minutes = (journey.end_time - journey.start_time).total_seconds() / 60.0
            session.commit()
            steps_seen = len(set(self.events["step_name"].dropna())) if "step_name" in self.events.columns else 0
            has_issue_cols = {"issue_flag","issue_code"}.intersection(self.events.columns)
            issue_rows = self.events[list(has_issue_cols)].notna().any(axis=1).sum() if has_issue_cols else 0
            logger.info(f"[SUMMARY] journey={self.journey_id} events={len(self.events)} steps_seen={steps_seen} issues_from_events={int(issue_rows)}")
            logger.info(f"[PROCESS] journey={self.journey_id} rows={len(self.events)} committed.")
        except IntegrityError as e:
            session.rollback()
            logger.error(f"[ROLLBACK] journey={self.journey_id} | {e}")
            raise
        finally:
            session.close()





def run_pipeline(df: pd.DataFrame, *, params: Dict[str, Any]) -> Dict[str, Any]:
    work = df.copy()

    if params.get("dropna"):
        work = work.dropna()

    if params.get("dedupe"):
        work = work.drop_duplicates()

    profile: Dict[str, Any] = {
        "row_count": int(len(work)),
        "numeric_columns": [c for c in work.columns if pd.api.types.is_numeric_dtype(work[c])],
        }

    # Simple numeric summary (limit to first 10 columns to avoid huge payloads)
    sample_numeric = work.select_dtypes(include=["number"]).iloc[:, :10]
    if not sample_numeric.empty:
        desc = sample_numeric.describe().to_dict()
    else:
        desc = {}

    # You could do your own long‑running job here (sync). For async/queue based
    # execution, integrate Celery/RQ later and return a job id instead.

    return {
        "profile": profile,
        "describe": desc,
        "params_used": params,
    }