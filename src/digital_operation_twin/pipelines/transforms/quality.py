import pandas as pd
from typing import Any, Dict, List

from digital_operation_twin.core.models.event import Event
from digital_operation_twin.pipelines.transforms.validator import ValidatorPipeline

import logging
logger = logging.getLogger(__name__)


class DataQualityService:
    def __init__(self, validator_cfg: Dict[str, Any]):
        self.validator_cfg = validator_cfg

    def _validate_event_schema(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """
        Attempts to create an Event object from raw data.
        Raises ValueError if required fields are missing.
        Returns a normalized dict (Event.to_dict()).
        """
        try:
            event = Event.from_dict(row)
            logger.debug("Schema validated successfully", extra={"event_id": getattr(event, "event_id", None)})
            return event.to_dict()
        except TypeError as e:
            # Event.__init__ missing required args ends up here
            logger.warning("Schema validation failed: %s", e)
            raise ValueError(f"Schema validation failed: {e}") from e

    def _validate_event_data(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """
        Runs additional DQ checks (nulls, ranges, regex) on the dataframe.
        Returns a list of issues (empty list means OK).
        """
        validator_pipeline = ValidatorPipeline(self.validator_cfg, df)
        issues = validator_pipeline.run()
        if issues:
            logger.warning("ValidatorPipeline found %s issues", len(issues))
        else:
            logger.debug("ValidatorPipeline found no issues")
        return issues

    def run(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Full DQ pipeline (DataFrame in / DataFrame out):
        - Schema validation per row (Event model)
        - Extra DQ validation on the resulting validated_df
        - Returns validated_df if OK
        - Raises ValueError if rejected
        """
        if not isinstance(df, pd.DataFrame):
            raise TypeError(f"DataQualityService expected DataFrame, got {type(df).__name__}")

        if df.empty:
            logger.info("DQ: empty DataFrame; nothing to validate")
            return df

        validated_rows: List[Dict[str, Any]] = []

        # 1) Schema validation row-by-row; rebuild normalized dataframe
        for idx, row in df.iterrows():
            try:
                validated_rows.append(self._validate_event_schema(row.to_dict()))
            except ValueError as e:
                logger.warning("DQ rejected at row=%s: %s", idx, e)
                raise

        validated_df = pd.DataFrame(validated_rows)

        # 2) Extra DQ rules
        issues = self._validate_event_data(validated_df)
        if issues:
            # keep details in exception (or you can attach to state in the step)
            raise ValueError(f"DQ rejected: {len(issues)} issues found")

        logger.info("DQ passed for %s rows", len(validated_df))
        return validated_df