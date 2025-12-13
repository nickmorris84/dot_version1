import pandas as pd

from typing import Any, Callable, Dict, Iterable, List, Optional, Union
from src.digital_operation_twin.core.models.event import Event
from digital_operation_twin.pipelines.transforms.validator import ValidatorPipeline

import logging
logger = logging.getLogger(__name__)

class DataQualityService():
    
    def __init__(self, validator_cfg: Dict[str, Any]):
        self.validator_cfg = validator_cfg
        self.validator_pipeline = None


    def validate_event_schema(self, row: Dict[str, Any]) -> "Event":
        """
        Attempts to create an Event object from raw data. 
        Will raise TypeError if required fields are missing.
        """
        try:
            event = Event.from_dict(row)
            logger.debug("Schema validated successfully")
            return event
        except TypeError as e:
            logger.warning(f"Schema validation failed: {e}")
            raise ValueError(f"Schema validation failed: {str(e)}")


    def validate_event_data(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """
        Runs additional DQ checks (nulls, ranges, regex) on the dataframe.
        """
        self.validator_pipeline = ValidatorPipeline(self.validator_cfg, df)
        issues = self.validator_pipeline.run()
        if issues:
            logger.warning(f"ValidatorPipeline found {len(issues)} issues")
        else:
            logger.debug("ValidatorPipeline found no issues")
        return issues
    
    
    def run(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """
        Full pipeline:
        - Optional mapping/normalization
        - Schema validation (Event dataclass)
        - Extra DQ validation
        - Return structured response
        """
        # Step 1: Schema check (required fields)
        try:
            event_obj = self.validate_event_schema(row)
        except ValueError as e:
            return {"status": "rejected", "error": str(e)}

        # Step 2: Build a single-row dataframe for DQ checks
        df = pd.DataFrame([event_obj.to_dict()])

        # Step 3: Run DQ validators
        issues = self.validate_event_data(df)
        if issues:
            return {
                "status": "rejected",
                "event_id": getattr(event_obj, "event_id", None),
                "issues": issues
            }

        # Step 4: If OK, persist or hand off
        logger.info(f"Event {event_obj.event_id} passed all checks")
        return {"status": "accepted", "event": event_obj.to_dict()}
