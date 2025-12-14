import logging
import os
from typing import Callable, Dict, List, Any, Optional
import pandas as pd

# ---------- Configure Logging ----------
APP_ENV = os.getenv("APP_ENV", "dev").lower()

logging.basicConfig(
    level=logging.DEBUG if APP_ENV == "dev" else logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
logger = logging.getLogger("ValidatorPipeline")

# ---------- FieldValidator: DataFrame -> Validation Report ----------
FieldValidator = Callable[[pd.DataFrame], List[Dict[str, Any]]]

# ---------- Step builders ----------
def check_required_columns(columns: List[str]) -> FieldValidator:
    def _fn(df: pd.DataFrame) -> List[Dict[str, Any]]:
        logger.debug(f"Running check_required_columns on: {columns}")
        issues = []
        for col in columns:
            if col not in df.columns:
                issue = {
                    "type": "missing_column",
                    "column": col,
                    "message": f"Missing required column: {col}"
                }
                logger.warning(issue["message"])
                issues.append(issue)
        return issues
    _fn.__name__ = f"check_required_columns[{','.join(columns)}]"
    return _fn

def check_nulls(columns: List[str]) -> FieldValidator:
    def _fn(df: pd.DataFrame) -> List[Dict[str, Any]]:
        logger.debug(f"Running check_nulls on: {columns}")
        issues = []
        for col in columns:
            if col in df.columns and df[col].isna().any():
                null_count = df[col].isna().sum()
                message = f"Column '{col}' has {null_count} nulls"
                logger.warning(message)
                issues.append({
                    "type": "null_values",
                    "column": col,
                    "message": message
                })
        return issues
    _fn.__name__ = f"check_nulls[{','.join(columns)}]"
    return _fn

def check_value_range(column: str, min_val: Optional[float], max_val: Optional[float]) -> FieldValidator:
    def _fn(df: pd.DataFrame) -> List[Dict[str, Any]]:
        logger.debug(f"Running check_value_range on: {column} with bounds min={min_val}, max={max_val}")
        issues = []
        if column in df.columns:
            if min_val is not None and (df[column] < min_val).any():
                message = f"Column '{column}' has values below {min_val}"
                logger.warning(message)
                issues.append({
                    "type": "value_below_min",
                    "column": column,
                    "message": message
                })
            if max_val is not None and (df[column] > max_val).any():
                message = f"Column '{column}' has values above {max_val}"
                logger.warning(message)
                issues.append({
                    "type": "value_above_max",
                    "column": column,
                    "message": message
                })
        return issues
    _fn.__name__ = f"check_value_range[{column}]"
    return _fn

# ---------- Validator Builder ----------
def build_validators(cfg: Dict[str, Any]) -> List[FieldValidator]:
    logger.info("Building validators from config...")
    steps = []

    if "required_columns" in cfg:
        logger.debug("Adding required column validator")
        steps.append(check_required_columns(cfg["required_columns"]))

    if "check_nulls" in cfg:
        logger.debug("Adding null value validator")
        steps.append(check_nulls(cfg["check_nulls"]))

    if "value_ranges" in cfg:
        for col, bounds in cfg["value_ranges"].items():
            logger.debug(f"Adding range validator for {col}")
            steps.append(check_value_range(col, bounds.get("min"), bounds.get("max")))

    logger.info(f"Total validators configured: {len(steps)}")
    return steps

# ---------- Pipeline Runner ----------
class ValidatorPipeline:
    def __init__(self, cfg: Dict[str, Any], df: pd.DataFrame):
        logger.info("Initializing ValidatorPipeline...")
        self.validators = build_validators(cfg)
        self.df = df

    def run(self) -> List[Dict[str, Any]]:
        logger.info("Running validation pipeline...")
        all_issues = []
        for validator in self.validators:
            logger.debug(f"Executing {validator.__name__}")
            try:
                issues = validator(self.df)
                all_issues.extend(issues)
                if issues:
                    logger.info(f"Issues found by {validator.__name__}: {len(issues)}")
            except Exception as e:
                logger.exception(f"Validator {validator.__name__} failed with error: {e}")
        logger.info(f"Validation completed. Total issues found: {len(all_issues)}")
        return all_issues



# VALIDATOR CONFIG EXAMPLE
# require_columns:
#   - application_id
#   - status
#   - product_type

# no_nulls:
#   - application_id
#   - status

# allowed_values:
#   status:
#     - approved
#     - pending
#     - rejected
#   product_type:
#     - credit_card
#     - personal_loan
#     - mortgage

# value_ranges:
#   credit_score:
#     min: 300
#     max: 900

# regex:
#   email: "^[\\w\\.-]+@[\\w\\.-]+\\.\\w+$"
