from typing import Callable, Dict, Any, List, Union
import pandas as pd
import re
import logging

# ----------------- Logging Setup -----------------
logger = logging.getLogger(__name__)

FieldMapper = Callable[[pd.DataFrame], pd.DataFrame]

# ----------------- Utility Function -----------------
def _resolve_columns(df: pd.DataFrame, columns: Union[List[str], str]) -> List[str]:
    if columns == "*":
        resolved = list(df.select_dtypes(include="object").columns)
    else:
        resolved = columns
    logger.debug(f"_resolve_columns resolved: {resolved}")
    return resolved

# ----------------- Normaliser Functions -----------------

def trim_strings(columns: Union[List[str], str] = "*") -> FieldMapper:
    def _fn(df: pd.DataFrame) -> pd.DataFrame:
        logger.debug("Running trim_strings")
        out = df.copy()
        cols_to_use = _resolve_columns(out, columns)
        for col in cols_to_use:
            if col in out.columns:
                out[col] = out[col].astype(str).str.strip()
        return out
    _fn.__name__ = f"trim_strings[{','.join(columns) if isinstance(columns, list) else '*'}]"
    return _fn

def normalize_case(columns: Union[List[str], str], case: str = "lower") -> FieldMapper:
    def _fn(df: pd.DataFrame) -> pd.DataFrame:
        logger.debug(f"Running normalize_case with case={case}")
        out = df.copy()
        cols_to_use = _resolve_columns(out, columns)
        for col in cols_to_use:
            if col in out.columns:
                out[col] = out[col].astype(str)
                if case == "lower":
                    out[col] = out[col].str.lower()
                elif case == "upper":
                    out[col] = out[col].str.upper()
                elif case == "title":
                    out[col] = out[col].str.title()
        return out
    _fn.__name__ = f"normalize_case[{case}][{','.join(columns) if isinstance(columns, list) else '*'}]"
    return _fn

def remove_special_chars(columns: Union[List[str], str], pattern: str = r"[^\w\s]") -> FieldMapper:
    def _fn(df: pd.DataFrame) -> pd.DataFrame:
        logger.debug(f"Running remove_special_chars with pattern={pattern}")
        out = df.copy()
        cols_to_use = _resolve_columns(out, columns)
        for col in cols_to_use:
            if col in out.columns:
                out[col] = out[col].astype(str).str.replace(pattern, '', regex=True)
        return out
    _fn.__name__ = f"remove_special_chars[{','.join(columns) if isinstance(columns, list) else '*'}]"
    return _fn

def replace_nulls(columns: Union[List[str], str], null_values: List[str] = ["", "n/a", "na", "null"]) -> FieldMapper:
    def _fn(df: pd.DataFrame) -> pd.DataFrame:
        logger.debug(f"Running replace_nulls with values={null_values}")
        out = df.copy()
        cols_to_use = _resolve_columns(out, columns)
        for col in cols_to_use:
            if col in out.columns:
                out[col] = out[col].replace(null_values, pd.NA)
        return out
    _fn.__name__ = f"replace_nulls[{','.join(columns) if isinstance(columns, list) else '*'}]"
    return _fn

def standardize_booleans(columns: Union[List[str], str], true_vals=None, false_vals=None) -> FieldMapper:
    true_vals = true_vals or ["yes", "y", "true", "1"]
    false_vals = false_vals or ["no", "n", "false", "0"]

    def _fn(df: pd.DataFrame) -> pd.DataFrame:
        logger.debug("Running standardize_booleans")
        out = df.copy()
        cols_to_use = _resolve_columns(out, columns)
        for col in cols_to_use:
            if col in out.columns:
                out[col] = out[col].astype(str).str.lower()
                out[col] = out[col].apply(
                    lambda x: True if x in true_vals else (False if x in false_vals else pd.NA)
                )
        return out
    _fn.__name__ = f"standardize_booleans[{','.join(columns) if isinstance(columns, list) else '*'}]"
    return _fn

def clean_currency(columns: Union[List[str], str]) -> FieldMapper:
    def _fn(df: pd.DataFrame) -> pd.DataFrame:
        logger.debug("Running clean_currency")
        out = df.copy()
        cols_to_use = _resolve_columns(out, columns)
        for col in cols_to_use:
            if col in out.columns:
                out[col] = (
                    out[col]
                    .astype(str)
                    .str.replace(r'[\$,]', '', regex=True)
                    .str.replace(' ', '')
                )
                out[col] = pd.to_numeric(out[col], errors="coerce")
        return out
    _fn.__name__ = f"clean_currency[{','.join(columns) if isinstance(columns, list) else '*'}]"
    return _fn

def normalize_dates(columns: Union[List[str], str], date_format: str = None) -> FieldMapper:
    def _fn(df: pd.DataFrame) -> pd.DataFrame:
        logger.debug(f"Running normalize_dates with format={date_format}")
        out = df.copy()
        cols_to_use = _resolve_columns(out, columns)
        for col in cols_to_use:
            if col in out.columns:
                out[col] = pd.to_datetime(out[col], errors="coerce", format=date_format)
        return out
    _fn.__name__ = f"normalize_dates[{','.join(columns) if isinstance(columns, list) else '*'}]"
    return _fn



def steps_from_normalizer_cfg(cfg: Dict[str, Any]) -> List[FieldMapper]:
    steps: List[FieldMapper] = []

    # Strip whitespace
    if cfg.get("trim_strings"):
        steps.append(trim_strings())

    # Normalize case
    case_cfg = cfg.get("normalize_case")
    if case_cfg:
        columns = case_cfg.get("columns", "*")
        case_type = case_cfg.get("case", "lower")
        steps.append(normalize_case(columns, case_type))

    # Remove special characters
    rsc_cfg = cfg.get("remove_special_chars")
    if rsc_cfg:
        columns = rsc_cfg.get("columns", "*")
        allowed_chars = rsc_cfg.get("allowed_chars", r"\w\s")
        steps.append(remove_special_chars(columns, allowed_chars))

    # Replace nulls
    rn_cfg = cfg.get("replace_nulls")
    if rn_cfg:
        columns = rn_cfg.get("columns", "*")
        value = rn_cfg.get("value", "")
        steps.append(replace_nulls(columns, value))

    # Standardize booleans
    if cfg.get("standardize_booleans"):
        steps.append(standardize_booleans())

    # Clean currency fields
    currency_cfg = cfg.get("clean_currency")
    if currency_cfg:
        columns = currency_cfg.get("columns", "*")
        steps.append(clean_currency(columns))

    # Normalize date fields
    date_cfg = cfg.get("normalize_dates")
    if date_cfg:
        columns = date_cfg.get("columns", "*")
        fmt = date_cfg.get("format", None)
        steps.append(normalize_dates(columns, fmt))

    return steps


class NormalizerPipeline:
    def __init__(self, cfg: Dict[str, Any], verbose: bool = False):
        self.steps = steps_from_normalizer_cfg(cfg)
        self.verbose = verbose
        logger.info(f"[NormalizerPipeline] Initialized with steps: {[s.__name__ for s in self.steps]}")

    def run(self, df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        for i, step in enumerate(self.steps, 1):
            name = getattr(step, "__name__", step.__class__.__name__)
            before = out.shape
            try:
                out = step(out)
                after = out.shape
                if self.verbose:
                    logger.debug(f"[Step {i}] {name}: {before} → {after}")
                else:
                    logger.info(f"[Step {i}] {name} applied.")
            except Exception as e:
                logger.error(f"[Step {i}] {name} failed: {str(e)}", exc_info=True)
                raise
        logger.info("[NormalizerPipeline] All steps completed.")
        return out



# NORMALISER CONFIG EXAMPLES
# normalize_case:
#   columns: "*"       # or specify: [name, status]
#   case: lower        # options: lower | upper | title

# strip_whitespace:
#   columns: "*"       # or specify: [name, comments]

# trim_strings: true   # applies .str.strip() to all object columns

# remove_special_chars:
#   columns: "*"       # or specify: [name, email]
#   regex: "[^a-zA-Z0-9@._ ]"

# replace_nulls:
#   columns: "*"       # or specify: [status]
#   value: "missing"

# dedupe_records:
#   subset: [customer_id, application_date]
#   keep: first         # options: first | last | False
