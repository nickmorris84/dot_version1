from typing import Callable, Dict, Any, Iterable, List, Union
import pandas as pd
import re
import logging

# ---------- Logging Setup ----------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# ---------- FieldMapper: DataFrame -> DataFrame ----------
FieldMapper = Callable[[pd.DataFrame], pd.DataFrame]

# ---------- Utility ----------
def _resolve_columns(df: pd.DataFrame, cols: Union[List[str], str]) -> List[str]:
    if cols == "*":
        return list(df.columns)
    return cols

# ---------- Step Builders ----------
def rename(cols: Dict[str, str]) -> FieldMapper:
    def _fn(df: pd.DataFrame) -> pd.DataFrame:
        logger.debug(f"[rename] Renaming columns: {cols}")
        return df.rename(columns=cols)
    _fn.__name__ = "rename"
    return _fn

def _to_snake_case_label(label: str) -> str:
    label = label.strip().replace(" ", "_")
    s1 = re.sub(r'(.)([A-Z][a-z]+)', r'\1_\2', label)
    s2 = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', s1)
    s3 = re.sub(r'__+', '_', s2)
    return s3.lower()

def to_snake_case() -> FieldMapper:
    def _fn(df: pd.DataFrame) -> pd.DataFrame:
        logger.debug("[to_snake_case] Converting all columns to snake_case")
        out = df.copy()
        out.columns = [_to_snake_case_label(col) for col in out.columns]
        return out
    _fn.__name__ = "to_snake_case"
    return _fn

def cast(dtypes: Union[Dict[str, str], str]) -> FieldMapper:
    def _fn(df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        dtypes_dict = dtypes
        if dtypes == "*":
            dtypes_dict = {col: "string" for col in df.columns}

        for col, dt in dtypes_dict.items():
            if col not in out.columns:
                logger.warning(f"[cast] Column {col} not found")
                continue
            try:
                if dt.startswith("datetime"):
                    logger.debug(f"[cast] Converting {col} to datetime")
                    out[col] = pd.to_datetime(out[col], errors="coerce")
                else:
                    logger.debug(f"[cast] Casting {col} to {dt}")
                    out[col] = out[col].astype(dt)
            except Exception as e:
                logger.error(f"[cast] Failed casting {col} to {dt}: {e}", exc_info=True)
        return out
    _fn.__name__ = "cast"
    return _fn

def value_map(columns: Union[List[str], str], mapping: Dict[Any, Any]) -> FieldMapper:
    def _fn(df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        cols_to_use = _resolve_columns(df, columns)
        for col in cols_to_use:
            if col not in df.columns:
                logger.warning(f"[value_map] Column {col} not found")
                continue
            out[col] = out[col].map(mapping).fillna(out[col])
        return out
    _fn.__name__ = f"value_map[{','.join(columns) if isinstance(columns, list) else '*'}]"
    return _fn

def require(columns: Union[List[str], str]) -> FieldMapper:
    def _fn(df: pd.DataFrame) -> pd.DataFrame:
        cols = _resolve_columns(df, columns)
        logger.debug(f"[require] Checking required columns: {cols}")
        missing = [c for c in cols if c not in df.columns or df[c].isna().any()]
        if missing:
            logger.error(f"[require] Required columns missing/NA: {missing}")
            raise ValueError(f"Required columns missing/NA: {missing}")
        return df
    _fn.__name__ = "require"
    return _fn

def drop_columns(columns: List[str]) -> FieldMapper:
    def _fn(df: pd.DataFrame) -> pd.DataFrame:
        logger.debug(f"[drop_columns] Dropping columns: {columns}")
        return df.drop(columns=[c for c in columns if c in df.columns], errors="ignore")
    _fn.__name__ = f"drop_columns[{','.join(columns)}]"
    return _fn

def fill_defaults(defaults: Union[Dict[str, Any], str]) -> FieldMapper:
    def _fn(df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        if defaults == "*":
            defaults = {col: "" for col in df.columns}
        logger.debug(f"[fill_defaults] Filling defaults: {defaults}")
        for col, value in defaults.items():
            if col in out.columns:
                out[col] = out[col].fillna(value)
        return out
    _fn.__name__ = f"fill_defaults[{','.join(defaults.keys()) if isinstance(defaults, dict) else '*'}]"
    return _fn

# ---------- Build steps list from config ----------
def steps_from_cfg(cfg: Union[Dict[str, Any], List]) -> List[FieldMapper]:
    steps: List[FieldMapper] = []
    logger.info("[steps_from_cfg] Building transformation steps")

    if cfg.get("to_snake_case"):
        logger.info(" → Adding: to_snake_case")
        steps.append(to_snake_case())

    if cfg.get("drop_columns"):
        logger.info(f" → Adding: drop_columns {cfg['drop_columns']}")
        steps.append(drop_columns(cfg["drop_columns"]))

    if cfg.get("fill_defaults"):
        steps.append(fill_defaults(cfg["fill_defaults"]))
        logger.info(" → Adding: fill_defaults")

    if cfg.get("rename"):
        rn_lc = {k.lower(): v for k, v in cfg["rename"].items()}
        logger.info(f" → Adding: rename {rn_lc}")
        steps.append(rename(rn_lc))

    if cfg.get("cast"):
        logger.info(" → Adding: cast")
        steps.append(cast(cfg["cast"]))

    if cfg.get("value_map"):
        vm = cfg["value_map"]
        if isinstance(vm, dict) and "columns" in vm:
            steps.append(value_map(vm["columns"], vm["mapping"]))
            logger.info(f" → Adding: value_map {vm['columns']}")
        else:
            for col, mapping in vm.items():
                steps.append(value_map([col], mapping))
                logger.info(f" → Adding: value_map[{col}]")

    if cfg.get("require"):
        steps.append(require(cfg["require"]))
        logger.info(f" → Adding: require {cfg['require']}")

    return steps

# ---------- Pipeline Runner ----------
class StandardiserPipeline:
    def __init__(self, cfg: Dict[str, Any], verbose: bool = False):
        logger.info("[StandardiserPipeline] Initialising with config")
        self.steps = steps_from_cfg(cfg)
        self.verbose = verbose

    def run(self, df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        for i, step in enumerate(self.steps, 1):
            name = getattr(step, "__name__", step.__class__.__name__)
            before_shape = out.shape
            try:
                out = step(out)
                after_shape = out.shape
                if self.verbose:
                    logger.debug(f"[{i}] {name}: {before_shape} -> {after_shape}")
                else:
                    logger.info(f"[{i}] Step {name} applied.")
            except Exception as e:
                logger.error(f"[{i}] Step {name} failed: {str(e)}", exc_info=True)
                raise
        logger.info("[StandardiserPipeline] All steps completed.")
        return out


# STANDARDISER CONFIG EXAMPLE
# to_snake_case: true

# rename:
#   CustID: customer_id
#   AppDate: application_date

# cast: "*"   # casts all columns to string (or you can provide dict)

# value_map:
#   columns: "*"
#   mapping:
#     "Yes": true
#     "No": false

# require: "*"

# drop_columns:
#   - temp_flag

# fill_defaults: "*"
