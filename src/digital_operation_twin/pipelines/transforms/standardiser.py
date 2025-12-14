from __future__ import annotations

from typing import Callable, Dict, Any, List, Union
import pandas as pd
import re
import logging

logger = logging.getLogger(__name__)

# ---------- FieldMapper: DataFrame -> DataFrame ----------
FieldMapper = Callable[[pd.DataFrame], pd.DataFrame]


# ---------- Utility ----------
def _resolve_columns(df: pd.DataFrame, cols: Union[List[str], str]) -> List[str]:
    if cols == "*":
        return list(df.columns)
    return cols


def _to_snake_case_label(label: str) -> str:
    label = label.strip().replace(" ", "_")
    s1 = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", label)
    s2 = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s1)
    s3 = re.sub(r"__+", "_", s2)
    return s3.lower()


def _diff_columns(before: List[str], after: List[str]) -> Dict[str, Any]:
    before_set, after_set = set(before), set(after)
    return {
        "added": sorted(after_set - before_set),
        "removed": sorted(before_set - after_set),
    }


def _diff_dtypes(before: pd.Series, after: pd.Series) -> Dict[str, str]:
    changed: Dict[str, str] = {}
    common = set(before.index).intersection(set(after.index))
    for col in common:
        b, a = str(before[col]), str(after[col])
        if b != a:
            changed[col] = f"{b} -> {a}"
    return changed


# ---------- Step Builders ----------
def rename(cols: Dict[str, str]) -> FieldMapper:
    """
    Rename columns using mapping {source_col: target_col}.
    Logs whether any rename actually applied (pandas rename is a no-op if keys don't match).
    """
    def _fn(df: pd.DataFrame) -> pd.DataFrame:
        before_cols = list(df.columns)
        out = df.rename(columns=cols)
        after_cols = list(out.columns)

        # Determine applied renames (keys that existed and changed)
        applied = {src: dst for src, dst in cols.items() if src in before_cols and src != dst}
        # Filter to those that truly changed the output set
        applied = {src: dst for src, dst in applied.items() if dst in after_cols and src not in after_cols}

        if applied:
            logger.info("[rename] applied=%s", applied)
        else:
            matched = [src for src in cols.keys() if src in before_cols]
            logger.warning(
                "[rename] no renames applied | matched_keys=%s | mapping_keys=%s",
                matched,
                list(cols.keys()),
            )

        return out

    _fn.__name__ = "rename"
    return _fn


def to_snake_case() -> FieldMapper:
    """
    Convert all column names to snake_case, logging what changed.
    """
    def _fn(df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        before = list(out.columns)
        after = [_to_snake_case_label(col) for col in before]
        out.columns = after

        if before != after:
            mapping = {b: a for b, a in zip(before, after) if b != a}
            logger.info("[to_snake_case] changed=%s", mapping)
        else:
            logger.info("[to_snake_case] no changes")
        return out

    _fn.__name__ = "to_snake_case"
    return _fn


def cast(dtypes: Union[Dict[str, str], str]) -> FieldMapper:
    def _fn(df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        dtypes_dict: Dict[str, str]
        if dtypes == "*":
            dtypes_dict = {col: "string" for col in df.columns}
        else:
            dtypes_dict = dtypes

        for col, dt in dtypes_dict.items():
            if col not in out.columns:
                logger.warning("[cast] column not found: %s", col)
                continue
            try:
                if dt.startswith("datetime"):
                    out[col] = pd.to_datetime(out[col], errors="coerce")
                else:
                    out[col] = out[col].astype(dt)
            except Exception as e:
                logger.error("[cast] failed casting %s to %s: %s", col, dt, e, exc_info=True)
                raise
        return out

    _fn.__name__ = "cast"
    return _fn


def value_map(columns: Union[List[str], str], mapping: Dict[Any, Any]) -> FieldMapper:
    def _fn(df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        cols_to_use = _resolve_columns(df, columns)
        for col in cols_to_use:
            if col not in df.columns:
                logger.warning("[value_map] column not found: %s", col)
                continue
            out[col] = out[col].map(mapping).fillna(out[col])
        return out

    _fn.__name__ = f"value_map[{','.join(columns) if isinstance(columns, list) else '*'}]"
    return _fn


def require(columns: Union[List[str], str]) -> FieldMapper:
    def _fn(df: pd.DataFrame) -> pd.DataFrame:
        cols = _resolve_columns(df, columns)
        missing = [c for c in cols if c not in df.columns or df[c].isna().any()]
        if missing:
            logger.error("[require] missing/NA columns=%s", missing)
            raise ValueError(f"Required columns missing/NA: {missing}")
        return df

    _fn.__name__ = "require"
    return _fn


def drop_columns(columns: List[str]) -> FieldMapper:
    def _fn(df: pd.DataFrame) -> pd.DataFrame:
        return df.drop(columns=[c for c in columns if c in df.columns], errors="ignore")

    _fn.__name__ = f"drop_columns[{','.join(columns)}]"
    return _fn


def fill_defaults(defaults: Union[Dict[str, Any], str]) -> FieldMapper:
    def _fn(df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()

        d: Dict[str, Any]
        if defaults == "*":
            d = {col: "" for col in out.columns}
        else:
            d = defaults

        for col, value in d.items():
            if col in out.columns:
                out[col] = out[col].fillna(value)
        return out

    if isinstance(defaults, dict):
        _fn.__name__ = f"fill_defaults[{','.join(defaults.keys())}]"
    else:
        _fn.__name__ = "fill_defaults[*]"
    return _fn


# ---------- Build steps list from config ----------
def steps_from_cfg(cfg: Union[Dict[str, Any], List]) -> List[FieldMapper]:
    steps: List[FieldMapper] = []
    logger.info("[steps_from_cfg] Building transformation steps")

    if not isinstance(cfg, dict):
        logger.warning("[steps_from_cfg] cfg is not a dict (type=%s). No steps.", type(cfg).__name__)
        return steps

    if cfg.get("to_snake_case"):
        logger.info(" → Adding: to_snake_case")
        steps.append(to_snake_case())

    if cfg.get("drop_columns"):
        logger.info(" → Adding: drop_columns %s", cfg["drop_columns"])
        steps.append(drop_columns(cfg["drop_columns"]))

    if cfg.get("fill_defaults"):
        logger.info(" → Adding: fill_defaults")
        steps.append(fill_defaults(cfg["fill_defaults"]))

    if cfg.get("rename"):
        # IMPORTANT: your pipeline applies to_snake_case first, so config keys should be snake_case too.
        # We normalise the keys to lower-case to reduce mismatch.
        rn_lc = {str(k).lower(): v for k, v in cfg["rename"].items()}
        logger.info(" → Adding: rename %s", rn_lc)
        steps.append(rename(rn_lc))

    if cfg.get("cast"):
        logger.info(" → Adding: cast")
        steps.append(cast(cfg["cast"]))

    if cfg.get("value_map"):
        vm = cfg["value_map"]
        if isinstance(vm, dict) and "columns" in vm:
            logger.info(" → Adding: value_map %s", vm["columns"])
            steps.append(value_map(vm["columns"], vm["mapping"]))
        elif isinstance(vm, dict):
            for col, mapping in vm.items():
                logger.info(" → Adding: value_map[%s]", col)
                steps.append(value_map([col], mapping))
        else:
            logger.warning("[steps_from_cfg] value_map is not a dict; skipping")

    if cfg.get("require"):
        logger.info(" → Adding: require %s", cfg["require"])
        steps.append(require(cfg["require"]))

    return steps


# ---------- Pipeline Runner ----------
class StandardiserPipeline:
    def __init__(self, cfg: Dict[str, Any], verbose: bool = False):
        self.steps = steps_from_cfg(cfg)
        self.verbose = verbose
        logger.info(
            "[StandardiserPipeline] Initialised with steps=%s",
            [getattr(s, "__name__", s.__class__.__name__) for s in self.steps],
        )

    def run(self, df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()

        for i, step in enumerate(self.steps, 1):
            name = getattr(step, "__name__", step.__class__.__name__)
            before_shape = out.shape
            before_cols = list(out.columns)
            before_dtypes = out.dtypes.copy()

            try:
                out = step(out)

                after_shape = out.shape
                after_cols = list(out.columns)
                after_dtypes = out.dtypes.copy()

                col_diff = _diff_columns(before_cols, after_cols)
                dtype_diff = _diff_dtypes(before_dtypes, after_dtypes)

                logger.info(
                    "[%s] Step %s applied | shape %s -> %s | added=%s removed=%s dtype_changes=%s",
                    i,
                    name,
                    before_shape,
                    after_shape,
                    col_diff["added"],
                    col_diff["removed"],
                    dtype_diff if dtype_diff else {},
                )

                if self.verbose:
                    preview = out.head(1).to_dict(orient="records")
                    logger.debug("[%s] %s preview=%s", i, name, preview)

            except Exception as e:
                logger.error("[%s] Step %s failed: %s", i, name, str(e), exc_info=True)
                raise

        logger.info("[StandardiserPipeline] All steps completed.")
        return out