from __future__ import annotations
import pandas as pd
def align_distributions(a: pd.Series, b: pd.Series):
    idx = sorted(set(a.index).union(set(b.index)))
    a = a.reindex(idx, fill_value=0.0)
    b = b.reindex(idx, fill_value=0.0)
    return a, b
def total_variation_distance(p: pd.Series, q: pd.Series) -> float:
    p, q = align_distributions(p, q)
    return 0.5 * (p.sub(q).abs().sum())
def rel_or_abs_ok(current: float, golden: float, max_rel: float, max_abs: float) -> bool:
    if pd.isna(current) or pd.isna(golden): return True
    if abs(current - golden) <= max_abs: return True
    denom = max(abs(golden), 1e-9)
    rel = abs(current - golden) / denom
    return rel <= max_rel
def simplify_dtype(dtype) -> str:
    s = str(dtype)
    if "datetime64" in s: return "datetime"
    if s.startswith("int"): return "int"
    if s.startswith("float"): return "float"
    if s in ("object", "string"): return "string"
    if s.startswith("bool"): return "bool"
    return s
