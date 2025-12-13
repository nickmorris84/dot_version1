# classifiers/risk.py
from ..registries import register, CLASSIFIERS
from ..types import Event

@register(CLASSIFIERS, "risk_flag")
def risk_flag(ev: Event, high=("high", "very_high")) -> bool:
    return (ev.risk_grade or "").lower() in {x.lower() for x in high}

@register(CLASSIFIERS, "status_label")
def status_label(ev: Event) -> str:
    s = (ev.status_after or "")
    if s in {"approved", "ok"}: return "positive"
    if s in {"rejected", "declined"}: return "negative"
    return "unknown"
