# dq/row_steps.py
from ..registries import register, DQ_ROW
from ..types import Event

@register(DQ_ROW, "normalize_status")
def normalize_status(ev: Event) -> Event:
    s = (ev.status_after or "").strip().lower() or None
    return Event(**{**ev.__dict__, "status_after": s})
