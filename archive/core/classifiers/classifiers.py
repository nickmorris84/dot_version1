# define optional classifiers
def status_cls(ev: Event) -> str:
    s = (ev.status_after or "").lower()
    return "positive" if s in {"approved", "ok"} else "negative" if s in {"rejected", "declined"} else "unknown"