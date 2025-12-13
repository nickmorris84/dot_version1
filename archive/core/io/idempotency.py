from __future__ import annotations
from typing import Optional
import time

class IdempotencyStore:
    """
    Replace with Redis/Dynamo/DB for production.
    Simple in-memory TTL cache to demo the API.
    """
    def __init__(self, ttl_seconds: int = 24*3600):
        self._seen = {}            # key -> expires_at
        self.ttl = ttl_seconds

    def seen(self, key: Optional[str]) -> bool:
        if not key:
            return False
        now = time.time()
        # clean
        expired = [k for k, exp in self._seen.items() if exp < now]
        for k in expired: self._seen.pop(k, None)
        # check/add
        if key in self._seen and self._seen[key] >= now:
            return True
        self._seen[key] = now + self.ttl
        return False
