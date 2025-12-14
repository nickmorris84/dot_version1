from __future__ import annotations
from typing import Optional
import time

class IdempotencyStore:
    """
    Replace with Redis/Dynamo/DB for production.
    Simple in-memory TTL cache.
    """
    def __init__(self, ttl_seconds: int = 24 * 3600):
        self._seen = {}   # key -> expires_at
        self.ttl = ttl_seconds

    def _cleanup(self) -> None:
        now = time.time()
        expired = [k for k, exp in self._seen.items() if exp < now]
        for k in expired:
            self._seen.pop(k, None)

    def seen(self, key: Optional[str]) -> bool:
        if not key:
            return False
        self._cleanup()
        now = time.time()
        return key in self._seen and self._seen[key] >= now

    def mark(self, key: Optional[str]) -> None:
        if not key:
            return
        self._cleanup()
        now = time.time()
        self._seen[key] = now + self.ttl

