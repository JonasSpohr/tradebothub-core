from __future__ import annotations
from collections import deque
import threading
from time import time
from typing import Deque, Dict, Iterable

from bot.health.config import ROLLING_WINDOW_SECONDS

_HEALTH_KEYS = (
    "rate_limit_hit",
    "candle_gap",
    "stream_disconnect",
    "indicator_error",
    "decision",
    "order_reject",
    "db_error",
)

_COUNT_FIELDS = {
    "rate_limit_hit": "rate_limit_hits_15m",
    "candle_gap": "candle_gap_count_15m",
    "stream_disconnect": "stream_disconnects_15m",
    "indicator_error": "indicator_error_count_15m",
    "decision": "decision_count_15m",
    "order_reject": "order_rejects_15m",
    "db_error": "db_error_count_15m",
}

class HealthWindow:
    def __init__(self, duration_seconds: int = ROLLING_WINDOW_SECONDS):
        self._duration_seconds = duration_seconds
        self._buckets: Dict[str, Deque[float]] = {key: deque() for key in _HEALTH_KEYS}
        self._lock = threading.Lock()

    def inc(self, key: str, timestamp: float | None = None) -> None:
        if key not in self._buckets:
            return
        now = timestamp if timestamp is not None else time()
        with self._lock:
            bucket = self._buckets[key]
            bucket.append(now)
            self._prune_bucket(bucket, now)

    def count15m(self, key: str, now: float | None = None) -> int:
        if key not in self._buckets:
            return 0
        current = now if now is not None else time()
        with self._lock:
            bucket = self._buckets[key]
            self._prune_bucket(bucket, current)
            return len(bucket)

    def snapshot(self, now: float | None = None) -> Dict[str, int]:
        counts: Dict[str, int] = {}
        current = now if now is not None else time()
        with self._lock:
            for key, bucket in self._buckets.items():
                self._prune_bucket(bucket, current)
                counts[_COUNT_FIELDS[key]] = len(bucket)
        return counts

    def _prune_bucket(self, bucket: Deque[float], current: float) -> None:
        cutoff = current - self._duration_seconds
        while bucket and bucket[0] < cutoff:
            bucket.popleft()


__all__ = ["HealthWindow"]
