import time
import random
from bot.core.safety import MIN_POLL_SECONDS

class JitterScheduler:
    """
    Drift-free scheduler with symmetric jitter that never drops below the configured minimum.
    """

    def __init__(self, base_seconds: int, jitter_seconds: int = 10, min_seconds: int | None = None):
        self.min_seconds = max(int(min_seconds) if min_seconds is not None else MIN_POLL_SECONDS, MIN_POLL_SECONDS)
        self.base = max(int(base_seconds), self.min_seconds)
        self.jitter = max(int(jitter_seconds), 0)

    def startup_stagger(self):
        delay = random.uniform(0, self.base)
        time.sleep(delay)

    def next_interval(
        self,
        base_override: int | None = None,
        jitter_override: int | None = None,
        min_override: int | None = None,
    ) -> float:
        if min_override is not None:
            self.min_seconds = max(int(min_override), MIN_POLL_SECONDS)

        base = self.base
        if base_override is not None:
            base = max(int(base_override), self.min_seconds)

        jitter = self.jitter
        if jitter_override is not None:
            jitter = max(int(jitter_override), 0)

        self.base = base
        self.jitter = jitter
        delta = random.uniform(-jitter, jitter) if jitter else 0.0
        return max(self.min_seconds, base + delta)

    def sleep_for(self, interval: float, started_at: float):
        target = started_at + interval
        remaining = target - time.monotonic()
        if remaining > 0:
            time.sleep(remaining)
