import time
import random
from bot.core.safety import MIN_POLL_SECONDS

class JitterScheduler:
    """
    Drift-free scheduler with positive jitter.
    """

    def __init__(self, base_seconds: int, jitter_seconds: int = 10):
        self.base = max(int(base_seconds), MIN_POLL_SECONDS)
        self.jitter = max(int(jitter_seconds), 0)

    def startup_stagger(self):
        delay = random.uniform(0, self.base)
        time.sleep(delay)

    def next_interval(self, base_override: int | None = None) -> float:
        base = self.base
        if base_override is not None:
            base = max(int(base_override), MIN_POLL_SECONDS)
            self.base = base
        return base + random.uniform(0, self.jitter)

    def sleep_for(self, interval: float, started_at: float):
        target = started_at + interval
        remaining = target - time.monotonic()
        if remaining > 0:
            time.sleep(remaining)
