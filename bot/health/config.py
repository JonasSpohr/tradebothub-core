from __future__ import annotations

FLUSH_INTERVALS_OUT_OF_POSITION = {
    "fast_5s": 60,
    "ultra_15s": 90,
    "fast_30s": 120,
    "standard": 180,
}
FLUSH_INTERVALS_IN_POSITION = {
    "fast_5s": 20,
    "ultra_15s": 45,
    "fast_30s": 75,
    "standard": 150,
}
DEFAULT_TIER = "standard"
ROLLING_WINDOW_SECONDS = 15 * 60
DEBOUNCE_SECONDS = 3.0
CRITICAL_DELAY_SECONDS = 1.0


def normalize_tier(tier: str | None) -> str:
    if not tier:
        return DEFAULT_TIER
    normalized = tier.strip().lower()
    if normalized in FLUSH_INTERVALS_OUT_OF_POSITION:
        return normalized
    return DEFAULT_TIER


def get_flush_interval(tier: str | None, in_position: bool) -> int:
    normalized = normalize_tier(tier)
    table = FLUSH_INTERVALS_IN_POSITION if in_position else FLUSH_INTERVALS_OUT_OF_POSITION
    return table.get(normalized, table[DEFAULT_TIER])
