from __future__ import annotations

_UNIT_MULTIPLIERS = {
    "m": 60.0,
    "h": 3600.0,
    "d": 86400.0,
    "w": 604800.0,
}


def timeframe_to_seconds(timeframe: str | None) -> float:
    if not timeframe:
        return 60.0
    tf = timeframe.strip().lower()
    if not tf:
        return 60.0
    unit = tf[-1]
    try:
        value = float(tf[:-1])
    except ValueError:
        return 60.0
    return value * _UNIT_MULTIPLIERS.get(unit, 60.0)
