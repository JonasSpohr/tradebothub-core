from __future__ import annotations

REASON_CODE_UNKNOWN = "UNKNOWN_ERROR"
REASON_CODE_INVALID_KEY = "INVALID_API_KEY"
REASON_CODE_INSUFFICIENT_BALANCE = "INSUFFICIENT_BALANCE"
REASON_CODE_MIN_NOTIONAL = "MIN_NOTIONAL"
REASON_CODE_RATE_LIMIT = "RATE_LIMIT"
REASON_CODE_WEBSOCKET = "WEBSOCKET_TIMEOUT"
REASON_CODE_POSITION_MISMATCH = "POSITION_MISMATCH"
REASON_CODE_DB_TIMEOUT = "DB_TIMEOUT"
REASON_CODE_INDICATOR = "INDICATOR_ERROR"

_REASON_PATTERNS = [
    ("invalid api", REASON_CODE_INVALID_KEY),
    ("invalid key", REASON_CODE_INVALID_KEY),
    ("insufficient balance", REASON_CODE_INSUFFICIENT_BALANCE),
    ("insufficient funds", REASON_CODE_INSUFFICIENT_BALANCE),
    ("min notional", REASON_CODE_MIN_NOTIONAL),
    ("min_notional", REASON_CODE_MIN_NOTIONAL),
    ("rate limit", REASON_CODE_RATE_LIMIT),
    ("ratelimit", REASON_CODE_RATE_LIMIT),
    ("ddos", REASON_CODE_RATE_LIMIT),
    ("timeout", REASON_CODE_WEBSOCKET),
    ("websocket", REASON_CODE_WEBSOCKET),
    ("position mismatch", REASON_CODE_POSITION_MISMATCH),
    ("db timeout", REASON_CODE_DB_TIMEOUT),
    ("db_timeout", REASON_CODE_DB_TIMEOUT),
    ("indicator", REASON_CODE_INDICATOR),
]


def map_exception_to_reason(exc: Exception | str | None) -> str:
    if exc is None:
        return REASON_CODE_UNKNOWN
    text = str(exc).lower()
    for pattern, code in _REASON_PATTERNS:
        if pattern in text:
            return code
    return REASON_CODE_UNKNOWN


def normalize_reason_code(code: str | None) -> str:
    if not code:
        return REASON_CODE_UNKNOWN
    return code.strip().upper()


def is_rate_limit_exception(exc: Exception | str | None) -> bool:
    if exc is None:
        return False
    text = str(exc).lower()
    return "rate limit" in text or "ratelimit" in text or "ddos" in text

__all__ = [
    "REASON_CODE_UNKNOWN",
    "REASON_CODE_INVALID_KEY",
    "REASON_CODE_INSUFFICIENT_BALANCE",
    "REASON_CODE_MIN_NOTIONAL",
    "REASON_CODE_RATE_LIMIT",
    "REASON_CODE_WEBSOCKET",
    "REASON_CODE_POSITION_MISMATCH",
    "REASON_CODE_DB_TIMEOUT",
    "REASON_CODE_INDICATOR",
    "map_exception_to_reason",
    "normalize_reason_code",
    "is_rate_limit_exception",
]
