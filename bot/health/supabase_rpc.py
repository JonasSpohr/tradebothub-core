from __future__ import annotations
import random
import time
from typing import Any, Dict, Optional

import requests
from bot.core.logging import log

MAX_ATTEMPTS = 3
_RETRY_DELAYS = (0.25, 1.0, 3.0)
_TRANSIENT_STATUS_CODES = {502, 503, 504}
_TIMEOUT_SECONDS = 10


class SupabaseRpcClient:
    def __init__(self, url: str, service_role_key: str, session: Optional[requests.Session] = None):
        self._endpoint = f"{url.rstrip('/')}/rest/v1/rpc/upsert_bot_health_evidence"
        self._session = session or requests.Session()
        self._headers = {
            "apikey": service_role_key,
            "Authorization": f"Bearer {service_role_key}",
            "Content-Type": "application/json",
        }

    def upsert_bot_health_evidence(self, bot_id: str, patch: Dict[str, Any]) -> tuple[bool, float]:
        payload = {"p_bot_id": bot_id, "p_patch": patch}
        for attempt, base_delay in enumerate(_RETRY_DELAYS, 1):
            start = time.monotonic()
            try:
                resp = self._session.post(self._endpoint, headers=self._headers, json=payload, timeout=_TIMEOUT_SECONDS)
                if 200 <= resp.status_code < 300:
                    elapsed_ms = (time.monotonic() - start) * 1000
                    return True, elapsed_ms
                self._log_attempt(attempt, resp.status_code, None)
                if resp.status_code in _TRANSIENT_STATUS_CODES and attempt < MAX_ATTEMPTS:
                    self._sleep_with_jitter(base_delay)
                    continue
                return False, (time.monotonic() - start) * 1000
            except requests.RequestException as exc:
                self._log_attempt(attempt, None, exc)
                if attempt >= MAX_ATTEMPTS:
                    return False, (time.monotonic() - start) * 1000
                self._sleep_with_jitter(base_delay)
        return False, 0.0

    def _sleep_with_jitter(self, delay: float) -> None:
        jitter = random.uniform(0.8, 1.2)
        time.sleep(delay * jitter)

    def _log_attempt(self, attempt: int, status_code: Optional[int], exc: Optional[Exception]) -> None:
        status = status_code if status_code is not None else "n/a"
        error_cls = type(exc).__name__ if exc else "HTTPError"
        log(f"[health rpc] attempt={attempt} status={status} error={error_cls}", level="WARN")


__all__ = ["SupabaseRpcClient"]
