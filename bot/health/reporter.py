from __future__ import annotations
import os
import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from bot.core.logging import log
from bot.health.config import (
    CRITICAL_DELAY_SECONDS,
    DEBOUNCE_SECONDS,
    DEFAULT_TIER,
    get_flush_interval,
)
from bot.health.supabase_rpc import SupabaseRpcClient
from bot.health.types import normalize_reason_code
from bot.health.window import HealthWindow

_GLOBAL_REPORTER: Optional["HealthReporter"] = None
_FLUSH_THREAD_STARTED = False


class HealthReporter:
    def __init__(self, bot_id: str, rpc_client: SupabaseRpcClient, tier: str = DEFAULT_TIER, in_position: bool = False):
        self.bot_id = bot_id
        self._rpc_client = rpc_client
        self._tier = tier.lower()
        self._in_position = in_position
        self._window = HealthWindow()
        self._pending_patch: Dict[str, Any] = {}
        self._lock = threading.Lock()
        self._last_flush_ts = 0.0
        self._scheduled_flush_ts = 0.0
        self._scheduled_reason: Optional[str] = None

    @classmethod
    def from_env(cls, bot_id: str, tier: str | None, in_position: bool = False) -> "HealthReporter":
        url = os.environ["SUPABASE_URL"]
        key = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
        rpc = SupabaseRpcClient(url, key)
        return cls(bot_id, rpc, tier or DEFAULT_TIER, in_position=in_position)

    def set_tier(self, tier: str | None) -> None:
        with self._lock:
            self._tier = (tier or DEFAULT_TIER).lower()

    def set_in_position(self, in_position: bool) -> None:
        with self._lock:
            self._in_position = in_position

    def mark_auth_ok(self) -> None:
        self._update_patch(
            {
                "exchange_auth_ok": True,
                "last_auth_ok_at": self._now_iso(),
            }
        )

    def mark_auth_fail(self, code: str | None = None) -> None:
        self._update_patch(
            {
                "exchange_auth_ok": False,
                "last_auth_fail_at": self._now_iso(),
                "last_auth_error_code": normalize_reason_code(code),
            }
        )
        self.flush_now("auth_fail")

    def record_rate_limit_hit(self) -> None:
        self._window.inc("rate_limit_hit")

    def record_candle_lag(self, lag_seconds: int) -> None:
        self._update_patch(
            {
                "market_data_ok": True,
                "candle_lag_seconds": max(0, int(lag_seconds)),
            }
        )

    def record_stream_disconnect(self) -> None:
        self._window.inc("stream_disconnect")
        self._update_patch({"market_data_ok": False})
        if self._window.count15m("stream_disconnect") >= 2:
            self.flush_now("stream_disconnect")

    def record_candle_gap(self) -> None:
        self._window.inc("candle_gap")
        self._update_patch({"market_data_ok": False})
        if self._in_position and self._window.count15m("candle_gap") >= 1:
            self.flush_now("candle_gap")

    def record_strategy_tick_ok(self) -> None:
        self._update_patch(
            {
                "strategy_ok": True,
                "last_strategy_tick_at": self._now_iso(),
            }
        )

    def record_strategy_tick_fail(self) -> None:
        self._update_patch(
            {
                "strategy_ok": False,
                "last_strategy_tick_at": self._now_iso(),
            }
        )

    def record_indicator_error(self, reason_code: str | None = None) -> None:
        self._window.inc("indicator_error")
        self._update_patch(
            {
                "strategy_ok": False,
                "last_strategy_tick_at": self._now_iso(),
                "last_indicator_error_code": normalize_reason_code(reason_code),
            }
        )
        if self._window.count15m("indicator_error") >= 3:
            self.flush_now("indicator_error_spike")

    def record_decision(self) -> None:
        self._window.inc("decision")

    def record_order_submit(self) -> None:
        self._update_patch({"order_flow_ok": True, "last_order_submit_at": self._now_iso()})
        self.flush_now("order_submit")

    def record_order_ack(self, latency_ms: int) -> None:
        self._update_patch(
            {
                "order_flow_ok": True,
                "last_order_ack_at": self._now_iso(),
                "order_ack_latency_ms": max(0, int(latency_ms)),
            }
        )
        self.flush_now("order_ack")

    def record_order_reject(self, reason: str) -> None:
        mapped = normalize_reason_code(reason)
        self._window.inc("order_reject")
        self._update_patch(
            {
                "order_flow_ok": False,
                "last_order_reject_reason": mapped,
                "last_order_reject_at": self._now_iso(),
            }
        )
        self.flush_now("order_reject")

    def record_position_sync(self, diff: float) -> None:
        safe_diff = max(0.0, float(diff))
        self._update_patch(
            {
                "position_ok": safe_diff <= 0.0,
                "last_position_sync_at": self._now_iso(),
                "position_sync_diff": safe_diff,
            }
        )
        if safe_diff > 0.0:
            self.flush_now("position_diff")

    def record_trailing_update(self) -> None:
        self._update_patch({"last_trailing_update_at": self._now_iso()})
        self.flush_now("trailing_update")

    def record_db_ok(self) -> None:
        self._update_patch(
            {
                "db_ok": True,
                "last_db_ok_at": self._now_iso(),
            }
        )

    def record_db_error(self) -> None:
        self._window.inc("db_error")
        self._update_patch({"db_ok": False})
        self.flush_now("db_error")

    def maybe_flush(self) -> None:
        token = self._claim_flush("scheduled", force=False)
        if token:
            reason, patch = token
            self._execute_flush(reason, patch)

    def flush_now(self, reason: str) -> None:
        token = self._claim_flush(reason, force=True)
        if token:
            reason, patch = token
            self._execute_flush(reason, patch)
            return
        now = time.monotonic()
        with self._lock:
            next_due = max(
                self._last_flush_ts + DEBOUNCE_SECONDS,
                now + CRITICAL_DELAY_SECONDS,
            )
            self._scheduled_flush_ts = max(self._scheduled_flush_ts, next_due)
            self._scheduled_reason = reason

    def _claim_flush(self, reason: str, force: bool) -> Optional[tuple[str, Dict[str, Any]]]:
        now = time.monotonic()
        with self._lock:
            interval = get_flush_interval(self._tier, self._in_position)
            due = now - self._last_flush_ts
            if self._scheduled_flush_ts and now < self._scheduled_flush_ts and not force:
                return None
            if self._scheduled_flush_ts and now >= self._scheduled_flush_ts:
                reason = self._scheduled_reason or reason
                self._scheduled_flush_ts = 0.0
                self._scheduled_reason = None
            if force:
                if due < DEBOUNCE_SECONDS:
                    return None
            else:
                if due < max(DEBOUNCE_SECONDS, interval):
                    return None
            patch = self._build_patch_snapshot()
            return reason, patch

    def _build_patch_snapshot(self) -> Dict[str, Any]:
        snapshot = {}
        snapshot.update(self._pending_patch)
        snapshot.update(self._window.snapshot())
        return snapshot

    def _execute_flush(self, reason: str, patch: Dict[str, Any]) -> None:
        success, elapsed_ms = self._rpc_client.upsert_bot_health_evidence(self.bot_id, patch)
        log(
            f"[health flush] bot={self.bot_id} tier={self._tier} in_position={self._in_position} reason={reason} keys={len(patch)} rpc_ms={elapsed_ms:.0f} success={success}",
            level="INFO",
        )
        with self._lock:
            if success:
                self._pending_patch.clear()
                self._last_flush_ts = time.monotonic()
            else:
                pass

    def _update_patch(self, fields: Dict[str, Any]) -> None:
        clean = {k: v for k, v in fields.items() if v is not None}
        with self._lock:
            self._pending_patch.update(clean)

    def _now_iso(self) -> str:
        return datetime.now(timezone.utc).isoformat()



def init_reporter(bot_id: str, tier: str | None = None, in_position: bool = False) -> HealthReporter:
    global _GLOBAL_REPORTER
    if _GLOBAL_REPORTER is None:
        _GLOBAL_REPORTER = HealthReporter.from_env(bot_id, tier, in_position=in_position)
    else:
        _GLOBAL_REPORTER.set_tier(tier)
        _GLOBAL_REPORTER.set_in_position(in_position)
    return _GLOBAL_REPORTER


def get_reporter() -> HealthReporter:
    if _GLOBAL_REPORTER is None:
        raise RuntimeError("Health reporter not initialized")
    return _GLOBAL_REPORTER


def get_reporter_optional() -> Optional[HealthReporter]:
    return _GLOBAL_REPORTER


def start_health_flush_loop(reporter: HealthReporter) -> None:
    global _FLUSH_THREAD_STARTED
    if _FLUSH_THREAD_STARTED:
        return
    _FLUSH_THREAD_STARTED = True

    def _loop() -> None:
        while True:
            try:
                reporter.maybe_flush()
            except Exception as exc:
                log(f"[health flush loop] {type(exc).__name__}: {exc}", level="WARN")
            time.sleep(5)

    thread = threading.Thread(target=_loop, daemon=True, name="health-flush-loop")
    thread.start()


__all__ = [
    "HealthReporter",
    "get_reporter",
    "get_reporter_optional",
    "init_reporter",
    "start_health_flush_loop",
]
