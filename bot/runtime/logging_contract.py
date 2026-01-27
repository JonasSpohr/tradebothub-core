import os
import time
import uuid
from typing import Any, Dict

from bot.core.config import POLLING_TIER_MINIMUMS
from bot.core.logging import send_structured_event


class BotLogContext:
    def __init__(self):
        self.boot_id = str(uuid.uuid4())
        self.heartbeat_seq = 0

    def next_seq(self) -> int:
        self.heartbeat_seq += 1
        return self.heartbeat_seq


class RuntimeMetrics:
    def __init__(self):
        self.begin_tick()

    def begin_tick(self):
        self.loop_start = time.monotonic()
        self.loop_ms = 0
        self.sleep_ms = 0
        self.ohlcv_fetch_ms = 0
        self.indicators_ms = 0
        self.decision_ms = 0
        self.exchange_calls = 0
        self.db_writes = 0
        self.bars_returned = 0
        self.cache_hit_ohlcv = False

    def finish_loop(self) -> None:
        if self.loop_start:
            self.loop_ms = int((time.monotonic() - self.loop_start) * 1000)

    def set_sleep_ms(self, interval_ms: float) -> None:
        self.sleep_ms = int(max(0.0, interval_ms))

    def snapshot(self) -> Dict[str, Any]:
        return {
            "loop_ms": int(self.loop_ms),
            "sleep_ms": int(self.sleep_ms),
            "ohlcv_fetch_ms": int(self.ohlcv_fetch_ms),
            "indicators_ms": int(self.indicators_ms),
            "decision_ms": int(self.decision_ms),
            "exchange_calls": int(self.exchange_calls),
            "db_writes": int(self.db_writes),
            "bars_returned": int(self.bars_returned),
            "cache_hit_ohlcv": bool(self.cache_hit_ohlcv),
        }


runtime_metrics = RuntimeMetrics()


def record_exchange_call(count: int = 1) -> None:
    runtime_metrics.exchange_calls += int(count)


def record_db_write() -> None:
    runtime_metrics.db_writes += 1


def record_ohlcv_fetch(duration_ms: float, bars: int | None = None, cache_hit: bool = False) -> None:
    runtime_metrics.ohlcv_fetch_ms = int(duration_ms)
    if bars is not None:
        runtime_metrics.bars_returned = int(bars)
    if cache_hit:
        runtime_metrics.cache_hit_ohlcv = True


def record_indicator_time(duration_ms: float) -> None:
    runtime_metrics.indicators_ms = int(duration_ms)


def record_decision_time(duration_ms: float) -> None:
    runtime_metrics.decision_ms = int(duration_ms)


def get_poll_settings(execution_config: Dict[str, Any]) -> Dict[str, int]:
    tier = str(execution_config.get("polling_tier") or "standard").lower()
    tier_min = POLLING_TIER_MINIMUMS.get(tier, POLLING_TIER_MINIMUMS.get("standard", 60))
    expected = int(
        execution_config.get("poll_interval_seconds")
        or execution_config.get("poll_interval")
        or execution_config.get("effective_poll_seconds")
        or tier_min
    )
    return {
        "tier": tier,
        "poll_expected_s": expected,
        "tier_min_s": tier_min,
        "poll_effective_s": max(expected, tier_min),
    }


def build_identity(ctx, position_snapshot: Dict[str, Any]) -> Dict[str, Any]:
    identity = {
        "bot_id": ctx.id,
        "user_id": ctx.user_id,
        "bot_status": ctx.status or "unknown",
        "mode": ctx.mode,
        "exchange": ctx.exchange_ccxt_id,
        "symbol": ctx.market_symbol,
        "timeframe": ctx.execution_config.get("timeframe"),
        "strategy": ctx.strategy,
        "bot_version": ctx.bot_version,
        "subscription_status": ctx.subscription_status,
        "trading_enabled": bool(ctx.control_config.get("trading_enabled", True)),
        "kill_switch": bool(ctx.control_config.get("kill_switch", False)),
        "runtime_provider": ctx.runtime_provider or os.getenv("RUNTIME_PROVIDER"),
        "fly_region": ctx.fly_region or os.getenv("FLY_REGION"),
        "fly_machine_id": ctx.fly_machine_id or os.getenv("FLY_MACHINE_ID"),
        "in_position": bool(position_snapshot.get("in_position")),
        "position_id": position_snapshot.get("position_id"),
        "position_side": position_snapshot.get("position_side"),
    }
    return identity


def emit_bot_heartbeat(ctx, log_ctx: BotLogContext, position_snapshot: Dict[str, Any]) -> None:
    poll = get_poll_settings(ctx.execution_config)
    log_ctx.next_seq()
    attributes = {
        "eventType": "BotHeartbeat",
        **build_identity(ctx, position_snapshot),
        "poll_expected_s": poll["poll_expected_s"],
        "tier_min_s": poll["tier_min_s"],
        "poll_effective_s": poll["poll_effective_s"],
        "boot_id": log_ctx.boot_id,
        "heartbeat_seq": log_ctx.heartbeat_seq,
    }
    send_structured_event("BotHeartbeat", attributes, level="info", message="heartbeat")


def emit_bot_loop(ctx, log_ctx: BotLogContext, position_snapshot: Dict[str, Any]) -> None:
    poll = get_poll_settings(ctx.execution_config)
    attrs = {
        "eventType": "BotLoop",
        **build_identity(ctx, position_snapshot),
        "poll_effective_s": poll["poll_effective_s"],
    }
    attrs.update(runtime_metrics.snapshot())
    send_structured_event("BotLoop", attrs, level="info", message="loop")


def emit_bot_gate(ctx, log_ctx: BotLogContext, position_snapshot: Dict[str, Any], gate_reason: str) -> None:
    attrs = {
        "eventType": "BotGate",
        **build_identity(ctx, position_snapshot),
        "gate_reason": gate_reason,
    }
    send_structured_event("BotGate", attrs, level="warn", message="gate")


def emit_bot_trade(
    ctx,
    action: str,
    side: str,
    qty: float,
    price: float,
    position_id: str | None,
    exchange_order_id: str | None,
    trade_id: str | None = None,
    notional_usd: float | None = None,
    slippage_bps: float | None = None,
    realized_pnl: float | None = None,
) -> None:
    attrs = {
        "eventType": "BotTrade",
        **build_identity(ctx, {"in_position": bool(position_id), "position_id": position_id, "position_side": side}),
        "action": action,
        "side": side,
        "qty": float(qty),
        "price": float(price),
        "exchange_order_id": exchange_order_id,
        "position_id": position_id,
        "trade_id": trade_id,
    }
    if notional_usd is not None:
        attrs["notional_usd"] = float(notional_usd)
    if slippage_bps is not None:
        attrs["slippage_bps"] = float(slippage_bps)
    if realized_pnl is not None:
        attrs["realized_pnl"] = float(realized_pnl)
    send_structured_event("BotTrade", attrs, level="info", message="trade")


def emit_bot_error(
    ctx,
    error_class: str,
    error_code: str | None,
    error_stage: str,
    retry_attempt: int,
    backoff_s: float,
    is_fatal: bool,
    exc_msg: str | None = None,
    position_snapshot: Dict[str, Any] | None = None,
) -> None:
    snapshot = position_snapshot or {"in_position": False, "position_id": None, "position_side": None}
    attrs = {
        "eventType": "BotError",
        **build_identity(ctx, snapshot),
        "error_class": error_class,
        "error_code": error_code,
        "error_stage": error_stage,
        "retry_attempt": int(retry_attempt),
        "backoff_s": float(backoff_s),
        "is_fatal": bool(is_fatal),
    }
    if exc_msg:
        attrs["error_message"] = exc_msg
    send_structured_event("BotError", attrs, level="error", message="error")
