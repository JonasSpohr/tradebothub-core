import time
from enum import Enum
from typing import Optional
from bot.core.logging import log
from bot.core.safety import MAX_CONSECUTIVE_ERRORS, ERROR_BACKOFF_SECONDS, MIN_POLL_SECONDS, MAX_LEVERAGE, MAX_ALLOCATION_FRAC
from bot.infra.db import write_event, notify, touch_heartbeat, refresh_controls
from bot.core.types import BotContext
from bot.strategies import get_strategy
from bot.trading.position import manage_open_position, try_open_position, STATE as POSITION_STATE, _exchange
from bot.infra.monitoring import ping_healthchecks
from bot.infra.healthcheck import ping_healthcheck, fail_healthcheck
from bot.infra.exchange import fetch_ohlcv_df
from bot.runtime.scheduler import JitterScheduler
from bot.core.config import normalize_configs

CONTROL_REFRESH_SECONDS = 60

class BotState(Enum):
    INIT = "init"
    IDLE = "idle"
    WAITING_FOR_ENTRY = "waiting_for_entry"
    IN_POSITION = "in_position"
    COOLDOWN = "cooldown"
    HALT = "halt"

def _pause_reason(ctx: BotContext) -> Optional[str]:
    cc = ctx.control_config or {}
    if ctx.subscription_status != "active":
        return "subscription_inactive"
    if cc.get("kill_switch"):
        return "kill_switch"
    if not cc.get("trading_enabled", False):
        return "trading_disabled"
    if cc.get("pause_requested"):
        return "pause_requested"
    return None

def _has_min_bars(ctx: BotContext, strategy) -> bool:
    try:
        ex = _exchange(ctx)
        df = fetch_ohlcv_df(ex, ctx.market_symbol, ctx.execution_config["timeframe"], ctx.execution_config["lookback_bars"])
        df = strategy.prepare(df, ctx.strategy_config)
        min_bars = int(ctx.strategy_config.get("min_bars", 500))
        return len(df) >= min_bars
    except Exception as e:
        log(f"[warmup] failed to fetch bars: {e}", level="WARN")
        return False

def _get_poll_seconds(ctx: BotContext) -> int:
    # pull from execution_config.poll_interval
    raw = ctx.execution_config.get("poll_interval", 300)
    try:
        val = int(raw)
    except Exception:
        val = 300
    return max(val, MIN_POLL_SECONDS)

def run_loop(ctx: BotContext):
    strategy = get_strategy(ctx.strategy)
    poll = _get_poll_seconds(ctx)
    # Clamps for safety
    if int(ctx.execution_config.get("lookback_bars", 0)) > 2000:
        ctx.execution_config["lookback_bars"] = 2000
        write_event(ctx.id, ctx.user_id, "config_clamped", "lookback_bars clamped to 2000")
    if float(ctx.risk_config.get("leverage", 0)) > MAX_LEVERAGE:
        ctx.risk_config["leverage"] = MAX_LEVERAGE
        write_event(ctx.id, ctx.user_id, "config_clamped", f"leverage clamped to {MAX_LEVERAGE}")
    if float(ctx.risk_config.get("allocation_frac", 0)) > MAX_ALLOCATION_FRAC:
        ctx.risk_config["allocation_frac"] = MAX_ALLOCATION_FRAC
        write_event(ctx.id, ctx.user_id, "config_clamped", f"allocation_frac clamped to {MAX_ALLOCATION_FRAC}")

    consec_errors = 0
    tick = 0
    state = BotState.INIT
    scheduler = JitterScheduler(base_seconds=poll, jitter_seconds=10)
    scheduler.startup_stagger()
    last_control_refresh = 0.0
    paused_reason = None

    log(
        f"=== RUN {ctx.name} strategy={ctx.strategy} symbol={ctx.market_symbol} tf={ctx.execution_config.get('timeframe')} poll={poll}s ===",
        level="INFO",
    )

    last_state = None
    while True:
        now = time.monotonic()
        try:
            tick += 1

            # periodic control refresh (includes execution_config so poll changes apply next cycle)
            if now - last_control_refresh >= CONTROL_REFRESH_SECONDS:
                try:
                    ctrl = refresh_controls(ctx.id)
                    ctx.control_config = ctrl.get("control_config", ctx.control_config)
                    ctx.subscription_status = ctrl.get("subscription_status", ctx.subscription_status)
                    if ctrl.get("execution_config"):
                        # re-normalize execution config to keep clamps
                        _, _, ec, _ = normalize_configs(None, None, ctrl.get("execution_config") or {}, None)
                        ctx.execution_config = ec
                except Exception as e:
                    log(f"[control_refresh] failed: {e}", level="WARN")
                last_control_refresh = now

            pause_reason = _pause_reason(ctx)

            if state == BotState.HALT:
                log("[halt] halting loop", level="ERROR")
                return

            if pause_reason or not ctx.control_config.get("trading_enabled", False):
                if state != BotState.IDLE:
                    paused_reason = pause_reason or "trading_disabled"
                    write_event(ctx.id, ctx.user_id, "paused", paused_reason)
                    log(f"[pause] entering idle due to {paused_reason}", level="WARN")
                state = BotState.IDLE

            if state == BotState.INIT:
                write_event(ctx.id, ctx.user_id, "started", f"strategy={ctx.strategy} tf={ctx.execution_config['timeframe']}")
                if pause_reason or not ctx.control_config.get("trading_enabled", False):
                    state = BotState.IDLE
                else:
                    state = BotState.IN_POSITION if POSITION_STATE.in_position else BotState.WAITING_FOR_ENTRY
                touch_heartbeat(ctx.id, ctx.user_id)
            elif state == BotState.IDLE:
                if POSITION_STATE.in_position:
                    log("[idle] managing open position only", level="INFO")
                    manage_open_position(ctx, strategy)
                touch_heartbeat(ctx.id, ctx.user_id)
                if not pause_reason and ctx.control_config.get("trading_enabled", False):
                    state = BotState.IN_POSITION if POSITION_STATE.in_position else BotState.WAITING_FOR_ENTRY
            elif state == BotState.WAITING_FOR_ENTRY:
                log("[state] WAITING_FOR_ENTRY: evaluating entries on new candles only", level="DEBUG")
                try_open_position(ctx, strategy)
                touch_heartbeat(ctx.id, ctx.user_id)
                if POSITION_STATE.in_position:
                    state = BotState.IN_POSITION
            elif state == BotState.IN_POSITION:
                log("[state] IN_POSITION: managing open position and exits", level="DEBUG")
                manage_open_position(ctx, strategy)
                touch_heartbeat(ctx.id, ctx.user_id)
                if not POSITION_STATE.in_position:
                    state = BotState.COOLDOWN
            elif state == BotState.COOLDOWN:
                log("[state] COOLDOWN: waiting one tick before re-entry", level="DEBUG")
                touch_heartbeat(ctx.id, ctx.user_id)
                state = BotState.WAITING_FOR_ENTRY
            else:
                touch_heartbeat(ctx.id, ctx.user_id)

            # healthcheck ping
            ping_healthchecks()
            # healthchecks.io ping
            ping_healthcheck(getattr(ctx, "_hc_ping_url", None))

            consec_errors = 0

            if state != last_state:
                log(f"[state] transition {last_state.value if last_state else 'none'} -> {state.value}", level="INFO")
                last_state = state

            # keep steady cadence with jitter, recomputing poll seconds each cycle to pick up config changes
            poll = _get_poll_seconds(ctx)
            interval = scheduler.next_interval(base_override=poll)
            log(f"[poll] finished state={state.value}; interval={interval:.2f}s base={poll}s", level="DEBUG")
            scheduler.sleep_for(interval, now)
        except Exception as e:
            consec_errors += 1
            write_event(ctx.id, ctx.user_id, "error", str(e))
            log(f"ERROR: {e} (consecutive={consec_errors})", level="ERROR")
            notify(ctx.user_id, ctx.id, "error", "Bot error", body=str(e), severity="critical")

            if consec_errors >= MAX_CONSECUTIVE_ERRORS:
                write_event(ctx.id, ctx.user_id, "stopped", "Too many consecutive errors")
                notify(
                    ctx.user_id,
                    ctx.id,
                    "bot_stopped",
                    "Bot stopped",
                    body="Too many consecutive errors",
                    severity="critical",
                )
                # Signal healthcheck failure
                fail_healthcheck(getattr(ctx, "_hc_ping_url", None))
                log("Too many consecutive errors; exiting.", level="ERROR")
                return

            state = BotState.HALT
