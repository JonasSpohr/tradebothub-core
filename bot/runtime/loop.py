import time
from enum import Enum
from typing import Optional
from bot.core.logging import log
from bot.core.safety import MAX_CONSECUTIVE_ERRORS, ERROR_BACKOFF_SECONDS, MIN_POLL_SECONDS
from bot.infra.db import write_event, notify, touch_heartbeat, refresh_controls
from bot.core.types import BotContext
from bot.strategies import get_strategy
from bot.trading.position import manage_open_position, try_open_position, STATE as POSITION_STATE, _exchange
from bot.infra.monitoring import ping_healthchecks
from bot.infra.healthcheck import ping_healthcheck, fail_healthcheck
from bot.infra.exchange import fetch_ohlcv_df

CONTROL_REFRESH_SECONDS = 60

class BotState(Enum):
    BOOTSTRAP = "bootstrap"
    WARMUP = "warmup"
    PAUSED = "paused"
    FLAT = "flat"
    IN_POSITION = "in_position"
    ERROR = "error"

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

def run_loop(ctx: BotContext):
    strategy = get_strategy(ctx.strategy)
    poll = max(MIN_POLL_SECONDS, int(ctx.execution_config["poll_interval"]))
    consec_errors = 0
    tick = 0
    state = BotState.BOOTSTRAP
    next_retry = None
    next_tick = time.monotonic()
    last_control_refresh = 0.0
    warmup_emitted = False
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

            # periodic control refresh
            if now - last_control_refresh >= CONTROL_REFRESH_SECONDS:
                try:
                    ctrl = refresh_controls(ctx.id)
                    ctx.control_config = ctrl.get("control_config", ctx.control_config)
                    ctx.subscription_status = ctrl.get("subscription_status", ctx.subscription_status)
                except Exception as e:
                    log(f"[control_refresh] failed: {e}", level="WARN")
                last_control_refresh = now

            pause_reason = _pause_reason(ctx)

            if state == BotState.ERROR:
                if next_retry is None or now >= next_retry:
                    log("[error] retrying bootstrap", level="WARN")
                    state = BotState.BOOTSTRAP
                    consec_errors = 0
                else:
                    time.sleep(ERROR_BACKOFF_SECONDS)
                    continue

            if pause_reason and state not in (BotState.BOOTSTRAP, BotState.ERROR):
                if state != BotState.PAUSED:
                    paused_reason = pause_reason
                    write_event(ctx.id, ctx.user_id, "paused", pause_reason)
                    notify(ctx.user_id, ctx.id, "paused", "Bot paused", body=pause_reason, severity="warning")
                    log(f"[pause] entering paused due to {pause_reason}", level="WARN")
                state = BotState.PAUSED

            if state == BotState.BOOTSTRAP:
                write_event(ctx.id, ctx.user_id, "started", f"strategy={ctx.strategy} tf={ctx.execution_config['timeframe']}")
                pause_reason = _pause_reason(ctx)
                if pause_reason:
                    state = BotState.PAUSED
                else:
                    min_bars = int(ctx.strategy_config.get("min_bars", 500))
                    ready = _has_min_bars(ctx, strategy)
                    if POSITION_STATE.in_position:
                        state = BotState.IN_POSITION
                    else:
                        state = BotState.FLAT if ready else BotState.WARMUP
                    if not ready and not warmup_emitted:
                        write_event(ctx.id, ctx.user_id, "warmup", f"min_bars={min_bars}")
                    log("[warmup] waiting for enough bars", level="INFO")
                    warmup_emitted = True
                touch_heartbeat(ctx.id, ctx.user_id)
            elif state == BotState.WARMUP:
                if pause_reason:
                    state = BotState.PAUSED
                else:
                    ready = _has_min_bars(ctx, strategy)
                    if ready:
                        state = BotState.IN_POSITION if POSITION_STATE.in_position else BotState.FLAT
                        log("[warmup] complete; moving on", level="INFO")
                touch_heartbeat(ctx.id, ctx.user_id)
            elif state == BotState.PAUSED:
                if not pause_reason:
                    write_event(ctx.id, ctx.user_id, "resumed", paused_reason or "controls_ok")
                    notify(ctx.user_id, ctx.id, "resumed", "Bot resumed", severity="info")
                    state = BotState.IN_POSITION if POSITION_STATE.in_position else BotState.FLAT
                    log("[pause] exiting paused", level="INFO")
                else:
                    if POSITION_STATE.in_position:
                        log("[paused] managing open position only (no new entries)", level="INFO")
                        manage_open_position(ctx, strategy)
                touch_heartbeat(ctx.id, ctx.user_id)
            elif state == BotState.FLAT:
                log("[state] FLAT: evaluating entries on new candles only", level="DEBUG")
                try_open_position(ctx, strategy)
                touch_heartbeat(ctx.id, ctx.user_id)
                if POSITION_STATE.in_position:
                    state = BotState.IN_POSITION
            elif state == BotState.IN_POSITION:
                log("[state] IN_POSITION: managing open position and exits", level="DEBUG")
                manage_open_position(ctx, strategy)
                touch_heartbeat(ctx.id, ctx.user_id)
                if not POSITION_STATE.in_position:
                    state = BotState.FLAT
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

            # keep steady cadence: schedule next tick and sleep remaining time if positive
            next_tick += poll
            sleep_for = max(0, next_tick - time.monotonic())
            log(f"[poll] finished state={state.value}; sleeping {sleep_for:.2f}s", level="DEBUG")
            time.sleep(sleep_for)
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
                try:
                    from bot.infra.db import notify_support
                    notify_support(
                        ctx.user_id,
                        ctx.id,
                        title="Bot stopped after consecutive errors",
                        body=str(e),
                        severity="critical",
                    )
                except Exception:
                    pass
                # Signal healthcheck failure
                fail_healthcheck(getattr(ctx, "_hc_ping_url", None))
                log("Too many consecutive errors; exiting.", level="ERROR")
                return

            state = BotState.ERROR
            next_retry = time.monotonic() + ERROR_BACKOFF_SECONDS
