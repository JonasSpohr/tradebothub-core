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
from bot.core.config import normalize_configs, POLLING_TIER_MINIMUMS

from bot.health.reporter import get_reporter
from bot.health.types import is_rate_limit_exception
from bot.services.exchange_sync import ExchangeSyncError
CONTROL_REFRESH_SECONDS = 60
CONTROL_REFRESH_POLLS = 20

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

def _resolve_polling(ctx: BotContext) -> tuple[int, int, int, str, int]:
    """
    Derive effective polling config with local clamps.
    Returns (effective_poll, poll_min, poll_jitter, tier, requested_poll).
    """
    ec = ctx.execution_config or {}
    tier = str(ec.get("polling_tier", "standard")).lower()
    tier_min = POLLING_TIER_MINIMUMS.get(tier, MIN_POLL_SECONDS)
    try:
        poll_min = int(ec.get("poll_min_seconds", tier_min))
    except Exception:
        poll_min = tier_min
    poll_min = max(poll_min, tier_min, MIN_POLL_SECONDS)
    try:
        requested = int(ec.get("poll_interval_seconds", ec.get("poll_interval", poll_min)))
    except Exception:
        requested = poll_min
    try:
        poll_jitter = int(ec.get("poll_jitter_seconds", 10))
    except Exception:
        poll_jitter = 10
    poll_jitter = max(poll_jitter, 0)
    effective_poll = max(requested, poll_min)
    ctx.execution_config["effective_poll_seconds"] = effective_poll
    return effective_poll, poll_min, poll_jitter, tier, requested

def run_loop(ctx: BotContext):
    strategy = getattr(ctx, "_strategy", None) or get_strategy(ctx.strategy)
    poll, poll_min, poll_jitter, poll_tier, requested_poll = _resolve_polling(ctx)
    reporter = get_reporter()
    exchange_sync = getattr(ctx, "_exchange_sync_service", None)
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
    scheduler = JitterScheduler(base_seconds=poll, jitter_seconds=poll_jitter, min_seconds=poll_min)
    scheduler.startup_stagger()
    last_control_refresh = 0.0
    control_refresh_polls = 0
    paused_reason = None

    log(
        f"=== RUN {ctx.name} strategy={ctx.strategy} symbol={ctx.market_symbol} tf={ctx.execution_config.get('timeframe')} poll={poll}s ===",
        level="INFO",
    )
    log(
        f"[polling] tier={poll_tier} requested={requested_poll}s effective={poll}s min={poll_min}s jitter=+/-{poll_jitter}s",
        level="INFO",
    )
    reporter.set_tier(poll_tier)

    last_state = None
    while True:
        now = time.monotonic()
        try:
            if exchange_sync:
                try:
                    exchange_sync.maybe_sync()
                except ExchangeSyncError as exc:
                    log(f"[exchange sync] failing fast: {exc}", level="ERROR")
                    raise
            reporter.set_in_position(POSITION_STATE.in_position)
            reporter.record_strategy_tick_ok()
            reporter.record_decision()
            tick += 1
            control_refresh_polls += 1

            # periodic control refresh (control + subscription only)
            if (now - last_control_refresh >= CONTROL_REFRESH_SECONDS) or control_refresh_polls >= CONTROL_REFRESH_POLLS:
                try:
                    ctrl = refresh_controls(ctx.id)
                    if ctrl:
                        _, _, _, cc = normalize_configs(None, None, None, ctrl.get("control_config") or ctx.control_config)
                        ctx.control_config = cc
                        ctx.subscription_status = ctrl.get("subscription_status", ctx.subscription_status)
                except Exception as e:
                    log(f"[control_refresh] failed: {e}", level="WARN")
                last_control_refresh = now
                control_refresh_polls = 0
                if ctx.subscription_status != "active":
                    write_event(ctx.id, ctx.user_id, "stopped_payment", "Subscription inactive; stopping bot")
                    log("[control_refresh] subscription inactive; stopping bot loop", level="WARN")
                    return

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
            poll, poll_min, poll_jitter, poll_tier, requested_poll = _resolve_polling(ctx)
            reporter.set_tier(poll_tier)
            interval = scheduler.next_interval(
                base_override=poll,
                jitter_override=poll_jitter,
                min_override=poll_min,
            )
            log(f"[poll] finished state={state.value}; interval={interval:.2f}s base={poll}s min={poll_min}s jitter=+/-{poll_jitter}s req={requested_poll}s", level="DEBUG")
            scheduler.sleep_for(interval, now)
        except ExchangeSyncError:
            raise
        except Exception as e:
            consec_errors += 1
            write_event(ctx.id, ctx.user_id, "error", str(e))
            log(f"ERROR: {e} (consecutive={consec_errors})", level="ERROR")
            reporter.record_strategy_tick_fail()
            reporter.flush_now("loop_error")
            if is_rate_limit_exception(e):
                reporter.record_rate_limit_hit()
            #notify(ctx.user_id, ctx.id, "error", "Bot error", body=str(e), severity="critical")

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
