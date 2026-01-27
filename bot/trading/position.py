import time
from datetime import datetime, timezone

from bot.core.logging import log
from bot.core.timeutil import utcnow_iso
from bot.health.reporter import get_reporter_optional
from bot.health.types import map_exception_to_reason
from bot.infra.crypto import decrypt
from bot.infra.exchange import create_exchange, fetch_ohlcv_df, fetch_last_price, fetch_quote_balance
from bot.infra.db import upsert_state, update_trade_status
from bot.runtime.logging_contract import (
    record_decision_time,
    record_exchange_call,
    record_indicator_time,
    record_ohlcv_fetch,
)
from bot.trading.orders import get_exchange_order_id, send_order
from bot.trading.sizing import compute_notional, compute_qty
from bot.trading.exits import atr_exit_reason
from bot.trading.pyramiding import maybe_pyramid, pyramid_add_notional
from bot.trading.journal import event, on_entry, on_exit, on_pyramid
from bot.state import PositionState

STATE = PositionState(week_trade_counts={})

def _exchange(ctx):
    if getattr(ctx, "_ex", None):
        return ctx._ex

    api_key = decrypt(ctx.api_key_encrypted)
    api_secret = decrypt(ctx.api_secret_encrypted)
    api_password = decrypt(ctx.api_password_encrypted)
    api_uid = decrypt(ctx.api_uid_encrypted)

    if not api_key or not api_secret:
        raise RuntimeError("Missing API key/secret after decrypt")

    ex = create_exchange(ctx.exchange_ccxt_id, api_key, api_secret, api_password, api_uid)
    ctx._ex = ex
    return ex

def _dry_run(ctx) -> bool:
    return bool(ctx.dry_run) or (ctx.mode == "paper")

def manage_open_position(ctx, strategy):
    global STATE
    if not STATE.in_position:
        return

    ex = _exchange(ctx)
    reporter = get_reporter_optional()
    symbol = ctx.market_symbol
    try:
        record_exchange_call()
        price = fetch_last_price(ex, symbol)
    except Exception as exc:
        _maybe_record_stream_disconnect(reporter, exc)
        raise

    try:
        start = time.monotonic()
        record_exchange_call()
        df = fetch_ohlcv_df(ex, symbol, ctx.execution_config["timeframe"], ctx.execution_config["lookback_bars"])
        duration = (time.monotonic() - start) * 1000
        record_ohlcv_fetch(duration, len(df))
    except Exception as exc:
        _maybe_record_stream_disconnect(reporter, exc)
        raise
    try:
        start = time.monotonic()
        df = strategy.prepare(df, ctx.strategy_config)
        duration = (time.monotonic() - start) * 1000
        record_indicator_time(duration)
    except Exception as exc:
        _handle_indicator_exception(exc, reporter)
        raise
    row = df.iloc[-1]
    atr = float(row.get("atr") or 0.0)

    unreal = (price - STATE.entry_price) * STATE.qty * (1 if STATE.direction == "long" else -1)
    STATE.max_unrealized_pnl = max(STATE.max_unrealized_pnl, unreal)
    STATE.min_unrealized_pnl = min(STATE.min_unrealized_pnl, unreal)
    STATE.last_price = price
    STATE.unrealized_pnl = unreal
    STATE.atr = atr

    sl_mult = float(ctx.strategy_config.get("sl_atr_mult", 1.5))
    tp_mult = float(ctx.strategy_config.get("tp_atr_mult", 3.5))
    trail_mult = float(ctx.strategy_config.get("trail_atr_mult", 1.5))
    trail_start_r = float(ctx.strategy_config.get("trail_start_r", 1.0))

    if STATE.direction == "long":
        sl = sl_mult * atr
        tp = tp_mult * atr
        STATE.stop_price = STATE.entry_price - sl
        STATE.take_profit_price = STATE.entry_price + tp
        STATE.peak_price = max(STATE.peak_price, price)
        STATE.trailing_active = unreal >= trail_start_r * sl
        if STATE.trailing_active:
            prev_trailing_stop = STATE.trailing_stop_price
            STATE.trailing_stop_price = STATE.peak_price - trail_mult * atr
            if reporter and STATE.trailing_stop_price != prev_trailing_stop:
                reporter.record_trailing_update()
    elif STATE.direction == "short":
        sl = sl_mult * atr
        tp = tp_mult * atr
        STATE.stop_price = STATE.entry_price + sl
        STATE.take_profit_price = STATE.entry_price - tp
        STATE.low_price = min(STATE.low_price, price)
        STATE.trailing_active = unreal >= trail_start_r * sl
        if STATE.trailing_active:
            prev_trailing_stop = STATE.trailing_stop_price
            STATE.trailing_stop_price = STATE.low_price + trail_mult * atr
            if reporter and STATE.trailing_stop_price != prev_trailing_stop:
                reporter.record_trailing_update()

    reason = atr_exit_reason(STATE, price, atr, ctx.strategy_config)
    if reason:
        pnl = (price - STATE.entry_price) * STATE.qty * (1 if STATE.direction == "long" else -1)
        close_side = "sell" if STATE.direction == "long" else "buy"

        expected = float(row["close"])
        max_slip = int(ctx.execution_config.get("max_slippage_bps", 20))
        order, client_order_id = send_order(
            ctx,
            ex,
            symbol,
            close_side,
            STATE.qty,
            _dry_run(ctx),
            expected_price=expected,
            max_slippage_bps=max_slip,
        )
        exit_order_id = get_exchange_order_id(order)

        exit_time = utcnow_iso()
        on_exit(
            ctx,
            STATE.position_id,
            STATE.direction,
            price,
            exit_time,
            STATE.qty,
            pnl,
            reason,
            exit_exchange_order_id=exit_order_id,
            exit_client_order_id=client_order_id,
            payload=order,
        )

        STATE.cumulative_pnl += pnl
        STATE.last_exit_time = exit_time

        keep_week = STATE.week_trade_counts
        keep_candle = STATE.last_candle_time
        keep_pnl = STATE.cumulative_pnl
        keep_exit = STATE.last_exit_time

        STATE = PositionState(
            in_position=False,
            week_trade_counts=keep_week,
            last_candle_time=keep_candle,
            cumulative_pnl=keep_pnl,
            last_exit_time=keep_exit,
        )
        STATE.last_manage_time = exit_time
        upsert_state(ctx.id, ctx.user_id, STATE.to_dict())
        if reporter:
            reporter.set_in_position(False)
        return

    move = (price - STATE.entry_price) / STATE.entry_price if STATE.direction == "long" else (STATE.entry_price - price) / STATE.entry_price
    while maybe_pyramid(ctx.strategy_config, move, STATE.added_levels):
        add_notional = pyramid_add_notional(STATE.base_notional, ctx.strategy_config)
        add_qty = compute_qty(add_notional, price)
        side = "buy" if STATE.direction == "long" else "sell"

        expected = float(row["close"])
        max_slip = int(ctx.execution_config.get("max_slippage_bps", 20))
        send_order(
            ctx,
            ex,
            symbol,
            side,
            add_qty,
            _dry_run(ctx),
            expected_price=expected,
            max_slippage_bps=max_slip,
        )

        STATE.qty += add_qty
        STATE.added_levels += 1

        on_pyramid(ctx, STATE.position_id, STATE.direction, price, add_qty, executed_at=utcnow_iso())
        event(ctx, "trade", f"PYRAMID {STATE.direction} level={STATE.added_levels} add_qty={add_qty:.6f}")

    upsert_state(ctx.id, ctx.user_id, STATE.to_dict())

def try_open_position(ctx, strategy):
    global STATE
    if STATE.in_position:
        return

    ex = _exchange(ctx)
    reporter = get_reporter_optional()
    symbol = ctx.market_symbol
    tf = ctx.execution_config["timeframe"]
    lb = ctx.execution_config["lookback_bars"]

    try:
        start = time.monotonic()
        record_exchange_call()
        df = fetch_ohlcv_df(ex, symbol, tf, lb)
        duration = (time.monotonic() - start) * 1000
        record_ohlcv_fetch(duration, len(df))
    except Exception as exc:
        _maybe_record_stream_disconnect(reporter, exc)
        raise
    try:
        start = time.monotonic()
        df = strategy.prepare(df, ctx.strategy_config)
        duration = (time.monotonic() - start) * 1000
        record_indicator_time(duration)
    except Exception as exc:
        _handle_indicator_exception(exc, reporter)
        raise

    if len(df) < int(ctx.strategy_config.get("min_bars", 500)):
        log(f"[entry] skip: not enough bars ({len(df)} < {int(ctx.strategy_config.get('min_bars', 500))})", level="DEBUG")
        return

    last_ts = df.index[-1]
    last_iso = last_ts.isoformat()
    row = df.iloc[-1]
    if reporter:
        _record_candle_metrics(reporter, last_ts, ctx)

    if STATE.last_candle_time == last_iso:
        log(f"[entry] skip: already processed candle {last_iso}", level="DEBUG")
        return
    STATE.last_candle_time = last_iso
    candle_updated = True

    iso = last_ts.isocalendar()
    week_key = f"{iso.year}-{iso.week}"
    STATE.week_trade_counts.setdefault(week_key, 0)

    if STATE.week_trade_counts[week_key] >= int(ctx.risk_config["max_trades_per_week"]):
        if candle_updated:
            upsert_state(ctx.id, ctx.user_id, STATE.to_dict())
        log(f"[entry] skip: max trades reached for week {week_key}", level="DEBUG")
        return

    decision_start = time.monotonic()
    try:
        try:
            long_ok = strategy.long_signal(row, ctx.strategy_config)
        except Exception as exc:
            _handle_indicator_exception(exc, reporter)
            raise
        try:
            short_ok = strategy.short_signal(row, ctx.strategy_config)
        except Exception as exc:
            _handle_indicator_exception(exc, reporter)
            raise
    finally:
        decision_duration = (time.monotonic() - decision_start) * 1000
        record_decision_time(decision_duration)
    if not long_ok and not short_ok:
        if candle_updated:
            upsert_state(ctx.id, ctx.user_id, STATE.to_dict())
        log(f"[entry] skip: no signal (long_ok={long_ok} short_ok={short_ok}) close={row.get('close')}", level="DEBUG")
        return

    expected_price = float(row["close"])
    price = expected_price

    try:
        record_exchange_call()
        bal = fetch_quote_balance(ex, symbol)
    except Exception as exc:
        _maybe_record_stream_disconnect(reporter, exc)
        raise
    notional = compute_notional(bal, float(ctx.risk_config["allocation_frac"]), float(ctx.risk_config["leverage"]))
    if notional < float(ctx.risk_config["min_notional_usd"]):
        if candle_updated:
            upsert_state(ctx.id, ctx.user_id, STATE.to_dict())
        log(f"[entry] skip: notional too small ({notional:.2f} < {float(ctx.risk_config['min_notional_usd'])})", level="DEBUG")
        return

    qty = compute_qty(notional, price)
    max_slip = int(ctx.execution_config.get("max_slippage_bps", 20))

    order_side = "buy" if long_ok else "sell"
    STATE.direction = "long" if long_ok else "short"
    order, client_order_id = send_order(
        ctx,
        ex,
        symbol,
        order_side,
        qty,
        _dry_run(ctx),
        expected_price=expected_price,
        max_slippage_bps=max_slip,
    )
    STATE.in_position = True

    STATE.position_id = on_entry(
        ctx,
        STATE.direction,
        price,
        last_iso,
        qty,
        entry_exchange_order_id=entry_order_id,
        entry_client_order_id=client_order_id,
        payload=order,
    )
    entry_order_id = get_exchange_order_id(order)
    if entry_order_id:
        update_trade_status(ctx.id, entry_order_id, updates={"position_id": STATE.position_id})

    STATE.entry_price = price
    STATE.entry_time = last_iso
    STATE.qty = qty
    STATE.base_notional = notional
    STATE.peak_price = price
    STATE.low_price = price
    STATE.added_levels = 0
    STATE.week_trade_counts[week_key] += 1
    STATE.max_unrealized_pnl = 0.0
    STATE.min_unrealized_pnl = 0.0

    event(ctx, "trade", f"ENTRY {STATE.direction} price={price:.6f} qty={qty:.6f} notional={notional:.2f}")
    STATE.last_manage_time = utcnow_iso()
    upsert_state(ctx.id, ctx.user_id, STATE.to_dict())
    if reporter:
        reporter.set_in_position(True)


def _handle_indicator_exception(exc: Exception, reporter):
    if reporter:
        reporter.record_indicator_error(map_exception_to_reason(exc))


def _record_candle_metrics(reporter, last_ts, ctx):
    now = datetime.now(timezone.utc)
    lag = int(max(0.0, (now - last_ts).total_seconds()))
    reporter.record_candle_lag(lag)
    prev_iso = STATE.last_candle_time
    if not prev_iso:
        return
    prev_ts = _parse_iso(prev_iso)
    if not prev_ts:
        return
    timeframe_secs = _timeframe_seconds(ctx.execution_config.get("timeframe"))
    gap_secs = (last_ts - prev_ts).total_seconds()
    if gap_secs > timeframe_secs * 1.5:
        reporter.record_candle_gap()


def _parse_iso(value: str) -> datetime | None:
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        try:
            return datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc)
        except Exception:
            return None


def _timeframe_seconds(timeframe: str | None) -> float:
    if not timeframe:
        return 60.0
    tf = timeframe.lower().strip()
    unit = tf[-1]
    try:
        value = float(tf[:-1])
    except ValueError:
        return 60.0
    multipliers = {"m": 60.0, "h": 3600.0, "d": 86400.0, "w": 604800.0}
    return value * multipliers.get(unit, 60.0)


def _maybe_record_stream_disconnect(reporter, exc: Exception):
    if reporter and _looks_like_stream_error(exc):
        reporter.record_stream_disconnect()


def _looks_like_stream_error(exc: Exception) -> bool:
    text = str(exc).lower()
    for token in ("timeout", "disconnect", "connection reset", "socket", "read", "network", "reset"):
        if token in text:
            return True
    return False
