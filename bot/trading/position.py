from bot.core.logging import log
from bot.core.timeutil import utcnow_iso
from bot.infra.crypto import decrypt
from bot.infra.exchange import create_exchange, fetch_ohlcv_df, fetch_last_price, fetch_quote_balance
from bot.infra.db import upsert_state
from bot.trading.orders import send_order
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
    symbol = ctx.market_symbol
    price = fetch_last_price(ex, symbol)

    df = fetch_ohlcv_df(ex, symbol, ctx.execution_config["timeframe"], ctx.execution_config["lookback_bars"])
    df = strategy.prepare(df, ctx.strategy_config)
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
            STATE.trailing_stop_price = STATE.peak_price - trail_mult * atr
    elif STATE.direction == "short":
        sl = sl_mult * atr
        tp = tp_mult * atr
        STATE.stop_price = STATE.entry_price + sl
        STATE.take_profit_price = STATE.entry_price - tp
        STATE.low_price = min(STATE.low_price, price)
        STATE.trailing_active = unreal >= trail_start_r * sl
        if STATE.trailing_active:
            STATE.trailing_stop_price = STATE.low_price + trail_mult * atr

    reason = atr_exit_reason(STATE, price, atr, ctx.strategy_config)
    if reason:
        pnl = (price - STATE.entry_price) * STATE.qty * (1 if STATE.direction == "long" else -1)
        close_side = "sell" if STATE.direction == "long" else "buy"

        expected = float(row["close"])
        max_slip = int(ctx.execution_config.get("max_slippage_bps", 20))
        send_order(ex, symbol, close_side, STATE.qty, _dry_run(ctx), expected_price=expected, max_slippage_bps=max_slip)

        exit_time = utcnow_iso()
        on_exit(ctx, STATE.position_id, STATE.direction, price, exit_time, STATE.qty, pnl, reason)

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
        return

    move = (price - STATE.entry_price) / STATE.entry_price if STATE.direction == "long" else (STATE.entry_price - price) / STATE.entry_price
    while maybe_pyramid(ctx.strategy_config, move, STATE.added_levels):
        add_notional = pyramid_add_notional(STATE.base_notional, ctx.strategy_config)
        add_qty = compute_qty(add_notional, price)
        side = "buy" if STATE.direction == "long" else "sell"

        expected = float(row["close"])
        max_slip = int(ctx.execution_config.get("max_slippage_bps", 20))
        send_order(ex, symbol, side, add_qty, _dry_run(ctx), expected_price=expected, max_slippage_bps=max_slip)

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
    symbol = ctx.market_symbol
    tf = ctx.execution_config["timeframe"]
    lb = ctx.execution_config["lookback_bars"]

    df = fetch_ohlcv_df(ex, symbol, tf, lb)
    df = strategy.prepare(df, ctx.strategy_config)

    if len(df) < int(ctx.strategy_config.get("min_bars", 500)):
        log(f"[entry] skip: not enough bars ({len(df)} < {int(ctx.strategy_config.get('min_bars', 500))})", level="DEBUG")
        return

    last_ts = df.index[-1]
    last_iso = last_ts.isoformat()
    row = df.iloc[-1]

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

    long_ok = strategy.long_signal(row, ctx.strategy_config)
    short_ok = strategy.short_signal(row, ctx.strategy_config)
    if not long_ok and not short_ok:
        if candle_updated:
            upsert_state(ctx.id, ctx.user_id, STATE.to_dict())
        log(f"[entry] skip: no signal (long_ok={long_ok} short_ok={short_ok}) close={row.get('close')}", level="DEBUG")
        return

    expected_price = float(row["close"])
    price = expected_price

    bal = fetch_quote_balance(ex, symbol)
    notional = compute_notional(bal, float(ctx.risk_config["allocation_frac"]), float(ctx.risk_config["leverage"]))
    if notional < float(ctx.risk_config["min_notional_usd"]):
        if candle_updated:
            upsert_state(ctx.id, ctx.user_id, STATE.to_dict())
        log(f"[entry] skip: notional too small ({notional:.2f} < {float(ctx.risk_config['min_notional_usd'])})", level="DEBUG")
        return

    qty = compute_qty(notional, price)
    max_slip = int(ctx.execution_config.get("max_slippage_bps", 20))

    if long_ok:
        send_order(ex, symbol, "buy", qty, _dry_run(ctx), expected_price=expected_price, max_slippage_bps=max_slip)
        STATE.in_position = True
        STATE.direction = "long"
    else:
        send_order(ex, symbol, "sell", qty, _dry_run(ctx), expected_price=expected_price, max_slippage_bps=max_slip)
        STATE.in_position = True
        STATE.direction = "short"

    STATE.position_id = on_entry(ctx, STATE.direction, price, last_iso, qty)

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
