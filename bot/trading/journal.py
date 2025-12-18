from bot.infra.db import write_event, insert_trade, insert_position_open, close_position

def event(ctx, typ: str, msg: str):
    write_event(ctx.id, ctx.user_id, typ, msg)

def on_entry(ctx, direction: str, entry_price: float, entry_time: str, qty: float) -> str:
    position_id = insert_position_open(ctx.id, ctx.user_id, direction, entry_price, entry_time, qty)

    insert_trade(
        bot_id=ctx.id,
        user_id=ctx.user_id,
        position_id=position_id,
        side="buy" if direction == "long" else "sell",
        price=entry_price,
        qty=qty,
        fee=None,
        pnl=None,
        exchange_order_id=None,
        executed_at=entry_time,
    )
    return position_id

def on_pyramid(ctx, position_id: str, direction: str, price: float, qty: float, executed_at: str):
    insert_trade(
        bot_id=ctx.id,
        user_id=ctx.user_id,
        position_id=position_id,
        side="buy" if direction == "long" else "sell",
        price=price,
        qty=qty,
        fee=None,
        pnl=None,
        exchange_order_id=None,
        executed_at=executed_at,
    )

def on_exit(ctx, position_id: str, direction: str, exit_price: float, exit_time: str, qty: float, realized_pnl: float, reason: str):
    close_position(position_id, exit_price, exit_time, realized_pnl)

    insert_trade(
        bot_id=ctx.id,
        user_id=ctx.user_id,
        position_id=position_id,
        side="sell" if direction == "long" else "buy",
        price=exit_price,
        qty=qty,
        fee=None,
        pnl=realized_pnl,
        exchange_order_id=None,
        executed_at=exit_time,
    )

    event(ctx, "trade", f"EXIT {direction} {reason} price={exit_price:.6f} pnl={realized_pnl:.4f}")