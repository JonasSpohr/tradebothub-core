from bot.infra.db import (
    write_event,
    insert_trade,
    insert_position_open,
    close_position,
    notify,
)

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
    notify(
        ctx.user_id,
        ctx.id,
        "trade_opened",
        f"Entered {direction.upper()}",
        body=f"price={entry_price:.6f} qty={qty}",
        severity="info",
        metadata={"position_id": position_id, "direction": direction, "price": entry_price, "qty": qty},
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
    notify(
        ctx.user_id,
        ctx.id,
        "trade_scaled",
        f"Scaled {direction.upper()}",
        body=f"price={price:.6f} qty={qty}",
        severity="info",
        metadata={"position_id": position_id, "direction": direction, "price": price, "qty": qty},
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
    severity = "warning" if realized_pnl < 0 else "info"
    notify(
        ctx.user_id,
        ctx.id,
        "trade_closed",
        f"Exited {direction.upper()}",
        body=f"{reason} price={exit_price:.6f} pnl={realized_pnl:.4f}",
        severity=severity,
        metadata={
            "position_id": position_id,
            "direction": direction,
            "price": exit_price,
            "qty": qty,
            "pnl": realized_pnl,
            "reason": reason,
        },
    )
