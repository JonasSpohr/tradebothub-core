from bot.infra.db import (
    close_position,
    insert_position_open,
    insert_trade,
    notify,
    queue_email_notification,
    write_event,
)
from bot.infra.notifications import notification_context_payload
from bot.utils.ids import generate_client_order_id

TRADE_OPEN_EMAIL_TEMPLATE = "bot_trade_opened"
TRADE_CLOSE_EMAIL_TEMPLATE = "bot_trade_closed"

def event(ctx, typ: str, msg: str):
    write_event(ctx.id, ctx.user_id, typ, msg)

def on_entry(
    ctx,
    direction: str,
    entry_price: float,
    entry_time: str,
    qty: float,
    *,
    entry_exchange_order_id: str | None = None,
    entry_client_order_id: str | None = None,
    payload: dict | None = None,
) -> str:
    position_id = insert_position_open(
        ctx.id,
        ctx.user_id,
        direction,
        entry_price,
        entry_time,
        qty,
        symbol=ctx.market_symbol,
        exchange=ctx.exchange_ccxt_id,
        margin_mode=str(ctx.execution_config.get("margin_mode", "") or "cross"),
        position_side=direction,
        entry_exchange_order_id=entry_exchange_order_id,
        entry_client_order_id=entry_client_order_id,
        exchange_account_ref=ctx.execution_config.get("exchange_account_ref"),
        exchange_payload=payload,
    )
    ctx.position_id = position_id

    insert_trade(
        bot_id=ctx.id,
        user_id=ctx.user_id,
        position_id=position_id,
        side="buy" if direction == "long" else "sell",
        price=entry_price,
        qty=qty,
        fee=None,
        pnl=None,
        exchange_order_id=entry_exchange_order_id,
        executed_at=entry_time,
        client_order_id=entry_client_order_id or generate_client_order_id(ctx.id, "entry"),
        symbol=ctx.market_symbol,
        order_type="market",
        order_status="entered",
        reduce_only=False,
        exchange_payload=payload,
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

    ctx_payload = notification_context_payload(ctx)
    queue_email_notification(
        user_id=ctx.user_id,
        bot_id=ctx.id,
        event_key="trade_opened",
        email_template=TRADE_OPEN_EMAIL_TEMPLATE,
        payload={
            **ctx_payload,
            "position_id": position_id,
            "direction": direction,
            "price": entry_price,
            "qty": qty,
        },
        dedup_id=position_id,
        throttle_seconds=0,
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
        client_order_id=generate_client_order_id(ctx.id, "pyramid"),
        symbol=ctx.market_symbol,
        order_type="market",
        order_status="scaled",
        reduce_only=False,
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

def on_exit(
    ctx,
    position_id: str,
    direction: str,
    exit_price: float,
    exit_time: str,
    qty: float,
    realized_pnl: float,
    reason: str,
    *,
    exit_exchange_order_id: str | None = None,
    exit_client_order_id: str | None = None,
    payload: dict | None = None,
):
    close_position(
        position_id,
        exit_price,
        exit_time,
        realized_pnl,
        bot_id=ctx.id,
        exit_exchange_order_id=exit_exchange_order_id,
        exit_client_order_id=exit_client_order_id,
        exchange_payload=payload,
    )
    ctx.position_id = ""

    insert_trade(
        bot_id=ctx.id,
        user_id=ctx.user_id,
        position_id=position_id,
        side="sell" if direction == "long" else "buy",
        price=exit_price,
        qty=qty,
        fee=None,
        pnl=realized_pnl,
        exchange_order_id=exit_exchange_order_id,
        executed_at=exit_time,
        client_order_id=exit_client_order_id or generate_client_order_id(ctx.id, "exit"),
        symbol=ctx.market_symbol,
        order_type="market",
        order_status="exited",
        reduce_only=False,
        exchange_payload=payload,
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

    ctx_payload = notification_context_payload(ctx)
    queue_email_notification(
        user_id=ctx.user_id,
        bot_id=ctx.id,
        event_key="trade_closed",
        email_template=TRADE_CLOSE_EMAIL_TEMPLATE,
        payload={
            **ctx_payload,
            "position_id": position_id,
            "direction": direction,
            "price": exit_price,
            "qty": qty,
            "pnl": realized_pnl,
            "reason": reason,
        },
        dedup_id=position_id,
        throttle_seconds=0,
    )
