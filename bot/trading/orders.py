from __future__ import annotations

from time import monotonic
from typing import Any, Dict

from bot.core.logging import log
from bot.core.types import BotContext
from bot.health.reporter import get_reporter_optional
from bot.health.types import map_exception_to_reason
from bot.infra.db import log_order_submission, update_trade_status
from bot.utils.ids import generate_client_order_id


def _bps_diff(a: float, b: float) -> float:
    if b <= 0:
        return 0.0
    return abs(a - b) / b * 10000.0


def _normalize_order_id(order: Dict[str, Any] | None) -> str | None:
    if not order:
        return None
    return str(order.get("id") or order.get("orderId") or order.get("exchangeOrderId"))


def _normalize_order_price(order: Dict[str, Any] | None, fallback: float) -> float:
    if not order:
        return fallback
    val = order.get("average") or order.get("price") or order.get("avgPrice")
    try:
        return float(val)
    except (TypeError, ValueError):
        return fallback


def _normalize_filled_qty(order: Dict[str, Any] | None) -> float:
    if not order:
        return 0.0
    val = order.get("filled") or order.get("amount")
    try:
        return float(val or 0.0)
    except (TypeError, ValueError):
        return 0.0


def send_order(
    ctx: BotContext,
    exchange,
    symbol: str,
    side: str,
    qty: float,
    dry_run: bool,
    expected_price: float,
    max_slippage_bps: int,
    *,
    order_type: str = "market",
    reduce_only: bool = False,
) -> tuple[Dict[str, Any] | None, str]:
    if qty <= 0:
        return None, ""

    client_order_id = generate_client_order_id(ctx.id)
    reporter = get_reporter_optional()

    if dry_run:
        log(f"[DRY RUN] {side.upper()} {qty:.6f} {symbol}")
        if reporter:
            reporter.record_order_submit()
        return None, client_order_id

    if reporter:
        reporter.record_order_submit()
    start = monotonic()
    try:
        ticker = exchange.fetch_ticker(symbol)
        live = float(ticker.get("last") or ticker.get("close") or expected_price)
        slip = _bps_diff(live, expected_price)

        if max_slippage_bps is not None and slip > float(max_slippage_bps):
            raise RuntimeError(
                f"Slippage guard: live={live} expected={expected_price} slip={slip:.1f}bps > {max_slippage_bps}bps"
            )

        log(f"[LIVE] {side.upper()} {qty:.6f} {symbol} (slip={slip:.1f}bps)")
        order = exchange.create_order(
            symbol,
            order_type,
            side,
            qty,
            None,
            {
                "clientOrderId": client_order_id,
                "reduceOnly": reduce_only,
            },
        )
        elapsed_ms = int((monotonic() - start) * 1000)
        if reporter:
            reporter.record_order_ack(elapsed_ms)

        exchange_order_id = _normalize_order_id(order)
        if exchange_order_id:
            log_order_submission(
                ctx.id,
                ctx.user_id,
                getattr(ctx, "position_id", None),
                exchange_order_id=exchange_order_id,
                client_order_id=client_order_id,
                symbol=symbol,
                side=side,
                order_type=order_type,
                reduce_only=reduce_only,
                order_status=order.get("status") or "open",
                order_amount=_normalize_filled_qty(order),
                order_price=_normalize_order_price(order, expected_price),
                payload=order,
            )
        return order, client_order_id
    except Exception as exc:
        reason = map_exception_to_reason(exc)
        if reporter:
            reporter.record_order_reject(reason)
        raise
