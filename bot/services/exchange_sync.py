from __future__ import annotations
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from bot.core.logging import log
from bot.core.types import BotContext
from bot.exchange.provider import ExchangeProvider
from bot.infra.db import (
    close_position,
    get_open_position,
    set_exchange_sync_status,
    update_position_from_exchange,
)
from bot.utils.timeframes import timeframe_to_seconds


class ExchangeSyncError(RuntimeError):
    pass


class ExchangeSyncService:
    REQUIRED_FIELDS = (
        "symbol",
        "entry_exchange_order_id",
        "entry_client_order_id",
        "position_side",
        "direction",
    )

    def __init__(self, ctx: BotContext, provider: ExchangeProvider):
        self._ctx = ctx
        self._provider = provider
        self._interval = self._compute_interval(ctx.execution_config.get("timeframe"))
        self._next_sync_at = 0.0

    def _compute_interval(self, timeframe: str | None) -> float:
        tf_sec = timeframe_to_seconds(timeframe)
        if tf_sec >= 300:
            return min(tf_sec * 2, 600)
        return 300.0

    def startup_sync(self) -> None:
        self._run_sync()

    def maybe_sync(self) -> None:
        now = time.monotonic()
        if now < self._next_sync_at:
            return
        self._next_sync_at = now + self._interval
        self._run_sync()

    def _run_sync(self) -> None:
        pos = get_open_position(self._ctx.id)
        if not pos:
            return
        self._sync_open_position(pos)

    def _sync_open_position(self, pos: Dict[str, Any]) -> None:
        missing = [f for f in self.REQUIRED_FIELDS if not pos.get(f)]
        if missing:
            set_exchange_sync_status(self._ctx.id, "mismatch")
            raise ExchangeSyncError(f"Missing identity fields: {missing}")

        try:
            self._provider.fetch_order_by_id(pos["symbol"], str(pos["entry_exchange_order_id"]))
        except Exception as exc:
            set_exchange_sync_status(self._ctx.id, "mismatch")
            raise ExchangeSyncError("Entry order lookup failed") from exc

        symbol = pos["symbol"]
        live = self._provider.fetch_position_for_symbol(symbol)
        if live:
            updates = self._build_live_updates(live, pos)
            update_position_from_exchange(self._ctx.id, pos["id"], **updates)
            set_exchange_sync_status(self._ctx.id, "ok")
            log(f"[exchange sync] refreshed {symbol} qty={updates['qty']}", level="INFO")
            return

        since_ms = self._entry_time_to_ms(pos.get("entry_time"))
        closed = self._provider.fetch_closed_pnl_since(symbol, since_ms)
        if closed.get("confirmed_closed"):
            exit_price = closed.get("exit_price") or 0.0
            realized = self._compute_realized_pnl(pos, exit_price)
            close_position(
                pos["id"],
                exit_price,
                closed.get("exit_time") or datetime.now(timezone.utc).isoformat(),
                realized,
                bot_id=self._ctx.id,
                exit_exchange_order_id=closed.get("payload", {}).get("id"),
                exit_client_order_id=closed.get("payload", {}).get("clientOrderId"),
                exchange_payload=closed.get("payload"),
            )
            set_exchange_sync_status(self._ctx.id, "ok")
            log(f"[exchange sync] closed missing position for {symbol} reported by exchange", level="WARN")
            return

        set_exchange_sync_status(self._ctx.id, "missing")
        raise ExchangeSyncError("Position missing and closure not confirmed")

    def _build_live_updates(self, live: Dict[str, Any], pos: Dict[str, Any]) -> Dict[str, Any]:
        qty = float(live.get("size") or live.get("positionAmt") or pos.get("qty") or 0.0)
        entry_price = float(live.get("entryPrice") or pos.get("entry_price") or 0.0)
        mark_price = live.get("markPrice")
        unrealized_pnl = float(live.get("unrealizedPnl") or live.get("pnl") or 0.0)
        return {
            "qty": qty,
            "entry_price": entry_price,
            "mark_price": float(mark_price) if mark_price is not None else None,
            "unrealized_pnl": unrealized_pnl,
            "symbol": pos["symbol"],
            "exchange": self._ctx.exchange_ccxt_id,
            "position_side": live.get("side") or pos.get("position_side") or pos.get("direction"),
            "margin_mode": live.get("marginMode") or self._ctx.execution_config.get("margin_mode"),
            "exchange_account_ref": live.get("account") or pos.get("exchange_account_ref"),
            "exchange_position_id": live.get("id") or live.get("position_id"),
            "exchange_position_key": live.get("positionKey"),
            "payload": live,
            "entry_payload": {"order": live},
        }

    def _entry_time_to_ms(self, entry_time: Optional[str]) -> int:
        if not entry_time:
            return 0
        try:
            ts = datetime.fromisoformat(entry_time)
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            return int(ts.timestamp() * 1000)
        except ValueError:
            return 0

    def _compute_realized_pnl(self, pos: Dict[str, Any], exit_price: float) -> float:
        qty = float(pos.get("qty") or 0.0)
        entry_price = float(pos.get("entry_price") or 0.0)
        direction = pos.get("direction") or pos.get("position_side")
        sign = 1 if direction == "long" else -1
        return (exit_price - entry_price) * qty * sign
