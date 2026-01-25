from __future__ import annotations
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from bot.core.logging import log

class ExchangeProvider:
    def fetch_order_by_id(self, symbol: str, order_id: str) -> Dict[str, Any]:
        raise NotImplementedError

    def fetch_position_for_symbol(self, symbol: str) -> Optional[Dict[str, Any]]:
        raise NotImplementedError

    def fetch_closed_pnl_since(self, symbol: str, since_ms: int) -> Dict[str, Any]:
        raise NotImplementedError


class CcxtExchangeProvider(ExchangeProvider):
    def __init__(self, exchange):
        self._exchange = exchange

    def fetch_order_by_id(self, symbol: str, order_id: str) -> Dict[str, Any]:
        return self._exchange.fetch_order(order_id, symbol)

    def fetch_position_for_symbol(self, symbol: str) -> Optional[Dict[str, Any]]:
        try:
            positions = self._exchange.fetch_positions([symbol])
            for pos in positions or []:
                if pos.get("symbol") == symbol:
                    return pos
        except Exception:
            pass
        try:
            return self._exchange.fetch_position(symbol)
        except Exception as exc:
            log(f"[exchange provider] failed to fetch position for {symbol}: {exc}", level="WARN")
            return None

    def fetch_closed_pnl_since(self, symbol: str, since_ms: int) -> Dict[str, Any]:
        closed_orders: List[Dict[str, Any]] = []
        try:
            resp = self._exchange.fetch_orders(symbol, since=since_ms, limit=50)
            closed_orders = [o for o in resp or [] if o.get("status") in {"closed", "filled", "canceled", "done"}]
        except Exception:
            pass
        if not closed_orders:
            try:
                trades = self._exchange.fetch_my_trades(symbol, since=since_ms, limit=50)
                closed_orders = trades or []
            except Exception:
                pass
        if not closed_orders:
            return {"confirmed_closed": False, "payload": None}
        latest = closed_orders[-1]
        timestamp = latest.get("timestamp")
        exit_time = None
        if timestamp:
            exit_time = datetime.fromtimestamp(int(timestamp) / 1000, tz=timezone.utc).isoformat()
        price = latest.get("average") or latest.get("price")
        return {
            "confirmed_closed": True,
            "exit_price": float(price) if price else None,
            "exit_time": exit_time,
            "payload": latest,
        }
