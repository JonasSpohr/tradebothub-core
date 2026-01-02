from dataclasses import dataclass, asdict
from typing import Dict, Any

@dataclass
class PositionState:
    in_position: bool = False
    position_id: str = ""          # DB bot_positions.id for current open position
    direction: str = ""            # long|short
    entry_price: float = 0.0
    entry_time: str = ""
    qty: float = 0.0
    base_notional: float = 0.0
    peak_price: float = 0.0
    low_price: float = 0.0
    added_levels: int = 0
    week_trade_counts: Dict[str, int] = None
    last_exit_time: str = ""
    last_candle_time: str = ""
    cumulative_pnl: float = 0.0
    max_unrealized_pnl: float = 0.0
    min_unrealized_pnl: float = 0.0
    last_price: float = 0.0
    unrealized_pnl: float = 0.0
    stop_price: float = 0.0
    take_profit_price: float = 0.0
    trailing_stop_price: float = 0.0
    trailing_active: bool = False
    atr: float = 0.0
    last_manage_time: str = ""
    heartbeat_at: str = ""

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["week_trade_counts"] = d["week_trade_counts"] or {}
        return d
