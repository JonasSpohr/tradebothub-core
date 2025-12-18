from typing import Any, Dict, Tuple
from bot.core.safety import (
    MIN_POLL_SECONDS, MAX_LOOKBACK_BARS, MAX_LEVERAGE, MAX_ALLOCATION_FRAC,
    MAX_TRADES_PER_WEEK, MAX_PYRAMID_LEVELS, MIN_NOTIONAL_USD, MAX_SLIPPAGE_BPS
)

def _i(v: Any, d: int) -> int:
    try: return int(v)
    except Exception: return d

def _f(v: Any, d: float) -> float:
    try: return float(v)
    except Exception: return d

def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))

def normalize_configs(
    strategy_cfg: Dict[str, Any],
    risk_cfg: Dict[str, Any],
    exec_cfg: Dict[str, Any],
    control_cfg: Dict[str, Any],
) -> Tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
    sc = dict(strategy_cfg or {})
    rc = dict(risk_cfg or {})
    ec = dict(exec_cfg or {})
    cc = dict(control_cfg or {})

    # Execution
    ec["timeframe"] = ec.get("timeframe") or "1h"
    ec["poll_interval"] = max(_i(ec.get("poll_interval", 300), 300), MIN_POLL_SECONDS)
    ec["lookback_bars"] = min(_i(ec.get("lookback_bars", 700), 700), MAX_LOOKBACK_BARS)
    ec["order_type"] = ec.get("order_type") or "market"
    ec["max_slippage_bps"] = min(_i(ec.get("max_slippage_bps", 20), 20), MAX_SLIPPAGE_BPS)

    # Risk
    rc["leverage"] = _clamp(_f(rc.get("leverage", 3.0), 3.0), 1.0, MAX_LEVERAGE)
    rc["allocation_frac"] = _clamp(_f(rc.get("allocation_frac", 0.5), 0.5), 0.05, MAX_ALLOCATION_FRAC)
    rc["max_trades_per_week"] = min(_i(rc.get("max_trades_per_week", 30), 30), MAX_TRADES_PER_WEEK)
    rc["min_notional_usd"] = max(_f(rc.get("min_notional_usd", 15.0), 15.0), MIN_NOTIONAL_USD)

    # Strategy
    sc["min_bars"] = _i(sc.get("min_bars", 500), 500)
    sc["pyramiding_enabled"] = bool(sc.get("pyramiding_enabled", False))
    sc["max_pyramid_levels"] = min(_i(sc.get("max_pyramid_levels", 0), 0), MAX_PYRAMID_LEVELS)

    # Control
    cc["trading_enabled"] = bool(cc.get("trading_enabled", True))
    cc["kill_switch"] = bool(cc.get("kill_switch", False))
    cc["admin_override"] = bool(cc.get("admin_override", False))

    return sc, rc, ec, cc