def atr_exit_reason(state, price: float, atr: float, cfg: dict) -> str | None:
    if atr <= 0:
        return None

    sl = float(cfg.get("sl_atr_mult", 1.5)) * atr
    tp = float(cfg.get("tp_atr_mult", 3.5)) * atr
    trail = float(cfg.get("trail_atr_mult", 1.5)) * atr
    trail_start_r = float(cfg.get("trail_start_r", 1.0))

    entry = state.entry_price

    if state.direction == "long":
        diff = price - entry
        if diff >= tp: return "TP_ATR"
        if diff <= -sl: return "SL_ATR"
        if diff >= trail_start_r * sl:
            state.peak_price = max(state.peak_price, price)
            if price <= state.peak_price - trail:
                return "TRAIL_ATR"

    if state.direction == "short":
        diff = entry - price
        if diff >= tp: return "TP_ATR"
        if diff <= -sl: return "SL_ATR"
        if diff >= trail_start_r * sl:
            state.low_price = min(state.low_price, price)
            if price >= state.low_price + trail:
                return "TRAIL_ATR"

    return None