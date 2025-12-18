def compute_notional(balance_quote: float, allocation_frac: float, leverage: float) -> float:
    return (balance_quote * allocation_frac) * leverage

def compute_qty(notional: float, price: float) -> float:
    return notional / price if price > 0 else 0.0