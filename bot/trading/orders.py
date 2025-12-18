from bot.core.logging import log

def _bps_diff(a: float, b: float) -> float:
    if b <= 0:
        return 0.0
    return abs(a - b) / b * 10000.0

def send_order(exchange, symbol: str, side: str, qty: float, dry_run: bool, expected_price: float, max_slippage_bps: int):
    if qty <= 0:
        return None

    if dry_run:
        log(f"[DRY RUN] {side.upper()} {qty:.6f} {symbol}")
        return None

    t = exchange.fetch_ticker(symbol)
    live = float(t.get("last") or t.get("close") or expected_price)
    slip = _bps_diff(live, expected_price)

    if max_slippage_bps is not None and slip > float(max_slippage_bps):
        raise RuntimeError(f"Slippage guard: live={live} expected={expected_price} slip={slip:.1f}bps > {max_slippage_bps}bps")

    log(f"[LIVE] {side.upper()} {qty:.6f} {symbol} (slip={slip:.1f}bps)")
    return exchange.create_order(symbol, "market", side, qty)