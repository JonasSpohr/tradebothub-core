def maybe_pyramid(cfg: dict, move: float, added_levels: int) -> bool:
    if not bool(cfg.get("pyramiding_enabled", False)):
        return False
    max_levels = int(cfg.get("max_pyramid_levels", 0))
    if added_levels >= max_levels:
        return False
    step = float(cfg.get("pyramid_step", 0.01))
    return move >= (added_levels + 1) * step

def pyramid_add_notional(base_notional: float, cfg: dict) -> float:
    return base_notional * float(cfg.get("pyramid_add_frac", 0.5))