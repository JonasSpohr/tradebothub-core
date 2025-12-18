from typing import Any, Dict
import pandas as pd
from bot.strategies.base import Strategy
from bot.indicators import compute_atr

class BreakoutStrategy(Strategy):
    name = "breakout"

    def prepare(self, df: pd.DataFrame, cfg: Dict[str, Any]) -> pd.DataFrame:
        df = df.copy()
        df["atr"] = compute_atr(df, int(cfg.get("atr_period", 14)))
        lb = int(cfg.get("range_lookback", 48))
        df["range_high"] = df["high"].rolling(lb).max()
        df["range_low"] = df["low"].rolling(lb).min()
        if bool(cfg.get("volume_filter_enabled", True)):
            p = int(cfg.get("volume_ma_period", 20))
            df["vol_ma"] = df["volume"].rolling(p).mean()
        return df

    def long_signal(self, row: pd.Series, cfg: Dict[str, Any]) -> bool:
        atr = float(row.get("atr") or 0.0)
        if atr <= 0: return False
        level = float(row["range_high"]) + float(cfg.get("breakout_buffer_atr", 0.2)) * atr * max(int(cfg.get("confirm_candles", 1)), 1)
        if bool(cfg.get("volume_filter_enabled", True)):
            mult = float(cfg.get("volume_mult", 1.2))
            vol_ma = float(row.get("vol_ma") or 0.0)
            if vol_ma > 0 and float(row["volume"]) < mult * vol_ma:
                return False
        return float(row["close"]) > level

    def short_signal(self, row: pd.Series, cfg: Dict[str, Any]) -> bool:
        atr = float(row.get("atr") or 0.0)
        if atr <= 0: return False
        level = float(row["range_low"]) - float(cfg.get("breakout_buffer_atr", 0.2)) * atr * max(int(cfg.get("confirm_candles", 1)), 1)
        if bool(cfg.get("volume_filter_enabled", True)):
            mult = float(cfg.get("volume_mult", 1.2))
            vol_ma = float(row.get("vol_ma") or 0.0)
            if vol_ma > 0 and float(row["volume"]) < mult * vol_ma:
                return False
        return float(row["close"]) < level