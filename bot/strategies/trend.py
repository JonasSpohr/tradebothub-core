from typing import Any, Dict
import pandas as pd
from bot.strategies.base import Strategy
from bot.indicators import compute_rsi, compute_atr

class TrendStrategy(Strategy):
    name = "trend"

    def prepare(self, df: pd.DataFrame, cfg: Dict[str, Any]) -> pd.DataFrame:
        df = df.copy()
        close = df["close"]
        df["ema_fast"] = close.ewm(span=int(cfg.get("ema_fast", 20))).mean()
        df["ema_slow"] = close.ewm(span=int(cfg.get("ema_slow", 50))).mean()
        df["ema_trend"] = close.ewm(span=int(cfg.get("ema_trend", 100))).mean()
        df["rsi"] = compute_rsi(close, int(cfg.get("rsi_period", 14)))
        df["atr"] = compute_atr(df, int(cfg.get("atr_period", 14)))
        return df

    def long_signal(self, row: pd.Series, cfg: Dict[str, Any]) -> bool:
        return (row["ema_fast"] > row["ema_slow"] > row["ema_trend"]) and (row["rsi"] >= float(cfg.get("rsi_entry_long", 55)))

    def short_signal(self, row: pd.Series, cfg: Dict[str, Any]) -> bool:
        return (row["ema_fast"] < row["ema_slow"] < row["ema_trend"]) and (row["rsi"] <= float(cfg.get("rsi_entry_short", 45)))