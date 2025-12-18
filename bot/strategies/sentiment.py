import os
from typing import Any, Dict
import pandas as pd
from bot.strategies.base import Strategy
from bot.indicators import compute_atr

class SentimentStrategy(Strategy):
    name = "sentiment"

    def prepare(self, df: pd.DataFrame, cfg: Dict[str, Any]) -> pd.DataFrame:
        df = df.copy()
        df["atr"] = compute_atr(df, int(cfg.get("atr_period", 14)))
        return df

    def _score(self) -> float:
        try:
            return float(os.getenv("SENTIMENT_SCORE", "0"))
        except Exception:
            return 0.0

    def long_signal(self, row: pd.Series, cfg: Dict[str, Any]) -> bool:
        return self._score() >= float(cfg.get("long_threshold", 0.55))

    def short_signal(self, row: pd.Series, cfg: Dict[str, Any]) -> bool:
        return self._score() <= float(cfg.get("short_threshold", -0.55))