from typing import Any, Dict
import pandas as pd

class Strategy:
    name = "base"

    def prepare(self, df: pd.DataFrame, cfg: Dict[str, Any]) -> pd.DataFrame:
        return df

    def long_signal(self, row: pd.Series, cfg: Dict[str, Any]) -> bool:
        return False

    def short_signal(self, row: pd.Series, cfg: Dict[str, Any]) -> bool:
        return False