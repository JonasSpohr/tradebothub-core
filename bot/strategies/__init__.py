from bot.strategies.trend import TrendStrategy
from bot.strategies.breakout import BreakoutStrategy
from bot.strategies.sentiment import SentimentStrategy

_REG = {
    "trend": TrendStrategy(),
    "breakout": BreakoutStrategy(),
    "sentiment": SentimentStrategy(),
}

def get_strategy(name: str):
    key = (name or "").strip().lower()
    if key not in _REG:
        raise RuntimeError(f"Unknown strategy: {name}")
    return _REG[key]