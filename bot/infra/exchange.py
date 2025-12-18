from typing import Optional
import ccxt
import pandas as pd

def create_exchange(exchange_id: str, api_key: str, api_secret: str, password: Optional[str], uid: Optional[str]):
    cls = getattr(ccxt, exchange_id)
    cfg = {"apiKey": api_key, "secret": api_secret, "enableRateLimit": True}
    if password: cfg["password"] = password
    if uid: cfg["uid"] = uid
    return cls(cfg)

def fetch_ohlcv_df(exchange, symbol: str, timeframe: str, limit: int) -> pd.DataFrame:
    ohlcv = exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
    df = pd.DataFrame(ohlcv, columns=["time","open","high","low","close","volume"])
    df["time"] = pd.to_datetime(df["time"], unit="ms", utc=True)
    df.set_index("time", inplace=True)
    return df

def fetch_last_price(exchange, symbol: str) -> float:
    t = exchange.fetch_ticker(symbol)
    return float(t.get("last") or t.get("close"))

def fetch_quote_balance(exchange, symbol: str) -> float:
    bal = exchange.fetch_balance()
    quote = symbol.split("/")[1] if "/" in symbol else symbol.split("-")[1]
    return float(bal[quote]["free"])