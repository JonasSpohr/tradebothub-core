from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Iterable, Tuple, Union

import numpy as np
import pandas as pd

IndicatorFn = Callable[
    [pd.DataFrame, int, str, Dict[str, Any]],
    Union[pd.Series, Dict[str, pd.Series]],
]


@dataclass(frozen=True)
class IndicatorSpec:
    id: str
    fn: IndicatorFn
    default_window: int
    min_window: int = 1
    max_window: int = 2000
    description: str = ""
    outputs: Tuple[str, ...] = field(default_factory=lambda: ("value",))
    params: Tuple[str, ...] = field(default_factory=tuple)
    requires_volume: bool = False


def _resolve_series(df: pd.DataFrame, source: str) -> pd.Series:
    if source in df.columns:
        return df[source]
    if source == "hl2":
        return (df["high"] + df["low"]) / 2
    if source == "hlc3":
        return (df["high"] + df["low"] + df["close"]) / 3
    if source == "ohlc4":
        return (df["open"] + df["high"] + df["low"] + df["close"]) / 4
    if source == "volume" and "volume" in df.columns:
        return df["volume"]
    return df["close"]


def _safe_window(value: Any, default: int, min_v: int, max_v: int) -> int:
    try:
        val = int(value)
    except Exception:
        val = default
    return max(min_v, min(max_v, val))


def _sma(df, window, source, params):
    series = _resolve_series(df, source)
    return series.rolling(window, min_periods=window).mean()


def _ema(df, window, source, params):
    series = _resolve_series(df, source)
    return series.ewm(span=window, adjust=False, min_periods=window).mean()


def _wma(df, window, source, params):
    series = _resolve_series(df, source)
    weights = np.arange(1, window + 1)
    numerator = (series * weights).rolling(window, min_periods=window).sum()
    return numerator / weights.sum()


def _dema(df, window, source, params):
    first = _ema(df, window, source, params)
    second = first.ewm(span=window, adjust=False, min_periods=window).mean()
    return 2 * first - second


def _tema(df, window, source, params):
    e1 = _ema(df, window, source, params)
    e2 = e1.ewm(span=window, adjust=False, min_periods=window).mean()
    e3 = e2.ewm(span=window, adjust=False, min_periods=window).mean()
    return 3 * e1 - 3 * e2 + e3


def _hma(df, window, source, params):
    half = max(1, window // 2)
    sqrt = max(1, int(math.sqrt(window)))
    wma_half = _wma(df, half, source, params)
    wma_full = _wma(df, window, source, params)
    raw = 2 * wma_half - wma_full
    return raw.rolling(sqrt, min_periods=sqrt).mean()


def _vwma(df, window, source, params):
    if "volume" not in df.columns:
        return pd.Series([np.nan] * len(df), index=df.index)
    price = _resolve_series(df, source)
    vol = df["volume"]
    num = (price * vol).rolling(window, min_periods=window).sum()
    den = vol.rolling(window, min_periods=window).sum()
    den = den.replace(0, np.nan)
    return num / den


def _kama(df, window, source, params):
    series = _resolve_series(df, source)
    change = series.diff(window).abs()
    vol = series.diff().abs().rolling(window, min_periods=window).sum()
    er = change / vol
    base_fast = 2 / (params.get("fast", 2) + 1)
    base_slow = 2 / (params.get("slow", 30) + 1)
    sc = (er * (base_fast - base_slow) + base_slow) ** 2
    kama = series.copy()
    kama.iloc[:window] = np.nan
    for i in range(window, len(series)):
        prev = kama.iloc[i - 1]
        kama.iloc[i] = prev + sc.iloc[i] * (series.iloc[i] - prev)
    return kama


def _alma(df, window, source, params):
    offset = float(params.get("offset", 0.85))
    sigma = float(params.get("sigma", 6.0))
    series = _resolve_series(df, source)
    m = offset * (window - 1)
    s = window / sigma
    weights = np.array([math.exp(-((i - m) ** 2) / (2 * s * s)) for i in range(window)])
    weights /= weights.sum()
    values = series.rolling(window, min_periods=window).apply(
        lambda x: np.dot(x, weights), raw=True
    )
    return values


def _rma(df, window, source, params):
    series = _resolve_series(df, source)
    alpha = 1 / window
    return series.ewm(alpha=alpha, adjust=False, min_periods=window).mean()


def _lsma(df, window, source, params):
    series = _resolve_series(df, source)
    x = np.arange(window)
    result = series.copy()
    for i in range(window - 1, len(series)):
        y = series.iloc[i - window + 1 : i + 1]
        if y.isna().any():
            result.iloc[i] = np.nan
            continue
        coef = np.polyfit(x, y.values, 1)
        result.iloc[i] = coef[0] * (window - 1) + coef[1]
    return result


def _macd(df, window, source, params):
    fast = params.get("fast", 12)
    slow = params.get("slow", 26)
    signal_len = params.get("signal", 9)
    fast_series = _ema(df, fast, source, params)
    slow_series = _ema(df, slow, source, params)
    macd = fast_series - slow_series
    signal = macd.ewm(span=signal_len, adjust=False).mean()
    hist = macd - signal
    return {"macd": macd, "signal": signal, "hist": hist}


def _stoch(df, window, source, params):
    high = df["high"].rolling(window, min_periods=window).max()
    low = df["low"].rolling(window, min_periods=window).min()
    close = _resolve_series(df, source)
    denom = (high - low).replace(0, np.nan)
    k = 100 * (close - low) / denom
    d = k.rolling(params.get("d_length", 3), min_periods=1).mean()
    return {"k": k, "d": d}


def _cci(df, window, source, params):
    tp = (_resolve_series(df, source) + df["high"] + df["low"] + df["close"]) / 4
    sma = tp.rolling(window, min_periods=window).mean()
    mad = tp.rolling(window, min_periods=window).apply(
        lambda x: np.mean(np.abs(x - np.mean(x))), raw=True
    )
    denom = 0.015 * mad.replace(0, np.nan)
    return (tp - sma) / denom


def _roc(df, window, source, params):
    series = _resolve_series(df, source)
    return 100 * (series / series.shift(window) - 1)


def _mom(df, window, source, params):
    series = _resolve_series(df, source)
    return series - series.shift(window)


def _willr(df, window, source, params):
    high = df["high"].rolling(window, min_periods=window).max()
    low = df["low"].rolling(window, min_periods=window).min()
    close = df["close"]
    denom = (high - low).replace(0, np.nan)
    return -100 * (high - close) / denom


def _atr(df, window, source, params):
    high, low, close = df["high"], df["low"], df["close"]
    prev = close.shift(1)
    tr = pd.concat(
        [(high - low), (high - prev).abs(), (low - prev).abs()], axis=1
    ).max(axis=1)
    return tr.ewm(alpha=1 / window, adjust=False, min_periods=window).mean()


def _bbands(df, window, source, params):
    series = _resolve_series(df, source)
    mid = series.rolling(window, min_periods=window).mean()
    std = series.rolling(window, min_periods=window).std(ddof=0)
    mult = params.get("std", 2)
    upper = mid + std * mult
    lower = mid - std * mult
    width = (upper - lower) / mid.replace(0, np.nan)
    return {"mid": mid, "upper": upper, "lower": lower, "width": width}


def _donchian(df, window, source, params):
    high = df["high"].rolling(window, min_periods=window).max()
    low = df["low"].rolling(window, min_periods=window).min()
    mid = (high + low) / 2
    return {"high": high, "low": low, "mid": mid}


def _tr(df, window, source, params):
    high, low, close = df["high"], df["low"], df["close"]
    prev = close.shift(1)
    tr = pd.concat([(high - low), (high - prev).abs(), (low - prev).abs()], axis=1).max(axis=1)
    return tr


def _keltner(df, window, source, params):
    length = window
    mult = params.get("mult", 1.5)
    mid = df["close"].ewm(span=length, adjust=False, min_periods=length).mean()
    atr = _atr(df, length, source, params)
    upper = mid + atr * mult
    lower = mid - atr * mult
    return {"mid": mid, "upper": upper, "lower": lower}


def _supertrend(df, window, source, params):
    mult = params.get("mult", 3.0)
    atr = _atr(df, window, source, params)
    hl2 = (df["high"] + df["low"]) / 2
    upper = hl2 + mult * atr
    lower = hl2 - mult * atr
    final_upper = upper.copy()
    final_lower = lower.copy()
    direction = pd.Series(1, index=df.index)
    for i in range(1, len(df)):
        if df["close"].iat[i - 1] <= final_upper.iat[i - 1]:
            final_upper.iat[i] = min(upper.iat[i], final_upper.iat[i - 1])
        else:
            final_upper.iat[i] = upper.iat[i]
        if df["close"].iat[i - 1] >= final_lower.iat[i - 1]:
            final_lower.iat[i] = max(lower.iat[i], final_lower.iat[i - 1])
        else:
            final_lower.iat[i] = lower.iat[i]
        if df["close"].iat[i] <= final_upper.iat[i]:
            direction.iat[i] = -1
        else:
            direction.iat[i] = 1
    trend = final_lower.where(direction > 0, final_upper)
    return {"supertrend": trend, "direction": direction}


def _psar(df, window, source, params):
    step = params.get("step", 0.02)
    max_step = params.get("max_step", 0.2)
    length = len(df)
    psar = pd.Series(index=df.index, dtype=float)
    direction = pd.Series(1, index=df.index)
    af = step
    ep = df["high"].iloc[0]
    psar.iloc[0] = df["low"].iloc[0]
    for i in range(1, length):
        prev = psar.iloc[i - 1]
        if direction.iloc[i - 1] > 0:
            psar.iloc[i] = prev + af * (ep - prev)
        else:
            psar.iloc[i] = prev + af * (ep - prev)
        if direction.iloc[i - 1] > 0:
            if df["low"].iloc[i] < psar.iloc[i]:
                direction.iloc[i] = -1
                psar.iloc[i] = ep
                af = step
                ep = df["low"].iloc[i]
            else:
                direction.iloc[i] = 1
                if df["high"].iloc[i] > ep:
                    ep = df["high"].iloc[i]
                    af = min(max_step, af + step)
        else:
            if df["high"].iloc[i] > psar.iloc[i]:
                direction.iloc[i] = 1
                psar.iloc[i] = ep
                af = step
                ep = df["high"].iloc[i]
            else:
                direction.iloc[i] = -1
                if df["low"].iloc[i] < ep:
                    ep = df["low"].iloc[i]
                    af = min(max_step, af + step)
    return {"psar": psar, "direction": direction}


def _adx(df, window, source, params):
    high = df["high"]
    low = df["low"]
    close = df["close"]
    tr = _tr(df, window, source, params)
    plus_dm = high.diff().clip(lower=0)
    minus_dm = -(low.diff()).clip(lower=0)
    plus_dm = plus_dm.where(plus_dm > minus_dm, 0)
    minus_dm = minus_dm.where(minus_dm > plus_dm, 0)
    smooth_tr = tr.ewm(alpha=1 / window, adjust=False).mean()
    plus = plus_dm.ewm(alpha=1 / window, adjust=False).mean() / smooth_tr
    minus = minus_dm.ewm(alpha=1 / window, adjust=False).mean() / smooth_tr
    dx = 100 * (plus - minus).abs() / (plus + minus).replace(0, np.nan)
    adx = dx.ewm(alpha=1 / window, adjust=False).mean()
    return {"adx": adx, "di_plus": 100 * plus, "di_minus": 100 * minus}


def _aroon(df, window, source, params):
    length = window
    highs = df["high"]
    lows = df["low"]
    aroon_up = highs.rolling(length, min_periods=length).apply(lambda x: (length - np.argmax(x[::-1])) / length * 100, raw=True)
    aroon_down = lows.rolling(length, min_periods=length).apply(lambda x: (length - np.argmin(x[::-1])) / length * 100, raw=True)
    return {"aroon_up": aroon_up, "aroon_down": aroon_down, "aroon_osc": aroon_up - aroon_down}


def _vortex(df, window, source, params):
    high = df["high"]
    low = df["low"]
    close = df["close"]
    tr = _tr(df, window, source, params)
    vm_plus = (high - low.shift(1)).abs().rolling(window, min_periods=window).sum()
    vm_minus = (low - high.shift(1)).abs().rolling(window, min_periods=window).sum()
    tr_sum = tr.rolling(window, min_periods=window).sum()
    return {"vi_plus": vm_plus / tr_sum, "vi_minus": vm_minus / tr_sum}


def _obv(df, window, source, params):
    close = df["close"]
    vol = df.get("volume", pd.Series([0] * len(df), index=df.index))
    direction = np.sign(close.diff()).fillna(0)
    return (vol * direction).cumsum()


def _vol_sma(df, window, source, params):
    if "volume" not in df:
        return pd.Series([np.nan] * len(df), index=df.index)
    return df["volume"].rolling(window, min_periods=window).mean()


def _rvol(df, window, source, params):
    if "volume" not in df:
        return pd.Series([np.nan] * len(df), index=df.index)
    vol = df["volume"]
    avg = vol.rolling(window, min_periods=window).mean()
    return vol / avg


def _vwap(df, window, source, params):
    if "volume" not in df:
        return pd.Series([np.nan] * len(df), index=df.index)
    price = _resolve_series(df, source)
    pv = price * df["volume"]
    cum_pv = pv.cumsum()
    cum_vol = df["volume"].cumsum()
    return cum_pv / cum_vol.replace(0, np.nan)


def _mfi(df, window, source, params):
    if "volume" not in df:
        return pd.Series([np.nan] * len(df), index=df.index)
    tp = (_resolve_series(df, source) + df["high"] + df["low"]) / 3
    change = tp.diff()
    pos = (change > 0) * tp * df["volume"]
    neg = (change < 0) * tp * df["volume"]
    pos_sum = pos.rolling(window, min_periods=window).sum()
    neg_sum = (-neg).rolling(window, min_periods=window).sum()
    mfr = pos_sum / neg_sum.replace(0, np.nan)
    return 100 - 100 / (1 + mfr)


def _cmf(df, window, source, params):
    if "volume" not in df:
        return pd.Series([np.nan] * len(df), index=df.index)
    mfm = ((df["close"] - df["low"]) - (df["high"] - df["close"])) / (df["high"] - df["low"]).replace(0, np.nan)
    mfv = mfm * df["volume"]
    return mfv.rolling(window, min_periods=window).sum() / df["volume"].rolling(window, min_periods=window).sum()


def _adl(df, window, source, params):
    if "volume" not in df:
        return pd.Series([np.nan] * len(df), index=df.index)
    mfm = ((df["close"] - df["low"]) - (df["high"] - df["close"])) / (df["high"] - df["low"]).replace(0, np.nan)
    return (mfm * df["volume"]).cumsum()


def _adosc(df, window, source, params):
    adl = _adl(df, window, source, params)
    fast = params.get("fast", 3)
    slow = params.get("slow", 10)
    return _ema(adl.to_frame("adl"), slow, "adl", {}) - _ema(adl.to_frame("adl"), fast, "adl", {})


def _force(df, window, source, params):
    if "volume" not in df:
        return pd.Series([np.nan] * len(df), index=df.index)
    fi = (df["close"] - df["close"].shift(1)) * df["volume"]
    return _ema(fi.to_frame("fi"), window, "fi", {})


def _eom(df, window, source, params):
    if "volume" not in df:
        return pd.Series([np.nan] * len(df), index=df.index)
    dm = ((df["high"] + df["low"]) / 2) - ((df["high"].shift(1) + df["low"].shift(1)) / 2)
    br = (df["volume"] / (df["high"] - df["low"]).replace(0, np.nan)).replace(np.inf, np.nan)
    eom = dm / br
    return eom.rolling(window, min_periods=window).mean()


def _vroc(df, window, source, params):
    if "volume" not in df:
        return pd.Series([np.nan] * len(df), index=df.index)
    vol = df["volume"]
    return 100 * (vol / vol.shift(window) - 1)


def _nvi(df, window, source, params):
    if "volume" not in df:
        return pd.Series([np.nan] * len(df), index=df.index)
    vol = df["volume"]
    close = df["close"]
    nvi = pd.Series(np.nan, index=df.index)
    nvi.iloc[0] = 1000
    for i in range(1, len(df)):
        if vol.iat[i] < vol.iat[i - 1]:
            nvi.iat[i] = nvi.iat[i - 1] * (1 + (close.iat[i] - close.iat[i - 1]) / close.iat[i - 1])
        else:
            nvi.iat[i] = nvi.iat[i - 1]
    return nvi


def _pvi(df, window, source, params):
    if "volume" not in df:
        return pd.Series([np.nan] * len(df), index=df.index)
    vol = df["volume"]
    close = df["close"]
    pvi = pd.Series(np.nan, index=df.index)
    pvi.iloc[0] = 1000
    for i in range(1, len(df)):
        if vol.iat[i] > vol.iat[i - 1]:
            pvi.iat[i] = pvi.iat[i - 1] * (1 + (close.iat[i] - close.iat[i - 1]) / close.iat[i - 1])
        else:
            pvi.iat[i] = pvi.iat[i - 1]
    return pvi


def _kvo(df, window, source, params):
    hl = df["high"] + df["low"] + df["close"]
    trend = np.sign(hl - hl.shift(1)).fillna(0)
    dm = df["high"] - df["low"]
    cm = dm.cumsum()
    vf = df["volume"] * trend * (2 * dm / cm.replace(0, np.nan) - 1).abs() * 100
    fast = params.get("fast", 34)
    slow = params.get("slow", 55)
    signal_len = params.get("signal", 13)
    kvo = _ema(vf.to_frame("vf"), fast, "vf", {})
    slow_kvo = _ema(vf.to_frame("vf"), slow, "vf", {})
    signal = (kvo - slow_kvo).ewm(span=signal_len, adjust=False).mean()
    hist = kvo - signal
    return {"kvo": kvo, "kvo_signal": signal, "kvo_hist": hist}


def _dpo(df, window, source, params):
    shift = window // 2 + 1
    series = _resolve_series(df, source)
    sma = series.shift(shift).rolling(window, min_periods=window).mean()
    return series.shift(shift) - sma


def _rvi(df, window, source, params):
    num = df["close"] - df["open"]
    den = df["high"] - df["low"]
    num_ma = num.ewm(span=window, adjust=False, min_periods=window).mean()
    den_ma = den.ewm(span=window, adjust=False, min_periods=window).mean()
    rvi = num_ma / den_ma.replace(0, np.nan)
    signal = rvi.ewm(span=4, adjust=False).mean()
    return {"rvi": rvi, "rvi_signal": signal}


def _fisher(df, window, source, params):
    series = _resolve_series(df, source)
    max_ = series.rolling(window, min_periods=window).max()
    min_ = series.rolling(window, min_periods=window).min()
    value = (series - min_) / (max_ - min_).replace(0, np.nan)
    value = 2 * (value - 0.5)
    value = value.clip(-0.999, 0.999)
    v = value.copy()
    fisher = 0.5 * np.log((1 + v) / (1 - v))
    signal = fisher.shift(1)
    return {"fisher": fisher, "fisher_signal": signal}


def _cmo(df, window, source, params):
    series = _resolve_series(df, source)
    delta = series.diff()
    up = delta.clip(lower=0).rolling(window, min_periods=window).sum()
    down = (-delta.clip(upper=0)).rolling(window, min_periods=window).sum()
    return 100 * (up - down) / (up + down).replace(0, np.nan)


def _tsi(df, window, source, params):
    series = _resolve_series(df, source)
    long = params.get("long", 25)
    short = params.get("short", 13)
    signal_len = params.get("signal", 7)
    m = series.diff()
    ema1 = m.ewm(span=short, adjust=False, min_periods=short).mean()
    ema2 = ema1.ewm(span=long, adjust=False, min_periods=long).mean()
    ema1a = m.abs().ewm(span=short, adjust=False, min_periods=short).mean()
    ema2a = ema1a.ewm(span=long, adjust=False, min_periods=long).mean()
    tsi = 100 * ema2 / ema2a.replace(0, np.nan)
    signal = tsi.ewm(span=signal_len, adjust=False, min_periods=signal_len).mean()
    return {"tsi": tsi, "tsi_signal": signal}


def _ppo(df, window, source, params):
    series = _resolve_series(df, source)
    fast = params.get("fast", 12)
    slow = params.get("slow", 26)
    signal_len = params.get("signal", 9)
    ema_fast = _ema(df, fast, source, params)
    ema_slow = _ema(df, slow, source, params)
    ppo = 100 * (ema_fast - ema_slow) / ema_slow.replace(0, np.nan)
    signal = ppo.ewm(span=signal_len, adjust=False, min_periods=signal_len).mean()
    hist = ppo - signal
    return {"ppo": ppo, "ppo_signal": signal, "ppo_hist": hist}


def _elder(df, window, source, params):
    ema = _ema(df, window, source, params)
    bull = df["high"] - ema
    bear = df["low"] - ema
    return {"bull_power": bull, "bear_power": bear}


def _connors(df, window, source, params):
    close = _resolve_series(df, source)
    rsi1 = _rsi(df, window, source, params)
    streak = close.diff().gt(0).cumsum()
    streak = streak.where(close.diff().ge(0), 0)
    streak_rsi = streak.rolling(params.get("streak_rsi_len", 2), min_periods=1).apply(
        lambda s: (s.max() - s.min()) if len(s) else 0, raw=True
    )
    roc_rank = close.diff().rank(pct=True)
    return (rsi1 + streak_rsi + roc_rank) / 3


def _gap(df, window, source, params):
    prev_close = df["close"].shift(1)
    gap = (df["open"] - prev_close).abs()
    atr_series = _atr(df, params.get("atr_length", window), source, params)
    gap_atr = gap / atr_series.replace(0, np.nan)
    flag = gap_atr > params.get("mult", 1.5)
    return {"gap_flag": flag, "gap_size_atr": gap_atr}


def _fractal(df, window, source, params):
    left = params.get("n", 2)
    right = params.get("n", 2)
    highs = df["high"]
    lows = df["low"]
    high_fractal = highs[(highs.shift(1) < highs) & (highs.shift(-1) < highs)]
    low_fractal = lows[(lows.shift(1) > lows) & (lows.shift(-1) > lows)]
    return {"fractal_high": high_fractal.notna().astype(float), "fractal_low": low_fractal.notna().astype(float)}


def _swing(df, window, source, params):
    left = params.get("left", 3)
    right = params.get("right", 3)
    highs = df["high"]
    lows = df["low"]
    swing_high = highs[
        (highs.shift(1) < highs) & (highs.shift(-1) < highs) & (highs.shift(left) < highs) & (highs.shift(-right) < highs)
    ]
    swing_low = lows[
        (lows.shift(1) > lows) & (lows.shift(-1) > lows) & (lows.shift(left) > lows) & (lows.shift(-right) > lows)
    ]
    last_high = swing_high.ffill()
    last_low = swing_low.ffill()
    return {"swing_high": swing_high.notna().astype(float), "swing_low": swing_low.notna().astype(float), "last_swing_high_price": last_high, "last_swing_low_price": last_low}
def _mass_index(df, window, source, params):
    hl = (df["high"] - df["low"]).rolling(window, min_periods=window).mean()
    hl_ema = hl.ewm(span=window, adjust=False).mean()
    hl_ema2 = hl_ema.ewm(span=window, adjust=False).mean()
    ratio = hl_ema / hl_ema2
    return ratio.rolling(params.get("sum_length", 25), min_periods=window).sum()


def _chop(df, window, source, params):
    tr = _tr(df, window, source, params).rolling(window, min_periods=window).sum()
    high = df["high"].rolling(window, min_periods=window).max()
    low = df["low"].rolling(window, min_periods=window).min()
    range_ = (high - low).replace(0, np.nan)
    return 100 * np.log10(tr / range_) / np.log10(window)


def _stoch_rsi(df, window, source, params):
    rsi_series = _rsi(df, window, source, params)
    stoch = (rsi_series - rsi_series.rolling(window, min_periods=window).min()) / (
        rsi_series.rolling(window, min_periods=window).max() - rsi_series.rolling(window, min_periods=window).min()
    )
    k = stoch * 100
    d = k.rolling(params.get("d_smooth", 3), min_periods=1).mean()
    return {"stochrsi_k": k, "stochrsi_d": d}

def _heikin_ashi(df, window, source, params):
    ha_close = (df["open"] + df["high"] + df["low"] + df["close"]) / 4
    ha_open = ha_close.copy()
    for i in range(1, len(df)):
        ha_open.iat[i] = (ha_open.iat[i - 1] + ha_close.iat[i - 1]) / 2
    ha_high = pd.concat([df["high"], ha_open, ha_close], axis=1).max(axis=1)
    ha_low = pd.concat([df["low"], ha_open, ha_close], axis=1).min(axis=1)
    return {"ha_open": ha_open, "ha_close": ha_close, "ha_high": ha_high, "ha_low": ha_low}


def _t3(df, window, source, params):
    vfactor = params.get("vfactor", 0.7)
    def ema(series, w):
        return series.ewm(span=w, adjust=False, min_periods=w).mean()

    series = _resolve_series(df, source)
    e1 = ema(series, window)
    e2 = ema(e1, window)
    e3 = ema(e2, window)
    e4 = ema(e3, window)
    e5 = ema(e4, window)
    e6 = ema(e5, window)
    c1 = -vfactor ** 3
    c2 = 3 * vfactor ** 2 + 3 * vfactor ** 3
    c3 = -6 * vfactor ** 2 - 3 * vfactor - 3 * vfactor ** 3
    c4 = 1 + 3 * vfactor + vfactor ** 3 + 3 * vfactor ** 2
    return c1 * e6 + c2 * e5 + c3 * e4 + c4 * e3


def _ichimoku(df, window, source, params):
    tenkan_len = params.get("tenkan_len", 9)
    kijun_len = params.get("kijun_len", 26)
    span_shift = params.get("shift", 26)
    series = _resolve_series(df, source)
    tenkan = (df["high"].rolling(tenkan_len, min_periods=tenkan_len).max() + df["low"].rolling(tenkan_len, min_periods=tenkan_len).min()) / 2
    kijun = (df["high"].rolling(kijun_len, min_periods=kijun_len).max() + df["low"].rolling(kijun_len, min_periods=kijun_len).min()) / 2
    span_a = ((tenkan + kijun) / 2).shift(span_shift)
    span_b = ((df["high"].rolling(52, min_periods=52).max() + df["low"].rolling(52, min_periods=52).min()) / 2).shift(span_shift)
    chikou = series.shift(-span_shift)
    return {
        "tenkan": tenkan,
        "kijun": kijun,
        "span_a": span_a,
        "span_b": span_b,
        "chikou": chikou,
    }


def _ma_slope(df, window, source, params):
    ma = _ema(df, window, source, params)
    lookback = params.get("lookback", window)
    slope = (ma - ma.shift(lookback)) / lookback
    normalize = params.get("normalize")
    if normalize == "price":
        base = _resolve_series(df, source)
        slope = slope / base.replace(0, np.nan)
    elif normalize == "atr":
        atr_length = params.get("atr_length", window)
        atr_series = _atr(df, atr_length, source, params)
        slope = slope / atr_series.replace(0, np.nan)
    return slope


def _price_vs_ma(df, window, source, params):
    ma = _ema(df, window, source, params)
    atr_length = params.get("atr_length", window)
    atr_series = _atr(df, atr_length, source, params)
    return (_resolve_series(df, source) - ma) / atr_series.replace(0, np.nan)


def _ma_ribbon(df, window, source, params):
    lengths = params.get("lengths", [8, 13, 21])
    emas = [ _ema(df, int(length), source, params) for length in lengths ]
    stacked = sum((emas[i] > emas[i + 1]).astype(int) for i in range(len(emas) - 1)) / max(len(emas) - 1, 1)
    spread = (max(emas) - min(emas)) / _resolve_series(df, source).replace(0, np.nan)
    return {"stack_score": stacked, "spread": spread}


def _pivot_points(df, window, source, params):
    lookback = params.get("period", 1)
    highs = df["high"].shift(1).rolling(lookback, min_periods=lookback).max()
    lows = df["low"].shift(1).rolling(lookback, min_periods=lookback).min()
    close = df["close"].shift(1)
    pp = (highs + lows + close) / 3
    r1 = 2 * pp - lows
    s1 = 2 * pp - highs
    r2 = pp + (highs - lows)
    s2 = pp - (highs - lows)
    r3 = highs + 2 * (pp - lows)
    s3 = lows - 2 * (highs - pp)
    return {
        "pivot": pp,
        "r1": r1,
        "r2": r2,
        "r3": r3,
        "s1": s1,
        "s2": s2,
        "s3": s3,
    }


def _fib_retracement(df, window, source, params):
    lookback = params.get("lookback", 20)
    high = df["high"].rolling(lookback, min_periods=lookback).max()
    low = df["low"].rolling(lookback, min_periods=lookback).min()
    diff = high - low
    levels = params.get("levels", [0.236, 0.382, 0.5, 0.618, 0.786])
    base = low
    return {f"fib_{int(level*1000)}": base + diff * level for level in levels}


def _linear_regression_channel(df, window, source, params):
    series = _resolve_series(df, source)
    lines = []
    for i in range(window - 1, len(df)):
        window_series = series.iloc[i - window + 1 : i + 1]
        if window_series.isna().any():
            lines.append((np.nan, np.nan, np.nan))
            continue
        x = np.arange(window)
        coef = np.polyfit(x, window_series.values, 1)
        fit = coef[0] * x + coef[1]
        resid_std = np.std(window_series.values - fit)
        mid = coef[0] * (window - 1) + coef[1]
        lines.append(
            (
                mid,
                mid + params.get("std_mult", 2) * resid_std,
                mid - params.get("std_mult", 2) * resid_std,
            )
        )
    mids, uppers, lowers = zip(*lines)
    return {"lrc_mid": pd.Series(mids, index=df.index), "lrc_upper": pd.Series(uppers, index=df.index), "lrc_lower": pd.Series(lowers, index=df.index)}


def _atr_channel(df, window, source, params):
    ema_len = params.get("ema_length", window)
    mult = params.get("mult", 2)
    mid = _ema(df, ema_len, source, params)
    atr_series = _atr(df, params.get("atr_length", window), source, params)
    upper = mid + mult * atr_series
    lower = mid - mult * atr_series
    return {"atr_mid": mid, "atr_upper": upper, "atr_lower": lower}


def _price_channels(df, window, source, params):
    high = df["high"].rolling(window, min_periods=window).max()
    low = df["low"].rolling(window, min_periods=window).min()
    return {"pc_high": high, "pc_low": low}


def _chandelier(df, window, source, params):
    mult = params.get("mult", 3)
    lookback = params.get("lookback", window)
    atr_series = _atr(df, window, source, params)
    highest = df["high"].rolling(lookback, min_periods=lookback).max()
    lowest = df["low"].rolling(lookback, min_periods=lookback).min()
    long_exit = highest - atr_series * mult
    short_exit = lowest + atr_series * mult
    return {"chand_long": long_exit, "chand_short": short_exit}


def _coppock(df, window, source, params):
    roc_long = params.get("roc_long", 14)
    roc_short = params.get("roc_short", 11)
    wma_length = params.get("wma_length", 10)
    close = _resolve_series(df, source)
    roc_long_series = (close / close.shift(roc_long) - 1) * 100
    roc_short_series = (close / close.shift(roc_short) - 1) * 100
    combined = roc_long_series + roc_short_series
    weights = np.arange(1, wma_length + 1)
    return (_resolve_series(df, source).rolling(wma_length).apply(lambda vals: np.dot(vals, weights) / weights.sum(), raw=True))


def _std(df, window, source, params):
    series = _resolve_series(df, source)
    return series.rolling(window, min_periods=window).std(ddof=0)


def _hist_vol(df, window, source, params):
    series = _resolve_series(df, source)
    log_ret = np.log(series / series.shift(1))
    hv = log_ret.rolling(window, min_periods=window).std(ddof=0)
    if params.get("annualize"):
        periods = params.get("periods_per_year", 365)
        hv = hv * math.sqrt(periods)
    return hv


def _chaikin_vol(df, window, source, params):
    hl = (df["high"] - df["low"])
    ema_short = hl.ewm(span=window, adjust=False, min_periods=window).mean()
    ema_long = ema_short.ewm(span=params.get("roc_length", 10), adjust=False, min_periods=params.get("roc_length", 10)).mean()
    return 100 * (ema_short / ema_long - 1)


def _anchored_vwap(df, window, source, params):
    anchor = params.get("anchor_index", 0)
    price = _resolve_series(df, source)
    vol = df.get("volume", pd.Series([np.nan] * len(df), index=df.index))
    anchor_idx = max(0, min(len(df) - 1, anchor))
    cum_price = (price * vol).iloc[anchor_idx:].cumsum()
    cum_vol = vol.iloc[anchor_idx:].cumsum().replace(0, np.nan)
    vwap = cum_price / cum_vol
    result = pd.Series(np.nan, index=df.index)
    result.iloc[anchor_idx:] = vwap.values
    return result


def _zigzag(df, window, source, params):
    price = _resolve_series(df, source)
    deviation = params.get("deviation_pct", 5.0) / 100
    last_extreme = price.iloc[0]
    last_dir = 0
    zigzag = pd.Series(np.nan, index=df.index)
    for i in range(1, len(price)):
        change = (price.iloc[i] - last_extreme) / last_extreme
        if last_dir >= 0 and change <= -deviation:
            last_dir = -1
            last_extreme = price.iloc[i]
        elif last_dir <= 0 and change >= deviation:
            last_dir = 1
            last_extreme = price.iloc[i]
        zigzag.iloc[i] = last_extreme
    return {"zigzag": zigzag}


def _kaufman_er(df, window, source, params):
    series = _resolve_series(df, source)
    change = (series.diff(window).abs())
    volatility = series.diff().abs().rolling(window, min_periods=window).sum()
    er = change / volatility.replace(0, np.nan)
    return er



INDICATOR_REGISTRY: Dict[str, IndicatorSpec] = {
    "sma": IndicatorSpec(id="sma", fn=_sma, default_window=20, description="Simple moving average"),
    "ema": IndicatorSpec(id="ema", fn=_ema, default_window=20, description="Exponential moving average"),
    "wma": IndicatorSpec(id="wma", fn=_wma, default_window=20, description="Weighted moving average"),
    "dema": IndicatorSpec(id="dema", fn=_dema, default_window=20, description="Double EMA"),
    "tema": IndicatorSpec(id="tema", fn=_tema, default_window=20, description="Triple EMA"),
    "hma": IndicatorSpec(id="hma", fn=_hma, default_window=20, description="Hull MA"),
    "vwma": IndicatorSpec(id="vwma", fn=_vwma, default_window=20, description="Volume weighted MA", requires_volume=True),
    "kama": IndicatorSpec(id="kama", fn=_kama, default_window=10, description="Kaufman Adaptive MA"),
    "alma": IndicatorSpec(id="alma", fn=_alma, default_window=9, description="Arnaud Legoux MA"),
    "rma": IndicatorSpec(id="rma", fn=_rma, default_window=14, description="Wilder's smoothing (RMA)"),
    "lsma": IndicatorSpec(id="lsma", fn=_lsma, default_window=20, description="Linear regression MA"),
    "macd": IndicatorSpec(
        id="macd",
        fn=_macd,
        default_window=26,
        description="MACD bundle (macd/signal/hist)",
        outputs=("macd", "signal", "hist"),
        params=("fast", "slow", "signal"),
    ),
    "stoch": IndicatorSpec(
        id="stoch",
        fn=_stoch,
        default_window=14,
        description="Stochastic oscillator (K/D)",
        outputs=("k", "d"),
        params=("d_length",),
    ),
    "cci": IndicatorSpec(id="cci", fn=_cci, default_window=20, description="Commodity channel index"),
    "roc": IndicatorSpec(id="roc", fn=_roc, default_window=12, description="Rate of change"),
    "mom": IndicatorSpec(id="mom", fn=_mom, default_window=10, description="Momentum"),
    "willr": IndicatorSpec(id="willr", fn=_willr, default_window=14, description="Williams %R"),
    "atr": IndicatorSpec(id="atr", fn=_atr, default_window=14, description="Average true range"),
    "bbands": IndicatorSpec(
        id="bbands",
        fn=_bbands,
        default_window=20,
        description="Bollinger Bands",
        outputs=("mid", "upper", "lower", "width"),
    ),
    "donchian": IndicatorSpec(
        id="donchian",
        fn=_donchian,
        default_window=20,
        description="Donchian Channels",
        outputs=("high", "low", "mid"),
    ),
    "tr": IndicatorSpec(id="tr", fn=_tr, default_window=1, description="True range"),
    "keltner": IndicatorSpec(
        id="keltner",
        fn=_keltner,
        default_window=20,
        description="Keltner Channels",
        outputs=("mid", "upper", "lower"),
    ),
    "supertrend": IndicatorSpec(
        id="supertrend",
        fn=_supertrend,
        default_window=10,
        description="Supertrend",
        outputs=("supertrend", "direction"),
    ),
    "psar": IndicatorSpec(
        id="psar",
        fn=_psar,
        default_window=5,
        description="Parabolic SAR",
        outputs=("psar", "direction"),
    ),
    "adx": IndicatorSpec(
        id="adx",
        fn=_adx,
        default_window=14,
        description="ADX + DI bundle",
        outputs=("adx", "di_plus", "di_minus"),
    ),
    "aroon": IndicatorSpec(
        id="aroon",
        fn=_aroon,
        default_window=25,
        description="Aroon oscillator",
        outputs=("aroon_up", "aroon_down", "aroon_osc"),
    ),
    "vortex": IndicatorSpec(
        id="vortex",
        fn=_vortex,
        default_window=14,
        description="Vortex indicator (VI+/-)",
        outputs=("vi_plus", "vi_minus"),
    ),
    "mass": IndicatorSpec(
        id="mass",
        fn=_mass_index,
        default_window=9,
        description="Mass index",
    ),
    "chop": IndicatorSpec(
        id="chop",
        fn=_chop,
        default_window=14,
        description="Choppiness index",
    ),
    "stoch_rsi": IndicatorSpec(
        id="stoch_rsi",
        fn=_stoch_rsi,
        default_window=14,
        description="Stochastic RSI",
        outputs=("stochrsi_k", "stochrsi_d"),
        params=("d_smooth",),
    ),
    "heikin_ashi": IndicatorSpec(
        id="heikin_ashi",
        fn=_heikin_ashi,
        default_window=2,
        description="Heikin-Ashi candles",
        outputs=("ha_open", "ha_close", "ha_high", "ha_low"),
    ),
    "t3": IndicatorSpec(id="t3", fn=_t3, default_window=20, description="T3 moving average"),
    "ichimoku": IndicatorSpec(
        id="ichimoku",
        fn=_ichimoku,
        default_window=26,
        description="Ichimoku components",
        outputs=("tenkan", "kijun", "span_a", "span_b", "chikou"),
    ),
    "ma_slope": IndicatorSpec(
        id="ma_slope",
        fn=_ma_slope,
        default_window=14,
        description="MA slope / normalized",
        params=("lookback", "normalize", "atr_length"),
    ),
    "price_vs_ma": IndicatorSpec(
        id="price_vs_ma",
        fn=_price_vs_ma,
        default_window=20,
        description="Price distance from MA in ATR units",
        params=("atr_length",),
    ),
    "ma_ribbon": IndicatorSpec(
        id="ma_ribbon",
        fn=_ma_ribbon,
        default_window=21,
        description="MA ribbon stack score + spread",
        outputs=("stack_score", "spread"),
        params=("lengths",),
    ),
    "obv": IndicatorSpec(id="obv", fn=_obv, default_window=1, description="On Balance Volume"),
    "vol_sma": IndicatorSpec(id="vol_sma", fn=_vol_sma, default_window=20, description="Volume SMA"),
    "rvol": IndicatorSpec(id="rvol", fn=_rvol, default_window=20, description="Relative volume"),
    "vwap": IndicatorSpec(id="vwap", fn=_vwap, default_window=1, description="VWAP"),
    "mfi": IndicatorSpec(id="mfi", fn=_mfi, default_window=14, description="Money flow index"),
    "cmf": IndicatorSpec(id="cmf", fn=_cmf, default_window=20, description="Chaikin money flow"),
    "adl": IndicatorSpec(id="adl", fn=_adl, default_window=14, description="Accumulation distribution line"),
    "adosc": IndicatorSpec(
        id="adosc",
        fn=_adosc,
        default_window=10,
        description="A/D oscillator",
    ),
    "force": IndicatorSpec(id="force", fn=_force, default_window=13, description="Force index"),
    "eom": IndicatorSpec(id="eom", fn=_eom, default_window=14, description="Ease of movement"),
    "vroc": IndicatorSpec(id="vroc", fn=_vroc, default_window=10, description="Volume ROC"),
    "nvi": IndicatorSpec(id="nvi", fn=_nvi, default_window=2, description="Negative volume index"),
    "pvi": IndicatorSpec(id="pvi", fn=_pvi, default_window=2, description="Positive volume index"),
    "kvo": IndicatorSpec(
        id="kvo",
        fn=_kvo,
        default_window=34,
        description="Klinger volume oscillator",
        outputs=("kvo", "kvo_signal", "kvo_hist"),
        params=("slow", "signal"),
    ),
    "t3": IndicatorSpec(id="t3", fn=_t3, default_window=20, description="T3 moving average"),
    "ichimoku": IndicatorSpec(
        id="ichimoku",
        fn=_ichimoku,
        default_window=26,
        description="Ichimoku components",
        outputs=("tenkan", "kijun", "span_a", "span_b", "chikou"),
    ),
    "ma_slope": IndicatorSpec(
        id="ma_slope",
        fn=_ma_slope,
        default_window=14,
        description="Normalized MA slope",
        params=("lookback", "normalize", "atr_length"),
    ),
    "price_vs_ma": IndicatorSpec(
        id="price_vs_ma",
        fn=_price_vs_ma,
        default_window=20,
        description="ATR distance from MA",
        params=("atr_length",),
    ),
    "ma_ribbon": IndicatorSpec(
        id="ma_ribbon",
        fn=_ma_ribbon,
        default_window=21,
        description="MA ribbon stack score + spread",
        outputs=("stack_score", "spread"),
        params=("lengths",),
    ),
    "pivot_points": IndicatorSpec(
        id="pivot_points",
        fn=_pivot_points,
        default_window=1,
        description="Classic pivot points",
        outputs=("pivot", "r1", "r2", "r3", "s1", "s2", "s3"),
        params=("period",),
    ),
    "fib_levels": IndicatorSpec(
        id="fib_levels",
        fn=_fib_retracement,
        default_window=20,
        description="Fibonacci retracements",
        params=("lookback", "levels"),
    ),
    "linear_regression_channel": IndicatorSpec(
        id="linear_regression_channel",
        fn=_linear_regression_channel,
        default_window=100,
        description="Linear regression channel",
        outputs=("lrc_mid", "lrc_upper", "lrc_lower"),
        params=("std_mult",),
    ),
    "atr_channel": IndicatorSpec(
        id="atr_channel",
        fn=_atr_channel,
        default_window=20,
        description="EMA ± ATR channel",
        outputs=("atr_mid", "atr_upper", "atr_lower"),
        params=("ema_length", "atr_length", "mult"),
    ),
    "chandelier": IndicatorSpec(
        id="chandelier",
        fn=_chandelier,
        default_window=22,
        description="Chandelier exit lines",
        outputs=("chand_long", "chand_short"),
        params=("atr_length", "mult", "lookback"),
    ),
    "coppock": IndicatorSpec(
        id="coppock",
        fn=_coppock,
        default_window=14,
        description="Coppock curve",
        params=("roc_long", "roc_short", "wma_length"),
    ),
    "dpo": IndicatorSpec(id="dpo", fn=_dpo, default_window=20, description="Detrended Price Oscillator"),
    "rvi": IndicatorSpec(
        id="rvi",
        fn=_rvi,
        default_window=10,
        description="Relative Vigor Index (with signal)",
        outputs=("rvi", "rvi_signal"),
    ),
    "fisher": IndicatorSpec(
        id="fisher",
        fn=_fisher,
        default_window=10,
        description="Fisher transform",
        outputs=("fisher", "fisher_signal"),
    ),
    "cmo": IndicatorSpec(id="cmo", fn=_cmo, default_window=14, description="Chande momentum oscillator"),
    "tsi": IndicatorSpec(
        id="tsi",
        fn=_tsi,
        default_window=25,
        description="True strength index, with signal",
        outputs=("tsi", "tsi_signal"),
        params=("long", "short", "signal"),
    ),
    "ppo": IndicatorSpec(
        id="ppo",
        fn=_ppo,
        default_window=26,
        description="Percentage price oscillator bundle",
        outputs=("ppo", "ppo_signal", "ppo_hist"),
        params=("fast", "slow", "signal"),
    ),
    "elder": IndicatorSpec(
        id="elder",
        fn=_elder,
        default_window=13,
        description="Elder ray bull/bear power",
        outputs=("bull_power", "bear_power"),
    ),
    "connors": IndicatorSpec(
        id="connors",
        fn=_connors,
        default_window=100,
        description="Connors RSI composite",
        params=("roc_len", "streak_rsi_len", "rsi_len"),
    ),
    "gap": IndicatorSpec(
        id="gap",
        fn=_gap,
        default_window=14,
        description="Gap detector (ATR units)",
        outputs=("gap_flag", "gap_size_atr"),
        params=("atr_length", "mult"),
    ),
    "fractal": IndicatorSpec(
        id="fractal",
        fn=_fractal,
        default_window=5,
        description="Bill Williams fractal markers",
        outputs=("fractal_high", "fractal_low"),
        params=("n",),
    ),
    "swing": IndicatorSpec(
        id="swing",
        fn=_swing,
        default_window=7,
        description="Pivot swing high/low markers and prices",
        outputs=("swing_high", "swing_low", "last_swing_high_price", "last_swing_low_price"),
        params=("left", "right"),
    ),
    "stddev": IndicatorSpec(id="stddev", fn=_std, default_window=20, description="Rolling standard deviation"),
    "hist_vol": IndicatorSpec(
        id="hist_vol",
        fn=_hist_vol,
        default_window=20,
        description="Historical volatility (log returns)",
        params=("annualize", "periods_per_year"),
    ),
    "chaikin_vol": IndicatorSpec(
        id="chaikin_vol",
        fn=_chaikin_vol,
        default_window=10,
        description="Chaikin volatility",
        params=("roc_length",),
    ),
    "anchored_vwap": IndicatorSpec(
        id="anchored_vwap",
        fn=_anchored_vwap,
        default_window=2,
        description="Anchored VWAP",
        params=("anchor_index",),
        requires_volume=True,
    ),
    "zigzag": IndicatorSpec(
        id="zigzag",
        fn=_zigzag,
        default_window=5,
        description="ZigZag swing curve",
        outputs=("zigzag",),
        params=("deviation_pct",),
    ),
    "price_channels": IndicatorSpec(
        id="price_channels",
        fn=_price_channels,
        default_window=20,
        description="Price channels (highest/lowest)",
        outputs=("pc_high", "pc_low"),
    ),
    "kaufman_er": IndicatorSpec(
        id="kaufman_er",
        fn=_kaufman_er,
        default_window=10,
        description="Kaufman efficiency ratio",
    ),
}


def compute_indicator(
    indicator_id: str,
    df: pd.DataFrame,
    params: Dict[str, Any] | None = None,
) -> Union[pd.Series, Dict[str, pd.Series]]:
    spec = INDICATOR_REGISTRY.get(indicator_id)
    if not spec:
        raise ValueError(f"Unsupported indicator fn: {indicator_id}")
    params = params or {}
    window = _safe_window(params.get("window"), spec.default_window, spec.min_window, spec.max_window)
    source = params.get("source", "close")
    result = spec.fn(df, window, source, params)
    if isinstance(result, dict):
        return {key: value for key, value in result.items() if isinstance(value, pd.Series)}
    return result


def available_indicators() -> Iterable[str]:
    return INDICATOR_REGISTRY.keys()


def _ensure_df(df_or_series: Union[pd.DataFrame, pd.Series], source: str):
    if isinstance(df_or_series, pd.Series):
        return df_or_series.to_frame(name=source)
    return df_or_series


def compute_rsi(series_or_df: Union[pd.Series, pd.DataFrame], window: int = 14, source: str = "close") -> pd.Series:
    df = _ensure_df(series_or_df, source)
    result = compute_indicator("rsi", df, {"window": window, "source": source})
    if isinstance(result, dict):
        return result.get("rsi") or pd.Series(dtype=float)
    return result


def compute_atr(df: Union[pd.Series, pd.DataFrame], window: int = 14, source: str = "close") -> pd.Series:
    data = _ensure_df(df, source)
    result = compute_indicator("atr", data, {"window": window, "source": source})
    if isinstance(result, dict):
        return result.get("atr") or pd.Series(dtype=float)
    return result
