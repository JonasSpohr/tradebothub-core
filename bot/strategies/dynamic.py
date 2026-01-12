import ast
import os
from typing import Any, Dict, List
import pandas as pd
from bot.strategies.base import Strategy
from bot.indicators import compute_rsi, compute_atr

# Whitelisted indicator functions
def _series_from_source(df: pd.DataFrame, source: str) -> pd.Series:
    return df[source] if source in df else df["close"]

def _ema(df: pd.DataFrame, window: int, source: str, params: Dict[str, Any]) -> pd.Series:
    return _series_from_source(df, source).ewm(span=max(1, window), adjust=False).mean()

def _sma(df: pd.DataFrame, window: int, source: str, params: Dict[str, Any]) -> pd.Series:
    span = max(1, window)
    return _series_from_source(df, source).rolling(window=span, min_periods=span).mean()

def _rsi(df: pd.DataFrame, window: int, source: str, params: Dict[str, Any]) -> pd.Series:
    return compute_rsi(_series_from_source(df, source), max(1, window))

def _atr(df: pd.DataFrame, window: int, source: str, params: Dict[str, Any]) -> pd.Series:
    return compute_atr(df, max(1, window))

def _donchian_high(df: pd.DataFrame, window: int, source: str, params: Dict[str, Any]) -> pd.Series:
    span = max(1, window)
    target = source if source in df else "high"
    return df[target].rolling(window=span, min_periods=span).max()

def _donchian_low(df: pd.DataFrame, window: int, source: str, params: Dict[str, Any]) -> pd.Series:
    span = max(1, window)
    target = source if source in df else "low"
    return df[target].rolling(window=span, min_periods=span).min()

def _vol_sma(df: pd.DataFrame, window: int, source: str, params: Dict[str, Any]) -> pd.Series:
    span = max(1, window)
    if source in df:
        series = df[source]
    else:
        series = df.get("volume", pd.Series([0.0] * len(df), index=df.index))
    return series.rolling(window=span, min_periods=span).mean()

def _bb_mid(df: pd.DataFrame, window: int, source: str, params: Dict[str, Any]) -> pd.Series:
    span = max(1, window)
    return _series_from_source(df, source).rolling(window=span, min_periods=span).mean()

def _bb_width(df: pd.DataFrame, window: int, source: str, params: Dict[str, Any]) -> pd.Series:
    span = max(1, window)
    series = _series_from_source(df, source)
    mid = series.rolling(window=span, min_periods=span).mean()
    std = series.rolling(window=span, min_periods=span).std(ddof=0).fillna(0.0)
    mult = float(params.get("mult") or 2.0)
    upper = mid + (std * mult)
    lower = mid - (std * mult)
    width = upper - lower
    denom = mid.copy()
    denom[denom == 0] = 1.0
    return width / denom

def _sentiment_score(df: pd.DataFrame, window: int, source: str, params: Dict[str, Any]) -> pd.Series:
    env_var = params.get("env_var") or "SENTIMENT_SCORE"
    try:
        score = float(os.getenv(env_var, "0"))
    except ValueError:
        score = 0.0
    return pd.Series([score] * len(df), index=df.index)

INDICATOR_FNS = {
    "ema": _ema,
    "sma": _sma,
    "rsi": _rsi,
    "atr": _atr,
    "donchian_high": _donchian_high,
    "donchian_low": _donchian_low,
    "vol_sma": _vol_sma,
    "bb_mid": _bb_mid,
    "bb_width": _bb_width,
    "sentiment_score": _sentiment_score,
}


def _safe_name(name: str) -> str:
    return str(name).strip()


def _deep_get(d: Dict[str, Any], key: str, default: Any = None) -> Any:
    val = d.get(key)
    return val if isinstance(val, dict) else default


def _clamp_int(val: Any, default: int, min_v: int, max_v: int) -> int:
    try:
        i = int(val)
    except Exception:
        i = default
    return max(min_v, min(max_v, i))


class _SafeExpr:
    """
    Tiny AST-based boolean evaluator for indicator expressions.
    Allows: and/or/not, comparisons, parentheses, numbers, names from row.
    """

    ALLOWED_NODES = (
        ast.Expression,
        ast.BoolOp,
        ast.UnaryOp,
        ast.BinOp,
        ast.Compare,
        ast.Name,
        ast.Constant,
        ast.And,
        ast.Or,
        ast.Not,
        ast.USub,
        ast.UAdd,
        ast.Add,
        ast.Sub,
        ast.Mult,
        ast.Div,
        ast.Eq,
        ast.NotEq,
        ast.Lt,
        ast.LtE,
        ast.Gt,
        ast.GtE,
        ast.Expr,
        ast.Load,
    )
    ALLOWED_BOOL_OPS = (ast.And, ast.Or)
    ALLOWED_UNARY_OPS = (ast.Not, ast.USub, ast.UAdd)
    ALLOWED_BIN_OPS = (ast.Add, ast.Sub, ast.Mult, ast.Div)
    ALLOWED_CMP_OPS = (ast.Eq, ast.NotEq, ast.Lt, ast.LtE, ast.Gt, ast.GtE)

    def __init__(self, expr: str):
        self.expr = expr or ""
        self._ast = ast.parse(self.expr, mode="eval")
        self._validate(self._ast)

    def _validate(self, node):
        if not isinstance(node, self.ALLOWED_NODES):
            raise ValueError(f"Disallowed expression node: {type(node).__name__}")
        for child in ast.iter_child_nodes(node):
            self._validate(child)
        if isinstance(node, ast.BoolOp) and not isinstance(node.op, self.ALLOWED_BOOL_OPS):
            raise ValueError("Disallowed boolean operator")
        if isinstance(node, ast.UnaryOp) and not isinstance(node.op, self.ALLOWED_UNARY_OPS):
            raise ValueError("Disallowed unary operator")
        if isinstance(node, ast.BinOp) and not isinstance(node.op, self.ALLOWED_BIN_OPS):
            raise ValueError("Disallowed binary operator")
        if isinstance(node, ast.Compare):
            for op in node.ops:
                if not isinstance(op, self.ALLOWED_CMP_OPS):
                    raise ValueError("Disallowed comparison operator")

    def eval(self, ctx: Dict[str, Any]) -> bool:
        def _eval(node):
            if isinstance(node, ast.Expression):
                return _eval(node.body)
            if isinstance(node, ast.Constant):
                return node.value
            if isinstance(node, ast.Name):
                return ctx.get(node.id)
            if isinstance(node, ast.UnaryOp):
                val = _eval(node.operand)
                if isinstance(node.op, ast.Not):
                    return not bool(val)
                if isinstance(node.op, ast.USub):
                    return -float(val)
                if isinstance(node.op, ast.UAdd):
                    return +float(val)
            if isinstance(node, ast.BoolOp):
                vals = [_eval(v) for v in node.values]
                if isinstance(node.op, ast.And):
                    return all(bool(v) for v in vals)
                if isinstance(node.op, ast.Or):
                    return any(bool(v) for v in vals)
            if isinstance(node, ast.BinOp):
                left = _eval(node.left)
                right = _eval(node.right)
                if isinstance(node.op, ast.Add):
                    return float(left) + float(right)
                if isinstance(node.op, ast.Sub):
                    return float(left) - float(right)
                if isinstance(node.op, ast.Mult):
                    return float(left) * float(right)
                if isinstance(node.op, ast.Div):
                    return float(left) / float(right)
            if isinstance(node, ast.Compare):
                left = _eval(node.left)
                for op, comparator in zip(node.ops, node.comparators):
                    right = _eval(comparator)
                    if isinstance(op, ast.Eq) and not (left == right):
                        return False
                    if isinstance(op, ast.NotEq) and not (left != right):
                        return False
                    if isinstance(op, ast.Lt) and not (left < right):
                        return False
                    if isinstance(op, ast.LtE) and not (left <= right):
                        return False
                    if isinstance(op, ast.Gt) and not (left > right):
                        return False
                    if isinstance(op, ast.GtE) and not (left >= right):
                        return False
                    left = right
                return True
            raise ValueError("Unsupported expression")

        return bool(_eval(self._ast))


class DynamicStrategy(Strategy):
    """
    Strategy backed by DB-provided definition JSON.
    Expects definition shape:
    {
      "indicators": [{id, fn, source?, window?, params?}],
      "signals": {"entry_long": {"expr": "..."},"entry_short": {"expr": "..."}},
      "constraints": {"windows": {"min":2,"max":500}}
    }
    """

    def __init__(self, definition: Dict[str, Any]):
        self.definition = definition or {}
        signals = self.definition.get("signals") or {}
        self._long_expr = _SafeExpr((signals.get("entry_long") or {}).get("expr", "False"))
        self._short_expr = _SafeExpr((signals.get("entry_short") or {}).get("expr", "False"))
        self.name = _safe_name(self.definition.get("name") or self.definition.get("strategy_key") or "dynamic")

        constraints = self.definition.get("constraints") or {}
        windows = constraints.get("windows") or {}
        self.window_min = int(windows.get("min", 1))
        self.window_max = int(windows.get("max", 500))

    def _compute_indicator(self, df: pd.DataFrame, ind: Dict[str, Any]) -> pd.Series:
        fn_key = _safe_name(ind.get("fn"))
        func = INDICATOR_FNS.get(fn_key)
        if not func:
            raise ValueError(f"Unsupported indicator fn: {fn_key}")
        span = _clamp_int(ind.get("window"), 14, self.window_min, self.window_max)
        source = _safe_name(ind.get("source") or "close")
        params = ind.get("params") or {}
        return func(df, span, source, params)

    def prepare(self, df: pd.DataFrame, cfg: Dict[str, Any]) -> pd.DataFrame:
        df = df.copy()
        indicators: List[Dict[str, Any]] = self.definition.get("indicators") or []
        for ind in indicators:
            ind_id = _safe_name(ind.get("id"))
            if not ind_id:
                continue
            df[ind_id] = self._compute_indicator(df, ind)
        # Ensure ATR column present for exits/trailing logic
        if "atr" not in df.columns:
            try:
                window = _clamp_int(cfg.get("atr_period", 14), 14, self.window_min, self.window_max)
                df["atr"] = compute_atr(df, window)
            except Exception:
                pass
        return df

    def long_signal(self, row: pd.Series, cfg: Dict[str, Any]) -> bool:
        ctx = {k: row.get(k) for k in row.index}
        return self._long_expr.eval(ctx)

    def short_signal(self, row: pd.Series, cfg: Dict[str, Any]) -> bool:
        ctx = {k: row.get(k) for k in row.index}
        return self._short_expr.eval(ctx)
