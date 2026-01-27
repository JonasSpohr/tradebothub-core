import ast
from typing import Any, Dict, List

import pandas as pd

from bot.indicators import compute_indicator
from bot.strategies.base import Strategy


def _safe_name(value: Any) -> str:
    return str(value or "").strip()


def _clamp_int(val: Any, default: int, min_v: int, max_v: int) -> int:
    try:
        i = int(val)
    except Exception:
        i = default
    return max(min_v, min(max_v, i))


class _SafeExpr:
    ALLOWED = {
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
    }
    BOOL_OPS = {ast.And, ast.Or}
    UNARY_OPS = {ast.Not, ast.USub, ast.UAdd}
    BIN_OPS = {ast.Add, ast.Sub, ast.Mult, ast.Div}
    CMP_OPS = {ast.Eq, ast.NotEq, ast.Lt, ast.LtE, ast.Gt, ast.GtE}

    def __init__(self, expr: str):
        self.expr = expr or "False"
        self._ast = ast.parse(self.expr, mode="eval")
        self._validate(self._ast)

    def _validate(self, node: ast.AST) -> None:
        if type(node) not in self.ALLOWED:
            raise ValueError(f"Disallowed expression node: {type(node).__name__}")
        for child in ast.iter_child_nodes(node):
            self._validate(child)
        if isinstance(node, ast.BoolOp) and type(node.op) not in self.BOOL_OPS:
            raise ValueError("Disallowed boolean operator")
        if isinstance(node, ast.UnaryOp) and type(node.op) not in self.UNARY_OPS:
            raise ValueError("Disallowed unary operator")
        if isinstance(node, ast.BinOp) and type(node.op) not in self.BIN_OPS:
            raise ValueError("Disallowed binary operator")
        if isinstance(node, ast.Compare):
            for op in node.ops:
                if type(op) not in self.CMP_OPS:
                    raise ValueError("Disallowed comparison operator")

    def eval(self, ctx: Dict[str, Any]) -> bool:
        def _eval(node: ast.AST) -> Any:
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

    def _compute_indicator(self, df: pd.DataFrame, ind: Dict[str, Any]) -> Dict[str, pd.Series]:
        fn_key = _safe_name(ind.get("fn"))
        params = dict(ind.get("params") or {})
        if "window" in ind:
            params["window"] = _clamp_int(ind.get("window"), 14, self.window_min, self.window_max)
        params["source"] = _safe_name(ind.get("source") or "close")
        result = compute_indicator(fn_key, df, params)
        if isinstance(result, pd.Series):
            return {"value": result}
        return result

    def prepare(self, df: pd.DataFrame, cfg: Dict[str, Any]) -> pd.DataFrame:
        df = df.copy()
        indicators: List[Dict[str, Any]] = self.definition.get("indicators") or []
        for ind in indicators:
            ind_id = _safe_name(ind.get("id"))
            if not ind_id:
                continue
            computed = self._compute_indicator(df, ind)
            if len(computed) == 1 and "value" in computed:
                df[ind_id] = computed["value"]
            else:
                for suffix, series in computed.items():
                    df[f"{ind_id}__{suffix}"] = series
        # Ensure ATR column present for exits/trailing logic
        if "atr" not in df.columns:
            try:
                window = _clamp_int(cfg.get("atr_period", 14), 14, self.window_min, self.window_max)
                atr = compute_indicator("atr", df, {"window": window})
                df["atr"] = atr if isinstance(atr, pd.Series) else atr.get("value", pd.Series([pd.NA] * len(df), index=df.index))
            except Exception:
                pass
        return df

    def long_signal(self, row: pd.Series, cfg: Dict[str, Any]) -> bool:
        ctx = {k: row.get(k) for k in row.index}
        return self._long_expr.eval(ctx)

    def short_signal(self, row: pd.Series, cfg: Dict[str, Any]) -> bool:
        ctx = {k: row.get(k) for k in row.index}
        return self._short_expr.eval(ctx)
