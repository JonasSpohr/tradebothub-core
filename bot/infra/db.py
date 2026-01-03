import os, json
from datetime import datetime, timezone
from typing import Any, Dict, Optional
from supabase import Client, create_client

_supabase: Optional[Client] = None

def supabase_client() -> Client:
    """
    Lazy-init Supabase client using the service role key for full DB access.
    """
    global _supabase
    if _supabase is None:
        url = os.environ["SUPABASE_URL"]
        key = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
        _supabase = create_client(url, key)
    return _supabase

def _ensure_data(resp, ctx: str):
    if getattr(resp, "error", None):
        raise RuntimeError(f"{ctx}: {resp.error}")
    if resp.data is None:
        raise RuntimeError(f"{ctx}: empty response")
    return resp.data

def notify(
    user_id: str,
    bot_id: Optional[str],
    typ: str,
    title: str,
    body: Optional[str] = None,
    severity: str = "info",
    channel: str = "in_app",
    metadata: Optional[Dict[str, Any]] = None,
):
    """
    Fire-and-forget notification insert; failures are ignored to avoid impacting the bot loop.
    """
    try:
        sb = supabase_client()
        sb.table("notifications").insert({
            "user_id": user_id,
            "bot_id": bot_id,
            "channel": channel,
            "type": typ,
            "severity": severity,
            "title": title,
            "body": body,
            "metadata": metadata or {},
        }).execute()
    except Exception:
        pass

def notify_support(
    user_id: str,
    bot_id: Optional[str],
    title: str,
    body: Optional[str] = None,
    severity: str = "critical",
    target_email: Optional[str] = None,
):
    """
    Send an email-channel notification to support. target_email can override default.
    """
    try:
        sb = supabase_client()
        email = target_email or os.getenv("SUPPORT_EMAIL") or "botneedsattention@tradebothub.pro"
        sb.table("notifications").insert({
            "user_id": user_id,
            "bot_id": bot_id,
            "channel": "email",
            "type": "support_alert",
            "severity": severity,
            "title": title,
            "body": body,
            "metadata": {"target_email": email},
        }).execute()
    except Exception:
        pass

def set_bot_status(bot_id: str, status: str):
    """
    Update bot status field on bots table. Best-effort to avoid interrupting runtime.
    """
    try:
        sb = supabase_client()
        sb.table("bots").update({"status": status}).eq("id", bot_id).execute()
    except Exception:
        pass

def refresh_controls(bot_id: str) -> Dict[str, Any]:
    """
    Fetch lightweight control + subscription data to allow runtime toggles.
    """
    sb = supabase_client()
    resp = sb.table("bot_context_view").select("control_config,subscription_status,execution_config").eq("id", bot_id).single().execute()
    return _ensure_data(resp, "refresh_controls")

def touch_heartbeat(bot_id: str, user_id: str):
    """
    Update heartbeat timestamp on bot_state (preferred) or latest heartbeat event.
    """
    try:
        sb = supabase_client()
        iso = datetime.now(timezone.utc).isoformat()
        sb.table("bot_state").update({"heartbeat_at": iso, "updated_at": iso}).eq("bot_id", bot_id).execute()
        try:
            resp = sb.table("bot_events")\
                .select("id")\
                .eq("bot_id", bot_id)\
                .eq("event_type", "heartbeat")\
                .order("created_at", desc=True)\
                .limit(1)\
                .execute()
            data = resp.data or []
            if data:
                sb.table("bot_events").update({"message": iso}).eq("id", data[0]["id"]).execute()
        except Exception:
            pass
    except Exception:
        pass

def fetch_bot_context_row(bot_id: str) -> Dict[str, Any]:
    sb = supabase_client()
    # Expect a view named bot_context_view that returns the joined bot context (see deploy SQL).
    resp = sb.table("bot_context_view").select("*").eq("id", bot_id).single().execute()
    row = resp.data
    if not row:
        raise RuntimeError(f"bot_not_found: {bot_id}")
    return dict(row)

def write_event(bot_id: str, user_id: str, event_type: str, message: str):
    try:
        sb = supabase_client()
        sb.table("bot_events").insert({
            "bot_id": bot_id,
            "user_id": user_id,
            "event_type": event_type,
            "message": message,
        }).execute()
    except Exception:
        pass

def upsert_state(bot_id: str, user_id: str, state: Dict[str, Any]):
    sb = supabase_client()
    def _none_if_empty(val):
        return None if (val is None or val == "") else val

    now_iso = datetime.now(timezone.utc).isoformat()
    heartbeat = _none_if_empty(state.get("heartbeat_at")) or now_iso

    payload = {
        "bot_id": bot_id,
        "user_id": user_id,
        "in_position": bool(state.get("in_position", False)),
        "direction": _none_if_empty(state.get("direction")),
        "entry_price": state.get("entry_price"),
        "entry_time": _none_if_empty(state.get("entry_time")),
        "qty": state.get("qty"),
        "base_notional": state.get("base_notional"),
        "peak_price": state.get("peak_price"),
        "low_price": state.get("low_price"),
        "added_levels": int(state.get("added_levels", 0)),
        "week_trade_counts": state.get("week_trade_counts", {}) or {},
        "last_exit_time": _none_if_empty(state.get("last_exit_time")),
        "last_candle_time": _none_if_empty(state.get("last_candle_time")),
        "cumulative_pnl": float(state.get("cumulative_pnl", 0.0)),
        "max_unrealized_pnl": float(state.get("max_unrealized_pnl", 0.0)),
        "min_unrealized_pnl": float(state.get("min_unrealized_pnl", 0.0)),
        "last_price": state.get("last_price"),
        "unrealized_pnl": float(state.get("unrealized_pnl", 0.0)),
        "stop_price": state.get("stop_price"),
        "take_profit_price": state.get("take_profit_price"),
        "trailing_stop_price": state.get("trailing_stop_price"),
        "trailing_active": bool(state.get("trailing_active", False)),
        "atr": state.get("atr"),
        "last_manage_time": _none_if_empty(state.get("last_manage_time")),
        "heartbeat_at": heartbeat,
        # updated_at handled by DB default/trigger if present
    }
    sb.table("bot_state").upsert(payload, on_conflict="bot_id").execute()

def insert_position_open(bot_id: str, user_id: str, direction: str, entry_price: float, entry_time: str, qty: float) -> str:
    sb = supabase_client()
    resp = sb.table("bot_positions").insert({
        "bot_id": bot_id,
        "user_id": user_id,
        "direction": direction,
        "entry_price": entry_price,
        "entry_time": entry_time,
        "qty": qty,
        "status": "open",
    }).execute()
    data = _ensure_data(resp, "insert_position_open")
    return str(data[0]["id"])

def close_position(position_id: str, exit_price: float, exit_time: str, realized_pnl: float):
    sb = supabase_client()
    sb.table("bot_positions").update({
        "exit_price": exit_price,
        "exit_time": exit_time,
        "realized_pnl": realized_pnl,
        "status": "closed",
    }).eq("id", position_id).execute()

def insert_trade(
    bot_id: str,
    user_id: str,
    position_id: str | None,
    side: str,
    price: float,
    qty: float,
    fee: float | None,
    pnl: float | None,
    exchange_order_id: str | None,
    executed_at: str,
):
    sb = supabase_client()
    sb.table("bot_trades").insert({
        "bot_id": bot_id,
        "user_id": user_id,
        "position_id": position_id,
        "side": side,
        "price": price,
        "qty": qty,
        "fee": fee,
        "pnl": pnl,
        "exchange_order_id": exchange_order_id,
        "executed_at": executed_at,
    }).execute()
