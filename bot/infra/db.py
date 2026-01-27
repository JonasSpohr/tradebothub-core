import os
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, Optional

import requests
from supabase import Client, create_client

from bot.health.reporter import get_reporter_optional
from bot.runtime.logging_contract import record_db_write
from bot.utils.ids import generate_client_order_id

_supabase: Optional[Client] = None
_rpc_session: Optional[requests.Session] = None

EMAIL_NOTIFICATION_DEFAULT_THROTTLE_SECONDS = 10 * 60


def _rpc_headers() -> Dict[str, str]:
    key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
    token = os.environ.get("RUNTIME_TOKEN")
    if not key:
        raise RuntimeError("SUPABASE_SERVICE_ROLE_KEY is required for RPC calls")
    if not token:
        raise RuntimeError("RUNTIME_TOKEN is required for RPC calls")
    return {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
        "x-runtime-token": token,
    }


def _rpc_url(function: str) -> str:
    base = os.environ.get("SUPABASE_URL")
    if not base:
        raise RuntimeError("SUPABASE_URL is required for RPC calls")
    return f"{base.rstrip('/')}/rest/v1/rpc/{function}"


def _rpc_session_instance() -> requests.Session:
    global _rpc_session
    if _rpc_session is None:
        _rpc_session = requests.Session()
    return _rpc_session


def _call_rpc(function: str, payload: Dict[str, Any]) -> Any:
    session = _rpc_session_instance()
    resp = session.post(_rpc_url(function), json=payload, headers=_rpc_headers(), timeout=15)
    try:
        resp.raise_for_status()
    except requests.HTTPError as exc:
        raise RuntimeError(f"RPC {function} failed [{resp.status_code}]: {resp.text}") from exc
    if resp.status_code == 204:
        return None
    return resp.json()

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

def _record_db_ok():
    reporter = get_reporter_optional()
    try:
        record_db_write()
    except Exception:
        pass
    if reporter:
        reporter.record_db_ok()


def _record_db_error():
    reporter = get_reporter_optional()
    if reporter:
        reporter.record_db_error()

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def get_open_position(bot_id: str) -> Optional[Dict[str, Any]]:
    try:
        data = _call_rpc("bot_runtime_get_position", {"p_bot_id": bot_id, "p_status": "open"})
        _record_db_ok()
        return dict(data) if data else None
    except Exception:
        _record_db_error()
        return None

def set_exchange_sync_status(bot_id: str, status: str):
    try:
        _call_rpc("bot_runtime_heartbeat", {"p_bot_id": bot_id, "p_payload": {"exchange_sync_status": status}})
        _record_db_ok()
    except Exception:
        _record_db_error()
        raise

def update_position_from_exchange(
    bot_id: str,
    position_id: str,
    *,
    qty: float,
    entry_price: float,
    mark_price: float | None,
    unrealized_pnl: float,
    symbol: str,
    exchange: str,
    position_side: str,
    margin_mode: str | None,
    exchange_account_ref: str | None = None,
    exchange_position_id: str | None = None,
    exchange_position_key: str | None = None,
    payload: dict | None = None,
    entry_payload: dict | None = None,
):
    try:
        exchange_payload: Dict[str, Any] = {}
        if payload:
            exchange_payload.update(payload)
        if entry_payload:
            exchange_payload.update(entry_payload)
        payload_data: Dict[str, Any] = {
            "position_id": position_id,
            "qty": qty,
            "entry_price": entry_price,
            "mark_price": mark_price,
            "unrealized_pnl": unrealized_pnl,
            "unrealized_pnl_source": "exchange",
            "symbol": symbol,
            "exchange": exchange,
            "position_side": position_side,
            "margin_mode": margin_mode,
            "exchange_account_ref": exchange_account_ref,
            "exchange_position_id": exchange_position_id,
            "exchange_position_key": exchange_position_key,
            "last_exchange_sync_at": _now_iso(),
            "exchange_payload": exchange_payload,
            "status": "open",
        }
        _call_rpc("bot_runtime_upsert_position", {"p_bot_id": bot_id, "p_payload": payload_data})
        _record_db_ok()
    except Exception:
        _record_db_error()
        raise

def log_order_submission(
    bot_id: str,
    user_id: str,
    position_id: str | None,
    *,
    exchange_order_id: str,
    client_order_id: str,
    symbol: str,
    side: str,
    order_type: str,
    reduce_only: bool,
    order_status: str,
    order_amount: float,
    order_price: float | None,
    payload: dict | None,
):
    try:
        rpc_payload: Dict[str, Any] = {
            "position_id": position_id,
            "client_order_id": client_order_id,
            "symbol": symbol,
            "side": side,
            "order_type": order_type,
            "order_status": order_status,
            "reduce_only": reduce_only,
            "filled_qty": order_amount,
            "avg_fill_price": order_price,
            "exchange_payload": payload or {},
        }
        _call_rpc(
            "bot_runtime_upsert_trade",
            {
                "p_bot_id": bot_id,
                "p_exchange_order_id": exchange_order_id,
                "p_payload": rpc_payload,
            },
        )
        _record_db_ok()
    except Exception:
        _record_db_error()
        raise

def update_trade_status(
    bot_id: str,
    exchange_order_id: str,
    *,
    updates: Dict[str, Any],
):
    try:
        _call_rpc(
            "bot_runtime_upsert_trade",
            {
                "p_bot_id": bot_id,
                "p_exchange_order_id": exchange_order_id,
                "p_payload": updates,
            },
        )
        _record_db_ok()
    except Exception:
        _record_db_error()
        raise

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
    if not bot_id:
        return
    try:
        payload = {
            "type": typ,
            "severity": severity,
            "title": title,
            "body": body,
            "metadata": metadata or {},
        }
        _call_rpc(
            "bot_runtime_notify",
            {
                "p_bot_id": bot_id,
                "p_channel": channel,
                "p_payload": payload,
            },
        )
        _record_db_ok()
    except Exception:
        _record_db_error()
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
        _record_db_ok()
    except Exception:
        _record_db_error()
        pass

def queue_email_notification(
    user_id: str,
    bot_id: Optional[str],
    event_key: str,
    email_template: str,
    payload: Optional[Dict[str, Any]] = None,
    throttle_seconds: int = EMAIL_NOTIFICATION_DEFAULT_THROTTLE_SECONDS,
    dedup_id: Optional[str] = None,
    delay_seconds: int = 0,
):
    """
    Enqueue throttled email notifications without raising so the bot loop isn't blocked.
    """
    if not user_id:
        return
    try:
        sb = supabase_client()
        now = datetime.now(timezone.utc)
        send_after = now
        if delay_seconds:
            send_after = now + timedelta(seconds=delay_seconds)
        window_token = None
        if throttle_seconds and throttle_seconds > 0:
            window_token = int(now.timestamp()) // throttle_seconds
        components = [
            user_id or "",
            bot_id or "",
            event_key,
        ]
        if dedup_id:
            components.append(str(dedup_id))
        if window_token is not None:
            components.append(str(window_token))
        idempotency_key = "|".join(components)
        sb.table("notification_queue").insert({
            "user_id": user_id,
            "bot_id": bot_id,
            "event_key": event_key,
            "email_template": email_template,
            "payload": payload or {},
            "idempotency_key": idempotency_key,
            "send_after": send_after.isoformat(),
        }).execute()
        _record_db_ok()
    except Exception:
        _record_db_error()
        pass

def set_bot_status(bot_id: str, status: str):
    """
    Update bot status field on bots table. Best-effort to avoid interrupting runtime.
    """
    try:
        sb = supabase_client()
        sb.table("bots").update({"status": status}).eq("id", bot_id).execute()
        _record_db_ok()
    except Exception:
        _record_db_error()
        pass

def refresh_controls(bot_id: str) -> Dict[str, Any]:
    """
    Fetch lightweight control + subscription data to allow runtime toggles.
    """
    try:
        payload = _call_rpc("bot_runtime_refresh_controls", {"p_bot_id": bot_id})
        _record_db_ok()
        return dict(payload or {})
    except Exception:
        _record_db_error()
        raise

def touch_heartbeat(bot_id: str, user_id: str):
    """
    Update heartbeat timestamp on bot_state (preferred) or latest heartbeat event.
    """
    iso = datetime.now(timezone.utc).isoformat()
    try:
        _call_rpc(
            "bot_runtime_heartbeat",
            {"p_bot_id": bot_id, "p_payload": {"heartbeat_at": iso}},
        )
        _record_db_ok()
    except Exception:
        _record_db_error()
    try:
        sb = supabase_client()
        resp = (
            sb.table("bot_events")
            .select("id")
            .eq("bot_id", bot_id)
            .eq("event_type", "heartbeat")
            .order("created_at", desc=True)
            .limit(1)
            .execute()
        )
        data = resp.data or []
        if data:
            sb.table("bot_events").update({"message": iso}).eq("id", data[0]["id"]).execute()
    except Exception:
        pass

def fetch_bot_context_row(bot_id: str) -> Dict[str, Any]:
    """
    Fetch bot context via RPC contract.
    """
    try:
        data = _call_rpc("bot_runtime_get_context", {"p_bot_id": bot_id})
        _record_db_ok()
    except Exception:
        _record_db_error()
        raise
    if not data:
        raise RuntimeError(f"bot_runtime_get_context returned no data for bot_id={bot_id}")
    bot = data.get("bot") or {}
    api_keys = data.get("api_keys") or {}
    exchange = data.get("supported_exchange") or {}
    market = data.get("supported_market") or {}
    subscription = data.get("subscription") or {}
    strategy_profile = data.get("strategy_profile") or {}

    row: Dict[str, Any] = dict(bot)
    row["api_key_encrypted"] = api_keys.get("api_key_encrypted")
    row["api_secret_encrypted"] = api_keys.get("api_secret_encrypted")
    row["api_password_encrypted"] = api_keys.get("api_password_encrypted")
    row["api_uid_encrypted"] = api_keys.get("api_uid_encrypted")
    row["exchange_ccxt_id"] = exchange.get("ccxt_id")
    row["market_symbol"] = market.get("symbol")
    row["subscription_status"] = subscription.get("status") or "inactive"
    row["strategy_profile_overrides"] = strategy_profile.get("overrides") or {}
    row["strategy_profile_key"] = strategy_profile.get("profile_key") or bot.get("profile_key")
    if not row.get("strategy_definition"):
        row["strategy_definition"] = bot.get("strategy_definition") or {}
    row["strategy_key"] = bot.get("strategy_key") or bot.get("strategy")
    return row

def write_event(bot_id: str, user_id: str, event_type: str, message: str):
    try:
        sb = supabase_client()
        sb.table("bot_events").insert({
            "bot_id": bot_id,
            "user_id": user_id,
            "event_type": event_type,
            "message": message,
        }).execute()
        _record_db_ok()
    except Exception:
        _record_db_error()
        pass

def upsert_state(bot_id: str, user_id: str, state: Dict[str, Any]):
    try:
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
        _record_db_ok()
    except Exception:
        _record_db_error()
        raise

def insert_position_open(
    bot_id: str,
    user_id: str,
    direction: str,
    entry_price: float,
    entry_time: str,
    qty: float,
    *,
    symbol: str | None = None,
    exchange: str | None = None,
    margin_mode: str | None = None,
    position_side: str | None = None,
    entry_exchange_order_id: str | None = None,
    entry_client_order_id: str | None = None,
    exchange_account_ref: str | None = None,
    mark_price: float | None = None,
    exchange_payload: Dict[str, Any] | None = None,
) -> str:
    try:
        payload = {
            "direction": direction,
            "entry_price": entry_price,
            "entry_time": entry_time,
            "qty": qty,
            "status": "open",
            "symbol": symbol,
            "exchange": exchange,
            "margin_mode": margin_mode,
            "position_side": position_side,
            "entry_exchange_order_id": entry_exchange_order_id,
            "entry_client_order_id": entry_client_order_id,
            "exchange_account_ref": exchange_account_ref,
            "mark_price": mark_price,
            "exchange_payload": exchange_payload or {},
            "user_id": user_id,
        }
        resp = _call_rpc("bot_runtime_upsert_position", {"p_bot_id": bot_id, "p_payload": payload})
        _record_db_ok()
        if isinstance(resp, dict) and resp.get("id"):
            return str(resp["id"])
        raise RuntimeError("Failed to insert position via RPC")
    except Exception:
        _record_db_error()
        raise

def close_position(
    position_id: str,
    exit_price: float,
    exit_time: str,
    realized_pnl: float,
    *,
    bot_id: str | None = None,
    exit_exchange_order_id: str | None = None,
    exit_client_order_id: str | None = None,
    exchange_payload: Dict[str, Any] | None = None,
):
    try:
        if not bot_id:
            raise RuntimeError("bot_id is required to close a position")
        payload = {
            "position_id": position_id,
            "status": "closed",
            "exit_price": exit_price,
            "exit_time": exit_time,
            "realized_pnl": realized_pnl,
            "exit_exchange_order_id": exit_exchange_order_id,
            "exit_client_order_id": exit_client_order_id,
            "realized_pnl_source": "exchange",
            "last_exchange_sync_at": _now_iso(),
            "exchange_payload": exchange_payload or {},
        }
        _call_rpc("bot_runtime_upsert_position", {"p_bot_id": bot_id, "p_payload": payload})
        _record_db_ok()
    except Exception:
        _record_db_error()
        raise

def insert_trade(
    bot_id: str,
    user_id: str,
    position_id: str | None,
    *,
    side: str,
    price: float,
    qty: float,
    fee: float | None,
    pnl: float | None,
    exchange_order_id: str | None,
    executed_at: str,
    client_order_id: str | None = None,
    symbol: str | None = None,
    order_type: str | None = None,
    order_status: str | None = None,
    reduce_only: bool | None = None,
    filled_qty: float | None = None,
    avg_fill_price: float | None = None,
    exchange_payload: Dict[str, Any] | None = None,
):
    try:
        client_id = client_order_id or generate_client_order_id(bot_id, "manual")
        payload: Dict[str, Any] = {
            "position_id": position_id,
            "side": side,
            "price": price,
            "qty": qty,
            "fee": fee,
            "pnl": pnl,
            "executed_at": executed_at,
            "client_order_id": client_id,
            "symbol": symbol,
            "order_type": order_type,
            "order_status": order_status or "manual",
            "reduce_only": reduce_only,
            "filled_qty": filled_qty if filled_qty is not None else qty,
            "avg_fill_price": avg_fill_price or price,
            "exchange_payload": exchange_payload or {},
        }
        _call_rpc(
            "bot_runtime_upsert_trade",
            {
                "p_bot_id": bot_id,
                "p_exchange_order_id": exchange_order_id or "",
                "p_payload": payload,
            },
        )
        _record_db_ok()
    except Exception:
        _record_db_error()
        raise
