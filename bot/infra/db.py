import os
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, Optional
from supabase import Client, create_client

from bot.health.reporter import get_reporter_optional

_supabase: Optional[Client] = None

EMAIL_NOTIFICATION_DEFAULT_THROTTLE_SECONDS = 10 * 60

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

def _record_db_ok():
    reporter = get_reporter_optional()
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
        sb = supabase_client()
        resp = (
            sb.table("bot_positions")
            .select("*")
            .eq("bot_id", bot_id)
            .eq("status", "open")
            .limit(1)
            .execute()
        )
        data = resp.data or []
        if not data:
            _record_db_ok()
            return None
        _record_db_ok()
        return dict(data[0])
    except Exception:
        _record_db_error()
        return None

def set_exchange_sync_status(bot_id: str, status: str):
    try:
        sb = supabase_client()
        sb.table("bot_state").update(
            {
                "exchange_sync_status": status,
                "updated_at": _now_iso(),
            }
        ).eq("bot_id", bot_id).execute()
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
        sb = supabase_client()
        now_iso = _now_iso()
        updates: Dict[str, Any] = {
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
            "last_exchange_sync_at": now_iso,
            "exchange_payload": payload or {},
            "status": "open",
        }
        if entry_payload:
            updates["exchange_payload"] = {**updates["exchange_payload"], **entry_payload}
        sb.table("bot_positions").update(
            updates
        ).eq("bot_id", bot_id).eq("id", position_id).execute()
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
        sb = supabase_client()
        sb.table("bot_trades").insert(
            {
                "bot_id": bot_id,
                "user_id": user_id,
                "position_id": position_id,
                "exchange_order_id": exchange_order_id,
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
        ).execute()
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
        sb = supabase_client()
        sb.table("bot_trades").update(updates).eq("bot_id", bot_id).eq("exchange_order_id", exchange_order_id).execute()
        _record_db_ok()
    except Exception:
        _record_db_error()
        raise

def _single(table: str, filters: Dict[str, Any]) -> Dict[str, Any]:
    sb = supabase_client()
    q = sb.table(table).select("*")
    for k, v in filters.items():
        q = q.eq(k, v)
    resp = q.single().execute()
    data = _ensure_data(resp, f"select {table}")
    if not data:
        raise RuntimeError(f"{table}: not found")
    return dict(data)

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
    bot = _single("bots", {"id": bot_id})
    control_config = bot.get("control_config") or {}
    execution_config = bot.get("execution_config") or {}
    sub_status = "inactive"
    try:
        sb = supabase_client()
        resp = sb.table("subscriptions").select("status").eq("bot_id", bot_id).limit(1).execute()
        data = resp.data or []
        if data:
            sub_status = data[0].get("status") or sub_status
    except Exception:
        pass
    return {
        "control_config": control_config,
        "execution_config": execution_config,
        "subscription_status": sub_status,
    }

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
        _record_db_ok()
    except Exception:
        _record_db_error()
        pass

def fetch_bot_context_row(bot_id: str) -> Dict[str, Any]:
    """
    Fetch bot context directly from tables to avoid relying on a view.
    Returns a dict that includes bot fields, api keys, market/exchange info, subscription status,
    strategy definition, and strategy profile overrides.
    """
    sb = supabase_client()

    bot = _single("bots", {"id": bot_id})

    # API keys (encrypted)
    api_keys = _single("api_keys", {"bot_id": bot_id})

    # Exchange + market metadata
    ex = _single("supported_exchanges", {"id": bot["exchange_id"]})
    market = _single("supported_markets", {"id": bot["market_id"]})

    # Subscription status (optional)
    sub_status = "inactive"
    try:
        sub_resp = sb.table("subscriptions").select("status").eq("bot_id", bot_id).limit(1).execute()
        sub_data = sub_resp.data or []
        if sub_data:
            sub_status = sub_data[0].get("status") or sub_status
    except Exception:
        pass

    # Strategy definition + profile overrides
    strategy_definition = {}
    strategy_version = None
    strategy_key = bot.get("strategy")
    profile_overrides: Dict[str, Any] = {}
    profile_key = bot.get("profile_key") or bot.get("profile")

    if bot.get("strategy_version_id"):
        strategy_version = _single("strategy_versions", {"id": bot["strategy_version_id"]})
        strategy_definition = strategy_version.get("definition") or {}
        try:
            strat_row = _single("strategies", {"id": strategy_version["strategy_id"]})
            strategy_key = strat_row.get("strategy_key", strategy_key)
        except Exception:
            pass
        try:
            if bot.get("strategy_profile_id"):
                sp = _single("strategy_profiles", {"id": bot["strategy_profile_id"]})
                profile_overrides = sp.get("overrides") or {}
                profile_key = sp.get("profile_key", profile_key)
            elif profile_key:
                sp_resp = (
                    sb.table("strategy_profiles")
                    .select("id,overrides,profile_key")
                    .eq("strategy_version_id", bot["strategy_version_id"])
                    .eq("profile_key", profile_key)
                    .limit(1)
                    .execute()
                )
                sp_data = sp_resp.data or []
                if sp_data:
                    profile_overrides = sp_data[0].get("overrides") or {}
        except Exception:
            pass

    row = {
        **bot,
        "strategy_key": strategy_key,
        "api_key_encrypted": api_keys.get("api_key_encrypted"),
        "api_secret_encrypted": api_keys.get("api_secret_encrypted"),
        "api_password_encrypted": api_keys.get("api_password_encrypted"),
        "api_uid_encrypted": api_keys.get("api_uid_encrypted"),
        "exchange_ccxt_id": ex.get("ccxt_id"),
        "market_symbol": market.get("symbol"),
        "subscription_status": sub_status,
        "strategy_definition": strategy_definition,
        "strategy_profile_overrides": profile_overrides,
        "strategy_profile_key": profile_key,
    }
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
        sb = supabase_client()
        resp = sb.table("bot_positions").insert({
            "bot_id": bot_id,
            "user_id": user_id,
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
        }).execute()
        data = _ensure_data(resp, "insert_position_open")
        _record_db_ok()
        return str(data[0]["id"])
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
        sb = supabase_client()
        q = sb.table("bot_positions").update({
            "exit_price": exit_price,
            "exit_time": exit_time,
            "realized_pnl": realized_pnl,
            "status": "closed",
            "exit_exchange_order_id": exit_exchange_order_id,
            "exit_client_order_id": exit_client_order_id,
            "realized_pnl_source": "exchange",
            "last_exchange_sync_at": _now_iso(),
            "exchange_payload": exchange_payload or {},
        })
        q = q.eq("id", position_id)
        if bot_id:
            q = q.eq("bot_id", bot_id)
        q.execute()
        _record_db_ok()
    except Exception:
        _record_db_error()
        raise

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
    try:
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
        _record_db_ok()
    except Exception:
        _record_db_error()
        raise
