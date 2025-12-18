import os, json
from typing import Any, Dict
import psycopg2
import psycopg2.extras

def conn():
    return psycopg2.connect(
        host=os.environ["DB_HOST"],
        dbname=os.environ["DB_NAME"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        sslmode="require",
    )

def fetch_bot_context_row(bot_id: str) -> Dict[str, Any]:
    q = """
    select
      b.id, b.user_id, b.name, b.strategy, b.mode, b.dry_run,
      b.strategy_config, b.risk_config, b.execution_config, b.control_config,
      s.status as subscription_status,
      se.ccxt_id as exchange_ccxt_id,
      sm.symbol as market_symbol,
      ak.api_key_encrypted, ak.api_secret_encrypted, ak.api_password_encrypted, ak.api_uid_encrypted
    from public.bots b
    join public.subscriptions s on s.bot_id = b.id
    join public.supported_exchanges se on se.id = b.exchange_id
    join public.supported_markets sm on sm.id = b.market_id
    join public.api_keys ak on ak.bot_id = b.id
    where b.id = %s
    limit 1
    """
    with conn() as c:
        with c.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(q, (bot_id,))
            row = cur.fetchone()
    if not row:
        raise RuntimeError(f"bot_not_found: {bot_id}")
    return dict(row)

def write_event(bot_id: str, user_id: str, event_type: str, message: str):
    q = """
    insert into public.bot_events (bot_id, user_id, event_type, message)
    values (%s, %s, %s, %s)
    """
    try:
        with conn() as c:
            with c.cursor() as cur:
                cur.execute(q, (bot_id, user_id, event_type, message))
                c.commit()
    except Exception:
        pass

def upsert_state(bot_id: str, user_id: str, state: Dict[str, Any]):
    q = """
    insert into public.bot_state (
      bot_id, user_id,
      in_position, direction, entry_price, entry_time, qty, base_notional,
      peak_price, low_price, added_levels, week_trade_counts,
      last_exit_time, last_candle_time,
      cumulative_pnl, max_unrealized_pnl, min_unrealized_pnl,
      updated_at
    ) values (
      %s, %s,
      %s, %s, %s, %s, %s, %s,
      %s, %s, %s, %s::jsonb,
      %s, %s,
      %s, %s, %s,
      now()
    )
    on conflict (bot_id) do update set
      in_position = excluded.in_position,
      direction = excluded.direction,
      entry_price = excluded.entry_price,
      entry_time = excluded.entry_time,
      qty = excluded.qty,
      base_notional = excluded.base_notional,
      peak_price = excluded.peak_price,
      low_price = excluded.low_price,
      added_levels = excluded.added_levels,
      week_trade_counts = excluded.week_trade_counts,
      last_exit_time = excluded.last_exit_time,
      last_candle_time = excluded.last_candle_time,
      cumulative_pnl = excluded.cumulative_pnl,
      max_unrealized_pnl = excluded.max_unrealized_pnl,
      min_unrealized_pnl = excluded.min_unrealized_pnl,
      updated_at = now()
    """
    with conn() as c:
        with c.cursor() as cur:
            cur.execute(q, (
                bot_id, user_id,
                bool(state.get("in_position", False)),
                state.get("direction") or None,
                state.get("entry_price"),
                state.get("entry_time"),
                state.get("qty"),
                state.get("base_notional"),
                state.get("peak_price"),
                state.get("low_price"),
                int(state.get("added_levels", 0)),
                json.dumps(state.get("week_trade_counts", {}) or {}),
                state.get("last_exit_time"),
                state.get("last_candle_time"),
                float(state.get("cumulative_pnl", 0.0)),
                float(state.get("max_unrealized_pnl", 0.0)),
                float(state.get("min_unrealized_pnl", 0.0)),
            ))
            c.commit()

def insert_position_open(bot_id: str, user_id: str, direction: str, entry_price: float, entry_time: str, qty: float) -> str:
    q = """
    insert into public.bot_positions (bot_id, user_id, direction, entry_price, entry_time, qty, status)
    values (%s, %s, %s, %s, %s::timestamptz, %s, 'open')
    returning id
    """
    with conn() as c:
        with c.cursor() as cur:
            cur.execute(q, (bot_id, user_id, direction, entry_price, entry_time, qty))
            pid = cur.fetchone()[0]
            c.commit()
            return str(pid)

def close_position(position_id: str, exit_price: float, exit_time: str, realized_pnl: float):
    q = """
    update public.bot_positions
    set exit_price = %s,
        exit_time = %s::timestamptz,
        realized_pnl = %s,
        status = 'closed'
    where id = %s
    """
    with conn() as c:
        with c.cursor() as cur:
            cur.execute(q, (exit_price, exit_time, realized_pnl, position_id))
            c.commit()

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
    q = """
    insert into public.bot_trades (
      bot_id, user_id, position_id,
      side, price, qty, fee, pnl, exchange_order_id,
      executed_at
    ) values (
      %s, %s, %s,
      %s, %s, %s, %s, %s, %s,
      %s::timestamptz
    )
    """
    with conn() as c:
        with c.cursor() as cur:
            cur.execute(q, (
                bot_id, user_id, position_id,
                side, price, qty, fee, pnl, exchange_order_id,
                executed_at
            ))
            c.commit()