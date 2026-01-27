# Supabase RPC Contracts for Bot Runtime

All RPCs below must be called with the **service role** key (`SUPABASE_SERVICE_ROLE_KEY`). Each call reads or mutates data for a single `bot_id`; requests must reject or return an error if `bot_id` is missing, invalid, or if the caller is not the owning runtime.

## 1. `rpc.get_bot_context`
- **Method**: `SELECT public.get_bot_context(p_bot_id => uuid)`
- **Return**: single record that joins
  - `bots` (strategy, mode, exchange, symbol, configs)
  - `api_keys` (encrypted credentials)
  - `supported_exchanges`, `supported_markets` metadata
  - `subscriptions` status (default `inactive`)
  - `strategy_profiles` overrides (when present)
- **Validation**: ensure `p_bot_id` exists and is active; error otherwise.

## 2. `rpc.get_bot_position`
- **Method**: `SELECT public.get_bot_position(p_bot_id => uuid, p_status => text DEFAULT 'open')`
- **Return**: the canonical row from `bot_positions` filtered by `(bot_id, status)` plus columns defined in `migrations/20260125_exchange_truth_full_sync.sql` (entry/exchange IDs, payloads, sync timestamps).
- **Validation**: enforce `bot_id` matches the runtime caller; throw if no matching row when status is `open` (caller expects existing position) or return empty when none.

## 3. `rpc.upsert_bot_position`
- **Method**: `SELECT public.upsert_bot_position(p_bot_id => uuid, p_payload => jsonb)`
- **Payload Keys**: optionally include any of `position_id`, `status`, `qty`, `entry_price`, `mark_price`, `unrealized_pnl`, `symbol`, `exchange`, `margin_mode`, `position_side`, `exchange_payload`, `entry_exchange_order_id`, `entry_client_order_id`, `exit_exchange_order_id`, `exit_client_order_id`, `realized_pnl`, `last_exchange_sync_at`.
- **Behavior**: update the targeted `bot_positions` row scoped to `(bot_id, position_id)` or insert when `position_id` missing for new open positions, and always set `exchange_payload`/source fields as needed. The trigger will sync `bot_state`.
- **Validation**: reject if payload attempts to mutate rows outside the caller’s `bot_id`, or if essential identity fields are missing while closing.

## 4. `rpc.upsert_bot_trade`
- **Method**: `SELECT public.upsert_bot_trade(p_bot_id => uuid, p_exchange_order_id => text, p_payload => jsonb)`
- **Payload Keys**: `client_order_id`, `position_id`, `order_type`, `order_status`, `reduce_only`, `filled_qty`, `avg_fill_price`, `symbol`, `side`, `exchange_payload`, `order_price`, etc.
- **Behavior**: insert-or-update `bot_trades` keyed by `(bot_id, exchange_order_id)`;
  set `position_id`/`status`/`filled_qty`, and keep `exchange_payload` as jsonb.
- **Validation**: ensure `exchange_order_id` provided and non-empty; reject if `bot_id` does not own the trade.

## 5. `rpc.tick_heartbeat`
- **Method**: `SELECT public.tick_heartbeat(p_bot_id => uuid, p_payload => jsonb)`
- **Payload**: include `heartbeat_at`, optional event message, `exchange_sync_status`, etc. Updates `bot_state` + `bot_events` in a single transaction if necessary.
- **Validation**: reject if `bot_id` missing.

## 6. `rpc.refresh_controls`
- **Method**: `SELECT public.refresh_controls(p_bot_id => uuid)`
- **Return**: `control_config`, `execution_config`, `subscription_status`. Enables runtime to get control overrides without inspecting multiple tables.
- **Validation**: ensure `bot_id` exists.

## 7. `rpc.write_notification`
- **Method**: `SELECT public.write_notification(p_bot_id => uuid, p_channel => text, p_payload => jsonb)`
- **Behavior**: inserts into `notifications` or `notification_queue` depending on channel, using service role.
- **Validation**: ensure `bot_id` owns notification context.

## Permissions
- All RPCs must be defined with `SECURITY DEFINER` and owner schema `public`. They verify `p_bot_id` belongs to the runtime (e.g., by comparing to a session variable or performing a lightweight join) so a running bot cannot mutate another bot's data.
- Use the service role key inside the bot runtime: the key is stored in `SUPABASE_SERVICE_ROLE_KEY` and must never be replaced with the anon key. Each RPC request should include headers `apikey` + `Authorization: Bearer` both set to the service key.

## Summary
These RPCs allow the runtime to
1. load context data via one call,
2. query/update the canonical position row,
3. journal order/trade updates,
4. emit heartbeats, and
5. refresh controls without scanning tables manually.
