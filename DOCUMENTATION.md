# TradeBothub Bot Runtime Documentation

This document captures everything a new engineer needs to know about the bot runtime: what services it provides, how health evidence is emitted, how Supabase RPCs are consumed, and which operational conventions must be respected. It references the Python-side implementation (`bot/infra/db.py`, `bot/health`, `bot/trading`, `bot/runtime`) and the Supabase contract summarized in `RPCs.md`.

## 1. Runtime responsibilities

1. **Context loading (`bot/runtime/bootstrap.py`).**  
   * Reads `BOT_ID` at startup and calls `bot_runtime_get_context` (via `bot/infra/db.fetch_bot_context_row`) to hydrate the `BotContext`, including configs, API keys, subscriptions, and strategy definitions.  
   * Sets up encryption, exchange client, health reporter, and exchange sync service, then enters the main loop (`bot/runtime/loop.py`).

2. **Strategy loop (`bot/runtime/loop.py`).**  
   * Runs the selected strategy, refreshes control overrides periodically through `bot_runtime_refresh_controls`, touches heartbeats, polls the exchange sync service, and handles pause/kill conditions.  
   * Errors are recorded with `write_event` & `notify` (RPC-based) and flow through the health reporter for fast flushes on critical issues.

3. **Exchange sync (`bot/services/exchange_sync.py`).**  
   * Reads the canonical open position via `bot_runtime_get_position` and validates identity fields.  
   * Fetches the entry order and symbol-scoped exchange position and, if present, writes the exchange snapshot to `bot_runtime_upsert_position`.  
   * If the symbol position is missing but the close is confirmed, closes the DB row via RPC; otherwise marks the bot as `missing/mismatch`.

4. **Health reporting (`bot/health`).**  
   * The `HealthReporter` (with `config.py`, `window.py`, `types.py`) tracks facts about auth, market data, strategy ticks, orders, position sync, and DB writes.  
   * Rolling counters live in `HealthWindow` (15-minute sliding windows per key).  
   * `reporter.maybe_flush()` runs every 5 seconds via `start_health_flush_loop`, while `flush_now(reason)` is triggered on critical events (order ack/reject, stream disconnect spikes, indicator errors, DB failures, etc.).  
   * Flushes call `public.upsert_bot_health_evidence(p_bot_id, p_patch)` through `SupabaseRpcClient` in `bot/health/supabase_rpc.py` (service-role key + backoff retries) and log bot_id, tier, in_position, reason, patch size, rpc latency. Failures are non blocking.

## 2. Supabase RPC Contracts (`RPCs.md`)

All RPCs use service-role credentials (`SUPABASE_URL`, `SUPABASE_SERVICE_ROLE_KEY`) and the per-runtime token (`x-runtime-token`). The bot enforces:

* `bot_runtime_register` / `bot_runtime_revoke` for runtime lifecycle (not in Python yet).
* `bot_runtime_get_context` loads bot metadata; the helper in `fetch_bot_context_row` now relies on this RPC instead of multiple tables.
* `bot_runtime_get_position` / `bot_runtime_upsert_position` model canonical exchange sync.
* `bot_runtime_upsert_trade` handles the journal via dual-key (client_order_id pre-ack, exchange_order_id afterwards).
* `bot_runtime_heartbeat`, `bot_runtime_refresh_controls`, `bot_runtime_notify` manage heartbeat, control refresh, and notifications.

## 3. Trade lifecycle (client-order-id-first)

1. **Client order ID generation (`bot/utils/ids.py`).**  
   * Each order uses `generate_client_order_id(bot_id, suffix)` before sending to the exchange (`bot/trading/orders.py`).
2. **RPC-only journaling (`bot/infra/db.insert_trade`).**  
   * Every trade write goes through `bot_runtime_upsert_trade`. Emergency/manual trades also call this helper with an RPC payload containing `client_order_id`, symbol, status, and optionally `exchange_order_id`.
   * Dual-key RPC ensures the row is first upserted by client ID and later attached to the stable `exchange_order_id`.
3. **Journal events (`bot/trading/journal.py`).**  
   * Entry/pyramid/exit events now pass client/exchange IDs and descriptive statuses to `insert_trade`, ensuring all journal entries hit the RPC boundary. Direct table inserts have been eliminated.

## 4. Notifications and events

* `write_event`, `notify`, and `queue_email_notification` now use `bot_runtime_notify`/service-role RPCs or direct Supabase clients as needed.  
* Health reporter logging includes event metadata for operators to trace flush triggers.

## 5. Operational requirements

* **Environments:** `SUPABASE_URL`, `SUPABASE_SERVICE_ROLE_KEY`, `BOT_ID`, `POLLING_TIER` (optional), and `RUNTIME_TOKEN` must be set. The runtime consistently uses service-role headers for RPCs and never the anon key.  
* **Health evidence throttles:** Tier determines flush intervals (fast/in position vs standard) plus debounce (no more than one flush every 3 seconds).  
* **Database migrations:** `migrations/20260125_exchange_truth_full_sync.sql` and the RPC ownership migration must be applied so the RPCs expect the correct columns and indices.  
* **Testing:** Refer to `tests/` for unit coverage (HealthWindow, HealthReporter) and integration mocks for trade/stream/position events.

## 6. What to check when onboarding

1. Verify the RPC (`bot_runtime_upsert_trade`) supports dual keys and the unique indexes exist.  
2. Ensure the `bot_runtime_*` functions honor `x-runtime-token`; the runtime must refresh tokens before expiry.  
3. Observe health reporting delays; critical events should flush immediately via `reporter.flush_now()`.  
4. Watch the exchange sync service: it should never scan all positions and always write through `bot_runtime_upsert_position`.  
5. Confirm logs contain tier/reason metrics and the service-role header usage is enforced (look at `bot/infra/db._rpc_headers`).  

## 7. New Relic structured logging

The runtime now emits `BotHeartbeat`, `BotLoop`, `BotGate`, `BotTrade`, and `BotError` events through `bot.runtime.logging_contract`. Each event bundles identity/metadata from `BotContext`, rolling metrics, position snapshots, and policy fields such as `poll_effective_s` and `gate_reason`.  

- `BotHeartbeat` is emitted every tick from `run_loop`, includes `boot_id`, `heartbeat_seq`, and `poll_*` attributes, and flags `in_position` severity.  
- `BotLoop` carries the lightweight counters (`loop_ms`, `sleep_ms`, `exchange_calls`, `db_writes`, etc.) computed by the new `RuntimeMetrics` object.  
- `BotGate` fires when trading is blocked by subscriptions/kill switches.  
- `BotTrade` is emitted from every journal entry (entry/pyramid/exit) once the client-order-id flow hits the RPC boundary.  
- `BotError` surfaces clustered runtime failures and includes stage/retry/backoff information.

Structured events are sent through the existing New Relic log API integration (`bot.core.logging.send_structured_event`), keeping the transport unchanged while exposing the required `eventType` schema for dashboards and alerts.

This document and the source code together provide the blueprint for onboarding a new engineer to the TradeBothub bot runtime. Follow the RPC contracts, keep journaling through the service key boundary, and rely on the health reporter for visibility. If something seems missing, check `RPCs.md`, the migrations, and the health module (`bot/health`). 
