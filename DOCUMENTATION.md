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

#### 7.1 Event templates & identity

The canonical logging templates keep payloads small, rely on `eventType`, and avoid raw candle data. The heartbeat and loop samples below show the attributes every event must carry:

```jsonc
{
  "eventType": "BotHeartbeat",
  "bot_id": "uuid",
  "user_id": "uuid",
  "bot_status": "running",
  "mode": "paper",
  "exchange": "binance",
  "symbol": "BTC/USDT",
  "timeframe": "1m",
  "strategy": "ema_cross",
  "bot_version": "v1.2.3",
  "subscription_status": "active",
  "trading_enabled": true,
  "kill_switch": false,
  "poll_expected_s": 30,
  "tier_min_s": 30,
  "poll_effective_s": 30,
  "tier": "standard",
  "in_position": true,
  "position_id": "uuid",
  "position_side": "long",
  "boot_id": "uuid",
  "heartbeat_seq": 123,
  "runtime_provider": "fly",
  "fly_region": "syd",
  "fly_machine_id": "machine-123"
}
```

```jsonc
{
  "eventType": "BotLoop",
  "bot_id": "uuid",
  "user_id": "uuid",
  "mode": "paper",
  "exchange": "binance",
  "symbol": "BTC/USDT",
  "timeframe": "1m",
  "strategy": "ema_cross",
  "bot_version": "v1.2.3",
  "poll_effective_s": 30,
  "in_position": true,
  "loop_ms": 380,
  "sleep_ms": 0,
  "ohlcv_fetch_ms": 120,
  "indicators_ms": 70,
  "decision_ms": 25,
  "exchange_calls": 6,
  "db_writes": 2,
  "bars_returned": 200,
  "cache_hit_ohlcv": false
}
```

BotGate/Trade/Error events reuse the identity block above and add only fields specific to that signal (gate reason, trade details, error metadata). Emit them only when the related condition occurs so dashboards focus on changes.

#### 7.2 Field mapping rules

| Current field | New field | Required | Default | Source | Notes |
| --- | --- | --- | --- | --- | --- |
| `bot_id` | `bot_id` | yes | n/a | DB | From `ctx.id`. |
| `user_id` | `user_id` | yes | n/a | DB | `ctx.user_id`. |
| `bot_status` | `bot_status` | yes | `unknown` | DB | `ctx.status`. |
| persisted mode | `mode` | yes | persisted value | DB | `ctx.mode` (`paper` when `dry_run`). |
| `exchange_ccxt_id` | `exchange` | yes | `unknown` | DB | `ctx.exchange_ccxt_id`. |
| `market_symbol` | `symbol` | yes | `unknown` | DB | `ctx.market_symbol`. |
| `strategy` | `strategy` | yes | `unknown` | DB | `ctx.strategy`. |
| `execution_config.timeframe` | `timeframe` | yes | `None` | DB | Execution config value. |
| `bot_version` | `bot_version` | optional | `None` | DB | Derived from `ctx.bot_version`. |
| `subscriptions.status` | `subscription_status` | yes | `inactive` | DB | Latest subscription row. |
| `control_config.trading_enabled` | `trading_enabled` | yes | `True` | DB | Defaults to true. |
| `control_config.kill_switch` | `kill_switch` | yes | `False` | DB | Defaults to false. |
| Polling configs | `poll_expected_s`, `tier_min_s`, `poll_effective_s`, `tier` | yes | derived | DB/runtime | `bot.runtime.logging_contract.get_poll_settings`. |
| Position snapshot | `in_position`, `position_id`, `position_side` | yes | `False`, `null`, `null` | runtime | Based on `PositionState`. |
| Runtime metadata | `runtime_provider`, `fly_region`, `fly_machine_id` | optional | env fallback | context | Part of `BotContext`. |
| Runtime metrics | `loop_ms`, `sleep_ms`, `ohlcv_fetch_ms`, `indicators_ms`, `decision_ms`, `exchange_calls`, `db_writes`, `bars_returned`, `cache_hit_ohlcv` | yes for BotLoop | 0 | runtime | Captured by `RuntimeMetrics.snapshot()`. |

#### 7.3 Python helper usage

The helpers in `bot/runtime/logging_contract.py` already enforce these schemas. Use them like this:

```python
from bot.runtime.logging_contract import (
    BotLogContext,
    emit_bot_loop,
    emit_bot_heartbeat,
    emit_bot_gate,
    emit_bot_trade,
    emit_bot_error,
    runtime_metrics,
)

ctx = <BotContext>  # hydrated via fetch_bot_context_row
log_ctx = ctx._log_context or BotLogContext()
position_snapshot = {
    "in_position": POSITION_STATE.in_position,
    "position_id": POSITION_STATE.position_id,
    "position_side": POSITION_STATE.direction,
}

runtime_metrics.begin_tick()
# after running the strategy tick
runtime_metrics.finish_loop()
runtime_metrics.set_sleep_ms(interval_ms)

emit_bot_loop(ctx, log_ctx, position_snapshot)
emit_bot_heartbeat(ctx, log_ctx, position_snapshot)

emit_bot_gate(ctx, log_ctx, position_snapshot, gate_reason="subscription_inactive")

emit_bot_trade(
    ctx,
    action="entry",
    side="long",
    qty=0.003,
    price=68250.5,
    position_id=position_snapshot["position_id"],
    exchange_order_id="binance_123",
    trade_id="trade-uuid",
    notional_usd=204.75,
    slippage_bps=12.3,
    realized_pnl=4.22,
)

emit_bot_error(
    ctx,
    error_class="ccxt.NetworkError",
    error_code="ETIMEDOUT",
    error_stage="fetch_ohlcv",
    retry_attempt=2,
    backoff_s=5.0,
    is_fatal=False,
)
```

#### 7.4 NRQL recipes

1. **Seconds since last heartbeat**
   ```nrql
   FROM Log
   SELECT (now() - latest(timestamp)) / 1000 AS seconds_since_heartbeat
   WHERE eventType = 'BotHeartbeat'
   FACET bot_id, in_position, poll_effective_s, mode, exchange, symbol
   LIMIT 200
   ```

2. **In-position bots missing heartbeat**
   ```nrql
   FROM Log
   SELECT (now() - latest(timestamp)) / 1000 AS gap_s
   WHERE eventType = 'BotHeartbeat' AND in_position = true
   FACET bot_id, poll_effective_s, exchange, symbol
   LIMIT 200
   ```

3. **Exchange call volume**
   ```nrql
   FROM Log
   SELECT average(exchange_calls) AS avg_calls, max(exchange_calls) AS max_calls
   WHERE eventType = 'BotLoop'
   FACET bot_id, exchange, poll_effective_s
   SINCE 30 minutes ago
   ```

4. **Indicator runtime cost**
   ```nrql
   FROM Log
   SELECT average(indicators_ms) AS avg_indicators_ms, max(indicators_ms) AS max_indicators_ms
   WHERE eventType = 'BotLoop'
   FACET bot_id, strategy, timeframe
   SINCE 30 minutes ago
   ```

5. **Error rate by class/stage**
   ```nrql
   FROM Log
   SELECT count(*)
   WHERE eventType = 'BotError'
   FACET error_class, error_stage, exchange
   SINCE 60 minutes ago
   ```

#### 7.5 Heartbeat alert policy

- Warning when `seconds_since_heartbeat > 2 * poll_effective_s` and `in_position = false`.  
- Critical when `seconds_since_heartbeat > 1.5 * poll_effective_s` and `in_position = true` (use tighter thresholds for fast tiers).  
- Always compute `poll_effective_s = max(poll_expected_s, tier_min_s)` so alerts respect the tier minima defined in `POLLING_TIER_MINIMUMS`.

## 8. Dry run behavior

When a bot is configured with `dry_run = true`, the runtime forces its `mode` to `"paper"` immediately after loading context (`bot/runtime/bootstrap.py`). This guarantees every downstream component—logging, order placement, metrics, and structured events—consistently sees the bot as paper-trading, even if the persisted `mode` is `live`.

## 9. Indicator registry & catalog

`bot/indicators.py` now owns the full indicator catalog. Every indicator is registered with `IndicatorSpec` metadata (id, defaults, allowed window range, optional parameters, and outputs). Strategies call `compute_indicator(indicator_id, df, params)` to fetch a `pd.Series` or multi-output dictionary, and the registry enforces source normalization (`close`, `hl2`, `hlc3`, `ohlc4`, `volume`) plus warm-up (min bars).

`DynamicStrategy` reads JSON definitions from the DB, asks the registry for each indicator, and merges multi-output series as `{indicator_id}__{suffix}` columns before evaluating signals via `_SafeExpr`. The registry currently exposes moving averages (SMA/EMA/WMA/DEMA/TEMA/HMA/VWMA/KAMA/RMA/LSMA/ALMA/T3), oscillators (RSI, Stochastic, MACD, CCI, ROC, Momentum, Williams %R, Ultimate Oscillator, Stoch RSI, Fisher, CMO, TSI, PPO, DPO), volatility/range (ATR, TR, Bollinger, Keltner, Donchian, Supertrend, PSAR, Chaikin Volatility, Choppiness, Mass Index, price vs MA, MA slope/ribbon, Chaikin Vol, Historical Vol, Standard Deviation), volume/breadth (OBV, Volume SMA, RVOL, VWAP/Anchored VWAP, MFI, CMF, ADL, AD Osc, Force Index, Ease of Movement, VROC, NVI, PVI, KVO, VWAP), trend quality (ADX/DI, Aroon, Vortex, Ichimoku, Elder Ray, Connors, Kaufman ER), candle helpers (Heikin Ashi, ATR body, gap/fractal/swing detectors), support/resistance/channel helpers (pivot points, Fibonacci levels, linear regression channel, ATR channel, Chandelier exit, Coppock, MA ribbon stacks, price channels, ZigZag), and structure helpers (True Range, MA ribbon stack). The rest of the 100-item whitelist will be added incrementally; refer to `bot/indicators.py` for the full catalog and parameter guidance.

This document and the source code together provide the blueprint for onboarding a new engineer to the TradeBothub bot runtime. Follow the RPC contracts, keep journaling through the service key boundary, and rely on the health reporter for visibility. If something seems missing, check `RPCs.md`, the migrations, and the health module (`bot/health`). 
