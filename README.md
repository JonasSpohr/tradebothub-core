# TradeBothub Bot Runtime

This repository contains the Python runtime that drives an individual TradeBothub bot. The runtime:

- loads the bot configuration and encrypted credentials via Supabase RPCs.
- spins up the selected strategy (e.g., `DynamicStrategy`) plus exchange sync, health reporting, and logging helpers.
- journals every trade/order/position update through the service-role `bot_runtime_*` RPCs (never write directly to tables).
- emits structured health evidence and New Relic events so operators can monitor the bot without digging into the database.

All of the runtime logic is guarded by the runtime ownership model documented in `RPCs.md`—the bot only operates on a single `bot_id`, uses a per-runtime token (`x-runtime-token`), and never scans global tables on the exchange.

## Quick local run

1. **Install dependencies**  
   ```bash
   pip install -r requirements.txt
   ```

2. **Prepare environment variables** *(see notes below)*  

3. **Run the bot process**  
   ```bash
   BOT_ID=<uuid> python -m bot.main
   ```

   The entrypoint in production (`docker-entrypoint.sh`) simply wraps this command with `newrelic-admin run-program` so your `BOT_LOG_DIR`, `NEW_RELIC_APP_NAME`, and New Relic instrumentation behave the same locally.

## Required environment variables

| Name | Description |
| --- | --- |
| `BOT_ID` | UUID of the bot row this process owns. |
| `SUPABASE_URL` | Base URL for the Supabase project hosting the tables/RPCs. |
| `SUPABASE_SERVICE_ROLE_KEY` | Service-role key—**never** substitute the anon key. Used for all RPC calls. |
| `RUNTIME_TOKEN` | One-time secret hashed/stored in `bot_runtime.runtime_token_hash`; send this as `x-runtime-token` header. |
| `POLLING_TIER` | Optional override (`fast_5s`, `ultra_15s`, `fast_30s`, `standard`). |
| `BOT_LOG_DIR` | Optional path to persist `tradebothub.log`. Defaults to `/app/logs`. |
| `NEW_RELIC_LICENSE_KEY` | Optional—leave unset to skip monitoring (production uses `newrelic.ini`). |
| `ENABLE_HEALTHCHECKS_IO` | Set to `true` to enable healthchecks.io integration; requires `HEALTHCHECKS_API_KEY`. Defaults to `false`. |

Additional runtime metadata (provider, region, machine ID) comes from `bot_runtime` and is surfaced in structured logs.

## Health reporting & structured logging

- **Health evidence**: `bot/health` implements `HealthReporter`, `HealthWindow`, and `SupabaseRpcClient`. Every flush calls `public.upsert_bot_health_evidence(p_bot_id, p_patch)` with rolling counters, auth/market/order facts, and DB health evidence. Flush intervals are tier-aware (see `bot/health/config.py`), and critical events call `flush_now(reason)` immediately. When `ENABLE_HEALTHCHECKS_IO=true` (plus `HEALTHCHECKS_API_KEY`), the runtime will also manage healthchecks.io pings and fail notifications.
- **New Relic events**: `bot/runtime/logging_contract.py` surfaces `BotHeartbeat`, `BotLoop`, `BotGate`, `BotTrade`, and `BotError`. Emit `BotHeartbeat`+`BotLoop` every tick, and emit the others only on gate changes/errors/trades. The templates guarantee consistent attributes (`bot_id`, `mode`, `poll_effective_s`, `in_position`, etc.) for dashboards/alerts.

## Supabase RPC contract

All reads/writes go through the Supabase RPCs defined in `RPCs.md`. Pay attention to:

- `bot_runtime_get_context` / `_get_position` / `_upsert_position` for exchange truth + bot_state sync.
- `bot_runtime_upsert_trade` for dual-key journaling (client-order-id first, exchange-order-id later).
- `bot_runtime_heartbeat`, `bot_runtime_refresh_controls`, `bot_runtime_notify` for heartbeat/control/notification flows.
- `bot_runtime_register` / `bot_runtime_revoke` for lifecycle management (RPC ownership enforced via `x-runtime-token` + service-role).

Before running the runtime locally, ensure the migrations under `migrations/` are applied so the RPCs see the expected schema/indices.

## Running tests

```bash
pytest
```

The `tests/` directory holds unit coverage for `HealthWindow`, `HealthReporter`, and RPC integrations that mock the Supabase endpoint. Add regression tests when touching health reporting, automatic flushing, or indicator registry logic.

-## Docker & production

Production containers:

- Build: `docker build -t tradebothub-bot .`
- Run with `newrelic-admin` instrumentation and all env vars:
- 
- ```bash
- docker run --rm \
-   -e BOT_ID=<uuid> \
-   -e SUPABASE_URL=<supabase-url> \
-   -e SUPABASE_SERVICE_ROLE_KEY=<service-key> \
-   -e RUNTIME_TOKEN=<runtime-token> \
-   -e NEW_RELIC_LICENSE_KEY=<license> \
-   -e BOT_LOG_DIR=/app/logs \
-   tradebothub-bot
- ```
- 
- The entrypoint lives in `docker-entrypoint.sh`, which sets `BOT_LOG_DIR`, `BOT_LOG_FILE`, and wraps the runtime with `newrelic-admin run-program`.
- Log files stream to `${BOT_LOG_DIR}/tradebothub.log`; override `BOT_LOG_DIR` if you want logs in a different place.

To run locally with Docker but without New Relic:

```bash
docker run --rm \
  -e BOT_ID=<uuid> \
  -e SUPABASE_URL=<supabase-url> \
  -e SUPABASE_SERVICE_ROLE_KEY=<service-key> \
  -e RUNTIME_TOKEN=<runtime-token> \
  -e BOT_LOG_DIR=/tmp/logs \
  tradebothub-bot \
  sh -c "BOT_LOG_DIR=/tmp/logs python -m bot.main"
```

This bypasses the `newrelic-admin` wrapper and writes logs to `/tmp/logs` inside the container.

Use `docker-compose` or other orchestration locally by supplying the same env vars and mapping volumes for consistent logs.

Production containers:

- Build: `docker build -t tradebothub-bot .`
- Run with the required env vars (at minimum: `BOT_ID`, `SUPABASE_URL`, `SUPABASE_SERVICE_ROLE_KEY`, `RUNTIME_TOKEN`, `NEW_RELIC_LICENSE_KEY` if monitoring).
- The entrypoint launches `python -m bot.main` via `newrelic-admin run-program`. Logs go to `BOT_LOG_FILE`, and the process secrets are rotated as migration updates occur.

## Indicators & strategies

`bot/indicators.py` contains the catalog of whitelist indicators (moving averages, oscillators, volatility, volume-derived metrics, candle helpers, etc.). `DynamicStrategy` loads definitions from Supabase and asks the registry for each spec—every indicator enforces parameter validation, warm-up, aliasing, and consistent column naming (`{id}__{output}` or `{id}__{source}...`). Strategies only reference series columns and constants; they never execute indicator functions directly.

## Further reading

- `DOCUMENTATION.md` – In-depth onboarding guide (health evidence, logging, RPC expectations).  
- `RPCs.md` – Supabase RPC contracts and ownership guards.  
- `migrations/` – SQL updates required before deploying the runtime.  
- `bot/runtime` – Bootstrapping, loops, gates, and runtime-specific helpers.  

If something is missing, search for `_assert_service_role`, `health reporter`, or `bot_runtime_*` within the repo—the architecture is centered on a single canonical bot ID, enforced RPC ownership, and structured observability.
