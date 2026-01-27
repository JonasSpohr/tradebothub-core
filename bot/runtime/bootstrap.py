import os
import threading
import time

from bot.core.types import BotContext
from bot.core.config import normalize_configs
from bot.infra.db import (
    fetch_bot_context_row,
    queue_email_notification,
    write_event,
    notify,
    upsert_state,
    set_bot_status,
)
from bot.infra.notifications import notification_context_payload
from bot.core.logging import log, set_log_context
from bot.state import PositionState
from bot.infra.crypto import decrypt
from bot.infra.exchange import create_exchange, fetch_ohlcv_df, fetch_last_price
from bot.infra.monitoring import record_exception
from bot.infra.healthcheck import ensure_healthcheck, healthchecks_enabled
from bot.strategies.dynamic import DynamicStrategy
from bot.trading.position import STATE, _exchange
from bot.health.reporter import HealthReporter, init_reporter, start_health_flush_loop
from bot.health.types import map_exception_to_reason, is_rate_limit_exception
from bot.exchange.provider import CcxtExchangeProvider
from bot.services.exchange_sync import ExchangeSyncError, ExchangeSyncService
from bot.runtime.logging_contract import BotLogContext, runtime_metrics

def _merge_section(base: dict, overlay: dict) -> dict:
    out = dict(base or {})
    for k, v in (overlay or {}).items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _merge_section(out[k], v)
        else:
            out[k] = v
    return out

def _extract_sections(source: dict) -> tuple[dict, dict, dict, dict]:
    if not isinstance(source, dict):
        return {}, {}, {}, {}
    # support both {strategy: {...}} and {strategy_config: {...}}
    def _sec(names):
        for n in names:
            val = source.get(n)
            if isinstance(val, dict):
                return dict(val)
        return {}
    return (
        _sec(["strategy", "strategy_config"]),
        _sec(["risk", "risk_config"]),
        _sec(["execution", "execution_config"]),
        _sec(["control", "control_config"]),
    )

def _resolve_configs(definition: dict, profile_overrides: dict, user_overrides: dict, bot_cfgs: dict):
    """
    Merge definition defaults -> profile overrides -> user overrides -> persisted bot configs.
    """
    defaults = definition.get("defaults") or {}
    def_sc, def_rc, def_ec, def_cc = _extract_sections(defaults)
    ov_sc, ov_rc, ov_ec, ov_cc = _extract_sections(profile_overrides or {})
    user_sc, user_rc, user_ec, user_cc = _extract_sections(user_overrides or {})
    bot_sc, bot_rc, bot_ec, bot_cc = (
        bot_cfgs.get("strategy_config") or {},
        bot_cfgs.get("risk_config") or {},
        bot_cfgs.get("execution_config") or {},
        bot_cfgs.get("control_config") or {},
    )

    strategy_cfg = _merge_section(_merge_section(_merge_section(def_sc, ov_sc), user_sc), bot_sc)
    risk_cfg = _merge_section(_merge_section(_merge_section(def_rc, ov_rc), user_rc), bot_rc)
    exec_cfg = _merge_section(_merge_section(_merge_section(def_ec, ov_ec), user_ec), bot_ec)
    control_cfg = _merge_section(_merge_section(_merge_section(def_cc, ov_cc), user_cc), bot_cc)
    return strategy_cfg, risk_cfg, exec_cfg, control_cfg

def load_context(bot_id: str) -> BotContext:
    row = fetch_bot_context_row(bot_id)

    sc, rc, ec, cc = _resolve_configs(
        row.get("strategy_definition") or {},
        row.get("strategy_profile_overrides") or {},
        row.get("user_overrides") or {},
        {
            "strategy_config": row.get("strategy_config") or {},
            "risk_config": row.get("risk_config") or {},
            "execution_config": row.get("execution_config") or {},
            "control_config": row.get("control_config") or {},
        },
    )
    sc, rc, ec, cc = normalize_configs(sc, rc, ec, cc)

    bot_status = row.get("status") or row.get("bot_status") or "unknown"
    bot_version = row.get("bot_version") or row.get("version") or row.get("bot_version_id")
    runtime_provider = row.get("runtime_provider") or os.getenv("RUNTIME_PROVIDER")
    fly_region = row.get("fly_region") or os.getenv("FLY_REGION")
    fly_machine_id = row.get("fly_machine_id") or os.getenv("FLY_MACHINE_ID")

    ctx = BotContext(
        id=row["id"],
        user_id=row["user_id"],
        name=row["name"],
        strategy=row.get("strategy_key") or row.get("strategy"),
        mode=row["mode"],
        dry_run=bool(row["dry_run"]),

        status=bot_status,
        bot_version=bot_version,

        subscription_status=row["subscription_status"],
        exchange_ccxt_id=row["exchange_ccxt_id"],
        market_symbol=row["market_symbol"],
        api_key_encrypted=row["api_key_encrypted"],
        api_secret_encrypted=row["api_secret_encrypted"],
        api_password_encrypted=row.get("api_password_encrypted"),
        api_uid_encrypted=row.get("api_uid_encrypted"),
        strategy_config=sc,
        risk_config=rc,
        execution_config=ec,
        control_config=cc,

        runtime_provider=runtime_provider,
        fly_region=fly_region,
        fly_machine_id=fly_machine_id,
    )
    if ctx.dry_run:
        # Dry-run configs must always behave as paper trading, regardless of stored mode.
        ctx.mode = "paper"
    # Attach dynamic strategy instance if definition is present
    if row.get("strategy_definition"):
        ctx._strategy = DynamicStrategy(row["strategy_definition"])
    ctx.strategy_definition = row.get("strategy_definition")
    ctx.strategy_profile_key = row.get("strategy_profile_key")
    ctx.user_overrides = row.get("user_overrides")
    return ctx

def start(bot_id: str):
    from bot.infra.monitoring import init_newrelic
    from bot.runtime.loop import run_loop
    from bot.runtime.gates import startup_gate

    init_newrelic()
    try:
        ctx = load_context(bot_id)
        log(f"Loaded context for bot {ctx.id} ({ctx.name})")
        tier_env = os.environ.get("POLLING_TIER")
        tier_cfg = ctx.execution_config.get("polling_tier")
        tier = tier_env or tier_cfg or "standard"
        reporter = init_reporter(ctx.id, tier=tier)
        ctx._log_context = BotLogContext()
        runtime_metrics.begin_tick()
        start_health_flush_loop(reporter)
        exchange_client = _exchange(ctx)
        exchange_sync = ExchangeSyncService(ctx, CcxtExchangeProvider(exchange_client))
        ctx._exchange_sync_service = exchange_sync
        exchange_sync.startup_sync()
        _start_position_sync_loop(ctx, reporter)
        set_log_context(
            bot_id=ctx.id,
            user_id=ctx.user_id,
            exchange=ctx.exchange_ccxt_id,
            market=ctx.market_symbol,
            strategy=ctx.strategy,
            mode=ctx.mode,
        )

        # Initialize persisted bot state early with defaults.
        upsert_state(ctx.id, ctx.user_id, PositionState(week_trade_counts={}).to_dict())
        write_event(ctx.id, ctx.user_id, "status", "starting")
        set_bot_status(ctx.id, "starting")

        # Subscription / kill / trading checks
        if ctx.subscription_status != "active":
            write_event(ctx.id, ctx.user_id, "inactive_subscription_exit", "Subscription not active")
            log("Subscription inactive; exiting.", level="WARN")
            return
        ok, reason = startup_gate(ctx)
        if not ok:
            write_event(ctx.id, ctx.user_id, "kill_switch_exit", f"Startup blocked: {reason}")
            notify(
                ctx.user_id,
                ctx.id,
                "startup_blocked",
                "Bot startup blocked",
                body=reason,
                severity="warning",
            )
            log(f"Startup blocked: {reason}", level="WARN")
            return

        # Sanity check exchange connectivity and market before entering loop.
        try:
            _assert_connectivity(ctx, reporter)
        except Exception as exc:
            reporter.mark_auth_fail(map_exception_to_reason(exc))
            raise
        log("Connectivity verified; entering main loop")
        reporter.mark_auth_ok()

        # Ensure healthcheck exists and stash ping URL on context
        poll_seconds = int(ctx.execution_config.get("effective_poll_seconds", ctx.execution_config.get("poll_interval", 300)))
        if healthchecks_enabled():
            ctx._hc_ping_url = ensure_healthcheck(ctx.id, f"bot-{ctx.name}", poll_seconds)
        else:
            ctx._hc_ping_url = None

        write_event(ctx.id, ctx.user_id, "started", f"strategy={ctx.strategy} tf={ctx.execution_config['timeframe']}")
        write_event(ctx.id, ctx.user_id, "status", "running")
        set_bot_status(ctx.id, "running")
        log("Entering main loop")
        run_loop(ctx)
    except Exception as e:
        # Friendly message + detailed New Relic report.
        msg = "Startup failed: could not establish connectivity with exchange. Review your API keys and market settings."
        log(msg, level="ERROR")
        log(f"[startup debug] {type(e).__name__}: {e}", level="ERROR")
        record_exception(e, {"bot_id": bot_id})
        ctx_obj = locals().get("ctx")
        user_id = getattr(ctx_obj, "user_id", None) if ctx_obj else None
        user_id_value = user_id or ""
        try:
            write_event(bot_id, user_id_value, "error", msg)
            set_bot_status(bot_id, "error")
            notify(
                user_id_value,
                bot_id,
                "startup_failed",
                "Bot failed to start",
                body=str(e),
                severity="critical",
            )
            ctx_payload = notification_context_payload(ctx_obj)
            queue_email_notification(
                user_id=user_id,
                bot_id=bot_id,
                event_key="startup_failed",
                email_template="bot_startup_failure",
                payload={
                    **ctx_payload,
                    "message": msg,
                    "error": str(e),
                },
            )
            try:
                from bot.infra.healthcheck import fail_healthcheck

                fail_healthcheck(getattr(ctx_obj, "_hc_ping_url", None), str(e))
            except Exception:
                pass
        except Exception:
            pass
        return

def _assert_connectivity(ctx: BotContext, reporter: HealthReporter):
    """
    Verify we can decrypt keys, create exchange client, fetch ticker and a small OHLCV sample.
    Raise user-friendly errors if something fails.
    """
    log(f"Connectivity check: decrypting keys for {ctx.exchange_ccxt_id}")
    try:
        api_key = decrypt(ctx.api_key_encrypted)
        api_secret = decrypt(ctx.api_secret_encrypted)
        api_password = decrypt(ctx.api_password_encrypted)
        api_uid = decrypt(ctx.api_uid_encrypted)
    except Exception as e:
        _maybe_record_rate_limit(reporter, e)
        raise RuntimeError("Could not decrypt API credentials. Check BOT_ENC_KEY and stored keys.") from e

    if not api_key or not api_secret:
        raise RuntimeError("Missing API key/secret after decrypt. Please re-enter your exchange keys.")

    try:
        log(f"Connectivity check: creating exchange client {ctx.exchange_ccxt_id}")
        ex = create_exchange(ctx.exchange_ccxt_id, api_key, api_secret, api_password, api_uid)
    except Exception as e:
        _maybe_record_rate_limit(reporter, e)
        raise RuntimeError(f"Failed to create exchange client ({ctx.exchange_ccxt_id}). Check credentials and exchange id.") from e

    try:
        log(f"Connectivity check: fetching ticker for {ctx.market_symbol}")
        fetch_last_price(ex, ctx.market_symbol)
    except Exception as e:
        _maybe_record_rate_limit(reporter, e)
        raise RuntimeError(f"Could not fetch ticker for {ctx.market_symbol}. Verify the symbol is correct and supported.") from e

    try:
        log(f"Connectivity check: fetching OHLCV for {ctx.market_symbol} tf={ctx.execution_config['timeframe']}")
        fetch_ohlcv_df(ex, ctx.market_symbol, ctx.execution_config["timeframe"], 5)
    except Exception as e:
        _maybe_record_rate_limit(reporter, e)
        raise RuntimeError(f"Could not fetch market data for {ctx.market_symbol} on timeframe {ctx.execution_config['timeframe']}.") from e

    try:
        from bot.infra.exchange import fetch_quote_balance

        log(f"Connectivity check: fetching balance for quote currency of {ctx.market_symbol}")
        bal = fetch_quote_balance(ex, ctx.market_symbol)
        log(f"Connectivity check: balance={bal}")
    except Exception as e:
        _maybe_record_rate_limit(reporter, e)
        raise RuntimeError(f"Could not fetch account balance. Verify API key permissions (trading/reading balances).") from e

    # Connectivity confirmed; record event/notification.
    write_event(ctx.id, ctx.user_id, "connectivity_ok", f"{ctx.exchange_ccxt_id} {ctx.market_symbol}")
    notify(
        ctx.user_id,
        ctx.id,
        "connectivity_ok",
        "Exchange connectivity verified",
        body=f"{ctx.exchange_ccxt_id} {ctx.market_symbol}",
        severity="info",
    )


def _maybe_record_rate_limit(reporter: HealthReporter, exc: Exception) -> None:
    try:
        if is_rate_limit_exception(exc):
            reporter.record_rate_limit_hit()
    except Exception:
        pass


def _start_position_sync_loop(ctx: BotContext, reporter: HealthReporter) -> None:
    def _worker() -> None:
        while True:
            if not STATE.in_position:
                time.sleep(5)
                continue
            diff = _estimate_position_diff(ctx)
            reporter.record_position_sync(diff)
            time.sleep(60)

    thread = threading.Thread(target=_worker, daemon=True, name="health-position-sync")
    thread.start()


def _estimate_position_diff(ctx: BotContext) -> float:
    try:
        if not STATE.in_position:
            return 0.0
        ex = _exchange(ctx)
        base = _extract_base_asset(ctx.market_symbol)
        balance = ex.fetch_balance()
        asset_entry = balance.get(base) or {}
        actual_qty = float(asset_entry.get("total") or asset_entry.get("free") or 0.0)
        return abs(STATE.qty - actual_qty)
    except Exception as exc:
        log(f"[health position] sync failed: {type(exc).__name__}: {exc}", level="WARN")
        return 0.0


def _extract_base_asset(symbol: str) -> str:
    if "/" in symbol:
        return symbol.split("/")[0]
    if "-" in symbol:
        return symbol.split("-")[0]
    return symbol
