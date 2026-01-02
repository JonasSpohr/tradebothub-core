from bot.core.types import BotContext
from bot.core.config import normalize_configs
from bot.infra.db import fetch_bot_context_row, write_event, notify, upsert_state, set_bot_status
from bot.core.logging import log, set_log_context
from bot.state import PositionState
from bot.infra.crypto import decrypt
from bot.infra.exchange import create_exchange, fetch_ohlcv_df, fetch_last_price
from bot.infra.monitoring import record_exception

def load_context(bot_id: str) -> BotContext:
    row = fetch_bot_context_row(bot_id)

    sc, rc, ec, cc = normalize_configs(
        row.get("strategy_config") or {},
        row.get("risk_config") or {},
        row.get("execution_config") or {},
        row.get("control_config") or {},
    )

    return BotContext(
        id=row["id"],
        user_id=row["user_id"],
        name=row["name"],
        strategy=row["strategy"],
        mode=row["mode"],
        dry_run=bool(row["dry_run"]),
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
    )

def start(bot_id: str):
    from bot.infra.monitoring import init_newrelic
    from bot.runtime.loop import run_loop
    from bot.runtime.gates import startup_gate

    init_newrelic()
    try:
        ctx = load_context(bot_id)
        log(f"Loaded context for bot {ctx.id} ({ctx.name})")
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

        ok, reason = startup_gate(ctx)
        if not ok:
            write_event(ctx.id, ctx.user_id, "stopped", f"Startup blocked: {reason}")
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
        _assert_connectivity(ctx)
        log("Connectivity verified; entering main loop")

        write_event(ctx.id, ctx.user_id, "started", f"strategy={ctx.strategy} tf={ctx.execution_config['timeframe']}")
        write_event(ctx.id, ctx.user_id, "status", "running")
        set_bot_status(ctx.id, "running")
        notify(
            ctx.user_id,
            ctx.id,
            "bot_started",
            "Bot started",
            body=f"strategy={ctx.strategy} tf={ctx.execution_config['timeframe']}",
            severity="info",
        )
        log("Entering main loop")
        run_loop(ctx)
    except Exception as e:
        # Friendly message + detailed New Relic report.
        msg = "Startup failed: could not establish connectivity with exchange. Review your API keys and market settings."
        log(msg, level="ERROR")
        log(f"[startup debug] {type(e).__name__}: {e}", level="ERROR")
        record_exception(e, {"bot_id": bot_id})
        try:
            write_event(bot_id, getattr(ctx, "user_id", None) or "", "error", msg)
            set_bot_status(bot_id, "error")
            notify(
                getattr(ctx, "user_id", None) or "",
                bot_id,
                "startup_failed",
                "Bot failed to start",
                body=str(e),
                severity="critical",
            )
        except Exception:
            pass
        return

def _assert_connectivity(ctx: BotContext):
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
        raise RuntimeError("Could not decrypt API credentials. Check BOT_ENC_KEY and stored keys.") from e

    if not api_key or not api_secret:
        raise RuntimeError("Missing API key/secret after decrypt. Please re-enter your exchange keys.")

    try:
        log(f"Connectivity check: creating exchange client {ctx.exchange_ccxt_id}")
        ex = create_exchange(ctx.exchange_ccxt_id, api_key, api_secret, api_password, api_uid)
    except Exception as e:
        raise RuntimeError(f"Failed to create exchange client ({ctx.exchange_ccxt_id}). Check credentials and exchange id.") from e

    try:
        log(f"Connectivity check: fetching ticker for {ctx.market_symbol}")
        fetch_last_price(ex, ctx.market_symbol)
    except Exception as e:
        raise RuntimeError(f"Could not fetch ticker for {ctx.market_symbol}. Verify the symbol is correct and supported.") from e

    try:
        log(f"Connectivity check: fetching OHLCV for {ctx.market_symbol} tf={ctx.execution_config['timeframe']}")
        fetch_ohlcv_df(ex, ctx.market_symbol, ctx.execution_config["timeframe"], 5)
    except Exception as e:
        raise RuntimeError(f"Could not fetch market data for {ctx.market_symbol} on timeframe {ctx.execution_config['timeframe']}.") from e

    try:
        from bot.infra.exchange import fetch_quote_balance
        log(f"Connectivity check: fetching balance for quote currency of {ctx.market_symbol}")
        bal = fetch_quote_balance(ex, ctx.market_symbol)
        log(f"Connectivity check: balance={bal}")
    except Exception as e:
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
