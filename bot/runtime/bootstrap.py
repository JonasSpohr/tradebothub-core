from bot.core.types import BotContext
from bot.core.config import normalize_configs
from bot.infra.db import fetch_bot_context_row, write_event, notify
from bot.core.logging import log

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
    ctx = load_context(bot_id)

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
        log(f"Startup blocked: {reason}")
        return

    write_event(ctx.id, ctx.user_id, "started", f"strategy={ctx.strategy} tf={ctx.execution_config['timeframe']}")
    notify(
        ctx.user_id,
        ctx.id,
        "bot_started",
        "Bot started",
        body=f"strategy={ctx.strategy} tf={ctx.execution_config['timeframe']}",
        severity="info",
    )
    run_loop(ctx)
