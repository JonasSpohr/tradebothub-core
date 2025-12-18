import time
from bot.core.logging import log
from bot.core.safety import MAX_CONSECUTIVE_ERRORS, ERROR_BACKOFF_SECONDS
from bot.infra.db import write_event
from bot.core.types import BotContext
from bot.strategies import get_strategy
from bot.trading.position import manage_open_position, try_open_position
from bot.infra.monitoring import ping_healthchecks

def run_loop(ctx: BotContext):
    strategy = get_strategy(ctx.strategy)
    poll = int(ctx.execution_config["poll_interval"])
    consec = 0

    log(f"=== RUN {ctx.name} strategy={ctx.strategy} symbol={ctx.market_symbol} poll={poll}s ===")

    while True:
        try:
            manage_open_position(ctx, strategy)
            try_open_position(ctx, strategy)
            ping_healthchecks()
            consec = 0
            time.sleep(poll)
        except Exception as e:
            consec += 1
            write_event(ctx.id, ctx.user_id, "error", str(e))
            log(f"ERROR: {e} (consecutive={consec})")

            if consec >= MAX_CONSECUTIVE_ERRORS:
                write_event(ctx.id, ctx.user_id, "stopped", "Too many consecutive errors")
                log("Too many consecutive errors; exiting.")
                return

            time.sleep(ERROR_BACKOFF_SECONDS)