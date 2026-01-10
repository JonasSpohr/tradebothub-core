from typing import Dict, Optional

from bot.core.types import BotContext


def notification_context_payload(ctx: Optional[BotContext]) -> Dict[str, str]:
    """
    Provide high-level bot/exchange metadata to include alongside queued notifications.
    """
    if not ctx:
        return {}
    return {
        "bot_id": ctx.id,
        "bot_name": ctx.name,
        "user_id": ctx.user_id,
        "strategy": ctx.strategy,
        "mode": ctx.mode,
        "exchange": ctx.exchange_ccxt_id,
        "market": ctx.market_symbol,
    }
