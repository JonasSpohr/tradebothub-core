from bot.core.types import BotContext

def startup_gate(ctx: BotContext) -> tuple[bool, str]:
    if ctx.subscription_status != "active":
        return False, "subscription_not_active"
    if ctx.control_config.get("admin_override", False):
        return False, "admin_override"
    if ctx.control_config.get("kill_switch", False):
        return False, "kill_switch"
    if not ctx.control_config.get("trading_enabled", True):
        return False, "trading_disabled"
    return True, "ok"