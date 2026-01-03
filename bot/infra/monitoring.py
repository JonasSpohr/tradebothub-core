import os
import tempfile
from bot.core.logging import log
from bot.core.logging import attach_newrelic_handler

def _write_newrelic_config(path: str, app_name: str, license_key: str):
    cfg = f"""[newrelic]
license_key = {license_key}
app_name = {app_name}
distributed_tracing.enabled = true
application_logging.enabled = true
application_logging.forwarding.enabled = true
application_logging.forwarding.log_level = info
"""
    with open(path, "w", encoding="utf-8") as f:
        f.write(cfg)

def init_newrelic():
    license_key = os.getenv("NEW_RELIC_LICENSE_KEY")
    if not license_key:
        return
    app_name = os.getenv("NEW_RELIC_APP_NAME") or os.getenv("BOT_ID") or "tradebothub-bot"
    try:
        import newrelic.agent
        cfg_path = os.path.join(tempfile.gettempdir(), "newrelic.ini")
        _write_newrelic_config(cfg_path, app_name, license_key)
        newrelic.agent.initialize(cfg_path)
        attach_newrelic_handler()
        log(f"New Relic initialized (app={app_name})")
    except Exception as e:
        log(f"[WARN] New Relic init failed: {e}")

def record_exception(exc: Exception, params: dict | None = None):
    if not os.getenv("NEW_RELIC_LICENSE_KEY"):
        return
    try:
        import newrelic.agent
        newrelic.agent.record_exception(exc=exc, params=params or {})
    except Exception:
        pass

def ping_healthchecks():
    url = os.getenv("HC_PING_URL")
    if not url:
        return
    try:
        import requests
        requests.get(url, timeout=5)
    except Exception:
        pass
