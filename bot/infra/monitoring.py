import os
from bot.core.logging import log

def init_newrelic():
    if not os.getenv("NEW_RELIC_LICENSE_KEY"):
        return
    try:
        import newrelic.agent
        newrelic.agent.initialize()
        log("New Relic initialized")
    except Exception as e:
        log(f"[WARN] New Relic init failed: {e}")

def ping_healthchecks():
    url = os.getenv("HC_PING_URL")
    if not url:
        return
    try:
        import requests
        requests.get(url, timeout=5)
    except Exception:
        pass