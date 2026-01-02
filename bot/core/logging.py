import logging
import sys
from datetime import datetime, timezone
import os
import json
import threading
import requests

_logger = logging.getLogger("bot")
if not _logger.handlers:
    _logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.DEBUG)
    try:
        from newrelic.api.log import NewRelicContextFormatter
        fmt = NewRelicContextFormatter("%(asctime)s %(levelname)s %(message)s")
    except Exception:
        fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    handler.setFormatter(fmt)
    _logger.addHandler(handler)
    _logger.propagate = True

_LEVEL_MAP = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARN": logging.WARNING,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
    "CRITICAL": logging.CRITICAL,
}

_context_attrs: dict = {}

def log(msg: str, level: str = "INFO"):
    lvl = _LEVEL_MAP.get(level.upper(), logging.INFO)
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S%z")
    _logger.log(lvl, f"[{ts}] {msg}")
    _maybe_send_log_api(msg, level.upper(), ts)

def attach_newrelic_handler():
    """
    Attach New Relic log handler to forward logs via agent, if available.
    Safe to call multiple times.
    """
    try:
        from newrelic.api.log_handler import NewRelicLogHandler
    except Exception:
        return
    for h in _logger.handlers:
        if isinstance(h, NewRelicLogHandler):
            return
    try:
        h = NewRelicLogHandler()
        h.setLevel(logging.INFO)
        _logger.addHandler(h)
    except Exception:
        pass

def set_log_context(**kwargs):
    """
    Set additional attributes to include with every log sent via API.
    """
    global _context_attrs
    for k, v in kwargs.items():
        if v is not None:
            _context_attrs[k] = v

def _maybe_send_log_api(message: str, level: str, ts: str):
    """
    Optional: send logs via New Relic Log API for testing. Controlled by NEW_RELIC_LICENSE_KEY.
    """
    license_key = os.getenv("NEW_RELIC_LICENSE_KEY")
    if not license_key:
        return
    endpoint = os.getenv("NEW_RELIC_LOG_API", "https://log-api.newrelic.com/log/v1")
    service = os.getenv("NEW_RELIC_APP_NAME") or os.getenv("BOT_ID") or "tradebothub-bot"

    common_attrs = {
        "service.name": service,
        "env": os.getenv("ENV") or "test",
        "bot_id": os.getenv("BOT_ID"),
        "exchange": os.getenv("EXCHANGE") or os.getenv("EXCHANGE_ID"),
        "market": os.getenv("MARKET") or os.getenv("MARKET_SYMBOL"),
    }

    msg_text = f"{ts} [{level}] {message}"

    payload = [
        {
            "common": {"attributes": common_attrs},
            "logs": [
                {
                    "timestamp": int(datetime.now(timezone.utc).timestamp() * 1000),
                    "message": msg_text,
                    "attributes": {
                        **common_attrs,
                        "level": level.lower(),
                        "message": msg_text,
                        "message_raw": message,
                        "ts": ts,
                        **_context_attrs,
                    },
                }
            ],
        }
    ]

    def _send():
        try:
            requests.post(
                endpoint,
                headers={
                    "Api-Key": license_key,
                    "Content-Type": "application/json",
                },
                data=json.dumps(payload),
                timeout=3,
            )
        except Exception:
            pass

    threading.Thread(target=_send, daemon=True).start()
