import logging
import sys
from datetime import datetime, timezone
import json
import os
import threading
import requests

_LOG_FILE_PATH = os.getenv("BOT_LOG_FILE") or os.path.join(
    os.getenv("BOT_LOG_DIR") or "logs", "tradebothub.log"
)

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
    if _LOG_FILE_PATH:
        try:
            os.makedirs(os.path.dirname(_LOG_FILE_PATH), exist_ok=True)
            file_handler = logging.FileHandler(_LOG_FILE_PATH, encoding="utf-8")
            file_handler.setLevel(logging.DEBUG)
            file_handler.setFormatter(fmt)
            _logger.addHandler(file_handler)
            _logger.info(f"Logging to file: {_LOG_FILE_PATH}")
        except Exception:
            pass
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
    lvl_name = level.upper()
    lvl = _LEVEL_MAP.get(lvl_name, logging.INFO)
    ts = datetime.now(timezone.utc).isoformat()
    line = json.dumps(
        {
            "ts": ts,
            "level": lvl_name.lower(),
            "msg": msg,
            **_context_attrs,
        },
        separators=(",", ":"),
        ensure_ascii=False,
    )
    _logger.log(lvl, line)
    _maybe_send_log_api(msg, lvl_name, ts)

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

def send_structured_event(
    event_type: str,
    attributes: dict,
    level: str = "info",
    message: str | None = None,
):
    license_key = os.getenv("NEW_RELIC_LICENSE_KEY")
    if not license_key:
        return
    common_attrs = _build_common_attrs()
    now_ts = datetime.now(timezone.utc)
    log_entry = {
        "timestamp": int(now_ts.timestamp() * 1000),
        "message": message or event_type,
        "attributes": {
            **common_attrs,
            "eventType": event_type,
            "level": level.lower(),
            **_context_attrs,
            **attributes,
        },
    }
    payload = [
        {
            "common": {"attributes": common_attrs},
            "logs": [log_entry],
        }
    ]
    _post_new_relic_payload(payload, license_key)

def _maybe_send_log_api(message: str, level: str, ts: str):
    """
    Optional: send logs via New Relic Log API for testing. Controlled by NEW_RELIC_LICENSE_KEY.
    """
    license_key = os.getenv("NEW_RELIC_LICENSE_KEY")
    if not license_key:
        return
    service = os.getenv("NEW_RELIC_APP_NAME") or os.getenv("BOT_ID") or "tradebothub-bot"
    common_attrs = _build_common_attrs(service_override=service)
    msg_text = f"{ts} [{level}] {message}"
    log_entry = {
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
    payload = [
        {
            "common": {"attributes": common_attrs},
            "logs": [log_entry],
        }
    ]
    _post_new_relic_payload(payload, license_key)

def _build_common_attrs(service_override: str | None = None) -> dict:
    service = service_override or os.getenv("NEW_RELIC_APP_NAME") or os.getenv("BOT_ID") or "tradebothub-bot"
    return {
        "service.name": service,
        "env": os.getenv("ENV") or "test",
        "bot_id": os.getenv("BOT_ID"),
        "exchange": os.getenv("EXCHANGE") or os.getenv("EXCHANGE_ID"),
        "market": os.getenv("MARKET") or os.getenv("MARKET_SYMBOL"),
    }

def _post_new_relic_payload(payload: list[dict], license_key: str):
    endpoint = os.getenv("NEW_RELIC_LOG_API", "https://log-api.newrelic.com/log/v1")

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
