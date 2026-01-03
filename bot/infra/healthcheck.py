import os
from typing import Optional, Dict, Any
import requests
from bot.core.logging import log
from bot.infra.db import supabase_client

RUNTIME_TABLE = "bot_runtime"
DEFAULT_API_BASE = "https://healthchecks.io/api/v3"
DEFAULT_PING_BASE = "https://hc-ping.com"

def _hc_headers():
    key = os.getenv("HEALTHCHECKS_API_KEY")
    return {"X-Api-Key": key} if key else {}

def _get_hc_row(bot_id: str) -> Optional[Dict[str, Any]]:
    try:
        sb = supabase_client()
        resp = sb.table(RUNTIME_TABLE).select("hio_uuid").eq("bot_id", bot_id).limit(1).execute()
        data = resp.data or []
        return data[0] if data else None
    except Exception as e:
        log(f"[healthcheck] fetch failed: {e}", level="WARN")
        return None

def _save_hc_row(bot_id: str, hio_uuid: str):
    try:
        sb = supabase_client()
        payload = {"bot_id": bot_id, "hio_uuid": hio_uuid}
        sb.table(RUNTIME_TABLE).upsert(payload, on_conflict="bot_id").execute()
    except Exception as e:
        log(f"[healthcheck] save failed: {e}", level="WARN")

def _create_healthcheck(bot_id: str, name: str, timeout_seconds: int, grace_seconds: int) -> Optional[Dict[str, str]]:
    key = os.getenv("HEALTHCHECKS_API_KEY")
    if not key:
        log("[healthcheck] HEALTHCHECKS_API_KEY not set; skipping creation", level="WARN")
        return None
    api_base = os.getenv("HEALTHCHECKS_API_BASE", DEFAULT_API_BASE)
    # Default: route to configured channels, or all (*) if none provided.
    channels = os.getenv("HEALTHCHECKS_CHANNELS") or "*"
    # Prefer explicit support email if provided; include in desc for visibility.
    support_email = os.getenv("SUPPORT_EMAIL") or "botneedsattention@tradebot.hub"
    payload = {
        "name": f"b-{bot_id}",
        "tags": f"bot {bot_id} tradebothub",
        "timeout": timeout_seconds,
        "grace": grace_seconds,
        "channels": channels,
        "desc": f"Bot {bot_id} alert to {support_email}",
    }
    try:
        res = requests.post(
            f"{api_base}/checks/",
            headers={**_hc_headers(), "Content-Type": "application/json"},
            json=payload,
            timeout=5,
        )
        res.raise_for_status()
        data = res.json()
        ping_url = data.get("ping_url")
        hc_uuid = data.get("unique_key") or (ping_url.rstrip("/").split("/")[-1] if ping_url else None)
        if ping_url:
            log(f"[healthcheck] created new check for bot {bot_id}", level="INFO")
            _save_hc_row(bot_id, hc_uuid)
            return {"ping_url": ping_url, "hio_uuid": hc_uuid}
        log(f"[healthcheck] create response missing ping_url: {data}", level="WARN")
    except Exception as e:
        log(f"[healthcheck] create failed: {e}", level="WARN")
    return None

def _update_healthcheck(hc_uuid: str, timeout_seconds: int, grace_seconds: int) -> bool:
    key = os.getenv("HEALTHCHECKS_API_KEY")
    if not key:
        return False
    api_base = os.getenv("HEALTHCHECKS_API_BASE", DEFAULT_API_BASE)
    channels = os.getenv("HEALTHCHECKS_CHANNELS")
    payload = {
        "timeout": timeout_seconds,
        "grace": grace_seconds,
    }
    if channels:
        payload["channels"] = channels
    try:
        res = requests.post(
            f"{api_base}/checks/{hc_uuid}",
            headers={**_hc_headers(), "Content-Type": "application/json"},
            json=payload,
            timeout=5,
        )
        res.raise_for_status()
        log(f"[healthcheck] updated grace/timeout", level="INFO")
        return True
    except Exception as e:
        log(f"[healthcheck] update failed: {e}", level="WARN")
    return False

def ensure_healthcheck(bot_id: str, name: str, poll_interval: int) -> Optional[str]:
    """
    Ensure a healthcheck exists for this bot. Creates via API if none saved.
    Adjusts grace/timeout on startup if they differ.
    """
    timeout_seconds = max(60, poll_interval * 2)
    grace_env = os.getenv("HEALTHCHECKS_GRACE_SECONDS")
    grace_seconds = int(grace_env) if grace_env else 900  # default 15 minutes
    existing = _get_hc_row(bot_id)
    hio_uuid = existing.get("hio_uuid") if existing else None
    if hio_uuid:
        derived_ping = f"{DEFAULT_PING_BASE}/{hio_uuid}"
        updated = False
        try:
            updated = _update_healthcheck(hio_uuid, timeout_seconds, grace_seconds)
        except Exception:
            updated = False
        if updated:
            return derived_ping
        # if update failed, try creating a fresh check and replacing stored UUID/ping_url
        log("[healthcheck] update failed; creating new check", level="WARN")
    created = _create_healthcheck(bot_id, name, timeout_seconds, grace_seconds)
    if created and created.get("hio_uuid"):
        _save_hc_row(bot_id, created["hio_uuid"])
    return created.get("ping_url") if created else None

def ping_healthcheck(ping_url: Optional[str]):
    if not ping_url:
        return
    try:
        requests.get(ping_url, timeout=3)
    except Exception as e:
        log(f"[healthcheck] ping failed: {e}", level="WARN")

def fail_healthcheck(ping_url: Optional[str], message: str | None = None):
    if not ping_url:
        return
    try:
        if not ping_url.endswith("/"):
            ping_url = ping_url + "/"
        url = ping_url + "fail"
        if message:
            url = url + f"?msg={requests.utils.quote(message)}"
        requests.get(url, timeout=3)
    except Exception as e:
        log(f"[healthcheck] fail ping failed: {e}", level="WARN")
