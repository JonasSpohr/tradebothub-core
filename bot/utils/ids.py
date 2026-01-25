from __future__ import annotations
from uuid import uuid4


def generate_client_order_id(bot_id: str, suffix: str | None = None) -> str:
    base = f"{bot_id}-{uuid4().hex[:10]}"
    if suffix:
        return f"{base}-{suffix}"
    return base
