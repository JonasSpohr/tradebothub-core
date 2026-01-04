import os
from typing import Optional
from cryptography.fernet import Fernet

def decrypt(token: Optional[str]) -> Optional[str]:
    if not token:
        return None
    # Prefer FERNET_KEY, fall back to BOT_ENC_KEY for compatibility.
    key = (os.getenv("FERNET_KEY") or os.environ["BOT_ENC_KEY"]).encode("utf-8")
    return Fernet(key).decrypt(token.encode("utf-8")).decode("utf-8")
