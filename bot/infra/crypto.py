import os
from typing import Optional
from cryptography.fernet import Fernet

def decrypt(token: Optional[str]) -> Optional[str]:
    if not token:
        return None
    key = os.environ["BOT_ENC_KEY"].encode("utf-8")
    return Fernet(key).decrypt(token.encode("utf-8")).decode("utf-8")