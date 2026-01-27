from dataclasses import dataclass
from typing import Any, Dict, Optional

Json = Dict[str, Any]

@dataclass
class BotContext:
    id: str
    user_id: str
    name: str
    strategy: str
    mode: str
    dry_run: bool

    status: str
    subscription_status: str

    exchange_ccxt_id: str
    market_symbol: str

    api_key_encrypted: str
    api_secret_encrypted: str
    api_password_encrypted: Optional[str]
    api_uid_encrypted: Optional[str]

    strategy_config: Json
    risk_config: Json
    execution_config: Json
    control_config: Json

    bot_version: Optional[str] = None
    runtime_provider: Optional[str] = None
    fly_region: Optional[str] = None
    fly_machine_id: Optional[str] = None
