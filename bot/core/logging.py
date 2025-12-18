from datetime import datetime, timezone

def log(msg: str):
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S%z")
    print(f"[{ts}] {msg}", flush=True)