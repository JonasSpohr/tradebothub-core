import os
from bot.runtime.bootstrap import start

if __name__ == "__main__":
    start(os.environ["BOT_ID"])