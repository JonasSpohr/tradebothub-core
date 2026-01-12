#!/bin/sh
set -euo pipefail

LOG_DIR=${BOT_LOG_DIR:-/app/logs}
mkdir -p "${LOG_DIR}"
export BOT_LOG_DIR="${LOG_DIR}"
export BOT_LOG_FILE="${LOG_DIR}/tradebothub.log"

BOT_ID_SAFE=${BOT_ID:-unknown}
export NEW_RELIC_APP_NAME="tradebothub-bot,b-${BOT_ID_SAFE}"

if [ $# -eq 0 ]; then
    set -- python -m bot.main
fi

exec newrelic-admin run-program "$@"
