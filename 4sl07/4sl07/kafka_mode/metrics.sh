#!/bin/bash

set -euo pipefail

STATE_FILE="scripts/deployed_state.json"
SSH_OPTS=(-o BatchMode=yes -o StrictHostKeyChecking=no -o ConnectTimeout=10)

if [ ! -f "$STATE_FILE" ]; then
    echo "No Kafka deployment state found at $STATE_FILE" >&2
    echo "Run: bash deploy.sh <user> [workers] [wet_files]" >&2
    exit 1
fi

readarray -t STATE_VALUES < <(python3 - "$STATE_FILE" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as f:
    state = json.load(f)

print(state["user"])
print(state["tmp_dir"])
print(state["coordinator_host"])
for host in state.get("worker_hosts", []):
    print(host)
PY
)

USER_NAME=${STATE_VALUES[0]}
TMP_DIR=${STATE_VALUES[1]}
COORDINATOR_HOST=${STATE_VALUES[2]}
WORKER_HOSTS=("${STATE_VALUES[@]:3}")

collect_log() {
    local host=$1
    local path=$2
    ssh "${SSH_OPTS[@]}" "$USER_NAME@$host" "if [ -f $path ]; then grep '^\[metric\]' $path | sed 's/^\[metric\] //'; fi" 2>/dev/null || true
}

collect_log "$COORDINATOR_HOST" "$TMP_DIR/coordinator.log"
for host in "${WORKER_HOSTS[@]}"; do
    collect_log "$host" "$TMP_DIR/worker.log"
done