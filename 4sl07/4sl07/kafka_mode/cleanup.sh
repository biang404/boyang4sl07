#!/bin/bash

set -euo pipefail

STATE_FILE="scripts/deployed_state.json"
HOSTS_FILE="scripts/deployed_hosts.txt"

if [ ! -f "$STATE_FILE" ]; then
    echo "No Kafka deployment state found at $STATE_FILE"
    echo "Nothing to clean up."
    exit 1
fi

readarray -t STATE_VALUES < <(python3 - "$STATE_FILE" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as f:
    state = json.load(f)

print(state["user"])
print(state["job_id"])
print(state["bootstrap_servers"])
print(state["tmp_dir"])
print(state["coordinator_session"])
print(state["worker_session"])
print(state.get("broker_session", "none"))
for host in state.get("all_hosts", []):
    print(host)
PY
)

USER_NAME=${STATE_VALUES[0]}
JOB_ID=${STATE_VALUES[1]}
BOOTSTRAP_SERVERS=${STATE_VALUES[2]}
TMP_DIR=${STATE_VALUES[3]}
COORDINATOR_SESSION=${STATE_VALUES[4]}
WORKER_SESSION=${STATE_VALUES[5]}
BROKER_SESSION=${STATE_VALUES[6]}
ALL_HOSTS=("${STATE_VALUES[@]:7}")

# Auto-resolve: if the saved broker is localhost, use coordinator's FQDN
COORDINATOR_HOST=${ALL_HOSTS[0]:-}
if [[ "$BOOTSTRAP_SERVERS" == 127.0.0.1:* || "$BOOTSTRAP_SERVERS" == localhost:* ]]; then
    PORT="${BOOTSTRAP_SERVERS##*:}"
    BOOTSTRAP_SERVERS="${COORDINATOR_HOST}.enst.fr:${PORT}"
    echo "Broker auto-resolved to: $BOOTSTRAP_SERVERS"
fi

SSH_OPTS=(-o BatchMode=yes -o StrictHostKeyChecking=no -o ConnectTimeout=10)

echo "Cleaning Kafka topics for job_id=$JOB_ID on $BOOTSTRAP_SERVERS..."
cargo run --release -- cleaner --bootstrap-servers "$BOOTSTRAP_SERVERS" --job-id "$JOB_ID" \
    || echo "Warning: could not delete Kafka topics (broker unreachable or already gone). Topics will expire automatically."

echo "Cleaning remote tmux sessions and temporary files..."
for host in "${ALL_HOSTS[@]}"; do
    echo "== $host =="
    ssh "${SSH_OPTS[@]}" "$USER_NAME@$host" "
tmux kill-session -t $COORDINATOR_SESSION 2>/dev/null || true;
tmux kill-session -t $WORKER_SESSION 2>/dev/null || true;
if [ '$BROKER_SESSION' != 'none' ]; then tmux kill-session -t $BROKER_SESSION 2>/dev/null || true; fi;
rm -rf $TMP_DIR 2>/dev/null || true
" || echo "  [warning] $host unreachable, skipped"
done

rm -f "$STATE_FILE"
rm -f "$HOSTS_FILE"

echo "Kafka cleanup complete."