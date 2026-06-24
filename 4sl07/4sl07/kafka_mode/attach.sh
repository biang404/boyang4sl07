#!/bin/bash

set -euo pipefail

STATE_FILE="scripts/deployed_state.json"
ROLE=${1:-coordinator}
SSH_OPTS=(-o BatchMode=yes -o StrictHostKeyChecking=no -o ConnectTimeout=10)

if [ ! -f "$STATE_FILE" ]; then
    echo "No Kafka deployment state found at $STATE_FILE"
    echo "Run: bash deploy.sh <user> [workers] [wet_files]"
    exit 1
fi

readarray -t TARGET < <(python3 - "$STATE_FILE" "$ROLE" <<'PY'
import json
import sys

state_path, role = sys.argv[1], sys.argv[2]
with open(state_path, encoding="utf-8") as f:
    state = json.load(f)

if role == "coordinator":
    print(state["user"])
    print(state["coordinator_host"])
    print(state["coordinator_session"])
elif role == "broker":
    print(state["user"])
    print(state.get("broker_host", state["coordinator_host"]))
    print(state.get("broker_session", "none"))
elif role == "worker":
    workers = state.get("worker_hosts", [])
    if not workers:
        raise SystemExit("No worker hosts in deployed state")
    print(state["user"])
    print(workers[0])
    print(state["worker_session"])
elif role.startswith("worker:"):
    index = int(role.split(":", 1)[1])
    workers = state.get("worker_hosts", [])
    if index < 0 or index >= len(workers):
        raise SystemExit(f"worker index out of range: {index}; available 0..{len(workers)-1}")
    print(state["user"])
    print(workers[index])
    print(state["worker_session"])
else:
    raise SystemExit("Usage: bash attach.sh [coordinator|broker|worker|worker:<index>]")
PY
)

USER_NAME=${TARGET[0]}
HOST=${TARGET[1]}
SESSION=${TARGET[2]}

if [ "$SESSION" = "none" ]; then
    echo "No tmux session recorded for role: $ROLE"
    exit 1
fi

echo "Attaching to $ROLE on $HOST session $SESSION"
echo "Detach with: Ctrl-B then D"
ssh "${SSH_OPTS[@]}" -t "$USER_NAME@$HOST" "tmux attach -t $SESSION"