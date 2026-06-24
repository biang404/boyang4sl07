#!/bin/bash

set -euo pipefail

STATE_FILE="scripts/deployed_state.json"
SSH_OPTS=(-o BatchMode=yes -o StrictHostKeyChecking=no -o ConnectTimeout=10)

if [ ! -f "$STATE_FILE" ]; then
    echo "No Kafka deployment state found at $STATE_FILE"
    echo "Run: bash deploy.sh <user> [workers] [map_tasks]"
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
print(state.get("bootstrap_servers", "unknown"))
print(state["tmp_dir"])
print(state["coordinator_host"])
print(state.get("broker_host", "none"))
print(state.get("broker_session", "none"))
print("true" if state.get("broker_auto_started", False) else "false")
print(state["coordinator_session"])
print(state["worker_session"])
for host in state.get("worker_hosts", []):
    print(host)
PY
)

USER_NAME=${STATE_VALUES[0]}
JOB_ID=${STATE_VALUES[1]}
BOOTSTRAP_SERVERS=${STATE_VALUES[2]}
TMP_DIR=${STATE_VALUES[3]}
COORDINATOR_HOST=${STATE_VALUES[4]}
BROKER_HOST=${STATE_VALUES[5]}
BROKER_SESSION=${STATE_VALUES[6]}
BROKER_AUTO_STARTED=${STATE_VALUES[7]}
COORDINATOR_SESSION=${STATE_VALUES[8]}
WORKER_SESSION=${STATE_VALUES[9]}
WORKER_HOSTS=("${STATE_VALUES[@]:10}")

echo "Kafka deployment status"
echo "job_id: $JOB_ID"
echo "bootstrap_servers: $BOOTSTRAP_SERVERS"
echo "coordinator: $COORDINATOR_HOST"
echo "broker_host: $BROKER_HOST"
echo "broker_auto_started: $BROKER_AUTO_STARTED"
if [ ${#WORKER_HOSTS[@]} -gt 0 ]; then
    echo "workers: ${WORKER_HOSTS[*]}"
else
    echo "workers: none"
fi
echo "tmp_dir: $TMP_DIR"
echo

if [ "$BROKER_HOST" != "none" ] && [ "$BROKER_SESSION" != "none" ]; then
echo "Broker"
ssh "${SSH_OPTS[@]}" "$USER_NAME@$BROKER_HOST" "
echo '[tmux]';
tmux has-session -t $BROKER_SESSION 2>/dev/null && echo running || echo missing;
echo '[log tail]';
if [ -f $TMP_DIR/broker.log ]; then tail -n 20 $TMP_DIR/broker.log; else echo no broker.log; fi
"
echo
fi

echo "Coordinator"
ssh "${SSH_OPTS[@]}" "$USER_NAME@$COORDINATOR_HOST" "
echo '[tmux]';
tmux has-session -t $COORDINATOR_SESSION 2>/dev/null && echo running || echo missing;
echo '[exit]';
if [ -f $TMP_DIR/coordinator.exit ]; then cat $TMP_DIR/coordinator.exit; else echo no coordinator.exit; fi;
echo '[log tail]';
if [ -f $TMP_DIR/coordinator.log ]; then tail -n 20 $TMP_DIR/coordinator.log; else echo no coordinator.log; fi;
echo '[results]';
if [ -d $TMP_DIR/result ]; then find $TMP_DIR/result -maxdepth 1 -type f | sort; else echo no result directory yet; fi
"

echo
echo "Workers"
for host in "${WORKER_HOSTS[@]}"; do
    echo "== $host =="
    ssh "${SSH_OPTS[@]}" "$USER_NAME@$host" "
echo '[tmux]';
tmux has-session -t $WORKER_SESSION 2>/dev/null && echo running || echo missing;
echo '[exit]';
if [ -f $TMP_DIR/worker.exit ]; then cat $TMP_DIR/worker.exit; else echo no worker.exit; fi;
echo '[log tail]';
if [ -f $TMP_DIR/worker.log ]; then tail -n 10 $TMP_DIR/worker.log; else echo no worker.log; fi
"
    echo
done
