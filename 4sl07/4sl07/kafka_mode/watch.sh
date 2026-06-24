#!/bin/bash

set -euo pipefail

STATE_FILE="scripts/deployed_state.json"
SSH_OPTS=(-o BatchMode=yes -o StrictHostKeyChecking=no -o ConnectTimeout=10)
INTERVAL=${WATCH_INTERVAL:-5}

if [ ! -f "$STATE_FILE" ]; then
    echo "No Kafka deployment state found at $STATE_FILE"
    echo "Run: bash deploy.sh <user> [workers] [wet_files]"
    exit 1
fi

readarray -t STATE_VALUES < <(python3 - "$STATE_FILE" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as f:
    state = json.load(f)

print(state["user"])
print(state["job_id"])
print(state.get("bootstrap_servers", "unknown"))
print(state["tmp_dir"])
print(state["coordinator_host"])
print(state.get("broker_host", "none"))
print(state.get("broker_session", "none"))
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
COORDINATOR_SESSION=${STATE_VALUES[7]}
WORKER_SESSION=${STATE_VALUES[8]}
WORKER_HOSTS=("${STATE_VALUES[@]:9}")

clear_screen() {
    printf '\033[H\033[2J'
}

remote_status() {
    local host=$1
    local session=$2
    ssh "${SSH_OPTS[@]}" "$USER_NAME@$host" "tmux has-session -t $session 2>/dev/null && echo running || echo missing" 2>/dev/null || echo unreachable
}

remote_tail() {
    local host=$1
    local path=$2
    local lines=$3
    ssh "${SSH_OPTS[@]}" "$USER_NAME@$host" "if [ -f $path ]; then tail -n $lines $path; else echo no log; fi" 2>/dev/null || echo unreachable
}

print_progress() {
    ssh "${SSH_OPTS[@]}" "$USER_NAME@$COORDINATOR_HOST" "python3 - '$TMP_DIR/coordinator.log' '$TMP_DIR/result' <<'PY'
import glob
import os
import re
import sys

log_path, result_dir = sys.argv[1], sys.argv[2]
text = ''
if os.path.exists(log_path):
    with open(log_path, encoding='utf-8', errors='replace') as f:
        text = f.read()

def last(pattern):
    matches = re.findall(pattern, text)
    return matches[-1] if matches else None

published = last(r'Published (\d+) map tasks')
map_done = last(r'Map done: (\d+)/(\d+)')
files_processed_elapsed = last(r'All input files processed after filenames_sent: ([0-9.]+)s \((\d+) ms\)')
reduce_done = last(r'Reduce done: (\d+)/(\d+)')
reduce_result = last(r'Reduce result received: (\d+)/(\d+)')
dispatched = len(re.findall(r'Dispatched reduce task', text))
completed = bool(re.search(r'Job .* completed', text))
result_files = sorted(glob.glob(os.path.join(result_dir, 'reduce_*.json')))

print('Progress')
print(f'  published maps: {published or "?"}')
print(f'  map done:       {"/".join(map_done) if map_done else "0/?"}')
print(f'  file proc time: {files_processed_elapsed[0] + "s" if files_processed_elapsed else "running"}')
print(f'  reduce sent:    {dispatched}')
print(f'  reduce done:    {"/".join(reduce_done) if reduce_done else "0/?"}')
print(f'  results recv:   {"/".join(reduce_result) if reduce_result else "0/?"}')
print(f'  result files:   {len(result_files)}')
print(f'  completed:      {completed}')

payloads = [int(x) for x in re.findall(r'payload_bytes=(\d+)', text)]
if payloads:
    print(f'  coordinator payload max: {max(payloads)} bytes')
PY" 2>/dev/null || echo "Progress: coordinator unreachable"
}

print_worker_summary() {
    for host in "${WORKER_HOSTS[@]}"; do
        echo "== $host =="
        ssh "${SSH_OPTS[@]}" "$USER_NAME@$host" "python3 - '$TMP_DIR/worker.log' <<'PY'
import os
import re
import sys

path = sys.argv[1]
if not os.path.exists(path):
    print('no worker.log')
    raise SystemExit
with open(path, encoding='utf-8', errors='replace') as f:
    text = f.read()
maps = len(re.findall(r'\[debug\]\[map\]', text))
downloads = len(re.findall(r'\[debug\]\[download\]', text))
cleanups = len(re.findall(r'\[debug\]\[cleanup\]', text))
reduces = len(re.findall(r'\[debug\]\[reduce\]', text))
payloads = [int(x) for x in re.findall(r'payload_bytes=(\d+)', text)]
print(f'map partitions logged: {maps}; downloads: {downloads}; cleanups: {cleanups}; reduces: {reduces}')
if payloads:
    print(f'payload max: {max(payloads)} bytes')
print('tail:')
for line in text.splitlines()[-4:]:
    print('  ' + line)
PY" 2>/dev/null || echo "unreachable"
        echo
    done
}

while true; do
    clear_screen
    echo "Kafka MapReduce Watch"
    echo "time: $(date '+%Y-%m-%d %H:%M:%S')"
    echo "job_id: $JOB_ID"
    echo "bootstrap_servers: $BOOTSTRAP_SERVERS"
    echo "coordinator: $COORDINATOR_HOST ($(remote_status "$COORDINATOR_HOST" "$COORDINATOR_SESSION"))"
    if [ "$BROKER_HOST" != "none" ] && [ "$BROKER_SESSION" != "none" ]; then
        echo "broker: $BROKER_HOST ($(remote_status "$BROKER_HOST" "$BROKER_SESSION"))"
    fi
    echo "workers: ${WORKER_HOSTS[*]}"
    echo

    print_progress
    echo
    echo "Coordinator tail"
    remote_tail "$COORDINATOR_HOST" "$TMP_DIR/coordinator.log" 12
    echo
    echo "Workers"
    print_worker_summary
    echo "Press Ctrl-C to stop. Set WATCH_INTERVAL=N to change refresh seconds."
    sleep "$INTERVAL"
done