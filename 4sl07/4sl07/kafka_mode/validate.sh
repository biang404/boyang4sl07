#!/bin/bash

set -euo pipefail

STATE_FILE="scripts/deployed_state.json"
COMMAND_FILE="scripts/deploy_command.json"

if [ ! -f "$STATE_FILE" ]; then
    echo "No deployment state found at $STATE_FILE"
    echo "Run a deployment first, or pass explicit arguments to scripts/validate_result.py."
    exit 1
fi

if [ ! -f "$COMMAND_FILE" ]; then
    echo "No deploy command file found at $COMMAND_FILE"
    echo "Run a deployment first, or pass explicit arguments to scripts/validate_result.py."
    exit 1
fi

readarray -t VALIDATE_ARGS < <(python3 - "$STATE_FILE" "$COMMAND_FILE" <<'PY'
import json
import sys

state_path, command_path = sys.argv[1], sys.argv[2]
with open(state_path, encoding="utf-8") as f:
    state = json.load(f)
with open(command_path, encoding="utf-8") as f:
    command = json.load(f)

print(state["input_file"])
print(state["result_dir"])
print(state["job_id"])
print(command.get("map_task_count", 64))
print(command.get("chunk_size_bytes", 4 * 1024 * 1024))
print(command.get("reduce_count", 8))
PY
)

INPUT_FILE=${VALIDATE_ARGS[0]}
RESULT_DIR=${VALIDATE_ARGS[1]}
JOB_ID=${VALIDATE_ARGS[2]}
MAP_TASK_COUNT=${VALIDATE_ARGS[3]}
CHUNK_SIZE_BYTES=${VALIDATE_ARGS[4]}
REDUCE_COUNT=${VALIDATE_ARGS[5]}

python3 scripts/validate_result.py \
    --input-file "$INPUT_FILE" \
    --result-dir "$RESULT_DIR" \
    --job-id "$JOB_ID" \
    --map-task-count "$MAP_TASK_COUNT" \
    --chunk-size-bytes "$CHUNK_SIZE_BYTES" \
    --reduce-count "$REDUCE_COUNT"