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

python3 scripts/validate_result.py \
    --state-file "$STATE_FILE" \
    --deploy-command "$COMMAND_FILE"