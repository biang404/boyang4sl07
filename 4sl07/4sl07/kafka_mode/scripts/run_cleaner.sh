#!/bin/bash
set -euo pipefail

BOOTSTRAP=${1:?bootstrap servers required}
JOB_ID=${2:-run01}

cargo run --release -- cleaner \
  --bootstrap-servers "$BOOTSTRAP" \
  --job-id "$JOB_ID"
