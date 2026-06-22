#!/bin/bash
set -euo pipefail

BOOTSTRAP=${1:?bootstrap servers required}
JOB_ID=${2:-run01}
WORKERS=${3:-4}
MAP_TASKS=${4:-64}
CHUNK_SIZE=${5:-4194304}
REDUCE_COUNT=${6:-8}

cargo run --release -- worker \
  --bootstrap-servers "$BOOTSTRAP" \
  --job-id "$JOB_ID" \
  --workers "$WORKERS" \
  --map-task-count "$MAP_TASKS" \
  --chunk-size-bytes "$CHUNK_SIZE" \
  --reduce-count "$REDUCE_COUNT" \
  --version DefaultWithLanguageSplit
