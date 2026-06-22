#!/bin/bash
set -euo pipefail

BOOTSTRAP=${1:?bootstrap servers required}
JOB_ID=${2:-run01}
WORKERS=${3:-4}
MAP_TASKS=${4:-64}
CHUNK_SIZE=${5:-4194304}
REDUCE_COUNT=${6:-8}
INPUT_FILE=${7:-../data/CC-MAIN-0001.wet}
RESULT_DIR=${8:-./result}

cargo run --release -- coordinator \
  --bootstrap-servers "$BOOTSTRAP" \
  --job-id "$JOB_ID" \
  --workers "$WORKERS" \
  --map-task-count "$MAP_TASKS" \
  --chunk-size-bytes "$CHUNK_SIZE" \
  --reduce-count "$REDUCE_COUNT" \
  --version DefaultWithLanguageSplit \
  --input-file "$INPUT_FILE" \
  --result-dir "$RESULT_DIR"
