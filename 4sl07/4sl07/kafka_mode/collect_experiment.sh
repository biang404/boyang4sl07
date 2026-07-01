#!/bin/bash
#
# Collect metrics for the CURRENT deployment (from scripts/deployed_state.json)
# into a per-worker-count folder so the graphs can be drawn later.
#
# Usage:
#   bash collect_experiment.sh <workers> [experiments_dir]
#
# Example (after `bash deploy.sh <user> 5 ...` has finished):
#   bash collect_experiment.sh 5
#
# This creates experiments/05_workers/ with:
#   metrics.jsonl, summary.csv, phase_breakdown.csv, map_tasks.csv, reduce_tasks.csv
#
# Repeat for each worker count (5 10 15 25 35 50), then run:
#   python3 scripts/plot_experiments.py

set -euo pipefail

if [ $# -lt 1 ]; then
    echo "Usage: $0 <workers> [experiments_dir]" >&2
    echo "Example: $0 5" >&2
    exit 1
fi

WORKERS=$1
EXPERIMENTS_DIR=${2:-experiments}

# Zero-pad to two digits so folders sort naturally: 05_workers, 10_workers, ...
CONFIG=$(printf "%02d_workers" "$WORKERS")
OUT_DIR="$EXPERIMENTS_DIR/$CONFIG"
RAW_FILE="$OUT_DIR/metrics.jsonl"

mkdir -p "$OUT_DIR"

echo "Collecting remote metrics for $WORKERS workers -> $OUT_DIR ..."
bash metrics.sh > "$RAW_FILE"

if [ ! -s "$RAW_FILE" ]; then
    echo "WARNING: no metrics collected. Is the run finished? Check: bash status.sh" >&2
fi

echo "Exporting CSV/JSON summaries..."
python3 scripts/export_metrics.py \
    --input "$RAW_FILE" \
    --out-dir "$OUT_DIR"

echo "Done. Experiment data is in: $OUT_DIR"
echo "When all worker counts are collected, run: python3 scripts/plot_experiments.py --experiments-dir $EXPERIMENTS_DIR"
