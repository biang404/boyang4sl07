#!/bin/bash

set -euo pipefail

OUT_DIR=${1:-metrics_out}
RAW_FILE="$OUT_DIR/metrics.jsonl"

mkdir -p "$OUT_DIR"

echo "Collecting remote metrics..."
bash metrics.sh > "$RAW_FILE"

echo "Exporting CSV/JSON summaries..."
python3 scripts/export_metrics.py \
    --input "$RAW_FILE" \
    --out-dir "$OUT_DIR"

echo "Done. Files are in: $OUT_DIR"
