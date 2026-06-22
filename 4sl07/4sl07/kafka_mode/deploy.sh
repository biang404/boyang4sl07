#!/bin/bash

set -euo pipefail

if [ $# -lt 1 ] || [ $# -gt 6 ]; then
    echo "Usage: $0 <user> [workers] [map_tasks] [broker_host:port] [kafka_local_dir] [tmp_dir]"
    echo "Example: $0 bxu-24 4 64 tp-1a201-03.enst.fr:9092 ./../kafka_2.13-4.3.0/kafka_2.13-4.3.0 /tmp/kafka_mode_bxu_24"
    echo "  broker_host is the machine running Kafka."
    echo "  If omitted, deploy script auto-discovers a running broker,"
    echo "  or auto-starts one on selected machines by staging local Kafka if needed."
    exit 1
fi

USER_NAME=$1
WORKERS=${2:-4}
MAP_TASKS=${3:-64}
BROKER=${4:-}
KAFKA_LOCAL_DIR=${5:-}
TMP_DIR=${6:-/tmp/kafka_mode_${USER_NAME//-/_}}

echo "Building kafka_mode..."
cargo build --release

echo "Deploying Kafka coordinator and $WORKERS workers..."
if [ -n "$BROKER" ]; then
    if [ -n "$KAFKA_LOCAL_DIR" ]; then
        python3 scripts/deploy_kafka.py -u "$USER_NAME" -w "$WORKERS" -t "$MAP_TASKS" -b "$BROKER" --kafka-local-dir "$KAFKA_LOCAL_DIR" --tmp-dir "$TMP_DIR"
    else
        python3 scripts/deploy_kafka.py -u "$USER_NAME" -w "$WORKERS" -t "$MAP_TASKS" -b "$BROKER" --tmp-dir "$TMP_DIR"
    fi
else
    if [ -n "$KAFKA_LOCAL_DIR" ]; then
        python3 scripts/deploy_kafka.py -u "$USER_NAME" -w "$WORKERS" -t "$MAP_TASKS" --kafka-local-dir "$KAFKA_LOCAL_DIR" --tmp-dir "$TMP_DIR"
    else
        python3 scripts/deploy_kafka.py -u "$USER_NAME" -w "$WORKERS" -t "$MAP_TASKS" --tmp-dir "$TMP_DIR"
    fi
fi

echo "Deployment complete."
echo "Check status with: bash status.sh"
