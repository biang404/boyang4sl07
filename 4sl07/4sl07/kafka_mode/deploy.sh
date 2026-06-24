#!/bin/bash

set -euo pipefail

if [ $# -lt 1 ] || [ $# -gt 7 ]; then
    echo "Usage: $0 <user> [workers] [wet_files] [reduce_count] [broker_host:port] [kafka_local_dir] [tmp_dir]"
    echo "Example: $0 bxu-24 4 1 64 tp-1a201-03.enst.fr:9092 ./../kafka_2.13-4.3.0/kafka_2.13-4.3.0 /tmp/kafka_mode_bxu_24"
    echo "Example multi-WET run: $0 bxu-24 14 1000 64"
    echo "  In Kafka mode now, one WET file is one map task."
    echo "  reduce_count defaults to 8 when omitted."
    echo "  broker_host is the machine running Kafka."
    echo "  If omitted, deploy script auto-discovers a running broker,"
    echo "  or auto-starts one on selected machines by staging local Kafka if needed."
    exit 1
fi

USER_NAME=$1
shift

WORKERS=${1:-4}
if [ $# -gt 0 ]; then shift; fi

MAP_TASKS=0
WET_FILES=1
REDUCE_COUNT=8
BROKER=""
KAFKA_LOCAL_DIR=""
TMP_DIR="/tmp/kafka_mode_${USER_NAME//-/_}"

if [ $# -gt 0 ]; then
    if [[ "$1" == *":"* ]]; then
        BROKER=$1
        shift
    else
        WET_FILES=$1
        shift
    fi
fi

if [ $# -gt 0 ]; then
    if [[ "$1" =~ ^[0-9]+$ ]]; then
        REDUCE_COUNT=$1
        shift
    fi
fi

if [ $# -gt 0 ]; then
    if [ -z "$BROKER" ] && [[ "$1" == *":"* ]]; then
        BROKER=$1
        shift
    else
        KAFKA_LOCAL_DIR=$1
        shift
    fi
fi

if [ $# -gt 0 ]; then
    if [ -z "$KAFKA_LOCAL_DIR" ]; then
        KAFKA_LOCAL_DIR=$1
        shift
    else
        TMP_DIR=$1
        shift
    fi
fi

if [ $# -gt 0 ]; then
    TMP_DIR=$1
fi

echo "Building kafka_mode..."
cargo build --release

echo "Deploying Kafka coordinator and $WORKERS workers..."
echo "WET files: $WET_FILES; reduce count: $REDUCE_COUNT"
if [ -n "$BROKER" ]; then
    if [ -n "$KAFKA_LOCAL_DIR" ]; then
        python3 scripts/deploy_kafka.py -u "$USER_NAME" -w "$WORKERS" -t "$MAP_TASKS" --wet-file-count "$WET_FILES" --reduce-count "$REDUCE_COUNT" -b "$BROKER" --kafka-local-dir "$KAFKA_LOCAL_DIR" --tmp-dir "$TMP_DIR"
    else
        python3 scripts/deploy_kafka.py -u "$USER_NAME" -w "$WORKERS" -t "$MAP_TASKS" --wet-file-count "$WET_FILES" --reduce-count "$REDUCE_COUNT" -b "$BROKER" --tmp-dir "$TMP_DIR"
    fi
else
    if [ -n "$KAFKA_LOCAL_DIR" ]; then
        python3 scripts/deploy_kafka.py -u "$USER_NAME" -w "$WORKERS" -t "$MAP_TASKS" --wet-file-count "$WET_FILES" --reduce-count "$REDUCE_COUNT" --kafka-local-dir "$KAFKA_LOCAL_DIR" --tmp-dir "$TMP_DIR"
    else
        python3 scripts/deploy_kafka.py -u "$USER_NAME" -w "$WORKERS" -t "$MAP_TASKS" --wet-file-count "$WET_FILES" --reduce-count "$REDUCE_COUNT" --tmp-dir "$TMP_DIR"
    fi
fi

echo "Deployment complete."
echo "Check status with: bash status.sh"
