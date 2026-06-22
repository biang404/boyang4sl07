# Kafka MapReduce Mode

This folder is a standalone implementation of a Kafka topic-based MapReduce mode.
It does not modify or depend on existing deployment/message code.

## Features implemented

- Coordinator, worker, cleaner runtime commands.
- Runtime knobs for worker count and task sizing:
  - `--workers`
  - `--map-task-count`
  - `--chunk-size-bytes`
- Kafka-only message transfer between components.
- In-memory map and reduce intermediates (no persistent worker intermediate files).
- Run-scoped topics using `job_id` prefix.

## Build

```bash
cargo build --release
```

## Remote deployment (TP machines)

Kafka mode now supports the same operator-facing style as the normal project:
- one stable entrypoint: `deploy.sh`
- one stable status command: `status.sh`
- one stable cleanup command: `cleanup.sh`
- fixed remote work directory and predictable logs
- tmux-backed remote processes for easy inspection

`deploy_kafka.py` now:
- picks free TP machines automatically using the same availability source as the existing project scripts,
- assigns one coordinator + N workers,
- downloads one CommonCrawl WET file on all selected machines before launch,
- uses `/tmp/kafka_mode` on remote machines for all work (easy cleanup),
- captures all logs and curl output for debugging,
- runs coordinator and workers inside named `tmux` sessions,
- stores deployment state in `scripts/deployed_state.json` for later monitoring.

Design and review notes are tracked in `ARCHITECTURE_AUDIT.md`.

Recommended command:

```bash
bash deploy.sh <ssh_user> [workers] [map_tasks]
```

Example:

```bash
bash deploy.sh bxu-24 4 64
```

Then monitor the run with:

```bash
bash status.sh
```

When the run is finished, clean everything with:

```bash
bash cleanup.sh
```

Minimal command (customize only user, workers, tasks):

```bash
python3 scripts/deploy_kafka.py -u <ssh_user> -w 4 -t 64
```

Full command (optional advanced overrides):

```bash
python3 scripts/deploy_kafka.py \
  -u <ssh_user> \
  -b <broker:9092> \
  -j run01 \
  -w 4 \
  -t 64 \
  --chunk-size-bytes 4194304 \
  --reduce-count 8 \
  --wet-link-index 0 \
  --local-binary ./target/release/kafka_mode
```

Notes:
- `--wet-link-index 0` means most recent entry from `wet.paths`.
- Use `--coordinator-host <host>` to force a specific free TP machine as coordinator.
- Last used advanced values are stored in `scripts/deploy_command.json` and reused automatically.
- Last deployed hosts and sessions are stored in `scripts/deployed_state.json`.
- All work happens in `/tmp/kafka_mode` on each remote machine.
- Logs: `/tmp/kafka_mode/coordinator.log`, `/tmp/kafka_mode/worker.log`, `/tmp/kafka_mode/curl.log`.
- Results: `/tmp/kafka_mode/result/`.
- Sessions: `kafka-mode-coordinator-<job_id>` on the coordinator and `kafka-mode-worker-<job_id>` on each worker.
- `cleanup.sh` removes Kafka topics for the saved `job_id`, kills remote tmux sessions, deletes `/tmp/kafka_mode`, and clears local deployment state files.

## Run (example)

Coordinator:

```bash
cargo run --release -- coordinator \
  --bootstrap-servers <broker:9092> \
  --job-id run01 \
  --workers 4 \
  --map-task-count 64 \
  --chunk-size-bytes 4194304 \
  --reduce-count 8 \
  --version DefaultWithLanguageSplit \
  --input-file ../data/CC-MAIN-0001.wet \
  --result-dir ./result
```

Worker (run on N machines):

```bash
cargo run --release -- worker \
  --bootstrap-servers <broker:9092> \
  --job-id run01 \
  --workers 4 \
  --map-task-count 64 \
  --chunk-size-bytes 4194304 \
  --reduce-count 8 \
  --version DefaultWithLanguageSplit
```

Cleanup topics after run:

```bash
cargo run --release -- cleaner \
  --bootstrap-servers <broker:9092> \
  --job-id run01
```

## Notes

- Current map implementation uses chunk tokenization in memory.
- Current reduce implementation merges `(word, count)` entries in memory.
- Additional version-specific map/reduce parity can be added incrementally on top of this structure.
