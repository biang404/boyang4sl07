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
bash deploy.sh <ssh_user> [workers] [wet_files] [reduce_count]
```

Example:

```bash
bash deploy.sh bxu-24 4 50 64
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

## Scaling experiments and graphs

Collect timing data for several worker counts and draw the three scaling graphs
(total time, MAP phase breakdown, REDUCE phase breakdown).

For each worker count in `5 10 15 25 35 50`:

```bash
# 1. deploy + run with that worker count and wait until it finishes
bash deploy.sh <ssh_user> 5 <wet_files> <reduce_count>
bash status.sh          # wait until you see the job_done metric

# 2. collect this run's metrics into experiments/05_workers/
bash collect_experiment.sh 5

# 3. clean up before the next worker count
bash cleanup.sh
```

Repeat for `10`, `15`, `25`, `35`, `50` (the collector zero-pads to `NN_workers`).

When all runs are collected, draw the graphs:

```bash
python3 scripts/plot_experiments.py
```

Outputs (in `experiments/graphs/`):
- `01_total_execution_time.png` — total job time vs worker count.
- `02_map_phase_breakdown.png` — avg MAP time split into Download / Reading / Processing (Mapping) / Temp Saving.
- `03_reduce_phase_breakdown.png` — avg REDUCE time split into File Transfer / Processing (Reduce).

Sub-step timings are emitted by the worker as `map_task` / `reduce_task` metrics and
aggregated per run into `phase_breakdown.csv` by `scripts/export_metrics.py`.

