# Kafka Mode Architecture Audit (Quickstart-Aligned)

Date: 2026-06-19
Scope: `kafka_mode` deployment/runtime flow (`deploy_kafka.py`, `deploy.sh`, `status.sh`, `cleanup.sh`)

## Verdict

Most historical failures were **not caused by Kafka client syntax mistakes** in producer/consumer commands.
They were mainly caused by **deployment orchestration mismatches** around broker lifecycle and host address resolution:

- Broker not started or not reachable from selected TP hosts.
- KRaft storage path/state conflicts during repeated redeploy.
- Hostname/FQDN assumptions that can silently break cross-host connectivity.
- Status scripts using interactive SSH behavior in some paths.

## Refactor Applied

1. Broker address normalization and safer FQDN handling in `scripts/deploy_kafka.py`.
2. Localhost bootstrap auto-resolution extracted into a single helper to avoid inconsistent string assembly.
3. `status.sh` now uses non-interactive SSH options (`BatchMode`, `StrictHostKeyChecking=no`, `ConnectTimeout`) consistently.

## Why This Matches Kafka Quickstart Better

Quickstart sequence is: storage init -> broker up -> topic/data flow.
Our deployment now enforces that same operational order more reliably:

1. Ensure reachable machines and binary upload.
2. Resolve/start broker before coordinator/worker launch.
3. Verify coordinator-side broker connectivity before creating runtime activity.
4. Keep state/log paths deterministic under `/tmp/kafka_mode`.

## Quickstart Step Mapping

- Step 2 (Start env): covered by auto-start/discovery + broker reachability verification.
- Step 3/4/5 (topic + produce + consume): handled by Rust coordinator/worker runtime over Kafka topics.
- Step 8 (terminate/cleanup): handled by `cleanup.sh` (topic cleaner + tmux cleanup + tmp dir cleanup).

## Residual Risks (Need Runtime Validation)

- TP machine DNS/network policy changes can still impact FQDN reachability.
- Remote Kafka installation layout variance can still require `--kafka-home-hint` or `--kafka-local-dir`.
- End-to-end success still depends on current remote machine health and free host pool.

## Acceptance Checklist

- `python3 scripts/deploy_kafka.py --help` returns normally.
- `bash deploy.sh <user> <workers> <map_tasks>` launches without SSH password prompts.
- `bash status.sh` shows broker/coordinator/workers tmux sessions and non-empty logs.
- `bash cleanup.sh` removes topics/sessions/tmp artifacts without blocking on SSH interaction.

## Suggested Next Operational Test

1. `bash cleanup.sh`
2. `bash deploy.sh <your_user> 4 64`
3. `bash status.sh`
4. If failing, collect `broker.log`, `coordinator.log`, `worker.log` tails from status output.
