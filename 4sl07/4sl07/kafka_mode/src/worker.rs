use crate::config::{RunConfig, TopicNames};
use crate::core::map::{map_chunk_from_file, partition_map};
use crate::core::reduce::{map_to_sorted_vec, reduce_entries};
use crate::kafka::io::{commit_message, create_consumer, create_producer, recv_json, send_json};
use crate::messages::{MapPartitionMeta, MapTask, ReduceResultMeta, ReduceTaskMeta, TaskAck, TaskPhase, WorkerRegistration};
use anyhow::Result;
use std::process::Command;
use std::thread::sleep;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use uuid::Uuid;

const SSH_TRANSFER_ATTEMPTS: usize = 5;

pub async fn run_worker(run: RunConfig, worker_id: Option<String>) -> Result<()> {
    let worker_id = worker_id.unwrap_or_else(|| Uuid::new_v4().to_string());
    let topics = TopicNames::from_job(&run.job_id);
    let producer = create_producer(&run.bootstrap_servers)?;
    let local_host = local_host_fqdn();

    tokio::fs::create_dir_all(format!("{}/map_outputs", run.work_dir)).await.ok();
    tokio::fs::create_dir_all(format!("{}/reduce_inputs", run.work_dir)).await.ok();
    tokio::fs::create_dir_all(format!("{}/reduce_outputs", run.work_dir)).await.ok();

    let registration = WorkerRegistration {
        job_id: run.job_id.clone(),
        worker_id: worker_id.clone(),
        hostname: local_host.clone(),
        ts_ms: now_ms(),
    };
    send_json(&producer, &topics.worker_registration, &worker_id, &registration).await?;

    let map_consumer = create_consumer(
        &run.bootstrap_servers,
        &format!("worker-map-{}", run.job_id),
        &[topics.map_tasks.as_str()],
    )?;

    let reduce_consumer = create_consumer(
        &run.bootstrap_servers,
        &format!("worker-reduce-{}", run.job_id),
        &[topics.reduce_tasks.as_str()],
    )?;

    println!("Worker {} started", worker_id);

    loop {
        tokio::select! {
            map_msg = recv_json::<MapTask>(&map_consumer) => {
                if let Some((task, msg)) = map_msg? {
                    if task.job_id == run.job_id {
                        handle_map_task(&producer, &topics, &worker_id, &run.work_dir, task).await?;
                    }
                    commit_message(&map_consumer, &msg)?;
                }
            }
            reduce_msg = recv_json::<ReduceTaskMeta>(&reduce_consumer) => {
                if let Some((task, msg)) = reduce_msg? {
                    if task.job_id == run.job_id {
                        handle_reduce_task(&producer, &topics, &worker_id, &local_host, &run.work_dir, task).await?;
                    }
                    commit_message(&reduce_consumer, &msg)?;
                }
            }
        }
    }
}

async fn handle_map_task(
    producer: &rdkafka::producer::FutureProducer,
    topics: &TopicNames,
    worker_id: &str,
    work_dir: &str,
    task: MapTask,
) -> Result<()> {
    let mapped = map_chunk_from_file(&task.input_file, task.offset, task.chunk_size_bytes)?;
    let mut partitions = partition_map(mapped, task.reduce_count);

    for (reduce_id, entries) in partitions.drain(..).enumerate() {
        let entry_count = entries.len();
        let file_path = format!(
            "{}/map_outputs/map_{}_reduce_{}.json",
            work_dir, task.map_id, reduce_id
        );
        tokio::fs::write(&file_path, serde_json::to_string(&entries)?).await?;

        println!(
            "[debug][map] map_id={} reduce_id={} coordinator_host={} file={} entries={}",
            task.map_id, reduce_id, task.coordinator_host, file_path, entry_count
        );

        // Push to coordinator -- worker initiates SCP (coordinator cannot reach workers).
        scp_push(&file_path, &task.coordinator_host, &file_path)?;

        // Delete local copy immediately to free worker disk.
        tokio::fs::remove_file(&file_path).await.ok();

        // Notify coordinator: file is now at `file_path` on coordinator.
        let meta = MapPartitionMeta {
            job_id: task.job_id.clone(),
            worker_id: worker_id.to_string(),
            file_host: normalize_host(&task.coordinator_host),
            map_id: task.map_id,
            reduce_id,
            file_path,
            entry_count,
        };
        send_json(
            producer,
            &topics.map_results,
            &format!("map-{}-reduce-{}", task.map_id, reduce_id),
            &meta,
        )
        .await?;
    }

    send_json(
        producer,
        &topics.task_acks,
        &format!("ack-map-{}", task.map_id),
        &TaskAck {
            job_id: task.job_id,
            worker_id: worker_id.to_string(),
            phase: TaskPhase::Map,
            task_id: task.map_id,
            ts_ms: now_ms(),
        },
    )
    .await?;

    Ok(())
}

async fn handle_reduce_task(
    producer: &rdkafka::producer::FutureProducer,
    topics: &TopicNames,
    worker_id: &str,
    local_host: &str,
    work_dir: &str,
    meta: ReduceTaskMeta,
) -> Result<()> {
    let local_input = format!("{}/reduce_inputs/reduce_{}.json", work_dir, meta.reduce_id);

    println!(
        "[debug][reduce] reduce_id={} source_host={} source_file={} local_input={}",
        meta.reduce_id, meta.file_host, meta.file_path, local_input
    );

    if same_host(local_host, &meta.file_host) {
        // Coordinator is on this same machine -- just copy locally.
        if meta.file_path != local_input {
            tokio::fs::copy(&meta.file_path, &local_input).await?;
            tokio::fs::remove_file(&meta.file_path).await.ok();
        }
    } else {
        // Pull reduce input from coordinator, then tell coordinator to delete it.
        scp_pull(&meta.file_host, &meta.file_path, &local_input)?;
        ssh_remove_file(&meta.file_host, &meta.file_path)?;
    }

    let entries: Vec<(String, u32)> =
        serde_json::from_str(&tokio::fs::read_to_string(&local_input).await?)?;
    tokio::fs::remove_file(&local_input).await.ok();

    let output_entries = map_to_sorted_vec(reduce_entries(entries));
    let entry_count = output_entries.len();

    let local_result = format!("{}/reduce_outputs/reduce_{}.json", work_dir, meta.reduce_id);
    tokio::fs::write(&local_result, serde_json::to_string(&output_entries)?).await?;

    // Push result to coordinator, then delete local copy.
    scp_push(&local_result, &meta.file_host, &local_result)?;
    tokio::fs::remove_file(&local_result).await.ok();

    send_json(
        producer,
        &topics.reduce_results,
        &format!("reduce-result-{}", meta.reduce_id),
        &ReduceResultMeta {
            job_id: meta.job_id.clone(),
            worker_id: worker_id.to_string(),
            file_host: normalize_host(&meta.file_host),
            reduce_id: meta.reduce_id,
            file_path: local_result,
            entry_count,
        },
    )
    .await?;

    send_json(
        producer,
        &topics.task_acks,
        &format!("ack-reduce-{}", meta.reduce_id),
        &TaskAck {
            job_id: meta.job_id,
            worker_id: worker_id.to_string(),
            phase: TaskPhase::Reduce,
            task_id: meta.reduce_id,
            ts_ms: now_ms(),
        },
    )
    .await?;

    Ok(())
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn local_host_fqdn() -> String {
    let from_env = std::env::var("HOSTNAME").unwrap_or_default();
    if !from_env.is_empty() && from_env != "unknown" {
        return normalize_host(&from_env);
    }
    if let Ok(out) = std::process::Command::new("hostname").output() {
        let h = String::from_utf8_lossy(&out.stdout).trim().to_string();
        if !h.is_empty() {
            return normalize_host(&h);
        }
    }
    panic!("Cannot determine local hostname; check that `hostname` is available");
}

fn normalize_host(host: &str) -> String {
    let h = host.trim();
    if h.ends_with(".enst.fr") {
        h.to_string()
    } else {
        format!("{h}.enst.fr")
    }
}

fn same_host(a: &str, b: &str) -> bool {
    normalize_host(a) == normalize_host(b)
}

/// Returns "user@host.enst.fr" for SCP/SSH. TP machines require key-auth via FQDN with user prefix.
fn ssh_target(host: &str) -> String {
    let fqdn = normalize_host(host);
    let user = std::env::var("USER")
        .or_else(|_| std::env::var("LOGNAME"))
        .unwrap_or_else(|_| {
            Command::new("whoami")
                .output()
                .ok()
                .and_then(|o| String::from_utf8(o.stdout).ok())
                .map(|s| s.trim().to_string())
                .unwrap_or_else(|| "bxu-24".to_string())
        });
    format!("{user}@{fqdn}")
}

fn short_host(host: &str) -> String {
    let h = host.trim();
    if let Some(short) = h.strip_suffix(".enst.fr") {
        short.to_string()
    } else {
        h.to_string()
    }
}

fn ssh_target_candidates(host: &str) -> Vec<String> {
    let primary = ssh_target(host);
    let user = primary
        .split('@')
        .next()
        .map(|s| s.to_string())
        .unwrap_or_else(|| "bxu-24".to_string());
    let short = short_host(host);
    let fallback = format!("{user}@{short}");
    if fallback == primary {
        vec![primary]
    } else {
        vec![primary, fallback]
    }
}

/// SCP push: worker �� coordinator (worker initiates, coordinator cannot reach workers).
fn scp_push(local_path: &str, remote_host: &str, remote_path: &str) -> Result<()> {
    let mut errs: Vec<String> = Vec::new();
    for target in ssh_target_candidates(remote_host) {
        let args = vec![
            "-o".to_string(), "BatchMode=yes".to_string(),
            "-o".to_string(), "StrictHostKeyChecking=no".to_string(),
            "-o".to_string(), "ConnectTimeout=10".to_string(),
            "-4".to_string(),
            local_path.to_string(),
            format!("{target}:{remote_path}"),
        ];
        println!("[debug][scp_push] cmd=scp {}", args.join(" "));
        for attempt in 1..=SSH_TRANSFER_ATTEMPTS {
            let output = Command::new("scp")
                .args([
                    "-o", "BatchMode=yes",
                    "-o", "StrictHostKeyChecking=no",
                    "-o", "ConnectTimeout=10",
                    "-4",
                    local_path,
                    &format!("{target}:{remote_path}"),
                ])
                .output()?;
            let code = output.status.code().unwrap_or(-1);
            let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
            let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
            println!(
                "[debug][scp_push] target={} attempt={}/{} exit_code={} stdout='{}' stderr='{}'",
                target, attempt, SSH_TRANSFER_ATTEMPTS, code, stdout, stderr
            );
            if output.status.success() {
                return Ok(());
            }
            errs.push(format!(
                "{}:attempt={}/{} exit_code={} stderr='{}'",
                target, attempt, SSH_TRANSFER_ATTEMPTS, code, stderr
            ));
            sleep(Duration::from_millis(500 * attempt as u64));
        }
    }
    anyhow::bail!(
        "scp push failed: {} -> {} (attempts: {})",
        local_path,
        remote_path,
        errs.join(", ")
    )
}

/// SCP pull: worker pulls reduce_input from coordinator.
fn scp_pull(remote_host: &str, remote_path: &str, local_path: &str) -> Result<()> {
    let mut errs: Vec<String> = Vec::new();
    for target in ssh_target_candidates(remote_host) {
        let args = vec![
            "-o".to_string(), "BatchMode=yes".to_string(),
            "-o".to_string(), "StrictHostKeyChecking=no".to_string(),
            "-o".to_string(), "ConnectTimeout=10".to_string(),
            "-4".to_string(),
            format!("{target}:{remote_path}"),
            local_path.to_string(),
        ];
        println!("[debug][scp_pull] cmd=scp {}", args.join(" "));
        for attempt in 1..=SSH_TRANSFER_ATTEMPTS {
            let output = Command::new("scp")
                .args([
                    "-o", "BatchMode=yes",
                    "-o", "StrictHostKeyChecking=no",
                    "-o", "ConnectTimeout=10",
                    "-4",
                    &format!("{target}:{remote_path}"),
                    local_path,
                ])
                .output()?;
            let code = output.status.code().unwrap_or(-1);
            let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
            let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
            println!(
                "[debug][scp_pull] target={} attempt={}/{} exit_code={} stdout='{}' stderr='{}'",
                target, attempt, SSH_TRANSFER_ATTEMPTS, code, stdout, stderr
            );
            if output.status.success() {
                return Ok(());
            }
            errs.push(format!(
                "{}:attempt={}/{} exit_code={} stderr='{}'",
                target, attempt, SSH_TRANSFER_ATTEMPTS, code, stderr
            ));
            sleep(Duration::from_millis(500 * attempt as u64));
        }
    }
    anyhow::bail!(
        "scp pull failed: {} -> {} (attempts: {})",
        remote_path,
        local_path,
        errs.join(", ")
    )
}

/// Tell a remote host to delete a file via SSH.
fn ssh_remove_file(host: &str, path: &str) -> Result<()> {
    let mut errs: Vec<String> = Vec::new();
    for target in ssh_target_candidates(host) {
        let args = vec![
            "-o".to_string(), "BatchMode=yes".to_string(),
            "-o".to_string(), "StrictHostKeyChecking=no".to_string(),
            "-o".to_string(), "ConnectTimeout=10".to_string(),
            "-4".to_string(),
            target.clone(),
            "rm".to_string(), "-f".to_string(), path.to_string(),
        ];
        println!("[debug][ssh_rm] cmd=ssh {}", args.join(" "));
        for attempt in 1..=SSH_TRANSFER_ATTEMPTS {
            let output = Command::new("ssh")
                .args([
                    "-o", "BatchMode=yes",
                    "-o", "StrictHostKeyChecking=no",
                    "-o", "ConnectTimeout=10",
                    "-4",
                    target.as_str(),
                    "rm", "-f", path,
                ])
                .output()?;
            let code = output.status.code().unwrap_or(-1);
            let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
            let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
            println!(
                "[debug][ssh_rm] target={} attempt={}/{} exit_code={} stdout='{}' stderr='{}'",
                target, attempt, SSH_TRANSFER_ATTEMPTS, code, stdout, stderr
            );
            if output.status.success() {
                return Ok(());
            }
            errs.push(format!(
                "{}:attempt={}/{} exit_code={} stderr='{}'",
                target, attempt, SSH_TRANSFER_ATTEMPTS, code, stderr
            ));
            sleep(Duration::from_millis(500 * attempt as u64));
        }
    }
    anyhow::bail!(
        "ssh remove failed for {} (attempts: {})",
        path,
        errs.join(", ")
    )
}
