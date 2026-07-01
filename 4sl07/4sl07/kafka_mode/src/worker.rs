use crate::config::{RunConfig, TopicNames};
use crate::core::map::{map_bytes, partition_map, read_file_bytes};
use crate::core::reduce::{map_to_sorted_vec, reduce_entries};
use crate::kafka::io::{
    BinaryChunkCollector, commit_message, create_consumer, create_producer, recv_binary, recv_json,
    send_binary, send_json,
};
use crate::messages::{
    MapPartitionPayload, MapTask, ReduceResultPayload, ReduceTaskPayload, TaskAck, TaskPhase,
    WorkerRegistration,
};
use anyhow::Result;
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

pub async fn run_worker(run: RunConfig, worker_id: Option<String>) -> Result<()> {
    let worker_id = worker_id.unwrap_or_else(|| Uuid::new_v4().to_string());
    let topics = TopicNames::from_job(&run.job_id);
    let producer = create_producer(&run.bootstrap_servers)?;
    let local_host = local_host_fqdn();

    let registration = WorkerRegistration {
        job_id: run.job_id.clone(),
        worker_id: worker_id.clone(),
        hostname: local_host.clone(),
        ts_ms: now_ms(),
    };
    send_json(
        &producer,
        &topics.worker_registration,
        &worker_id,
        &registration,
    )
    .await?;

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
    let mut reduce_task_chunks = BinaryChunkCollector::default();

    loop {
        tokio::select! {
            map_msg = recv_json::<MapTask>(&map_consumer) => {
                if let Some((task, msg)) = map_msg? {
                    if task.job_id == run.job_id {
                        handle_map_task(&producer, &topics, &worker_id, task).await?;
                    }
                    commit_message(&map_consumer, &msg)?;
                }
            }
            reduce_msg = recv_binary::<ReduceTaskPayload>(&reduce_consumer, &mut reduce_task_chunks) => {
                if let Some((task, msg)) = reduce_msg? {
                    if task.job_id == run.job_id {
                        handle_reduce_task(&producer, &topics, &worker_id, task).await?;
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
    task: MapTask,
) -> Result<()> {
    let map_start_time = now_ms();
    let download_start_time = now_ms();
    let downloaded_input = prepare_input_file(&task)?;
    let download_end_time = now_ms();
    let result = handle_prepared_map_task(
        producer,
        topics,
        worker_id,
        &task,
        map_start_time,
        download_start_time,
        download_end_time,
    )
    .await;
    if downloaded_input {
        remove_input_file(&task.input_file);
    }
    result
}

async fn handle_prepared_map_task(
    producer: &rdkafka::producer::FutureProducer,
    topics: &TopicNames,
    worker_id: &str,
    task: &MapTask,
    map_start_time: u64,
    download_start_time: u64,
    download_end_time: u64,
) -> Result<()> {
    let input_bytes = std::fs::metadata(&task.input_file)?.len();
    let input_read_start_time = now_ms();
    let buf = read_file_bytes(&task.input_file)?;
    let input_read_end_time = now_ms();
    let map_process_start_time = now_ms();
    let mapped = map_bytes(buf)?;
    let mut partitions = partition_map(mapped, task.reduce_count);
    let map_process_end_time = now_ms();

    let temp_save_start_time = now_ms();
    for (reduce_id, entries) in partitions.drain(..).enumerate() {
        let entry_count = entries.len();
        let payload = MapPartitionPayload {
            job_id: task.job_id.clone(),
            worker_id: worker_id.to_string(),
            map_id: task.map_id,
            reduce_id,
            entry_count,
            entries,
        };
        let payload_bytes = bincode::serialized_size(&payload).unwrap_or(0);
        println!(
            "[debug][map] map_id={} reduce_id={} entries={} payload_bytes={} transfer=bincode-kafka",
            task.map_id, reduce_id, entry_count, payload_bytes
        );
        let shuffle_start_time = now_ms();
        send_binary(
            producer,
            &topics.map_results,
            &format!("map-{}-reduce-{}", task.map_id, reduce_id),
            &payload,
        )
        .await?;
        let shuffle_end_time = now_ms();
        log_metric(serde_json::json!({
            "event": "map_partition_shuffle",
            "job_id": task.job_id,
            "map_task_id": task.map_id,
            "reduce_id": reduce_id,
            "source_worker": worker_id,
            "destination_host": task.coordinator_host,
            "partition_file_size_bytes": payload_bytes,
            "shuffle_or_scp_start_time": shuffle_start_time,
            "shuffle_or_scp_end_time": shuffle_end_time,
            "shuffle_duration_ms": shuffle_end_time.saturating_sub(shuffle_start_time),
            "transport": "kafka_bincode",
        }));
    }
    let temp_save_end_time = now_ms();

    send_json(
        producer,
        &topics.task_acks,
        &format!("ack-map-{}", task.map_id),
        &TaskAck {
            job_id: task.job_id.clone(),
            worker_id: worker_id.to_string(),
            phase: TaskPhase::Map,
            task_id: task.map_id,
            ts_ms: now_ms(),
        },
    )
    .await?;
    let map_done_time = now_ms();
    log_metric(serde_json::json!({
        "event": "map_task",
        "job_id": task.job_id,
        "map_task_id": task.map_id,
        "worker_id": worker_id,
        "map_start_time": map_start_time,
        "download_start_time": download_start_time,
        "download_end_time": download_end_time,
        "input_read_start_time": input_read_start_time,
        "input_read_end_time": input_read_end_time,
        "map_process_start_time": map_process_start_time,
        "map_process_end_time": map_process_end_time,
        "temp_save_start_time": temp_save_start_time,
        "temp_save_end_time": temp_save_end_time,
        "input_bytes": input_bytes,
        "map_done_time": map_done_time,
        "download_duration_ms": download_end_time.saturating_sub(download_start_time),
        "input_read_duration_ms": input_read_end_time.saturating_sub(input_read_start_time),
        "map_process_duration_ms": map_process_end_time.saturating_sub(map_process_start_time),
        "temp_save_duration_ms": temp_save_end_time.saturating_sub(temp_save_start_time),
        "map_task_duration_ms": map_done_time.saturating_sub(map_start_time),
    }));

    Ok(())
}

fn prepare_input_file(task: &MapTask) -> Result<bool> {
    if task.input_url.is_empty() || std::path::Path::new(&task.input_file).exists() {
        return Ok(false);
    }

    if let Some(parent) = std::path::Path::new(&task.input_file).parent() {
        std::fs::create_dir_all(parent)?;
    }

    let compressed_path = format!("{}.gz", task.input_file);
    println!(
        "[debug][download] map_id={} url={} target={}",
        task.map_id, task.input_url, task.input_file
    );
    run_command(Command::new("curl").args([
        "-L",
        "--retry",
        "5",
        "--retry-delay",
        "3",
        "-C",
        "-",
        task.input_url.as_str(),
        "-o",
        compressed_path.as_str(),
    ]))?;
    run_command(Command::new("gunzip").args(["-f", compressed_path.as_str()]))?;
    Ok(true)
}

fn remove_input_file(path: &str) {
    match std::fs::remove_file(path) {
        Ok(()) => println!("[debug][cleanup] removed input_file={}", path),
        Err(err) => eprintln!(
            "[warn][cleanup] could not remove input_file={}: {}",
            path, err
        ),
    }
}

fn run_command(command: &mut Command) -> Result<()> {
    let output = command.output()?;
    if output.status.success() {
        return Ok(());
    }

    anyhow::bail!(
        "command failed with status {:?}: stdout='{}' stderr='{}'",
        output.status.code(),
        String::from_utf8_lossy(&output.stdout).trim(),
        String::from_utf8_lossy(&output.stderr).trim()
    )
}

async fn handle_reduce_task(
    producer: &rdkafka::producer::FutureProducer,
    topics: &TopicNames,
    worker_id: &str,
    payload: ReduceTaskPayload,
) -> Result<()> {
    let reduce_start_time = now_ms();
    let shuffle_all_inputs_done_time = reduce_start_time;
    println!(
        "[debug][reduce] reduce_id={} entries={} transfer=bincode-kafka",
        payload.reduce_id, payload.entry_count
    );

    let reduce_merge_start_time = now_ms();
    let reduced_entries = reduce_entries(payload.entries);
    let sort_start_time = now_ms();
    let output_entries = map_to_sorted_vec(reduced_entries);
    let sort_end_time = now_ms();
    let entry_count = output_entries.len();
    let result_payload = ReduceResultPayload {
        job_id: payload.job_id.clone(),
        worker_id: worker_id.to_string(),
        reduce_id: payload.reduce_id,
        entry_count,
        entries: output_entries,
    };
    let payload_bytes = bincode::serialized_size(&result_payload).unwrap_or(0);
    println!(
        "[debug][reduce-result] reduce_id={} entries={} payload_bytes={} transfer=bincode-kafka",
        payload.reduce_id, entry_count, payload_bytes
    );

    let output_write_start_time = now_ms();
    send_binary(
        producer,
        &topics.reduce_results,
        &format!("reduce-result-{}", payload.reduce_id),
        &result_payload,
    )
    .await?;
    let output_write_end_time = now_ms();

    send_json(
        producer,
        &topics.task_acks,
        &format!("ack-reduce-{}", payload.reduce_id),
        &TaskAck {
            job_id: payload.job_id,
            worker_id: worker_id.to_string(),
            phase: TaskPhase::Reduce,
            task_id: payload.reduce_id,
            ts_ms: now_ms(),
        },
    )
    .await?;
    let reduce_done_time = now_ms();
    log_metric(serde_json::json!({
        "event": "reduce_task",
        "job_id": result_payload.job_id,
        "reduce_task_id": result_payload.reduce_id,
        "worker_id": worker_id,
        "reduce_start_time": reduce_start_time,
        "shuffle_all_inputs_done_time": shuffle_all_inputs_done_time,
        "reduce_merge_start_time": reduce_merge_start_time,
        "sort_start_time": sort_start_time,
        "sort_end_time": sort_end_time,
        "output_write_start_time": output_write_start_time,
        "output_write_end_time": output_write_end_time,
        "output_bytes": payload_bytes,
        "output_kind": "kafka_reduce_result_payload",
        "reduce_done_time": reduce_done_time,
        "sort_duration_ms": sort_end_time.saturating_sub(sort_start_time),
        "output_write_duration_ms": output_write_end_time.saturating_sub(output_write_start_time),
        "reduce_process_duration_ms": output_write_start_time.saturating_sub(reduce_merge_start_time),
        "file_transfer_duration_ms": output_write_end_time.saturating_sub(output_write_start_time),
        "reduce_task_duration_ms": reduce_done_time.saturating_sub(reduce_start_time),
    }));

    Ok(())
}

fn log_metric(value: serde_json::Value) {
    println!("[metric] {}", value);
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
