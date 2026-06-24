use crate::config::{RunConfig, TopicNames};
use crate::core::map::{map_chunk_from_file, partition_map};
use crate::core::reduce::{map_to_sorted_vec, reduce_entries};
use crate::kafka::io::{
    commit_message, create_consumer, create_producer, recv_binary, recv_json, send_binary,
    send_json,
};
use crate::messages::{
    MapPartitionPayload, MapTask, ReduceResultPayload, ReduceTaskPayload, TaskAck, TaskPhase,
    WorkerRegistration,
};
use anyhow::Result;
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
            reduce_msg = recv_binary::<ReduceTaskPayload>(&reduce_consumer) => {
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
    let mapped = map_chunk_from_file(&task.input_file, task.offset, task.chunk_size_bytes)?;
    let mut partitions = partition_map(mapped, task.reduce_count);

    for (reduce_id, entries) in partitions.drain(..).enumerate() {
        let entry_count = entries.len();

        println!(
            "[debug][map] map_id={} reduce_id={} entries={} transfer=bincode-kafka",
            task.map_id, reduce_id, entry_count
        );

        let payload = MapPartitionPayload {
            job_id: task.job_id.clone(),
            worker_id: worker_id.to_string(),
            map_id: task.map_id,
            reduce_id,
            entry_count,
            entries,
        };
        send_binary(
            producer,
            &topics.map_results,
            &format!("map-{}-reduce-{}", task.map_id, reduce_id),
            &payload,
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
    payload: ReduceTaskPayload,
) -> Result<()> {
    println!(
        "[debug][reduce] reduce_id={} entries={} transfer=bincode-kafka",
        payload.reduce_id, payload.entry_count
    );

    let output_entries = map_to_sorted_vec(reduce_entries(payload.entries));
    let entry_count = output_entries.len();

    send_binary(
        producer,
        &topics.reduce_results,
        &format!("reduce-result-{}", payload.reduce_id),
        &ReduceResultPayload {
            job_id: payload.job_id.clone(),
            worker_id: worker_id.to_string(),
            reduce_id: payload.reduce_id,
            entry_count,
            entries: output_entries,
        },
    )
    .await?;

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
