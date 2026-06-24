use crate::config::{RunConfig, TopicNames};
use crate::kafka::admin::ensure_topics;
use crate::kafka::io::{
    commit_message, create_consumer, create_producer, recv_binary, recv_json, send_binary,
    send_json,
};
use crate::messages::{
    MapPartitionPayload, MapTask, ReduceResultPayload, ReduceTaskPayload, TaskAck, TaskPhase,
    WorkerRegistration,
};
use anyhow::Result;
use rustc_hash::{FxHashMap, FxHashSet};
use std::path::Path;
use tokio::fs;

pub async fn run_coordinator(run: RunConfig) -> Result<()> {
    let topics = TopicNames::from_job(&run.job_id);
    ensure_topics(
        &run.bootstrap_servers,
        &[
            (topics.worker_registration.as_str(), 1),
            (topics.map_tasks.as_str(), 8),
            (topics.map_results.as_str(), run.reduce_count as i32),
            (topics.reduce_tasks.as_str(), run.reduce_count as i32),
            (topics.reduce_results.as_str(), run.reduce_count as i32),
            (topics.task_acks.as_str(), 1),
        ],
    )
    .await?;

    let producer = create_producer(&run.bootstrap_servers)?;
    let local_host = local_host_fqdn();

    let registration_consumer = create_consumer(
        &run.bootstrap_servers,
        &format!("coord-{}-registration", run.job_id),
        &[topics.worker_registration.as_str()],
    )?;

    let mut online_workers: FxHashSet<String> = FxHashSet::default();
    println!("Waiting for {} workers...", run.workers);
    while online_workers.len() < run.workers {
        if let Some((reg, msg)) = recv_json::<WorkerRegistration>(&registration_consumer).await? {
            if reg.job_id == run.job_id {
                online_workers.insert(reg.worker_id);
            }
            commit_message(&registration_consumer, &msg)?;
            println!(
                "Registered workers: {}/{}",
                online_workers.len(),
                run.workers
            );
        }
    }

    let input_len = std::fs::metadata(&run.input_file)?.len();
    let computed_maps = if run.map_task_count > 0 {
        run.map_task_count
    } else {
        ((input_len as usize) / run.chunk_size_bytes).max(1)
    };

    for map_id in 0..computed_maps {
        let offset = (map_id * run.chunk_size_bytes) as u64;
        let task = MapTask {
            job_id: run.job_id.clone(),
            map_id,
            input_file: run.input_file.clone(),
            offset,
            chunk_size_bytes: run.chunk_size_bytes,
            reduce_count: run.reduce_count,
            version: run.version.clone(),
            coordinator_host: local_host.clone(),
        };
        send_json(
            &producer,
            &topics.map_tasks,
            &format!("map-{}", map_id),
            &task,
        )
        .await?;
    }
    println!("Published {} map tasks", computed_maps);

    let map_results_consumer = create_consumer(
        &run.bootstrap_servers,
        &format!("coord-{}-map-results", run.job_id),
        &[topics.map_results.as_str()],
    )?;
    let ack_consumer = create_consumer(
        &run.bootstrap_servers,
        &format!("coord-{}-acks", run.job_id),
        &[topics.task_acks.as_str()],
    )?;

    // In-memory aggregation per reduce partition. Map partitions are compact bincode payloads
    // carried by Kafka, so there is no SSH/SCP transfer in the shuffle path.
    let mut reduce_accumulators: Vec<FxHashMap<String, u32>> = (0..run.reduce_count)
        .map(|_| FxHashMap::default())
        .collect();
    let mut map_counts: Vec<usize> = vec![0usize; run.reduce_count];
    let mut reduce_dispatched: Vec<bool> = vec![false; run.reduce_count];
    let mut map_done: FxHashSet<usize> = FxHashSet::default();

    while map_done.len() < computed_maps || reduce_dispatched.iter().any(|dispatched| !*dispatched)
    {
        tokio::select! {
            res = recv_binary::<MapPartitionPayload>(&map_results_consumer) => {
                if let Some((payload, msg)) = res? {
                    if payload.job_id == run.job_id && payload.reduce_id < run.reduce_count {
                        let reduce_id = payload.reduce_id;
                        let acc = &mut reduce_accumulators[reduce_id];
                        for (k, v) in payload.entries {
                            *acc.entry(k).or_insert(0) += v;
                        }
                        map_counts[reduce_id] += 1;
                        if map_counts[reduce_id] == computed_maps
                            && !reduce_dispatched[reduce_id]
                        {
                            reduce_dispatched[reduce_id] = true;
                            dispatch_reduce_task(
                                reduce_id,
                                &reduce_accumulators[reduce_id],
                                &producer,
                                &topics,
                                &run,
                            ).await?;
                        }
                    }
                    commit_message(&map_results_consumer, &msg)?;
                }
            }
            ack = recv_json::<TaskAck>(&ack_consumer) => {
                if let Some((a, msg)) = ack? {
                    if a.job_id == run.job_id && matches!(a.phase, TaskPhase::Map) {
                        map_done.insert(a.task_id);
                        println!("Map done: {}/{}", map_done.len(), computed_maps);
                    }
                    commit_message(&ack_consumer, &msg)?;
                }
            }
        }
    }

    println!("All map tasks completed.");

    let reduce_results_consumer = create_consumer(
        &run.bootstrap_servers,
        &format!("coord-{}-reduce-results", run.job_id),
        &[topics.reduce_results.as_str()],
    )?;

    let mut reduce_done: FxHashSet<usize> = FxHashSet::default();
    let mut final_results: FxHashMap<usize, Vec<(String, u32)>> = FxHashMap::default();

    while final_results.len() < run.reduce_count {
        tokio::select! {
            res = recv_binary::<ReduceResultPayload>(&reduce_results_consumer) => {
                if let Some((payload, msg)) = res? {
                    if payload.job_id == run.job_id {
                        final_results.insert(payload.reduce_id, payload.entries);
                        println!("Reduce result received: {}/{}", final_results.len(), run.reduce_count);
                    }
                    commit_message(&reduce_results_consumer, &msg)?;
                }
            }
            ack = recv_json::<TaskAck>(&ack_consumer) => {
                if let Some((a, msg)) = ack? {
                    if a.job_id == run.job_id && matches!(a.phase, TaskPhase::Reduce) {
                        reduce_done.insert(a.task_id);
                        println!("Reduce done: {}/{}", reduce_done.len(), run.reduce_count);
                    }
                    commit_message(&ack_consumer, &msg)?;
                }
            }
        }
    }

    write_results(&run.result_dir, &run.job_id, final_results).await?;
    println!("Job {} completed", run.job_id);
    Ok(())
}

async fn dispatch_reduce_task(
    reduce_id: usize,
    accumulator: &FxHashMap<String, u32>,
    producer: &rdkafka::producer::FutureProducer,
    topics: &TopicNames,
    run: &RunConfig,
) -> Result<()> {
    let entries: Vec<(String, u32)> = accumulator.iter().map(|(k, v)| (k.clone(), *v)).collect();
    let payload = ReduceTaskPayload {
        job_id: run.job_id.clone(),
        reduce_id,
        entry_count: entries.len(),
        version: run.version.clone(),
        entries,
    };
    send_binary(
        producer,
        &topics.reduce_tasks,
        &format!("reduce-{}", reduce_id),
        &payload,
    )
    .await?;
    println!(
        "Dispatched reduce task {} ({} unique entries)",
        reduce_id, payload.entry_count
    );
    Ok(())
}

async fn write_results(
    result_dir: &str,
    job_id: &str,
    final_results: FxHashMap<usize, Vec<(String, u32)>>,
) -> Result<()> {
    if !Path::new(result_dir).exists() {
        fs::create_dir_all(result_dir).await?;
    }
    for (reduce_id, entries) in final_results {
        let path = format!("{result_dir}/reduce_{reduce_id}_{job_id}.json");
        let json = serde_json::to_string_pretty(&entries)?;
        fs::write(path, json).await?;
    }
    Ok(())
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
