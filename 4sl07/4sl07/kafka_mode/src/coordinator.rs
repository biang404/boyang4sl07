use crate::config::{RunConfig, TopicNames};
use crate::kafka::admin::ensure_topics;
use crate::kafka::io::{
    commit_message, create_consumer, create_producer, recv_binary, recv_json,
    send_binary_to_partition, send_json_to_partition,
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
    if run.workers == 0 {
        anyhow::bail!("--workers must be greater than 0");
    }
    if run.reduce_count == 0 {
        anyhow::bail!("--reduce-count must be greater than 0");
    }

    let map_plan = build_map_plan(&run)?;
    let computed_maps = map_plan.len();
    let map_task_partitions = computed_maps.min(run.workers).max(1) as i32;
    let reduce_task_partitions = run.reduce_count.min(run.workers).max(1) as i32;

    ensure_topics(
        &run.bootstrap_servers,
        &[
            (topics.worker_registration.as_str(), 1),
            (topics.map_tasks.as_str(), map_task_partitions),
            (topics.map_results.as_str(), run.reduce_count as i32),
            (topics.reduce_tasks.as_str(), reduce_task_partitions),
            (topics.reduce_results.as_str(), run.reduce_count as i32),
            (topics.task_acks.as_str(), 1),
        ],
    )
    .await?;
    println!(
        "Task topic partitions: map_tasks={}, reduce_tasks={}",
        map_task_partitions, reduce_task_partitions
    );

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

    for (map_id, input_file, input_url) in map_plan {
        let task = MapTask {
            job_id: run.job_id.clone(),
            map_id,
            input_file,
            input_url,
            reduce_count: run.reduce_count,
            version: run.version.clone(),
            coordinator_host: local_host.clone(),
        };
        send_json_to_partition(
            &producer,
            &topics.map_tasks,
            &format!("map-{}", map_id),
            (map_id as i32) % map_task_partitions,
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
                                reduce_task_partitions,
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

fn build_map_plan(run: &RunConfig) -> Result<Vec<(usize, String, String)>> {
    let input_files = input_files(run)?;
    let mut plan = Vec::new();

    for input in input_files {
        let map_id = plan.len();
        plan.push((map_id, input.path, input.url));
    }

    Ok(plan)
}

struct InputSpec {
    path: String,
    url: String,
}

fn input_files(run: &RunConfig) -> Result<Vec<InputSpec>> {
    if !run.input_manifest.trim().is_empty() {
        let content = std::fs::read_to_string(&run.input_manifest)?;
        let files: Vec<InputSpec> = content
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty())
            .map(|line| {
                let mut fields = line.split('\t');
                let path = fields.next().unwrap_or_default().to_string();
                let url = fields.next().unwrap_or_default().to_string();
                InputSpec { path, url }
            })
            .collect();
        if files.is_empty() {
            anyhow::bail!("input manifest {} is empty", run.input_manifest);
        }
        return Ok(files);
    }

    Ok(vec![InputSpec {
        path: run.input_file.clone(),
        url: run.input_url.clone(),
    }])
}

async fn dispatch_reduce_task(
    reduce_id: usize,
    accumulator: &FxHashMap<String, u32>,
    producer: &rdkafka::producer::FutureProducer,
    topics: &TopicNames,
    run: &RunConfig,
    reduce_task_partitions: i32,
) -> Result<()> {
    let entries: Vec<(String, u32)> = accumulator.iter().map(|(k, v)| (k.clone(), *v)).collect();
    let payload = ReduceTaskPayload {
        job_id: run.job_id.clone(),
        reduce_id,
        entry_count: entries.len(),
        version: run.version.clone(),
        entries,
    };
    let payload_bytes = bincode::serialized_size(&payload).unwrap_or(0);
    println!(
        "Dispatched reduce task {} ({} unique entries, payload_bytes={})",
        reduce_id, payload.entry_count, payload_bytes
    );
    send_binary_to_partition(
        producer,
        &topics.reduce_tasks,
        &format!("reduce-{}", reduce_id),
        (reduce_id as i32) % reduce_task_partitions,
        &payload,
    )
    .await?;
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
