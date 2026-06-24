use anyhow::{Context, Result};
use futures::StreamExt;
use rdkafka::ClientConfig;
use rdkafka::Message;
use rdkafka::client::ClientContext;
use rdkafka::consumer::{
    BaseConsumer, CommitMode, Consumer, ConsumerContext, Rebalance, StreamConsumer,
};
use rdkafka::message::BorrowedMessage;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::statistics::Statistics;
use rdkafka::topic_partition_list::TopicPartitionList;
use rustc_hash::FxHashMap;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use uuid::Uuid;

const DATA_MESSAGE_MAX_BYTES: usize = 67_108_864;
const BINARY_CHUNK_PAYLOAD_BYTES: usize = 48 * 1024 * 1024;
const CONSUMER_SESSION_TIMEOUT_MS: &str = "120000";
const CONSUMER_MAX_POLL_INTERVAL_MS: &str = "900000";
const CONSUMER_HEARTBEAT_INTERVAL_MS: &str = "3000";
const COMMIT_RETRY_ATTEMPTS: usize = 8;
const CONSUMER_STATS_INTERVAL_MS: &str = "10000";

pub type AppConsumer = StreamConsumer<LoggingConsumerContext>;

#[derive(Clone, Debug)]
pub struct LoggingConsumerContext {
    group_id: String,
    topics: Vec<String>,
}

impl LoggingConsumerContext {
    fn new(group_id: &str, topics: &[&str]) -> Self {
        Self {
            group_id: group_id.to_string(),
            topics: topics.iter().map(|topic| topic.to_string()).collect(),
        }
    }
}

impl ClientContext for LoggingConsumerContext {
    fn stats(&self, stats: Statistics) {
        let broker_states: Vec<serde_json::Value> = stats
            .brokers
            .values()
            .map(|broker| {
                serde_json::json!({
                    "name": broker.name,
                    "nodename": broker.nodename,
                    "state": broker.state,
                    "stateage_ms": broker.stateage / 1000,
                    "outbuf_cnt": broker.outbuf_cnt,
                    "waitresp_cnt": broker.waitresp_cnt,
                    "txerrs": broker.txerrs,
                    "txretries": broker.txretries,
                    "req_timeouts": broker.req_timeouts,
                    "rxerrs": broker.rxerrs,
                    "disconnects": broker.disconnects,
                })
            })
            .collect();

        let cgrp = stats.cgrp.as_ref().map(|group| {
            serde_json::json!({
                "state": group.state,
                "stateage_ms": group.stateage,
                "join_state": group.join_state,
                "rebalance_age_ms": group.rebalance_age,
                "rebalance_cnt": group.rebalance_cnt,
                "rebalance_reason": group.rebalance_reason,
                "assignment_size": group.assignment_size,
            })
        });

        log_metric(serde_json::json!({
            "event": "kafka_consumer_group_stats",
            "group_id": self.group_id,
            "topics": self.topics,
            "client_name": stats.name,
            "client_id": stats.client_id,
            "replyq": stats.replyq,
            "rxmsgs": stats.rxmsgs,
            "rxmsg_bytes": stats.rxmsg_bytes,
            "tx": stats.tx,
            "rx": stats.rx,
            "consumer_group": cgrp,
            "brokers": broker_states,
        }));
    }
}

impl ConsumerContext for LoggingConsumerContext {
    fn pre_rebalance(&self, _base_consumer: &BaseConsumer<Self>, rebalance: &Rebalance<'_>) {
        log_metric(serde_json::json!({
            "event": "kafka_consumer_rebalance_pre",
            "group_id": self.group_id,
            "topics": self.topics,
            "rebalance": rebalance_summary(rebalance),
            "assignment": rebalance_partitions(rebalance),
        }));
        eprintln!(
            "[warn][kafka-rebalance] pre group_id={} rebalance={:?}",
            self.group_id, rebalance
        );
    }

    fn post_rebalance(&self, _base_consumer: &BaseConsumer<Self>, rebalance: &Rebalance<'_>) {
        log_metric(serde_json::json!({
            "event": "kafka_consumer_rebalance_post",
            "group_id": self.group_id,
            "topics": self.topics,
            "rebalance": rebalance_summary(rebalance),
            "assignment": rebalance_partitions(rebalance),
        }));
        eprintln!(
            "[warn][kafka-rebalance] post group_id={} rebalance={:?}",
            self.group_id, rebalance
        );
    }
}

pub fn create_producer(bootstrap_servers: &str) -> Result<FutureProducer> {
    ClientConfig::new()
        .set("bootstrap.servers", bootstrap_servers)
        .set("message.max.bytes", DATA_MESSAGE_MAX_BYTES.to_string())
        .create()
        .context("failed to create Kafka producer")
}

pub fn create_consumer(
    bootstrap_servers: &str,
    group_id: &str,
    topics: &[&str],
) -> Result<AppConsumer> {
    let context = LoggingConsumerContext::new(group_id, topics);
    let consumer: AppConsumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap_servers)
        .set("group.id", group_id)
        .set("client.id", group_id)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", CONSUMER_SESSION_TIMEOUT_MS)
        .set("max.poll.interval.ms", CONSUMER_MAX_POLL_INTERVAL_MS)
        .set("heartbeat.interval.ms", CONSUMER_HEARTBEAT_INTERVAL_MS)
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .set("statistics.interval.ms", CONSUMER_STATS_INTERVAL_MS)
        .set("debug", "cgrp,broker,protocol")
        .set(
            "fetch.message.max.bytes",
            DATA_MESSAGE_MAX_BYTES.to_string(),
        )
        .set(
            "max.partition.fetch.bytes",
            DATA_MESSAGE_MAX_BYTES.to_string(),
        )
        .create_with_context(context)
        .context("failed to create Kafka consumer")?;
    consumer.subscribe(topics)?;
    Ok(consumer)
}

#[derive(Debug, Serialize, Deserialize)]
enum BinaryFrame {
    Whole {
        payload: Vec<u8>,
    },
    Chunk {
        transfer_id: String,
        chunk_index: u32,
        chunk_count: u32,
        total_bytes: usize,
        payload: Vec<u8>,
    },
}

#[derive(Default)]
pub struct BinaryChunkCollector {
    partial: FxHashMap<String, PartialBinaryPayload>,
}

struct PartialBinaryPayload {
    chunk_count: u32,
    total_bytes: usize,
    chunks: Vec<Option<Vec<u8>>>,
    received: u32,
}

pub async fn send_json<T: Serialize>(
    producer: &FutureProducer,
    topic: &str,
    key: &str,
    value: &T,
) -> Result<()> {
    let payload = serde_json::to_string(value)?;
    let _ = producer
        .send(
            FutureRecord::to(topic).key(key).payload(&payload),
            Duration::from_secs(10),
        )
        .await
        .map_err(|(e, _)| e)?;
    Ok(())
}

pub async fn send_json_to_partition<T: Serialize>(
    producer: &FutureProducer,
    topic: &str,
    key: &str,
    partition: i32,
    value: &T,
) -> Result<()> {
    let payload = serde_json::to_string(value)?;
    let _ = producer
        .send(
            FutureRecord::to(topic)
                .key(key)
                .partition(partition)
                .payload(&payload),
            Duration::from_secs(10),
        )
        .await
        .map_err(|(e, _)| e)?;
    Ok(())
}

pub async fn send_binary<T: Serialize>(
    producer: &FutureProducer,
    topic: &str,
    key: &str,
    value: &T,
) -> Result<()> {
    let payload = bincode::serialize(value)?;
    send_binary_payload(producer, topic, key, None, payload).await
}

pub async fn send_binary_to_partition<T: Serialize>(
    producer: &FutureProducer,
    topic: &str,
    key: &str,
    partition: i32,
    value: &T,
) -> Result<()> {
    let payload = bincode::serialize(value)?;
    send_binary_payload(producer, topic, key, Some(partition), payload).await
}

async fn send_binary_payload(
    producer: &FutureProducer,
    topic: &str,
    key: &str,
    partition: Option<i32>,
    payload: Vec<u8>,
) -> Result<()> {
    if payload.len() <= BINARY_CHUNK_PAYLOAD_BYTES {
        let frame = BinaryFrame::Whole { payload };
        return send_binary_frame(producer, topic, key, partition, &frame).await;
    }

    let chunk_count = payload.len().div_ceil(BINARY_CHUNK_PAYLOAD_BYTES);
    let transfer_id = Uuid::new_v4().to_string();
    println!(
        "[debug][kafka-chunk-send] topic={} key={} transfer_id={} payload_bytes={} chunk_payload_bytes={} chunks={} max_message_bytes={}",
        topic,
        key,
        transfer_id,
        payload.len(),
        BINARY_CHUNK_PAYLOAD_BYTES,
        chunk_count,
        DATA_MESSAGE_MAX_BYTES
    );

    for (chunk_index, chunk) in payload.chunks(BINARY_CHUNK_PAYLOAD_BYTES).enumerate() {
        let frame = BinaryFrame::Chunk {
            transfer_id: transfer_id.clone(),
            chunk_index: chunk_index as u32,
            chunk_count: chunk_count as u32,
            total_bytes: payload.len(),
            payload: chunk.to_vec(),
        };
        send_binary_frame(producer, topic, key, partition, &frame).await?;
    }

    Ok(())
}

async fn send_binary_frame(
    producer: &FutureProducer,
    topic: &str,
    key: &str,
    partition: Option<i32>,
    frame: &BinaryFrame,
) -> Result<()> {
    let payload = bincode::serialize(frame)?;
    if payload.len() > DATA_MESSAGE_MAX_BYTES {
        anyhow::bail!(
            "Kafka binary frame is too large: {} bytes > {} bytes",
            payload.len(),
            DATA_MESSAGE_MAX_BYTES
        );
    }
    let mut record = FutureRecord::to(topic).key(key).payload(&payload);
    if let Some(partition) = partition {
        record = record.partition(partition);
    }
    let _ = producer
        .send(record, Duration::from_secs(10))
        .await
        .map_err(|(e, _)| e)?;
    Ok(())
}

pub async fn recv_json<T: DeserializeOwned>(
    consumer: &AppConsumer,
) -> Result<Option<(T, BorrowedMessage<'_>)>> {
    let mut stream = consumer.stream();
    if let Some(msg) = stream.next().await {
        let msg = msg?;
        let payload = match msg.payload_view::<str>() {
            Some(Ok(v)) => v,
            _ => return Ok(None),
        };
        let decoded: T = serde_json::from_str(payload)?;
        Ok(Some((decoded, msg)))
    } else {
        Ok(None)
    }
}

pub async fn recv_binary<'a, T: DeserializeOwned>(
    consumer: &'a AppConsumer,
    collector: &mut BinaryChunkCollector,
) -> Result<Option<(T, BorrowedMessage<'a>)>> {
    loop {
        let mut stream = consumer.stream();
        if let Some(msg) = stream.next().await {
            let msg = msg?;
            let payload = match msg.payload() {
                Some(v) => v,
                None => return Ok(None),
            };
            if let Some(decoded) = decode_binary_payload(payload, collector)? {
                return Ok(Some((decoded, msg)));
            }
            continue;
        } else {
            return Ok(None);
        }
    }
}

fn decode_binary_payload<T: DeserializeOwned>(
    payload: &[u8],
    collector: &mut BinaryChunkCollector,
) -> Result<Option<T>> {
    let frame = match bincode::deserialize::<BinaryFrame>(payload) {
        Ok(frame) => frame,
        Err(_) => return Ok(Some(bincode::deserialize(payload)?)),
    };

    match frame {
        BinaryFrame::Whole { payload } => Ok(Some(bincode::deserialize(&payload)?)),
        BinaryFrame::Chunk {
            transfer_id,
            chunk_index,
            chunk_count,
            total_bytes,
            payload,
        } => {
            let complete = collector.push_chunk(
                transfer_id,
                chunk_index,
                chunk_count,
                total_bytes,
                payload,
            )?;
            match complete {
                Some(payload) => Ok(Some(bincode::deserialize(&payload)?)),
                None => Ok(None),
            }
        }
    }
}

impl BinaryChunkCollector {
    fn push_chunk(
        &mut self,
        transfer_id: String,
        chunk_index: u32,
        chunk_count: u32,
        total_bytes: usize,
        payload: Vec<u8>,
    ) -> Result<Option<Vec<u8>>> {
        if chunk_count == 0 {
            anyhow::bail!("invalid Kafka binary chunk with chunk_count=0");
        }
        if chunk_index >= chunk_count {
            anyhow::bail!(
                "invalid Kafka binary chunk index {} for chunk_count {}",
                chunk_index,
                chunk_count
            );
        }

        let entry =
            self.partial
                .entry(transfer_id.clone())
                .or_insert_with(|| PartialBinaryPayload {
                    chunk_count,
                    total_bytes,
                    chunks: vec![None; chunk_count as usize],
                    received: 0,
                });

        if entry.chunk_count != chunk_count || entry.total_bytes != total_bytes {
            anyhow::bail!(
                "inconsistent Kafka binary chunk metadata for {}",
                transfer_id
            );
        }

        let slot = &mut entry.chunks[chunk_index as usize];
        if slot.is_none() {
            *slot = Some(payload);
            entry.received += 1;
        }

        if entry.received != entry.chunk_count {
            return Ok(None);
        }

        let entry = self.partial.remove(&transfer_id).expect("entry exists");
        let mut merged = Vec::with_capacity(entry.total_bytes);
        for chunk in entry.chunks {
            let chunk = chunk.context("missing Kafka binary chunk during reassembly")?;
            merged.extend_from_slice(&chunk);
        }
        if merged.len() != entry.total_bytes {
            anyhow::bail!(
                "reassembled Kafka binary payload size mismatch for {}: got {}, expected {}",
                transfer_id,
                merged.len(),
                entry.total_bytes
            );
        }
        println!(
            "[debug][kafka-chunk-recv] transfer_id={} payload_bytes={} chunks={}",
            transfer_id, entry.total_bytes, entry.chunk_count
        );
        Ok(Some(merged))
    }
}

pub fn commit_message(consumer: &AppConsumer, msg: &BorrowedMessage<'_>) -> Result<()> {
    for attempt in 1..=COMMIT_RETRY_ATTEMPTS {
        match consumer.commit_message(msg, CommitMode::Sync) {
            Ok(()) => return Ok(()),
            Err(err) => {
                log_metric(serde_json::json!({
                    "event": "kafka_consumer_commit_error",
                    "attempt": attempt,
                    "max_attempts": COMMIT_RETRY_ATTEMPTS,
                    "topic": msg.topic(),
                    "partition": msg.partition(),
                    "offset": msg.offset(),
                    "error": err.to_string(),
                    "hint": commit_error_hint(&err.to_string()),
                }));
                eprintln!(
                    "[warn][kafka-commit] attempt {}/{} failed topic={} partition={} offset={} error={} hint={}",
                    attempt,
                    COMMIT_RETRY_ATTEMPTS,
                    msg.topic(),
                    msg.partition(),
                    msg.offset(),
                    err,
                    commit_error_hint(&err.to_string())
                );
                if attempt < COMMIT_RETRY_ATTEMPTS {
                    std::thread::sleep(Duration::from_millis(500 * attempt as u64));
                }
            }
        }
    }
    eprintln!("[warn][kafka-commit] giving up after retries; continuing without crashing");
    Ok(())
}

fn log_metric(value: serde_json::Value) {
    println!("[metric] {}", value);
}

fn rebalance_summary(rebalance: &Rebalance<'_>) -> serde_json::Value {
    match rebalance {
        Rebalance::Assign(_) => serde_json::json!({"kind": "assign"}),
        Rebalance::Revoke(_) => serde_json::json!({"kind": "revoke"}),
        Rebalance::Error(err) => {
            serde_json::json!({"kind": "error", "error": err.to_string()})
        }
    }
}

fn rebalance_partitions(rebalance: &Rebalance<'_>) -> Vec<serde_json::Value> {
    match rebalance {
        Rebalance::Assign(tpl) | Rebalance::Revoke(tpl) => topic_partition_list(tpl),
        Rebalance::Error(_) => Vec::new(),
    }
}

fn topic_partition_list(tpl: &TopicPartitionList) -> Vec<serde_json::Value> {
    tpl.elements()
        .into_iter()
        .map(|elem| {
            serde_json::json!({
                "topic": elem.topic(),
                "partition": elem.partition(),
                "offset": format!("{:?}", elem.offset()),
            })
        })
        .collect()
}

fn commit_error_hint(error: &str) -> &'static str {
    if error.contains("WaitingForCoordinator") {
        "consumer group coordinator is unavailable or group is rebalancing; check kafka_consumer_group_stats.rebalance_reason and broker logs"
    } else if error.contains("RebalanceInProgress") {
        "consumer group rebalance is in progress; commit can be retried after assignment stabilizes"
    } else if error.contains("IllegalGeneration") || error.contains("UnknownMemberId") {
        "consumer lost its group generation, often after session timeout or member eviction"
    } else if error.contains("MaxPollExceeded") {
        "consumer processing exceeded max.poll.interval.ms"
    } else {
        "inspect nearby kafka_consumer_rebalance_* and kafka_consumer_group_stats metrics"
    }
}
