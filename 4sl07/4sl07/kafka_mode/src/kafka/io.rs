use anyhow::{Context, Result};
use futures::StreamExt;
use rdkafka::ClientConfig;
use rdkafka::Message;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::BorrowedMessage;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rustc_hash::FxHashMap;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use uuid::Uuid;

const DATA_MESSAGE_MAX_BYTES: usize = 67_108_864;
const BINARY_CHUNK_PAYLOAD_BYTES: usize = 48 * 1024 * 1024;

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
) -> Result<StreamConsumer> {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap_servers)
        .set("group.id", group_id)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "10000")
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .set(
            "fetch.message.max.bytes",
            DATA_MESSAGE_MAX_BYTES.to_string(),
        )
        .set(
            "max.partition.fetch.bytes",
            DATA_MESSAGE_MAX_BYTES.to_string(),
        )
        .create()
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
    consumer: &StreamConsumer,
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
    consumer: &'a StreamConsumer,
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

pub fn commit_message(consumer: &StreamConsumer, msg: &BorrowedMessage<'_>) -> Result<()> {
    consumer.commit_message(msg, CommitMode::Sync)?;
    Ok(())
}
