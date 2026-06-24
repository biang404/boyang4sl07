use anyhow::{Context, Result};
use futures::StreamExt;
use rdkafka::ClientConfig;
use rdkafka::Message;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::BorrowedMessage;
use rdkafka::producer::{FutureProducer, FutureRecord};
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::time::Duration;

const DATA_MESSAGE_MAX_BYTES: &str = "67108864";

pub fn create_producer(bootstrap_servers: &str) -> Result<FutureProducer> {
    ClientConfig::new()
        .set("bootstrap.servers", bootstrap_servers)
        .set("message.max.bytes", DATA_MESSAGE_MAX_BYTES)
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
        .set("fetch.message.max.bytes", DATA_MESSAGE_MAX_BYTES)
        .set("max.partition.fetch.bytes", DATA_MESSAGE_MAX_BYTES)
        .create()
        .context("failed to create Kafka consumer")?;
    consumer.subscribe(topics)?;
    Ok(consumer)
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
    let _ = producer
        .send(
            FutureRecord::to(topic).key(key).payload(&payload),
            Duration::from_secs(10),
        )
        .await
        .map_err(|(e, _)| e)?;
    Ok(())
}

pub async fn send_binary_to_partition<T: Serialize>(
    producer: &FutureProducer,
    topic: &str,
    key: &str,
    partition: i32,
    value: &T,
) -> Result<()> {
    let payload = bincode::serialize(value)?;
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

pub async fn recv_binary<T: DeserializeOwned>(
    consumer: &StreamConsumer,
) -> Result<Option<(T, BorrowedMessage<'_>)>> {
    let mut stream = consumer.stream();
    if let Some(msg) = stream.next().await {
        let msg = msg?;
        let payload = match msg.payload() {
            Some(v) => v,
            None => return Ok(None),
        };
        let decoded: T = bincode::deserialize(payload)?;
        Ok(Some((decoded, msg)))
    } else {
        Ok(None)
    }
}

pub fn commit_message(consumer: &StreamConsumer, msg: &BorrowedMessage<'_>) -> Result<()> {
    consumer.commit_message(msg, CommitMode::Sync)?;
    Ok(())
}
