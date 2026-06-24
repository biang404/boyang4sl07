use anyhow::Result;
use rdkafka::ClientConfig;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};

const DATA_TOPIC_MAX_BYTES: &str = "67108864";

pub async fn ensure_topics(bootstrap_servers: &str, topics: &[(&str, i32)]) -> Result<()> {
    let admin: AdminClient<_> = ClientConfig::new()
        .set("bootstrap.servers", bootstrap_servers)
        .create()?;

    let new_topics: Vec<NewTopic<'_>> = topics
        .iter()
        .map(|(name, partitions)| {
            NewTopic::new(name, *partitions, TopicReplication::Fixed(1))
                .set("max.message.bytes", DATA_TOPIC_MAX_BYTES)
        })
        .collect();

    let _ = admin
        .create_topics(&new_topics, &AdminOptions::new())
        .await?;
    Ok(())
}

pub async fn delete_topics(bootstrap_servers: &str, topics: &[&str]) -> Result<()> {
    let admin: AdminClient<_> = ClientConfig::new()
        .set("bootstrap.servers", bootstrap_servers)
        .create()?;
    let _ = admin.delete_topics(topics, &AdminOptions::new()).await?;
    Ok(())
}
