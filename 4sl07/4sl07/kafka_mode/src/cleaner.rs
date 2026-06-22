use crate::config::TopicNames;
use crate::kafka::admin::delete_topics;
use anyhow::Result;

pub async fn run_cleaner(bootstrap_servers: &str, job_id: &str) -> Result<()> {
    let topics = TopicNames::from_job(job_id);
    delete_topics(bootstrap_servers, &topics.all()).await?;
    println!("Deleted job topics for {}", job_id);
    Ok(())
}
