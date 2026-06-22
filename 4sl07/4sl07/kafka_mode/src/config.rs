use clap::Args;

#[derive(Args, Debug, Clone)]
pub struct RunConfig {
    #[arg(long)]
    pub bootstrap_servers: String,
    #[arg(long)]
    pub job_id: String,
    #[arg(long, default_value_t = 3)]
    pub workers: usize,
    #[arg(long, default_value_t = 64)]
    pub map_task_count: usize,
    #[arg(long, default_value_t = 4 * 1024 * 1024)]
    pub chunk_size_bytes: usize,
    #[arg(long, default_value_t = 8)]
    pub reduce_count: usize,
    #[arg(long, default_value = "DefaultWithLanguageSplit")]
    pub version: String,
    #[arg(long, default_value = "./result")]
    pub result_dir: String,
    #[arg(long, default_value = "./data/CC-MAIN-0001.wet")]
    pub input_file: String,
    #[arg(long, default_value = "/tmp/kafka_mode")]
    pub work_dir: String,
}

#[derive(Debug, Clone)]
pub struct TopicNames {
    pub worker_registration: String,
    pub map_tasks: String,
    pub map_results: String,
    pub reduce_tasks: String,
    pub reduce_results: String,
    pub task_acks: String,
}

impl TopicNames {
    pub fn from_job(job_id: &str) -> Self {
        Self {
            worker_registration: format!("mr.{job_id}.worker.registration"),
            map_tasks: format!("mr.{job_id}.map.tasks"),
            map_results: format!("mr.{job_id}.map.results"),
            reduce_tasks: format!("mr.{job_id}.reduce.tasks"),
            reduce_results: format!("mr.{job_id}.reduce.results"),
            task_acks: format!("mr.{job_id}.task.acks"),
        }
    }

    pub fn all(&self) -> [&str; 6] {
        [
            self.worker_registration.as_str(),
            self.map_tasks.as_str(),
            self.map_results.as_str(),
            self.reduce_tasks.as_str(),
            self.reduce_results.as_str(),
            self.task_acks.as_str(),
        ]
    }
}
