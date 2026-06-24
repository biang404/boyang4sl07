use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerRegistration {
    pub job_id: String,
    pub worker_id: String,
    pub hostname: String,
    pub ts_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MapTask {
    pub job_id: String,
    pub map_id: usize,
    pub input_file: String,
    pub offset: u64,
    pub chunk_size_bytes: usize,
    pub reduce_count: usize,
    pub version: String,
    /// FQDN of the coordinator machine.
    pub coordinator_host: String,
}

/// Compact Kafka payload for one map partition output.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MapPartitionPayload {
    pub job_id: String,
    pub worker_id: String,
    pub map_id: usize,
    pub reduce_id: usize,
    pub entry_count: usize,
    pub entries: Vec<(String, u32)>,
}

/// Compact Kafka payload for one reduce task input.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReduceTaskPayload {
    pub job_id: String,
    pub reduce_id: usize,
    pub entry_count: usize,
    pub version: String,
    pub entries: Vec<(String, u32)>,
}

/// Compact Kafka payload for one reduce task output.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReduceResultPayload {
    pub job_id: String,
    pub worker_id: String,
    pub reduce_id: usize,
    pub entry_count: usize,
    pub entries: Vec<(String, u32)>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TaskPhase {
    Map,
    Reduce,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskAck {
    pub job_id: String,
    pub worker_id: String,
    pub phase: TaskPhase,
    pub task_id: usize,
    pub ts_ms: u64,
}
