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
    /// FQDN of the coordinator machine. Workers SCP-push map outputs here.
    pub coordinator_host: String,
}

/// Metadata for map partition output (file-based storage)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MapPartitionMeta {
    pub job_id: String,
    pub worker_id: String,
    pub file_host: String,
    pub map_id: usize,
    pub reduce_id: usize,
    pub file_path: String,
    pub entry_count: usize,
}

/// Metadata for reduce task (file-based storage)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReduceTaskMeta {
    pub job_id: String,
    pub reduce_id: usize,
    pub file_host: String,
    pub file_path: String,
    pub entry_count: usize,
    pub version: String,
}

/// Metadata for reduce result (file-based storage)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReduceResultMeta {
    pub job_id: String,
    pub worker_id: String,
    pub file_host: String,
    pub reduce_id: usize,
    pub file_path: String,
    pub entry_count: usize,
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
