# Kafka Mode: Data Flow Architecture Analysis

## Current Problem: MessageSizeTooLarge

**Root Cause:**
- `MapPartition` messages contain full `entries: Vec<(String, u32)>` (all word counts from one map task)
- `ReduceTask` messages contain all aggregated entries for a reduce partition
- For large input chunks (4MB+), intermediate vocabularies can produce JSON > 1MB

**Current Flow (❌ WRONG):**
```
MapTask → Worker: read_chunk → MapPartition(all_words) → Kafka ← 超大消息!
                                                    ↓
                                              Coordinator
                                                    ↓
                                    ReduceTask(all_words) → Kafka ← 也超大!
                                                    ↓
                                                  Worker
```

---

## Solution A: Shared Filesystem (Recommended for TP)

**Architecture:**
```
1. MapTask(offset, size)
   ↓
2. Worker: 
   - Reads chunk from input file (local to worker or NFS)
   - Produces map output → writes to /tmp/kafka_mode/map_outputs/{map_id}/{reduce_id}.json
   - Sends only metadata: MapPartitionMeta(map_id, reduce_id, "/tmp/kafka_mode/map_outputs/...")
   
3. Coordinator:
   - Receives all MapPartitionMeta messages
   - Reads referenced files directly from /tmp (shared filesystem)
   - Aggregates in memory
   - Writes reduce input to /tmp/kafka_mode/reduce_inputs/{reduce_id}.json
   - Sends only metadata: ReduceTaskMeta(reduce_id, "/tmp/kafka_mode/reduce_inputs/...")
   
4. ReduceWorker:
   - Receives ReduceTaskMeta
   - Reads from shared filesystem
   - Produces output to /tmp/kafka_mode/reduce_outputs/{reduce_id}.json
   - Sends ReduceResultMeta(reduce_id, "/tmp/...")
   
5. Coordinator:
   - Reads all reduce outputs from /tmp
   - Finalizes results
```

**Advantages:**
- ✅ Eliminates large messages entirely
- ✅ Leverages NFS/local /tmp on TP machines (all in same network)
- ✅ Simple to understand and debug
- ✅ No need to modify Kafka configuration

**Disadvantages:**
- Requires all machines have access to shared /tmp (likely already true)

---

## Solution B: Kafka with Chunked Messages (Fallback)

**If shared filesystem is unavailable:**
- Implement message chunking: split large `MapPartition` into smaller chunks (e.g., 500KB each)
- Reconstruct full partition on consumer side
- Use message key: `map-{map_id}-reduce-{reduce_id}-chunk-{seq}`

**Disadvantages:**
- ✗ Complex to implement (need reassembly logic)
- ✗ Requires custom protocol
- ✗ Still slower than filesystem approach

---

## Solution C: Kafka with Compression (Partial Mitigation)

- Enable snappy/zstd compression in producer config
- Reduces message size by 70-90% but may not be enough for very large chunks

---

## Recommendation

**Use Solution A (Shared Filesystem) because:**
1. TP machines already share `/tmp/kafka_mode` via script deployment
2. Eliminates complexity of message chunking
3. Aligns with typical MapReduce architecture (Hadoop uses HDFS, not messaging for intermediate data)
4. Makes debugging easier (files are visible and inspectable)

---

## Implementation Steps for Solution A

### 1. Modify Message Schema
```rust
// OLD (removed)
pub struct MapPartition {
    pub entries: Vec<(String, u32)>,  // ❌ TOO BIG
}

// NEW (metadata-only)
pub struct MapPartitionMeta {
    pub job_id: String,
    pub map_id: usize,
    pub reduce_id: usize,
    pub file_path: String,           // ✅ /tmp/kafka_mode/map_outputs/map_{id}_reduce_{rid}.json
    pub entry_count: usize,          // For diagnostics
}

// Similar for ReduceTask
pub struct ReduceTaskMeta {
    pub job_id: String,
    pub reduce_id: usize,
    pub file_path: String,           // /tmp/kafka_mode/reduce_inputs/reduce_{id}.json
    pub entry_count: usize,
}
```

### 2. Worker Map Task Handler
```rust
async fn handle_map_task(..., task: MapTask) -> Result<()> {
    let mapped = map_chunk_from_file(&task.input_file, task.offset, task.chunk_size_bytes)?;
    let mut partitions = partition_map(mapped, task.reduce_count);
    
    for (reduce_id, entries) in partitions.drain(..).enumerate() {
        // Write to disk instead of sending via Kafka
        let output_path = format!(
            "/tmp/kafka_mode/map_outputs/map_{}_reduce_{}.json",
            task.map_id, reduce_id
        );
        let json = serde_json::to_string(&entries)?;
        tokio::fs::write(&output_path, json).await?;
        
        // Send only metadata
        let meta = MapPartitionMeta {
            job_id: task.job_id.clone(),
            map_id: task.map_id,
            reduce_id,
            file_path: output_path.clone(),
            entry_count: entries.len(),
        };
        send_json(&producer, &topics.map_results_meta, ..., &meta).await?;
    }
    
    // Send task completion ack
    let ack = TaskAck { ... };
    send_json(&producer, &topics.task_acks, ..., &ack).await?;
}
```

### 3. Coordinator Aggregation
```rust
while map_done.len() < computed_maps {
    if let Some((meta, msg)) = recv_json::<MapPartitionMeta>(...) {
        // Read from file instead of extracting from message
        let entries: Vec<(String, u32)> = tokio::fs::read_to_string(&meta.file_path)
            .await?
            .lines()
            .map(|line| { /* parse */ })
            .collect();
        
        let bucket = &mut reduce_input[meta.reduce_id];
        for (k, v) in entries {
            *bucket.entry(k).or_insert(0) += v;
        }
    }
}
```

### 4. Coordinator Reduce Task Publishing
```rust
for (reduce_id, bucket) in reduce_input.into_iter().enumerate() {
    let entries: Vec<(String, u32)> = bucket.into_iter().collect();
    
    // Write to disk
    let output_path = format!("/tmp/kafka_mode/reduce_inputs/reduce_{}.json", reduce_id);
    let json = serde_json::to_string(&entries)?;
    tokio::fs::write(&output_path, json).await?;
    
    // Send metadata only
    let meta = ReduceTaskMeta {
        job_id: run.job_id.clone(),
        reduce_id,
        file_path: output_path,
        entry_count: entries.len(),
    };
    send_json(&producer, &topics.reduce_tasks_meta, ..., &meta).await?;
}
```

### 5. Worker Reduce Task Handler
```rust
async fn handle_reduce_task(..., meta: ReduceTaskMeta) -> Result<()> {
    // Read from file
    let entries: Vec<(String, u32)> = tokio::fs::read_to_string(&meta.file_path)
        .await?
        .parse_json()?;
    
    let reduced = reduce_entries(entries);
    let output_entries = map_to_sorted_vec(reduced);
    
    // Write result
    let result_path = format!("/tmp/kafka_mode/reduce_outputs/reduce_{}.json", meta.reduce_id);
    let json = serde_json::to_string(&output_entries)?;
    tokio::fs::write(&result_path, json).await?;
    
    // Send metadata only
    let result_meta = ReduceResultMeta {
        job_id: ...,
        reduce_id: meta.reduce_id,
        file_path: result_path,
        entry_count: output_entries.len(),
    };
    send_json(&producer, &topics.reduce_results_meta, ..., &result_meta).await?;
}
```

---

## Verification Checklist

- [ ] All intermediate data flows through `/tmp/kafka_mode/` files, not Kafka messages
- [ ] Kafka messages contain only metadata (paths, counts, job_id)
- [ ] Message size stays well under 1MB
- [ ] `bash deploy.sh ... && bash status.sh` shows successful map completion
- [ ] `bash status.sh` shows files in `/tmp/kafka_mode/map_outputs/`, `reduce_inputs/`, `reduce_outputs/`
- [ ] Final results in `/tmp/kafka_mode/result/` match expected output

