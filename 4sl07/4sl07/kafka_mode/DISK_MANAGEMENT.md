# Disk Space Management Strategy for Solution A

## Problem
With 64 map tasks and 8 reduce partitions:
- 512 intermediate map output files
- Each potentially 100KB-1MB
- Total could easily exceed available `/tmp` on TP machines

## Solution: Streaming Aggregation with Eager Cleanup

### Key Principle
**Do not wait for all maps to complete before reducing and cleaning.**

Instead, process reduce partitions independently and immediately reclaim space.

---

## Architecture: Streaming Reduce with Space Reclamation

```
MAP PHASE (Parallel):
  Map 0 → {reduce_0, reduce_1, ... reduce_7} → /tmp/map_outputs/
  Map 1 → {reduce_0, reduce_1, ... reduce_7} → /tmp/map_outputs/
  ...

COORDINATOR (Streaming Aggregation):
  for each reduce_id in 0..reduce_count:
      Wait for all maps to send their reduce_id partition
      {
        Read: /tmp/map_outputs/map_{0..63}_reduce_{reduce_id}.json
        Aggregate in memory
        Write: /tmp/reduce_inputs/reduce_{reduce_id}.json
        DELETE all /tmp/map_outputs/map_*_reduce_{reduce_id}.json ← CLEANUP!
        Publish: ReduceTaskMeta(reduce_id)
      }

REDUCE PHASE (Parallel):
  Reduce 0 → reads /tmp/reduce_inputs/reduce_0.json
           → processes
           → writes /tmp/reduce_outputs/reduce_0.json
           → DELETE /tmp/reduce_inputs/reduce_0.json ← CLEANUP!
           → sends ReduceResultMeta
  
FINAL PHASE:
  Read all /tmp/reduce_outputs/reduce_*.json
  Merge and finalize
  DELETE /tmp/reduce_outputs/ ← CLEANUP!
```

### Memory Profile
- **Without cleanup**: $O(num_maps * num_reduces * avg_partition_size)$
- **With cleanup**: $O(num_reduces * avg_partition_size)$ (one reduce partition at a time)

For 64 maps × 8 reduces × 1MB avg = 512MB peak **→ 8MB peak per partition**

---

## Disk Space Timeline

```
Timeline (with 64 maps, 8 reduces, 4MB chunks):

t0:    [Map 0-63 start writing outputs]
       /tmp/map_outputs/: growing (up to 512MB)

t1:    [First reduce partition ready: reduce_0 has all 64 map inputs]
       - Read map_*_reduce_0 files (64 files)
       - Aggregate (in memory, small)
       - Write reduce_inputs/reduce_0.json
       - DELETE all map_*_reduce_0.json ← Free 64MB!
       - /tmp/map_outputs/: now 448MB (512-64)

t2:    [Second reduce partition ready]
       - Same process → Free another 64MB
       - /tmp/map_outputs/: now 384MB

...

t8:    [All reduce inputs generated, map_outputs/ is empty]
       - /tmp/reduce_inputs/: ~8MB (8 files of ~1MB each)
       - /tmp/map_outputs/: 0B (all cleaned)

t9:    [Reduce workers process reduce_0]
       - Read reduce_inputs/reduce_0.json
       - Process
       - Write reduce_outputs/reduce_0.json
       - DELETE reduce_inputs/reduce_0.json ← Free 1MB!

...

t16:   [All reduce done, reduce_inputs/ is empty]
       - /tmp/reduce_outputs/: ~8MB
       - All intermediate files cleaned
       
t17:   [Finalize and move to /tmp/result/]
       - Final results in result/
       - Everything else cleaned
```

---

## Implementation Details

### 1. Coordinator: Parallel but Sequential per Reduce ID

```rust
pub async fn run_coordinator(run: RunConfig) -> Result<()> {
    // ... setup topics ...
    
    let mut map_outputs: Vec<Vec<String>> = vec![vec![]; run.reduce_count];
    // map_outputs[reduce_id] = vec of file paths for that reduce_id
    
    let mut reduce_ready: Vec<bool> = vec![false; run.reduce_count];
    let mut maps_completed: usize = 0;
    
    // Phase 1: Collect map outputs and process reduce partitions as they become ready
    while maps_completed < computed_maps {
        if let Some((meta, msg)) = recv_json::<MapPartitionMeta>(&map_results_consumer).await? {
            if meta.reduce_id < run.reduce_count {
                map_outputs[meta.reduce_id].push(meta.file_path.clone());
                
                // Check if this reduce partition is complete
                if map_outputs[meta.reduce_id].len() == computed_maps && !reduce_ready[meta.reduce_id] {
                    // Process and clean up this reduce partition
                    process_and_cleanup_reduce_partition(
                        meta.reduce_id,
                        &map_outputs[meta.reduce_id],
                        &producer,
                        &topics,
                        &run,
                    ).await?;
                    reduce_ready[meta.reduce_id] = true;
                }
            }
            commit_message(&map_results_consumer, &msg)?;
            maps_completed += 1;
        }
    }
    
    // ... rest of coordinator ...
}

async fn process_and_cleanup_reduce_partition(
    reduce_id: usize,
    map_files: &[String],
    producer: &rdkafka::producer::FutureProducer,
    topics: &TopicNames,
    run: &RunConfig,
) -> Result<()> {
    let mut aggregated: FxHashMap<String, u32> = FxHashMap::default();
    
    // Read all map outputs for this reduce_id
    for file_path in map_files {
        let content = tokio::fs::read_to_string(file_path).await?;
        let entries: Vec<(String, u32)> = serde_json::from_str(&content)?;
        for (k, v) in entries {
            *aggregated.entry(k).or_insert(0) += v;
        }
        // Delete immediately after reading
        tokio::fs::remove_file(file_path).await?;
    }
    
    // Write aggregated input for reduce
    let reduce_input_path = format!("/tmp/kafka_mode/reduce_inputs/reduce_{}.json", reduce_id);
    let json = serde_json::to_string(&aggregated)?;
    tokio::fs::write(&reduce_input_path, json).await?;
    
    // Send reduce task
    let meta = ReduceTaskMeta {
        job_id: run.job_id.clone(),
        reduce_id,
        file_path: reduce_input_path,
        entry_count: aggregated.len(),
    };
    send_json(producer, &topics.reduce_tasks_meta, &format!("reduce-{}", reduce_id), &meta).await?;
    
    println!("Processed reduce partition {}: aggregated to {} unique entries", reduce_id, aggregated.len());
    Ok(())
}
```

### 2. Worker: Clean Up After Each Task

```rust
async fn handle_map_task(..., task: MapTask) -> Result<()> {
    let mapped = map_chunk_from_file(&task.input_file, task.offset, task.chunk_size_bytes)?;
    let mut partitions = partition_map(mapped, task.reduce_count);
    
    for (reduce_id, entries) in partitions.drain(..).enumerate() {
        let output_path = format!(
            "/tmp/kafka_mode/map_outputs/map_{}_reduce_{}.json",
            task.map_id, reduce_id
        );
        let json = serde_json::to_string(&entries)?;
        tokio::fs::write(&output_path, json).await?;
        
        let meta = MapPartitionMeta { ... };
        send_json(&producer, &topics.map_results_meta, ..., &meta).await?;
    }
    
    // Ack task
    let ack = TaskAck { ... };
    send_json(&producer, &topics.task_acks, ..., &ack).await?;
    
    println!("Map task {} completed", task.map_id);
    Ok(())
}

async fn handle_reduce_task(..., meta: ReduceTaskMeta) -> Result<()> {
    let entries: Vec<(String, u32)> = tokio::fs::read_to_string(&meta.file_path)
        .await?
        .lines()
        .map(|line| serde_json::from_str(line).unwrap_or_default())
        .collect();
    
    let reduced = reduce_entries(entries);
    let output_entries = map_to_sorted_vec(reduced);
    
    let result_path = format!("/tmp/kafka_mode/reduce_outputs/reduce_{}.json", meta.reduce_id);
    let json = serde_json::to_string(&output_entries)?;
    tokio::fs::write(&result_path, json).await?;
    
    // DELETE reduce input file now that we're done with it
    tokio::fs::remove_file(&meta.file_path).await.ok();  // Ignore error if already deleted
    
    let result_meta = ReduceResultMeta { ... };
    send_json(&producer, &topics.reduce_results_meta, ..., &result_meta).await?;
    
    println!("Reduce task {} completed", meta.reduce_id);
    Ok(())
}
```

---

## Monitoring & Diagnostics

Add to `status.sh`:
```bash
echo "Disk usage in /tmp/kafka_mode:"
ssh "$USER_NAME@$COORDINATOR_HOST" "du -sh /tmp/kafka_mode/*"
echo "File count by type:"
ssh "$USER_NAME@$COORDINATOR_HOST" "echo 'map_outputs:' && find /tmp/kafka_mode/map_outputs -type f 2>/dev/null | wc -l && echo 'reduce_inputs:' && find /tmp/kafka_mode/reduce_inputs -type f 2>/dev/null | wc -l && echo 'reduce_outputs:' && find /tmp/kafka_mode/reduce_outputs -type f 2>/dev/null | wc -l"
```

---

## Safeguards

1. **Directory pre-creation**: Ensure `/tmp/kafka_mode/{map_outputs,reduce_inputs,reduce_outputs,result}/` exist before workers start
2. **Cleanup on exit**: `cleanup.sh` removes entire `/tmp/kafka_mode/` tree
3. **Idempotent deletes**: Use `.ok()` on `remove_file()` to ignore if already deleted
4. **Emergency cleanup**: If a worker/coordinator crashes, manually: `rm -rf /tmp/kafka_mode/`

