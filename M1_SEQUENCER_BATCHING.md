# M1: Sequencer + Micro-batching

## Goal
Build a high-performance event sequencing engine with multi-source ingestion and ordered micro-batch output.

## Duration
2-4 days

## Deliverables
- Multi-source ingestion (mock connectors)
- Heap-based ordering
- Micro-batch output
- Timestamp ordering
- Arrow-native memory

## Tasks

### 1. Connector Interface
- [ ] Define `Connector` trait
- [ ] Implement `MemoryConnector` (test generator)
- [ ] Implement `FileConnector` (basic file reader)
- [ ] Add connector configuration

### 2. Ingestion Layer
- [ ] Create async channel for batch transport
- [ ] Implement backpressure handling
- [ ] Add connector task spawning
- [ ] Implement batch size limits

### 3. Sequencer Core
- [ ] Define `SequencerConfig` struct
- [ ] Implement ordering modes (ByTimestamp, ByKeyThenTimestamp)
- [ ] Implement heap-based k-way merge
- [ ] Add cursor-based batch tracking
- [ ] Implement watermark calculation

### 4. Batch Builder
- [ ] Implement batch accumulation
- [ ] Add Arrow take() kernel usage
- [ ] Implement flush conditions (size, time, watermark)
- [ ] Add batch size configuration

### 5. Time Semantics
- [ ] Implement event-time handling
- [ ] Add watermark tracking
- [ ] Implement late data policy (Drop, Allow, SideOutput)
- [ ] Add max_lateness configuration

### 6. Tests
- [ ] Unit test for ordering correctness
- [ ] Unit test for watermark behavior
- [ ] Unit test for late data handling
- [ ] Property tests for invariants (sorted, no duplicates, no missing rows)
- [ ] Benchmark with synthetic Arrow batches

## Implementation Details

### Connector Trait
```rust
#[async_trait]
pub trait Connector: Send + Sync {
    async fn next_batch(&mut self) -> Option<RecordBatch>;
    fn schema(&self) -> SchemaRef;
    fn name(&self) -> &str;
}
```

### Sequencer Config
```rust
pub struct SequencerConfig {
    pub ordering: Ordering,
    pub max_lateness_ms: i64,
    pub batch_size: usize,
    pub flush_interval_ms: u64,
}
```

### Heap-based Merge
```rust
struct Cursor {
    batch: Arc<RecordBatch>,
    index: usize,
    len: usize,
}

struct HeapItem {
    timestamp: i64,
    cursor_id: usize,
}
```

### Watermark Calculation
```rust
watermark = min(connector_max_ts) - max_lateness
```

## Success Criteria
- [ ] Multiple connectors ingest simultaneously
- [ ] Events are ordered correctly by timestamp
- [ ] Micro-batches emit on configured conditions
- [ ] Late data handled according to policy
- [ ] Zero-copy Arrow memory throughout
- [ ] All tests pass
- [ ] Benchmarks show > 100k rows/sec

## Performance Targets
- Throughput: > 100k rows/sec (M1 baseline)
- Latency: < 50ms per micro-batch
- Memory overhead: < 1.5x input

## Dependencies
- `arrow` >= 51
- `tokio` with full features
- `async-trait`
- `priority-queue` or `binary-heap`
