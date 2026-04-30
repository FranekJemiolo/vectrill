# M7: Connectors + Real Workloads

## Goal
Make the engine usable in real scenarios with practical data sources.

## Duration
4-7 days

## Deliverables
- File connector
- Stdin connector
- Optional Kafka connector
- Streaming ingestion with backpressure
- Real-world pipeline examples

## Tasks

### 1. File Connector
- [ ] Implement `FileConnector` for CSV files
- [ ] Implement `FileConnector` for JSON files
- [ ] Implement `FileConnector` for Parquet files
- [ ] Add schema inference
- [ ] Add chunked reading (streaming)
- [ ] Handle file rotation (log files)
- [ ] Add file watching (optional)

### 2. Stdin Connector
- [ ] Implement `StdinConnector`
- [ ] Support JSON lines format
- [ ] Support CSV format
- [ ] Add newline delimiter handling
- [ ] Handle schema from first row
- [ ] Add buffering for performance

### 3. Memory Connector (Enhanced)
- [ ] Enhance existing `MemoryConnector`
- [ ] Add configurable data generation
- [ ] Support timestamp patterns
- [ ] Add replay mode
- [ ] Add rate limiting

### 4. Kafka Connector (Optional)
- [ ] Implement `KafkaConnector`
- [ ] Add topic subscription
- [ ] Add consumer group support
- [ ] Handle offset management
- [ ] Add schema registry integration (optional)
- [ ] Implement backpressure handling

### 5. S3 Connector (Optional)
- [ ] Implement `S3Connector`
- [ ] Add bucket listing
- [ ] Support S3 Select (pushdown)
- [ ] Handle multipart downloads
- [ ] Add credential management

### 6. Backpressure System
- [ ] Implement channel capacity limits
- [ ] Add flow control signals
- [ ] Implement pause/resume for connectors
- [ ] Add buffer size monitoring
- [ ] Add backpressure metrics

### 7. Connector Registry
- [ ] Create connector factory
- [ ] Add connector discovery
- [ ] Implement configuration loading
- [ ] Add connector validation
- [ ] Support dynamic connector loading

### 8. Error Handling
- [ ] Implement retry logic for transient errors
- [ ] Add dead letter queue for failed events
- [ ] Implement circuit breaker pattern
- [ ] Add error logging and metrics
- [ ] Handle schema mismatches

### 9. Integration Tests
- [ ] Test file connector with real files
- [ ] Test stdin connector with piped data
- [ ] Test memory connector with patterns
- [ ] Test backpressure under load
- [ ] Test error recovery
- [ ] End-to-end pipeline with real data

### 10. Documentation
- [ ] Document connector API
- [ ] Add connector configuration examples
- [ ] Create real-world pipeline examples
- [ ] Add performance tuning guide
- [ ] Document error handling strategies

## Implementation Details

### File Connector
```rust
pub struct FileConnector {
    path: PathBuf,
    format: FileFormat,
    reader: Option<Box<dyn BatchReader>>,
    schema: SchemaRef,
}

pub enum FileFormat {
    Csv,
    Json,
    Parquet,
}

#[async_trait]
impl Connector for FileConnector {
    async fn next_batch(&mut self) -> Option<RecordBatch> {
        self.reader.as_mut()?.next_batch().await
    }
    
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
```

### Stdin Connector
```rust
pub struct StdinConnector {
    reader: BufReader<Stdin>,
    format: StdinFormat,
    schema: SchemaRef,
}

pub enum StdinFormat {
    JsonLines,
    Csv,
}

#[async_trait]
impl Connector for StdinConnector {
    async fn next_batch(&mut self) -> Option<RecordBatch> {
        // Read lines, parse, convert to Arrow
    }
}
```

### Kafka Connector
```rust
pub struct KafkaConnector {
    consumer: StreamConsumer,
    topic: String,
    schema: SchemaRef,
    buffer: Vec<RecordBatch>,
}

#[async_trait]
impl Connector for KafkaConnector {
    async fn next_batch(&mut self) -> Option<RecordBatch> {
        // Poll messages, deserialize, convert to Arrow
    }
}
```

### Backpressure
```rust
pub struct BoundedChannel<T> {
    sender: mpsc::Sender<T>,
    capacity: usize,
}

impl<T> BoundedChannel<T> {
    pub async fn send(&mut self, item: T) -> Result<()> {
        if self.capacity > 0 {
            self.sender.reserve().await?;
        }
        self.sender.send(item).await
    }
}
```

### Connector Registry
```rust
pub struct ConnectorRegistry {
    connectors: HashMap<String, Box<dyn ConnectorFactory>>,
}

pub trait ConnectorFactory {
    fn create(&self, config: &ConnectorConfig) -> Result<Box<dyn Connector>>;
}
```

## Success Criteria
- [ ] File connector reads CSV/JSON/Parquet
- [ ] Stdin connector handles piped data
- [ ] Memory connector generates test data
- [ ] Kafka connector (if implemented) consumes messages
- [ ] Backpressure prevents memory overflow
- [ ] Errors handled gracefully with retries
- [ ] Real-world pipelines run successfully
- [ ] All integration tests pass

## Performance Targets
- File connector: > 10M rows/sec (Parquet)
- Stdin connector: > 1M rows/sec
- Kafka connector: > 100k msg/sec
- Backpressure response: < 10ms

## Example Pipelines

### Log File Processing
```python
stream = (
    source("file", path="logs/*.json")
    .filter("level == 'ERROR'")
    .window("1m")
    .agg({"count": "count"})
)
```

### Kafka Stream
```python
stream = (
    source("kafka", topic="events")
    .map("extract_fields(payload)")
    .group_by("user_id")
    .window("5m")
    .agg({"actions": "count"})
)
```

### Stdin Processing
```bash
cat data.json | python -c "
import vectrill as vt
import polars as pl

seq = vt.Sequencer()
seq.ingest_from_stdin('json')
while batch := seq.next_batch():
    print(batch)
"
```

## Dependencies
- `tokio` with full features
- `csv` crate (for CSV)
- `json` crate (for JSON)
- `parquet` crate (for Parquet)
- `rdkafka` (for Kafka, optional)
- `rusoto_s3` (for S3, optional)

## Critical Design Rules
1. All connectors emit Arrow RecordBatch
2. Connectors must support backpressure
3. Schema must be known or inferred
4. Connectors should be async and non-blocking
5. Error handling must not crash the pipeline
6. Zero-copy where possible (especially for Parquet)

## Future Extensions (Beyond M7)
- Database connectors (PostgreSQL, MySQL)
- WebSocket connector
- HTTP webhook connector
- Custom connector plugin system
