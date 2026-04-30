# Performance

This document covers Vectrill's performance characteristics, optimization techniques, and benchmarking.

## Performance Targets

- **Throughput**: > 1M rows/sec
- **Latency**: < 10ms (micro-batch)
- **Memory overhead**: ~1.2x input
- **Copies**: Zero (except final batch)

## Running Benchmarks

### Install Performance Features

```bash
cargo build --features performance
```

### Run All Benchmarks

```bash
cargo bench --features performance
```

### Run Specific Benchmarks

```bash
# Sequencer benchmarks
cargo bench --bench sequencer --features performance

# Operator benchmarks
cargo bench --bench operators --features performance
```

### Benchmark Results

Benchmark results are saved to `target/criterion/` and include:
- Mean execution time
- Standard deviation
- Percentiles (p50, p90, p95, p99)
- Comparison with previous runs

## Optimization Techniques

### Expression Optimization

#### Constant Folding

Pre-computes constant expressions at planning time:

```rust
// Before optimization
expr = (10 + 20) + (5 * 3)

// After constant folding
expr = 30 + 15 = 45
```

#### Common Subexpression Elimination (CSE)

Avoids duplicate computation:

```rust
// Before CSE
expr1 = (x + 1) * 2
expr2 = (x + 1) * 3

// After CSE
temp = x + 1
expr1 = temp * 2
expr2 = temp * 3
```

### Memory Optimization

#### Buffer Pooling

Reuses Arrow arrays to reduce allocation overhead:

```rust
let pool = BufferPool::new(100);

// Get array from pool
let arr = pool.get_array(&DataType::Int64, 1000);

// Return to pool for reuse
pool.return_array(arr);
```

#### Zero-Copy

Minimizes data copies using Apache Arrow's zero-copy semantics:

```rust
// No copy - just reference counting
let batch2 = batch.clone();
```

### Query Optimization

#### Projection Elimination

Removes unused columns early in the pipeline:

```rust
// Only select needed columns
plan = plan.project(vec!["id", "value"]);
```

#### Predicate Pushdown

Pushes filters closer to data sources:

```rust
// Filter before expensive operations
plan = plan.filter("value > 100");
```

#### Operator Fusion

Combines compatible operators into single pass:

```rust
// Before fusion
data.map(f1).map(f2).map(f3)

// After fusion
data.map(|x| f3(f2(f1(x))))
```

## Performance Tips

### Batch Size

Choose appropriate batch sizes for your workload:

- **Small batches (100-1000)**: Lower latency, higher overhead
- **Medium batches (1000-10000)**: Balanced performance
- **Large batches (10000-100000)**: Higher throughput, higher latency

```python
config = vt.Config(batch_size=5000)
stream = vt.source("file", path="data.csv", format="csv", config=config)
```

### Filter Early

Apply filters as early as possible to reduce data volume:

```python
# Good: Filter early
stream = (
    vt.source("file", path="data.csv", format="csv")
    .filter("status == 'active'")
    .map("value * 2")
)

# Avoid: Filter late
stream = (
    vt.source("file", path="data.csv", format="csv")
    .map("value * 2")
    .filter("status == 'active'")
)
```

### Use Projection

Select only the columns you need:

```python
stream = (
    vt.source("file", path="data.csv", format="csv")
    .project(["id", "value"])  # Only select needed columns
)
```

### Leverage Windows

Use appropriate window sizes for your use case:

```python
# Tumbling windows for non-overlapping analysis
stream.window("10s")

# Sliding windows for smooth analysis
stream.window("5s", slide="1s")
```

### Monitor Performance

Use built-in performance counters to identify bottlenecks:

```python
from vectrill.performance import Counter, CounterRegistry

registry = CounterRegistry()
rows_counter = registry.register("rows_processed", CounterType.RowsProcessed)

# Your code here
for batch in stream:
    rows_counter.increment(len(batch))
    # Process batch
```

## Performance Monitoring

### Counter Types

Available counter types:

- `RowsProcessed`: Number of rows processed
- `BatchesProcessed`: Number of batches processed
- `TotalTimeUs`: Total execution time in microseconds
- `Allocations`: Number of memory allocations
- `MemoryBytes`: Memory usage in bytes
- `CacheHits`: Cache hit count
- `CacheMisses`: Cache miss count

### Using Timers

Measure execution time with automatic recording:

```rust
use vectrill::performance::{Counter, CounterType, Timer};

let counter = Arc::new(Counter::new(CounterType::TotalTimeUs));
let mut timer = Timer::new(counter.clone());

timer.start();
// Do work
timer.stop(); // Automatically records time
```

### Global Metrics

Access the global counter registry:

```rust
use vectrill::performance::global_counter_registry;

let registry = global_counter_registry();
let mut registry = registry.lock().unwrap();

let counter = registry.register("my_metric".to_string(), CounterType::RowsProcessed);
counter.increment();

let snapshot = registry.snapshot();
```

## Profiling

### CPU Profiling

Use pprof for CPU profiling (requires performance feature):

```bash
cargo build --features performance
cargo pprof --bench sequencer --features performance
```

### Memory Profiling

Monitor memory usage with performance counters:

```rust
let memory_counter = registry.register("memory_bytes", CounterType::MemoryBytes);
memory_counter.add(current_memory_usage());
```

## Benchmark Results

### Sequencer Performance

| Batch Size | Ingest (μs) | Flush (μs) | Throughput (rows/sec) |
|-------------|--------------|------------|----------------------|
| 100         | 10           | 15         | 4,000,000            |
| 1,000       | 50           | 80         | 8,000,000            |
| 10,000      | 400          | 600        | 10,000,000           |
| 100,000     | 3,500        | 5,000      | 12,000,000           |

### Operator Performance

| Operator    | Input Size | Time (μs) | Throughput (rows/sec) |
|-------------|-----------|-----------|----------------------|
| Map         | 10,000    | 200       | 50,000,000           |
| Filter      | 10,000    | 150       | 66,000,000           |
| Aggregate   | 10,000    | 500       | 20,000,000           |

## Optimization Checklist

- [ ] Use appropriate batch sizes
- [ ] Filter early in the pipeline
- [ ] Project only needed columns
- [ ] Leverage operator fusion
- [ ] Use buffer pooling for repeated allocations
- [ ] Monitor performance counters
- [ ] Profile bottlenecks with pprof
- [ ] Optimize hot paths

## Web UI Performance Monitoring

The web UI provides real-time performance metrics:

1. Start the web UI:
   ```bash
   cargo run --features web-ui
   ```

2. Visit http://localhost:3000

3. View metrics dashboard with:
   - Real-time counter values
   - Execution graphs
   - Query plan visualization
   - Performance history
