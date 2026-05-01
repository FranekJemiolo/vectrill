---
layout: default
title: Examples
---

# Examples

This document contains practical examples of using Vectrill for various data processing tasks.Vectrill.

## Marimo Notebooks

Install with examples extras:
```bash
pip install -e ".[examples]"
```

### Getting Started

Run the getting started notebook:
```bash
marimo edit examples/getting_started.py
```

Topics covered:
- Installation
- Basic usage
- Core concepts (Sequencer, Micro-batching, Operators, Windows, Watermarks)

### Streaming

Run the streaming notebook:
```bash
marimo edit examples/streaming.py
```

Topics covered:
- Event sequencing
- Window operations (Tumbling, Sliding, Session)
- Micro-batching
- Watermarks and late data

### Advanced Features

Run the advanced notebook:
```bash
marimo edit examples/advanced.py
```

Topics covered:
- Expression optimization (Constant folding, CSE)
- Memory optimization (Buffer pooling)
- Performance counters
- Query planning and optimization
- Operator fusion

## Python Examples

### Basic Filtering

```python
import vectrill as vt

# Read from CSV and filter
stream = vt.source("file", path="data.csv", format="csv")
filtered = stream.filter("value > 100")

for batch in filtered.execute():
    print(batch)
```

### Aggregations

```python
import vectrill as vt

# Group and aggregate
stream = vt.source("file", path="data.csv", format="csv")
aggregated = (
    stream
    .group_by("category")
    .agg({"value": "avg", "count": "sum"})
)

for batch in aggregated.execute():
    print(batch)
```

### Window Operations

```python
import vectrill as vt

# Time window aggregation
stream = vt.source("file", path="events.csv", format="csv")
windowed = (
    stream
    .group_by("device_id")
    .window("10s")
    .agg({"temperature": "avg", "humidity": "max"})
)

for batch in windowed.execute():
    print(batch)
```

### Complex Pipeline

```python
import vectrill as vt

# Complex streaming pipeline
stream = vt.source("file", path="sensor_data.csv", format="csv")
pipeline = (
    stream
    .filter("temperature > 20")
    .map("temp_f = temperature * 1.8 + 32")
    .filter("temp_f < 100")
    .group_by("device_id", "location")
    .window("1m")
    .agg({
        "temp_f": ["avg", "max", "min"],
        "humidity": "avg",
        "pressure": "max"
    })
)

for batch in pipeline.execute():
    print(batch)
```

## Rust Examples

### Basic Sequencer

```rust
use vectrill::sequencer::{Sequencer, SequencerConfig};
use arrow::record_batch::RecordBatch;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = SequencerConfig::default();
    let mut sequencer = Sequencer::new(config);
    
    // Ingest batches
    sequencer.ingest(batch1)?;
    sequencer.ingest(batch2)?;
    
    // Get ordered results
    for batch in sequencer.flush()? {
        println!("{:?}", batch);
    }
    
    Ok(())
}
```

### Custom Operator

```rust
use vectrill::operators::{Operator, MapOperator};
use arrow::record_batch::RecordBatch;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create a custom map operator
    let mut op = MapOperator::new(|batch| {
        // Custom transformation logic
        Ok(batch.clone())
    });
    
    let result = op.process(input_batch)?;
    
    Ok(())
}
```

### Performance Monitoring

```rust
use vectrill::performance::{Counter, CounterType, CounterRegistry, Timer};

fn main() {
    let mut registry = CounterRegistry::new();
    let counter = registry.register("rows_processed".to_string(), CounterType::RowsProcessed);
    
    let timer = Timer::new(counter.clone());
    timer.start();
    
    // Do some work
    for _ in 0..1000 {
        counter.increment();
    }
    
    timer.stop();
    
    let snapshot = registry.snapshot();
    println!("Metrics: {:?}", snapshot);
}
```

## End-to-End Examples

### CSV Processing Pipeline

```python
import vectrill as vt
import polars as pl

# Read CSV, filter, transform, and aggregate
stream = (
    vt.source("file", path="sales.csv", format="csv")
    .filter("amount > 0")
    .map("amount_with_tax = amount * 1.1")
    .group_by("product_category")
    .agg({
        "amount_with_tax": "sum",
        "amount": "avg"
    })
)

results = []
for batch in stream.execute():
    results.append(batch)

# Convert to Polars DataFrame
df = pl.concat(results)
print(df)
```

### Real-time Sensor Data

```python
import vectrill as vt

# Process sensor data with time windows
stream = (
    vt.source("file", path="sensors.csv", format="csv")
    .filter("sensor_type == 'temperature'")
    .group_by("sensor_id")
    .window("30s")
    .agg({
        "value": ["avg", "max", "min", "std"],
        "quality": "avg"
    })
)

for batch in stream.execute():
    # Process each time window
    print(f"Window: {batch}")
```

### Multi-stage Pipeline

```python
import vectrill as vt

# Complex multi-stage pipeline
stream = vt.source("file", path="events.csv", format="csv")

# Stage 1: Filter and normalize
stage1 = (
    stream
    .filter("event_type in ['click', 'view']")
    .map("normalized_value = value / 1000.0")
)

# Stage 2: Time-based aggregation
stage2 = (
    stage1
    .group_by("user_id")
    .window("5m")
    .agg({
        "normalized_value": "sum",
        "event_type": "count"
    })
)

# Stage 3: Final filtering
stage3 = stage2.filter("normalized_value > 10")

for batch in stage3.execute():
    print(batch)
```

## Testing Examples

### Unit Test Example

```python
import pytest
import vectrill as vt

def test_filter_operator():
    stream = vt.source("file", path="test_data.csv", format="csv")
    filtered = stream.filter("value > 50")
    
    results = list(filtered.execute())
    assert len(results) > 0
    
    # Verify all values are > 50
    for batch in results:
        assert all(val > 50 for val in batch["value"])
```

### Integration Test Example

```python
import vectrill as vt
import polars as pl

def test_end_to_end_pipeline():
    # Create test data
    test_data = pl.DataFrame({
        "id": [1, 2, 3, 4, 5],
        "value": [10, 20, 30, 40, 50],
        "category": ["A", "B", "A", "B", "A"]
    })
    test_data.write_csv("test_input.csv")
    
    # Run pipeline
    stream = (
        vt.source("file", path="test_input.csv", format="csv")
        .filter("value > 20")
        .group_by("category")
        .agg({"value": "avg"})
    )
    
    results = list(stream.execute())
    assert len(results) > 0
```

## Performance Examples

### Optimized Pipeline

```python
import vectrill as vt

# Optimized pipeline with early filtering
stream = (
    vt.source("file", path="large_dataset.csv", format="csv")
    .filter("status == 'active'")  # Filter early to reduce data volume
    .map("processed = value * 2")
    .group_by("category")
    .agg({"processed": "sum"})
)

for batch in stream.execute():
    print(batch)
```

### Batch Size Tuning

```python
import vectrill as vt

# Configure batch size for performance
config = vt.Config(batch_size=10000)
stream = vt.source("file", path="data.csv", format="csv", config=config)

# Process in larger batches for better throughput
for batch in stream.execute():
    print(batch)
```
