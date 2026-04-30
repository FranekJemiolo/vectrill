# Getting Started

This guide will help you get started with Vectrill, from installation to your first streaming pipeline.

## Installation

### Prerequisites

- Rust 1.70+
- Python 3.12+
- maturin (for Python bindings)

### From Source

```bash
# Clone the repository
git clone https://github.com/FranekJemiolo/vectrill.git
cd vectrill

# Build the Rust library
cargo build --release

# Install Python package
pip install maturin
maturin develop
```

### With Optional Features

```bash
# Install with all features
cargo build --release --features python,cli,connectors-basic,web-ui

# Install with examples (marimo notebooks)
pip install -e ".[examples]"
```

## Quick Example

### Python API

```python
import vectrill as vt

# Create a streaming pipeline
stream = (
    vt.source("file", path="data.csv", format="csv")
    .filter("temperature > 20")
    .map("temp_f = temperature * 1.8 + 32")
    .execute()
)

# Process the stream
for batch in stream:
    print(batch)
```

### Rust API

```rust
use vectrill::sequencer::{Sequencer, SequencerConfig};
use arrow::record_batch::RecordBatch;

// Create a sequencer
let config = SequencerConfig::default();
let mut sequencer = Sequencer::new(config);

// Ingest data
sequencer.ingest(batch1)?;
sequencer.ingest(batch2)?;

// Get ordered results
for batch in sequencer.flush()? {
    println!("{:?}", batch);
}
```

## Running Tests

```bash
# Run all tests
cargo test

# Run e2e tests with docker-compose
./scripts/run_e2e_tests.sh

# Run specific test
cargo test test_sequencer_creation
```

## Running Benchmarks

```bash
# Run performance benchmarks
cargo bench --features performance

# Run specific benchmark
cargo bench --bench sequencer --features performance
```

## Web UI

Start the web dashboard:

```bash
cargo run --features web-ui
```

Then visit http://localhost:3000 to see the metrics dashboard.

## Next Steps

- Read the [Architecture Guide](architecture.md) to understand the system design
- Check out the [Examples](examples.md) for more usage patterns
- Explore the [Performance Guide](performance.md) for optimization tips
