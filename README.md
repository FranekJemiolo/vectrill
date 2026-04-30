# Vectrill

High-performance Arrow-native streaming engine with Python DSL and Rust execution core.

## Vision

Vectrill is a single-node streaming execution engine that combines:
- **Spark-like API** and query planning
- **Flink-like streaming semantics** (watermarks, windows)
- **Apache Arrow's** zero-copy columnar memory
- **Rust's** performance and memory safety
- **Python's** ergonomics and ecosystem

## Architecture

```
Python API (control plane)
          ↓
Logical Query / DAG Builder
          ↓
Physical Execution Plan
          ↓
Rust Streaming Runtime Engine
          ↓
Arrow-native operators (stateful + stateless)
          ↓
Micro-batched outputs
```

## Status

🚧 **Early Development** - This project is in active development.

### Completed Milestones
- ✅ **M0**: Core Engine Skeleton - Basic operator trait and pipeline
- ✅ **M1**: Sequencer + Micro-batching - Event ordering and watermarks
- ⏳ **M2**: Python Integration (FFI)
- ⏳ **M3**: Expression Engine
- ⏳ **M4**: Query Planner
- ⏳ **M5**: Operator Fusion
- ⏳ **M6**: Streaming Semantics
- ⏳ **M7**: Connectors + Real Workloads
- ⏳ **M8**: Performance + Advanced Features

See [docs/](docs/) for detailed milestone implementation plans.

## Installation

### From Source

```bash
# Clone the repository
git clone https://github.com/FranekJemiolo/vectrill.git
cd vectrill

# Build the Rust library
cargo build --release

# Install Python package (when M2 is complete)
maturin develop
```

## Usage (Planned)

```python
import vectrill as vt
import polars as pl

# Create a streaming pipeline
stream = (
    vt.source("kafka", topic="events")
    .filter("temperature > 20")
    .map("temp_f = temperature * 1.8 + 32")
    .group_by("device_id")
    .window("10s")
    .agg({"temp_f": "avg", "humidity": "max"})
)

# Execute the pipeline
for batch in stream.execute():
    print(batch)
```

## Development

### Prerequisites
- Rust 1.70+
- Python 3.12+
- uv (for Python dependency management)
- maturin (for Python bindings, when M2 is complete)

### Build

```bash
# Install Python dependencies
uv sync
uv sync --dev  # Include dev dependencies

# Check code
cargo check

# Run tests
cargo test

# Build with CLI
cargo build --features cli

# Build Python package (when M2 is complete)
maturin develop
```

### Project Structure

```
vectrill/
├── src/
│   ├── connectors/       # Data source connectors
│   ├── ingestion/       # Ingestion channels
│   ├── sequencer/       # Event sequencing
│   ├── batching/        # Micro-batch builder
│   ├── operators/       # Core operators
│   ├── planner/         # Query planner
│   ├── expressions/     # Expression engine
│   └── ffi/            # Python bindings
├── python/
│   └── vectrill/        # Python package
├── tests/
│   ├── python/          # Python tests
│   └── rust/            # Rust tests
└── docs/                # Documentation
```

## Performance Targets

- Throughput: > 1M rows/sec
- Latency: < 10ms (micro-batch)
- Memory overhead: ~1.2x input
- Copies: zero (except final batch)

## License

MIT OR Apache-2.0

## Contributing

Contributions are welcome! Please see [docs/PROJECT_PLAN.md](docs/PROJECT_PLAN.md) for the roadmap.

## Acknowledgments

Inspired by:
- Apache Spark (API and query planning)
- Apache Flink (streaming semantics)
- Apache Arrow (columnar memory)
- Polars (Python DataFrame interface)
