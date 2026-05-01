# Vectrill

<div align="center">

**High-performance Arrow-native streaming engine with Python DSL and Rust execution core**

[![CI/CD](https://github.com/FranekJemiolo/vectrill/workflows/CI/badge.svg)](https://github.com/FranekJemiolo/vectrill/actions)
[![GitHub Pages](https://github.com/FranekJemiolo/vectrill/workflows/pages/badge.svg)](https://FranekJemiolo.github.io/vectrill/)
[![License](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.70+-orange.svg)](https://www.rust-lang.org)
[![Python](https://img.shields.io/badge/python-3.12+-blue.svg)](https://www.python.org)

</div>

---

## 🎯 Vision

Vectrill is a single-node streaming execution engine that combines the best of multiple systems:

- **Spark-like API** and query planning for familiar data processing
- **Flink-like streaming semantics** with watermarks, windows, and stateful operations
- **Apache Arrow's** zero-copy columnar memory for maximum efficiency
- **Rust's** performance and memory safety for the execution core
- **Python's** ergonomics and ecosystem for the control plane

### Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                  Python API (Control Plane)                  │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│              Logical Query / DAG Builder                     │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│               Physical Execution Plan                        │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│              Rust Streaming Runtime Engine                   │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  Expression Optimization (Constant Folding, CSE)      │   │
│  │  Memory Optimization (Buffer Pooling)                 │   │
│  │  Performance Counters & Monitoring                    │   │
│  └──────────────────────────────────────────────────────┘   │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│           Arrow-Native Operators (Stateful + Stateless)      │
│  • Map, Filter, Aggregate, Join                           │
│  • Window Functions (Tumbling, Sliding, Session)            │
│  • Operator Fusion for Performance                        │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│                    Micro-batched Outputs                      │
└─────────────────────────────────────────────────────────────┘
```

---

## ✨ Features

### Core Engine
- **Expression Engine**: Full expression evaluation with arithmetic, comparison, boolean, and string operations
- **Query Planner**: Logical to physical plan conversion with optimization (projection elimination, predicate pushdown)
- **Operator Fusion**: Automatic fusion of compatible operators for better performance
- **Streaming Semantics**: Watermarks, windows (tumbling, sliding, session), and stateful processing

### Performance Optimizations
- **Constant Folding**: Pre-computes constant expressions at planning time
- **Common Subexpression Elimination (CSE)**: Avoids duplicate computation
- **Buffer Pooling**: Reuses Arrow arrays to reduce allocation overhead
- **Performance Counters**: Built-in metrics for rows processed, batch counts, and timing

### Connectors
- **File Connector**: CSV, JSON, and Parquet support
- **Memory Connector**: In-memory data source for testing
- **Extensible Design**: Easy to add new data sources (Kafka, PostgreSQL, etc.)

### Web UI
- **Real-time Metrics**: WebSocket-based streaming of execution metrics
- **Job Inspection**: View query plans and execution status
- **Dashboard**: Visual monitoring of streaming jobs

---

## 📊 Status

🎉 **All Core Milestones Complete!**

### Completed Milestones
- ✅ **M0**: Core Engine Skeleton - Basic operator trait and pipeline
- ✅ **M1**: Sequencer + Micro-batching - Event ordering and watermarks
- ✅ **M2**: Python Integration (FFI) - PyO3 bindings and Arrow C Data Interface
- ✅ **M3**: Expression Engine - Full expression evaluation and compilation
- ✅ **M4**: Query Planner - Logical to physical plan conversion
- ✅ **M5**: Operator Fusion - Automatic fusion of compatible operators
- ✅ **M6**: Streaming Semantics - Watermarks, windows, and state
- ✅ **M7**: Connectors - File connectors (CSV, JSON, Parquet)
- ✅ **M8**: Performance + Advanced Features - Expression optimization, buffer pooling, performance counters
- ✅ **M9**: Web UI - Real-time metrics and job inspection dashboard

### Test Coverage
- **114 tests** passing:
  - 89 library tests
  - 6 cross-reference tests
  - 10 e2e advanced features tests
  - 1 e2e connector test
  - 8 sequencer functional tests

See [docs/](docs/) for detailed milestone implementation plans.

---

## 🚀 Installation

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

### With Python Extras

```bash
# Install with marimo notebooks (optional)
pip install -e ".[examples]"
```

---

## 💻 Usage

### Python API

```python
import vectrill as vt
import polars as pl

# Create a streaming pipeline
stream = (
    vt.source("file", path="data.csv", format="csv")
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

### Rust API

```rust
use vectrill::{Sequencer, MicroBatch};
use arrow::record_batch::RecordBatch;

// Create a sequencer
let sequencer = Sequencer::new(config);

// Ingest micro-batches
sequencer.ingest(batch1)?;
sequencer.ingest(batch2)?;

// Get ordered results
for batch in sequencer.flush()? {
    println!("Batch: {:?}", batch);
}
```

---

## 🧪 Development

### Prerequisites
- Rust 1.70+
- Python 3.12+
- uv (for Python dependency management)
- maturin (for Python bindings)
- Docker (for e2e tests with docker-compose)

### Build

```bash
# Install Python dependencies
uv sync
uv sync --dev  # Include dev dependencies

# Check code
cargo check

# Run tests
cargo test

# Run e2e tests with docker-compose
./scripts/run_e2e_tests.sh

# Build with all features
cargo build --features connectors-full,web-ui,cli

# Build Python package
maturin develop
```

### Running Benchmarks

```bash
# Run performance benchmarks
cargo bench --features performance

# Run specific benchmark
cargo bench --bench sequencer --features performance
```

---

## 📁 Project Structure

```
vectrill/
├── src/
│   ├── connectors/       # Data source connectors (CSV, JSON, Parquet)
│   ├── expression/      # Expression engine and compiler
│   ├── operators/       # Core operators (Map, Filter, Aggregate)
│   ├── optimization/    # Query optimization (fusion, expression optimizer)
│   ├── planner/         # Query planner (logical, physical)
│   ├── sequencer/       # Event sequencing and micro-batching
│   ├── streaming/       # Streaming semantics (watermarks, windows, state)
│   ├── memory/          # Memory optimization (buffer pooling)
│   ├── metrics/         # Metrics collection and registry
│   ├── performance/     # Performance counters and monitoring
│   └── web/             # Web UI (dashboard, WebSocket, REST API)
├── python/
│   └── vectrill/        # Python package
├── tests/
│   ├── python/          # Python tests
│   ├── e2e/             # End-to-end tests
│   ├── cross_reference/  # Cross-reference tests with Arrow
│   └── sequencer_functional/  # Functional tests
├── examples/            # Marimo notebooks (optional extras)
├── benches/             # Performance benchmarks
├── docs/                # Documentation
└── scripts/             # Utility scripts (e2e test runner)
```

---

## ⚡ Performance

### Benchmark Results (MacBook Air M2, 16GB RAM)

**Realistic Data Processing Performance:**
- **Filter Operations**: ~500K rows/sec (value > 500 predicate)
- **Map Operations**: ~750K rows/sec (value * 2 + 10 arithmetic)
- **Sequencer Operations**: ~50K rows/sec (with ordering overhead)

**Performance Characteristics:**
- **Linear Scaling**: Consistent performance across data sizes (1K to 100K rows)
- **Real Workloads**: Actual filter predicates and arithmetic expressions
- **Realistic Data**: Deterministic patterns with proper data distributions
- **Sequencer Overhead**: ~10x slower due to ordering and state management

**Comparison with Function Call Overhead:**
- **Real Processing**: 2.4-177µs per operation vs 16-43ns function calls
- **Performance Difference**: ~100-4000x slower with actual data processing
- **Realistic Throughput**: 500K-750K rows/sec vs 60M batches/sec (no work)

**What We Can Now Conclude:**
- **Actual Processing Throughput**: ~500K rows/sec for core operations
- **Real-world Performance**: Measured with realistic predicates and expressions
- **Sequencer Performance**: ~50K rows/sec with ordering guarantees
- **Scalability**: Linear scaling confirmed with real workloads

### Performance Features
- **Expression Optimization**: Constant folding and CSE reduce computation overhead
- **Buffer Pooling**: Reuses Arrow arrays to reduce allocation overhead
- **Operator Fusion**: Combines multiple operators into single pass
- **Performance Counters**: Built-in metrics for monitoring

### Benchmarks
Run realistic data processing benchmarks:
```bash
# Run realistic benchmarks with actual workloads
cargo bench --features performance --bench realistic

# Run all benchmarks (including function call overhead)
cargo bench --features performance
```

**Detailed performance analysis available in [docs/BENCHMARKS.md](docs/BENCHMARKS.md)** with both micro-benchmarks and realistic workloads.

---

## 📚 Documentation

Full documentation is available at [https://FranekJemiolo.github.io/vectrill/](https://FranekJemiolo.github.io/vectrill/)

- [Getting Started](https://FranekJemiolo.github.io/vectrill/getting-started)
- [Architecture](https://FranekJemiolo.github.io/vectrill/architecture)
- [API Reference](https://FranekJemiolo.github.io/vectrill/api)
- [Examples](https://FranekJemiolo.github.io/vectrill/examples)
- [Performance](https://FranekJemiolo.github.io/vectrill/performance)

---

## 📖 Examples

### Marimo Notebooks

Install with examples extras:
```bash
pip install -e ".[examples]"
```

Available notebooks:
- `examples/getting_started.ipynb` - Basic usage and API overview
- `examples/streaming.ipynb` - Streaming data processing
- `examples/advanced.ipynb` - Advanced features and optimizations

---

## 🔧 Configuration

### Features

- `default`: No features enabled
- `python`: Python bindings (PyO3)
- `cli`: Command-line interface
- `connectors-basic`: CSV, JSON, Parquet connectors
- `connectors-full`: All connectors including Kafka
- `web-ui`: Web dashboard and metrics
- `performance`: Benchmarking tools

Build with features:
```bash
cargo build --features python,cli,connectors-basic,web-ui
```

---

## 🤝 Contributing

Contributions are welcome! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

### Development Workflow
1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run tests: `cargo test`
5. Run e2e tests: `./scripts/run_e2e_tests.sh`
6. Submit a pull request

---

## 📄 License

MIT OR Apache-2.0

---

## 🙏 Acknowledgments

Inspired by and built upon:
- **Apache Spark** - API design and query planning concepts
- **Apache Flink** - Streaming semantics and window operations
- **Apache Arrow** - Columnar memory format and compute kernels
- **Polars** - Python DataFrame interface design
- **DataFusion** - Query execution engine architecture
