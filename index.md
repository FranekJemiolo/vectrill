---
layout: default
title: Home
---

# Vectrill

**High-performance Arrow-native streaming engine with Python DSL and Rust execution core**

## 🎯 Vision

Vectrill is a single-node streaming execution engine that combines the best of multiple systems:

- **Spark-like API** and query planning for familiar data processing
- **Flink-like streaming semantics** with watermarks, windows, and stateful operations
- **Apache Arrow's** zero-copy columnar memory for maximum efficiency
- **Rust's** performance and memory safety for the execution core
- **Python's** ergonomics and ecosystem for the control plane

## 🚀 Quick Start

### Installation

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

### Basic Usage

```python
import vectrill as vt

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

## 📚 Documentation

- [Getting Started](getting-started) - Quick start guide and basic usage
- [Architecture](architecture) - System architecture and design principles
- [API Reference](api) - Complete API documentation
- [Examples](examples) - Practical examples and use cases
- [Performance](performance) - Performance characteristics and benchmarks
- [Contributing](contributing) - How to contribute to the project

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

## 🤝 Contributing

Contributions are welcome! Please see [Contributing](contributing) for guidelines.

### Development Workflow
1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run tests: `cargo test`
5. Run e2e tests: `./scripts/run_e2e_tests.sh`
6. Submit a pull request

## 📄 License

MIT OR Apache-2.0

## 🙏 Acknowledgments

Inspired by and built upon:
- **Apache Spark** - API design and query planning concepts
- **Apache Flink** - Streaming semantics and window operations
- **Apache Arrow** - Columnar memory format and compute kernels
- **Polars** - Python DataFrame interface design
- **DataFusion** - Query execution engine architecture
