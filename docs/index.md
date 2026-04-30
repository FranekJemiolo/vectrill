# Vectrill Documentation

<div align="center">

**High-performance Arrow-native streaming engine with Python DSL and Rust execution core**

[![CI/CD](https://github.com/FranekJemiolo/vectrill/workflows/CI/badge.svg)](https://github.com/FranekJemiolo/vectrill/actions)
[![License](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](LICENSE)

</div>

---

## Welcome to Vectrill

Vectrill is a single-node streaming execution engine that combines the best of multiple systems:

- **Spark-like API** and query planning for familiar data processing
- **Flink-like streaming semantics** with watermarks, windows, and stateful operations
- **Apache Arrow's** zero-copy columnar memory for maximum efficiency
- **Rust's** performance and memory safety for the execution core
- **Python's** ergonomics and ecosystem for the control plane

## Quick Start

### Installation

```bash
git clone https://github.com/FranekJemiolo/vectrill.git
cd vectrill
cargo build --release
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

## Documentation

- [Getting Started](getting-started.md) - Installation and basic usage
- [Architecture](architecture.md) - System architecture and design
- [API Reference](api.md) - Complete API documentation
- [Examples](examples.md) - Example notebooks and code
- [Performance](performance.md) - Benchmarks and optimization tips
- [Contributing](contributing.md) - How to contribute

## Project Status

🎉 **All Core Milestones Complete!**

- ✅ M0: Core Engine Skeleton
- ✅ M1: Sequencer + Micro-batching
- ✅ M2: Python Integration (FFI)
- ✅ M3: Expression Engine
- ✅ M4: Query Planner
- ✅ M5: Operator Fusion
- ✅ M6: Streaming Semantics
- ✅ M7: Connectors
- ✅ M8: Performance + Advanced Features
- ✅ M9: Web UI

## Features

### Core Engine
- Expression engine with full evaluation support
- Query planner with optimization
- Operator fusion for performance
- Streaming semantics with watermarks and windows

### Performance
- Constant folding and CSE
- Buffer pooling for memory optimization
- Performance counters and monitoring
- Zero-copy Arrow memory

### Connectors
- File connectors (CSV, JSON, Parquet)
- Memory connector for testing
- Extensible design for new sources

### Web UI
- Real-time metrics dashboard
- Job inspection and query plan visualization
- WebSocket-based streaming

## Resources

- [GitHub Repository](https://github.com/FranekJemiolo/vectrill)
- [Issue Tracker](https://github.com/FranekJemiolo/vectrill/issues)
- [PyPI Package](https://pypi.org/project/vectrill/)
