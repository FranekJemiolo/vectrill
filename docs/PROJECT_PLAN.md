# Vectrill Project Plan

## Overview

Vectrill is a high-performance, single-node streaming execution engine with:
- Arrow-native columnar memory (zero-copy)
- Rust execution core
- Python DSL and control plane
- Spark-like API with Flink-like streaming semantics
- Operator fusion for performance

## Vision

Build a local-first streaming engine that combines:
- Spark's API and query planning
- Flink's streaming semantics (watermarks, windows)
- Apache Arrow's zero-copy columnar memory
- Rust's performance and memory safety
- Python's ergonomics and ecosystem

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

## Core Design Principles

1. **Zero-copy first**: All data represented as RecordBatch, no row-level structs in hot path
2. **Columnar-native processing**: No row deserialization, use Arrow compute kernels
3. **Streaming semantics**: Configurable lateness, watermark-based flushing
4. **Connector abstraction**: Unified ingestion interface, pluggable sources
5. **Deterministic batching**: Same input → same output ordering

## Technology Stack

- **Rust**: Execution engine, performance-critical code
- **Python**: Orchestration, DSL, testing, UX
- **Apache Arrow**: Data model and memory format
- **Polars**: Python DataFrame interface
- **PyO3**: Rust-Python bindings
- **Maturin**: Python packaging for Rust extensions

## Milestones

### M0: Core Engine Skeleton (1-2 days)
- Project structure
- Basic RecordBatch pipeline
- Stub operator trait

### M1: Sequencer + Micro-batching (2-4 days)
- Multi-source ingestion (mock connectors)
- Heap-based ordering
- Micro-batch output
- Timestamp ordering

### M2: Python Integration (FFI) (2-3 days)
- PyO3 module
- Arrow C Data Interface bridge
- Python wrapper class
- Zero-copy Arrow ↔ Polars

### M3: Expression Engine (4-6 days)
- Python AST → IR compiler
- Rust expression evaluator
- Arrow kernel integration
- Column refs, literals, binary ops, boolean logic

### M4: Query Planner (Logical → Physical) (4-6 days)
- Python DSL builder
- Logical plan IR
- Optimizer (basic rules)
- Physical plan mapping

### M5: Operator Fusion (5-8 days)
- Fusion planner (segment builder)
- Fused operator implementation
- Expression tree merging
- Column pruning, predicate pushdown

### M6: Streaming Semantics (6-10 days)
- Watermark system
- Event-time handling
- Window operators (tumbling, sliding, session)
- Late data handling
- State management

### M7: Connectors + Real Workloads (4-7 days)
- File connector
- Stdin connector
- Optional Kafka connector
- Streaming ingestion with backpressure

### M8: Performance + Advanced Features (ongoing)
- Expression optimization (constant folding, CSE)
- Fusion improvements (DAG-level, kernel batching)
- Memory optimization (buffer reuse, allocation pooling)
- Optional JIT/codegen
- SIMD specialization

## Testing Strategy

- **Python-first testing**: Express expectations in Python with Polars
- **Rust unit tests**: Operator correctness, expression evaluation, sequencing logic
- **Benchmarks**: Track rows/sec, latency per batch, allocations
- **E2E tests**: Full pipeline validation with real data

## Performance Targets

- Throughput: > 1M rows/sec
- Latency: < 10ms (micro-batch)
- Memory overhead: ~1.2x input
- Copies: zero (except final batch)

## Non-goals

- Distributed execution (no Spark clone yet)
- SQL layer (initially)
- Full schema evolution
- Exactly-once semantics (initially)
