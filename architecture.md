---
layout: default
title: Architecture
---

# Architecture

This document describes the architecture of Vectrill, a high-performance streaming engine.

## Overview

Vectrill is a single-node streaming execution engine that combines Python's ergonomics with Rust's performance. The architecture is designed around the following principles:

- **Zero-copy**: Use Apache Arrow's columnar memory format throughout
- **Micro-batching**: Process data in small batches for efficiency
- **Optimization**: Apply query planning and expression optimization
- **Streaming**: Support event-time processing with watermarks and windows

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────┐
│                  Python API (Control Plane)                  │
│  • PyO3 bindings                                             │
│  • Arrow C Data Interface                                   │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│              Logical Query / DAG Builder                     │
│  • Expression parsing                                        │
│  • Query construction                                       │
│  • Logical plan validation                                  │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│               Physical Execution Plan                        │
│  • Logical to physical conversion                           │
│  • Query optimization                                       │
│  • Operator fusion                                          │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│              Rust Streaming Runtime Engine                   │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  Expression Optimization                               │   │
│  │  • Constant folding                                    │   │
│  │  • Common subexpression elimination (CSE)              │   │
│  └──────────────────────────────────────────────────────┘   │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  Memory Optimization                                   │   │
│  │  • Buffer pooling                                      │   │
│  │  • Array reuse                                         │   │
│  └──────────────────────────────────────────────────────┘   │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  Performance Monitoring                               │   │
│  │  • Performance counters                                │   │
│  │  • Metrics registry                                    │   │
│  │  • Timers                                               │   │
│  └──────────────────────────────────────────────────────┘   │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│           Arrow-Native Operators (Stateful + Stateless)      │
│  • Map, Filter, Aggregate                                   │
│  • Window Functions (Tumbling, Sliding, Session)            │
│  • State management                                          │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│                    Micro-batched Outputs                      │
│  • Sequencer for event ordering                             │
│  • Watermark tracking                                       │
│  • Late data handling                                       │
└─────────────────────────────────────────────────────────────┘
```

## Components

### Python API Layer

The Python API layer provides a familiar DataFrame-style interface for building streaming pipelines. It uses PyO3 for Rust bindings and the Arrow C Data Interface for zero-copy data transfer.

### Query Planner

The query planner converts logical queries into physical execution plans with optimizations:

- **Projection Elimination**: Remove unused columns early
- **Predicate Pushdown**: Push filters closer to data sources
- **Operator Fusion**: Combine compatible operators into single pass

### Expression Engine

The expression engine evaluates expressions with optimizations:

- **Constant Folding**: Pre-compute constant expressions
- **CSE**: Avoid duplicate computation
- **Type Coercion**: Handle type conversions

### Streaming Runtime

The streaming runtime manages execution with:

- **Micro-batching**: Group events into efficient batches
- **Sequencer**: Order events by timestamp
- **Watermarks**: Track event time progress
- **Windows**: Group data by time windows

### Operators

Operators transform and aggregate data:

- **Stateless**: Map, Filter, Projection
- **Stateful**: Aggregate, Join, Window functions

### Memory Management

Memory optimization features:

- **Buffer Pooling**: Reuse Arrow arrays
- **Zero-copy**: Minimize data copies
- **Memory counters**: Track allocations

### Connectors

Data source connectors:

- **File**: CSV, JSON, Parquet
- **Memory**: In-memory data
- **Extensible**: Easy to add new sources

## Performance Features

### Expression Optimization

- Constant folding at planning time
- Common subexpression elimination
- Type-based optimization

### Memory Optimization

- Buffer pooling for Arrow arrays
- Reuse of allocated buffers
- Global pool for application-wide sharing

### Query Optimization

- Projection elimination
- Predicate pushdown
- Operator fusion

## Web UI

The web UI provides:

- Real-time metrics dashboard
- Job inspection and monitoring
- Query plan visualization
- WebSocket-based streaming

## Extensibility

Vectrill is designed for extensibility:

- **Custom Operators**: Implement the Operator trait
- **Custom Connectors**: Implement the Connector trait
- **Custom Functions**: Add to the expression engine
- **Custom Metrics**: Register with the metrics registry
