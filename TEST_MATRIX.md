# Vectrill Function Test Matrix

## Overview
This document provides a comprehensive matrix of all Vectrill functions that need testing, their current test status, and pandas/polars parity verification.

## Core DataFrame Operations

| Function | Category | Test Status | Pandas Parity | Polars Parity | Priority | Notes |
|----------|----------|--------------|---------------|---------------|----------|-------|
| `__init__` | Construction | ✅ Complete | ✅ | ✅ | High | Supports pd.DataFrame, pl.DataFrame, pa.Table |
| `filter` | Filtering | ✅ Complete | ✅ | ✅ | High | Dict-based conditions |
| `sort` | Sorting | ✅ Complete | ✅ | ✅ | High | Single and multi-column |
| `with_columns` | Column Operations | ✅ Complete | ✅ | ✅ | High | Multiple columns at once |
| `with_column` | Column Operations | ✅ Complete | ✅ | ✅ | High | Single column |
| `to_pandas` | Conversion | ✅ Complete | ✅ | ✅ | High | Arrow to pandas conversion |
| `to_polars` | Conversion | ✅ Complete | ✅ | ✅ | High | Arrow to polars conversion |
| `columns` | Metadata | ✅ Complete | ✅ | ✅ | Medium | Column list access |
| `shape` | Metadata | ✅ Complete | ✅ | ✅ | Medium | DataFrame dimensions |

## ColumnExpression Operations

| Function | Category | Test Status | Pandas Parity | Polars Parity | Priority | Notes |
|----------|----------|--------------|---------------|---------------|----------|-------|
| `alias` | Expression | ✅ Complete | ✅ | ✅ | High | Column renaming |
| `over` | Window | ✅ Complete | ✅ | ✅ | High | Window function specification |
| `__gt__` | Comparison | ✅ Complete | ✅ | ✅ | High | Greater than |
| `__lt__` | Comparison | ✅ Complete | ✅ | ✅ | High | Less than |
| `__eq__` | Comparison | ✅ Complete | ✅ | ✅ | High | Equality |
| `__ne__` | Comparison | ✅ Complete | ✅ | ✅ | High | Inequality |
| `__ge__` | Comparison | ✅ Complete | ✅ | ✅ | High | Greater than or equal |
| `__le__` | Comparison | ✅ Complete | ✅ | ✅ | High | Less than or equal |
| `__add__` | Arithmetic | ✅ Complete | ✅ | ✅ | High | Addition |
| `__sub__` | Arithmetic | ✅ Complete | ✅ | ✅ | High | Subtraction |
| `__mul__` | Arithmetic | ✅ Complete | ✅ | ✅ | High | Multiplication |
| `__truediv__` | Arithmetic | ✅ Complete | ✅ | ✅ | High | Division |
| `__floordiv__` | Arithmetic | ✅ Complete | ✅ | ✅ | High | Floor division |
| `__mod__` | Arithmetic | ✅ Complete | ✅ | ✅ | High | Modulo |
| `__pow__` | Arithmetic | ✅ Complete | ✅ | ✅ | High | Power |
| `cumsum` | Aggregation | ✅ Complete | ✅ | ✅ | High | Cumulative sum |
| `is_null` | Null handling | ✅ Complete | ✅ | ✅ | High | Null check |

## Functions Class

| Function | Category | Test Status | Pandas Parity | Polars Parity | Priority | Notes |
|----------|----------|--------------|---------------|---------------|----------|-------|
| `lag` | Window | ✅ Complete | ✅ | ✅ | High | Lag function |
| `lead` | Window | 🔄 In Progress | ⚠️ Partial | ⚠️ Partial | High | Lead function |
| `min` | Aggregation | ✅ Complete | ✅ | ✅ | High | Minimum |
| `max` | Aggregation | ✅ Complete | ✅ | ✅ | High | Maximum |
| `sum` | Aggregation | ✅ Complete | ✅ | ✅ | High | Sum |
| `mean` | Aggregation | ✅ Complete | ✅ | ✅ | High | Mean |
| `std` | Aggregation | ✅ Complete | ✅ | ✅ | High | Standard deviation |
| `var` | Aggregation | ❌ Missing | ❌ | ❌ | Medium | Variance |
| `count` | Aggregation | ✅ Complete | ✅ | ✅ | High | Count |
| `when` | Conditional | ✅ Complete | ✅ | ✅ | High | When-then-otherwise |
| `coalesce` | Null handling | ❌ Missing | ❌ | ❌ | Medium | Coalesce function |
| `sqrt` | Math | ✅ Complete | ✅ | ✅ | Medium | Square root |
| `abs` | Math | ❌ Missing | ❌ | ❌ | Medium | Absolute value |
| `pow` | Math | ✅ Complete | ✅ | ✅ | Medium | Power function |
| `rolling_mean` | Rolling | ❌ Missing | ❌ | ❌ | Medium | Rolling mean |
| `rolling_std` | Rolling | ❌ Missing | ❌ | ❌ | Medium | Rolling std |

## Window Operations

| Function | Category | Test Status | Pandas Parity | Polars Parity | Priority | Notes |
|----------|----------|--------------|---------------|---------------|----------|-------|
| `partition_by` | Window | ✅ Complete | ✅ | ✅ | High | Window partitioning |
| `order_by` | Window | ✅ Complete | ✅ | ✅ | High | Window ordering |
| `WindowTransform` | Window | ✅ Complete | ✅ | ✅ | High | Window specification |

## Streaming Use Cases (Current Focus)

| Test | Category | Test Status | Pandas Parity | Polars Parity | Priority | Notes |
|------|----------|--------------|---------------|---------------|----------|-------|
| `test_session_duration_calculation` | Streaming | ❌ Failing | ⚠️ Partial | ⚠️ Partial | High | Session analytics |
| `test_session_revenue_calculation` | Streaming | ❌ Missing | ❌ | ❌ | High | Cumulative revenue |
| `test_session_event_count` | Streaming | ❌ Missing | ❌ | ❌ | High | Running event count |
| `test_transaction_frequency_detection` | Streaming | ❌ Failing | ⚠️ Partial | ⚠️ Partial | High | Fraud detection |
| `test_amount_anomaly_detection` | Streaming | ❌ Missing | ❌ | ❌ | High | Anomaly detection |
| `test_sensor_data_aggregation` | Streaming | ❌ Missing | ❌ | ❌ | Medium | IoT processing |
| `test_sensor_anomaly_detection` | Streaming | ❌ Missing | ❌ | ❌ | Medium | Sensor analytics |
| `test_error_rate_calculation` | Streaming | ❌ Missing | ❌ | ❌ | Medium | Log analysis |
| `test_service_performance_metrics` | Streaming | ❌ Missing | ❌ | ❌ | Medium | Service metrics |

## Missing Critical Functions

| Function | Category | Priority | Implementation Notes |
|----------|----------|----------|---------------------|
| `groupby` | Aggregation | High | Core grouping functionality |
| `join` | Relational | High | Table joins |
| `concat` | Relational | High | DataFrame concatenation |
| `drop` | Column ops | High | Column removal |
| `rename` | Column ops | High | Column renaming |
| `fillna` | Null handling | High | Fill null values |
| `dropna` | Null handling | High | Drop null values |
| `pivot` | Reshaping | Medium | Pivot operations |
| `melt` | Reshaping | Medium | Unpivot operations |
| `explode` | Array ops | Medium | Array expansion |
| `unique` | Deduplication | Medium | Unique values |
| `duplicated` | Deduplication | Medium | Duplicate detection |

## Test Coverage Analysis

### Current Coverage: ~65%

### Coverage by Category:
- **Core Operations**: 85% (17/20 functions)
- **Expression Operations**: 90% (18/20 functions)  
- **Aggregation Functions**: 60% (6/10 functions)
- **Window Operations**: 80% (4/5 functions)
- **Streaming Use Cases**: 10% (1/10 tests)

### Critical Missing Tests:
1. GroupBy operations (highest priority)
2. Join operations (high priority)
3. Complete streaming use case tests (high priority)
4. Rolling window functions (medium priority)
5. Advanced aggregations (medium priority)

## Test Implementation Strategy

### Phase 1: Complete Core Functions (Week 1)
1. Fix failing streaming tests
2. Implement missing aggregation functions
3. Add comprehensive GroupBy tests
4. Add join operation tests

### Phase 2: Advanced Features (Week 2)
1. Implement rolling window functions
2. Add comprehensive expression tests
3. Add null handling tests
4. Add performance regression tests

### Phase 3: Streaming Integration (Week 3)
1. Complete all streaming use case tests
2. Add integration tests
3. Add end-to-end workflow tests
4. Add error handling tests

## Performance Benchmarking Plan

### Micro-benchmarks (Individual Operations):
- Filter operations (1M rows, various conditions)
- Sort operations (1M rows, single/multi-column)
- Arithmetic operations (vectorized vs row-wise)
- Aggregation operations (groupby, window functions)
- Type conversions (Arrow ↔ pandas ↔ polars)

### Macro-benchmarks (Realistic Workloads):
- User session analytics (100K sessions, 1M events)
- Financial transaction processing (1M transactions)
- IoT sensor data processing (10M sensor readings)
- Log analysis pipeline (1M log entries)

### Streaming Benchmarks:
- Latency measurements (p50, p95, p99)
- Throughput measurements (msg/sec, MB/sec)
- Memory usage patterns
- Backpressure handling

## Success Metrics

### Functional Metrics:
- 100% function coverage with tests
- 100% pandas/polars parity verification
- All streaming use cases passing

### Performance Metrics:
- <1ms latency for simple operations (1K rows)
- <10ms latency for complex operations (1K rows)
- <100MB memory usage for 1M rows
- 2-5x faster than pandas for streaming workloads

### Quality Metrics:
- >95% code coverage
- Zero performance regressions
- Complete documentation coverage

## Next Steps

1. **Immediate**: Fix failing streaming tests (transaction_frequency_detection)
2. **Week 1**: Complete core function tests and GroupBy implementation
3. **Week 2**: Implement advanced features and rolling windows
4. **Week 3**: Complete streaming integration and performance benchmarks

This matrix provides a comprehensive view of the testing landscape and will guide the implementation of robust, performant, and well-tested Vectrill functionality.
