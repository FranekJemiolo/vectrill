# Vectrill Performance Benchmarks

## Test Environment

- **Hardware**: MacBook Air M2 (Apple M2, 16GB RAM)
- **OS**: macOS 15.2 (Sequoia)
- **Rust**: 1.84.0
- **Python**: 3.12.13
- **Build**: Release mode with optimizations

## Core Engine Benchmarks

### Sequencer Operations

| Operation | Data Size | Time (ns) | Operations/sec |
|-----------|------------|-----------|----------------|
| Ingest 100 rows | 100 | 43.639 ± 1.69 | 22.9M batches/sec |
| Ingest 1,000 rows | 1,000 | 44.222 ± 0.81 | 22.6M batches/sec |
| Ingest 10,000 rows | 10,000 | 43.983 ± 0.52 | 22.7M batches/sec |
| Ingest 100,000 rows | 100,000 | 43.244 ± 0.62 | 23.1M batches/sec |
| Flush 100 rows | 100 | 1.2006 ± 0.02 | 833M ops/sec |
| Flush 1,000 rows | 1,000 | 1.1853 ± 0.03 | 843M ops/sec |
| Flush 10,000 rows | 10,000 | 1.1886 ± 0.02 | 841M ops/sec |
| Flush 100,000 rows | 100,000 | 1.2199 ± 0.25 | 820M ops/sec |

### Operator Pipeline

| Operation | Data Size | Time (ns) | Operations/sec |
|-----------|------------|-----------|----------------|
| PassThrough 100 rows | 100 | 16.598 ± 0.03 | 60.2M batches/sec |
| PassThrough 1,000 rows | 1,000 | 16.849 ± 0.22 | 59.3M batches/sec |
| PassThrough 10,000 rows | 10,000 | 16.620 ± 0.03 | 60.1M batches/sec |
| PassThrough 100,000 rows | 100,000 | 16.620 ± 0.04 | 60.1M batches/sec |

## Important: Benchmark Interpretation

**What These Numbers Actually Mean:**

1. **Sequencer Ingest**: Time to call `sequencer.ingest()` once per batch
   - **NOT** processing 100K rows in 43ns total
   - **IS** 43ns per BATCH operation
   - **Actual throughput**: ~23M batches/sec

2. **PassThrough Operator**: Time to call `op.process()` once per batch
   - **NOT** 6.0B rows/sec
   - **IS** ~60M batches/sec
   - PassThrough does no actual data processing

3. **Sequencer Flush**: Time to call `sequencer.flush()` once
   - **NOT** 8.4B ops/sec on data
   - **IS** ~820M flush operations/sec
   - Flush just returns accumulated results, minimal work

## Benchmark Limitations

**Current Issues:**
1. **Micro-benchmarking**: Measures function call overhead, not actual data processing
2. **No Real Work**: PassThrough does nothing, sequencer flush just returns data
3. **Synthetic Data**: Simple sequential patterns, no realistic complexity
4. **No I/O**: All in-memory operations
5. **Missing Real Operations**: No filter, map, aggregation benchmarks with real work

**Sample Data Used:**
- **Size**: 100, 1,000, 10,000, 100,000 rows
- **Schema**: timestamp (int64), key (string), value (int64)
- **Pattern**: Sequential integers, simple string formatting
- **Distribution**: Not realistic (perfectly ordered)

## Performance Analysis

### What We Can Conclude

1. **Function Call Overhead**: Very low (~16-43ns per operation)
2. **Memory Management**: Efficient (flush operations ~1ns)
3. **Batch Processing**: Consistent performance across batch sizes
4. **Zero-Copy Operations**: Arrow format enables efficient data handling

### What We Cannot Conclude

1. **Real Throughput**: No actual data processing work measured
2. **Complex Operations**: No filter/map/aggregation with real expressions
3. **Memory Bandwidth**: Not tested with realistic data access patterns
4. **Cache Performance**: Simple data patterns don't stress CPU caches

## Comparison with Other Systems

**Current benchmarks are NOT comparable to pandas/Arrow because:**
- Different workloads measured
- No actual data processing in Vectrill benchmarks
- Synthetic vs real-world data patterns
- Micro-benchmarks vs end-to-end operations

**For meaningful comparisons, we need:**
- Real filter operations with predicates
- Map operations with expressions
- Aggregation with GROUP BY
- Realistic data distributions
- End-to-end pipeline benchmarks

## Next Steps for Accurate Benchmarking

1. **Create Realistic Workloads**:
   - Filter operations with complex predicates
   - Map operations with mathematical expressions
   - Aggregation with GROUP BY
   - Window operations

2. **Use Realistic Data**:
   - Random distributions
   - Real-world data patterns
   - Mixed data types
   - Null values and edge cases

3. **Measure End-to-End Performance**:
   - Full query execution
   - Memory usage patterns
   - Cache performance
   - I/O bottlenecks

## Realistic Data Processing Benchmarks

### Actual Performance with Real Workloads

**Filter Operations (value > 500):**
| Data Size | Time (µs) | Rows/sec | Throughput |
|-----------|-----------|----------|------------|
| 1,000 rows | 2.44 µs | 410K rows/sec | 410K ops/sec |
| 10,000 rows | 18.43 µs | 543K rows/sec | 5.4M ops/sec |
| 100,000 rows | 176.94 µs | 565K rows/sec | 56.5M ops/sec |

**Map Operations (value * 2 + 10):**
| Data Size | Time (µs) | Rows/sec | Throughput |
|-----------|-----------|----------|------------|
| 1,000 rows | 2.42 µs | 413K rows/sec | 413K ops/sec |
| 10,000 rows | 18.17 µs | 550K rows/sec | 5.5M ops/sec |
| 100,000 rows | 133.09 µs | 751K rows/sec | 75.1M ops/sec |

**Sequencer Operations (with realistic data):**
| Data Size | Time (µs/ms) | Rows/sec | Throughput |
|-----------|---------------|----------|------------|
| 1,000 rows | 21.99 µs | 45K rows/sec | 45K ops/sec |
| 10,000 rows | 207.55 µs | 48K rows/sec | 480K ops/sec |
| 100,000 rows | 2.01 ms | 50K rows/sec | 50M ops/sec |

### Performance Analysis

**Key Findings:**
1. **Linear Scaling**: Performance scales linearly with data size for all operations
2. **Filter vs Map**: Map operations are ~25% faster than filter operations
3. **Sequencer Overhead**: Sequencer adds ~10x overhead due to ordering and state management
4. **Consistent Throughput**: ~500K rows/sec for core operations, ~50K rows/sec for sequencer

**Realistic Data Characteristics:**
- **Schema**: id (Int64), category (String), value (Int64), timestamp (Int64)
- **Data Distribution**: Deterministic but realistic patterns using prime multipliers
- **Filter Predicate**: `value > 500` (filters ~50% of rows)
- **Map Expression**: `value * 2 + 10` (arithmetic computation)

### Comparison with Original Benchmarks

| Operation | Original (PassThrough) | Realistic (Filter/Map) | Performance Difference |
|-----------|------------------------|------------------------|----------------------|
| Function Call | 16-43ns | 2.4-177µs | ~100-4000x slower (real work) |
| Data Processing | None | 2.4-177µs | Actual processing measured |
| Throughput | 60M batches/sec | 500K rows/sec | Realistic throughput |

## Reproducing Benchmarks

```bash
# Install dependencies
cargo install cargo-criterion

# Run original micro-benchmarks (function call overhead)
cargo bench --features performance

# Run realistic data processing benchmarks
cargo bench --features performance --bench realistic

# Run specific realistic benchmark
cargo bench --features performance --bench realistic filter_realistic
```

**Note**: Original benchmarks measure function call overhead, while realistic benchmarks measure actual data processing performance.

## Conclusion

The current benchmark results show **excellent function call overhead** and **efficient memory management**, but **do not represent real data processing performance**. For accurate performance assessment, we need benchmarks with actual data processing workloads and realistic data patterns.

**Key Takeaway**: The engine has a solid foundation with minimal overhead, but real-world performance needs to be measured with actual processing workloads.
