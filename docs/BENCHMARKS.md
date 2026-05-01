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

## Reproducing Current Benchmarks

```bash
# Install dependencies
cargo install cargo-criterion

# Run current benchmarks
cargo bench --features performance

# Note: These measure function call overhead, not real processing
```

## Conclusion

The current benchmark results show **excellent function call overhead** and **efficient memory management**, but **do not represent real data processing performance**. For accurate performance assessment, we need benchmarks with actual data processing workloads and realistic data patterns.

**Key Takeaway**: The engine has a solid foundation with minimal overhead, but real-world performance needs to be measured with actual processing workloads.
