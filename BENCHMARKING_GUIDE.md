# Vectrill Benchmarking Guide

This guide provides comprehensive benchmarking capabilities for comparing Vectrill against pandas and polars in realistic streaming scenarios.

## Overview

The benchmarking suite is designed to showcase Vectrill's unique streaming advantages in use cases where traditional DataFrame libraries struggle. The focus is on realistic scenarios that demonstrate the fundamental architectural differences between streaming and batch-oriented systems.

## Available Benchmarks

### Realistic Use Case Benchmark (`realistic_use_case_benchmark.py`)

This is the primary benchmark that tests Vectrill in realistic streaming scenarios:

#### Use Cases Tested

1. **User Session Analytics**
   - Real-time session tracking and analytics
   - Window functions on streaming data
   - Session-level metric calculation

2. **Real-Time Fraud Detection**
   - Sequential pattern detection
   - Multi-window analysis
   - Low-latency processing requirements

3. **IoT Sensor Processing**
   - Multi-stream sensor data fusion
   - Time-series window operations
   - Real-time aggregation

#### Running the Benchmark

```bash
cd /Users/franek/personal_workspace/vectrill
python realistic_use_case_benchmark.py
```

#### What it Tests

- **Streaming vs Batch Processing**: Direct comparison of streaming vs batch approaches
- **Memory Efficiency**: Peak memory usage during processing
- **Latency**: Processing time for realistic data volumes
- **Window Functions**: Real-time window operations
- **Multi-Stream Processing**: Handling multiple concurrent data streams

### Legacy Benchmarks

The following benchmarks are maintained for reference but may not reflect Vectrill's current capabilities:

- `streaming_benchmark.py` - Original streaming operations benchmark
- `benchmark_quick.py` - Quick performance tests
- `benchmark_test.py` - Basic functionality tests

## Understanding the Results

### Performance Metrics

1. **Processing Time**: Wall-clock time for the operation
2. **Peak Memory**: Maximum memory usage during processing
3. **Approach**: Processing method (streaming, lazy, batch)
4. **Success**: Whether the operation completed successfully

### Approach Indicators

- 🚀 **Streaming**: True streaming processing (Vectrill)
- ⚡ **Lazy**: Lazy evaluation (Polars)
- 📦 **Batch**: Batch processing (Pandas)

### Interpreting Results

**Vectrill Advantages**:
- Lower memory usage for large datasets
- Consistent performance regardless of data size
- True streaming capabilities
- Multi-stream processing efficiency

**Traditional Library Limitations**:
- Memory grows with data size
- Materialization requirements
- Limited streaming capabilities
- Single-threaded batch processing

## Use Case Selection

### When Vectrill Excels

✅ **Real-Time Analytics**
- Live dashboards
- Real-time monitoring
- Immediate alerting

✅ **Multi-Stream Processing**
- IoT sensor fusion
- Log aggregation
- Cross-stream analytics

✅ **Sequential Processing**
- Session tracking
- Time-series analysis
- Pattern detection

✅ **Memory-Constrained Environments**
- Edge computing
- Resource-limited systems
- Large dataset processing

### When Traditional Libraries May Be Better

❌ **Historical Data Analysis**
- One-off analysis
- Static datasets
- Complex statistical operations

❌ **Simple Aggregations**
- Basic summary statistics
- Small datasets
- Simple transformations

❌ **Batch ETL Processes**
- Data warehousing
- Batch reporting
- Historical data processing

## Running Benchmarks

### Prerequisites

Ensure all dependencies are installed:

```bash
pip install pandas polars numpy psutil
```

### Environment Setup

For accurate results:
- Close unnecessary applications
- Use consistent hardware
- Run multiple iterations
- Monitor system resources

### Command Line Options

The benchmark script supports various options:

```bash
python realistic_use_case_benchmark.py --help
```

## Custom Benchmarks

### Creating New Use Cases

To add new benchmark scenarios:

1. Define the use case in the `use_cases` list
2. Implement the benchmark method
3. Add routing logic in `run_realistic_benchmark`
4. Update documentation

### Best Practices

- Use realistic data volumes
- Test multiple data sizes
- Include proper error handling
- Document the use case clearly
- Compare against appropriate alternatives

## Performance Optimization

### Vectrill Optimization

- Use appropriate batch sizes
- Leverage window functions
- Minimize materialization
- Optimize stream ordering

### Comparison Fairness

- Use equivalent operations
- Same data volumes
- Consistent timing methodology
- Proper memory measurement

## Troubleshooting

### Common Issues

1. **Memory Errors**: Reduce data size or batch size
2. **Import Errors**: Check Python path and dependencies
3. **Timeout Issues**: Increase timeout or reduce complexity
4. **Inconsistent Results**: Run multiple iterations

### Debug Mode

Enable debug output:

```bash
python realistic_use_case_benchmark.py --debug
```

## Contributing

When adding benchmarks:

1. Follow existing patterns
2. Include proper documentation
3. Test with all libraries
4. Update this guide
5. Add use case documentation

## Results Analysis

### Key Metrics to Watch

1. **Memory Efficiency**: How memory scales with data size
2. **Latency Consistency**: Performance across different data sizes
3. **Streaming Advantages**: Where Vectrill truly excels
4. **Library Limitations**: Where traditional libraries struggle

### Reporting Results

Generate comprehensive reports:

```bash
python realistic_use_case_benchmark.py --report
```

Results are saved to:
- `realistic_use_case_results.json` - Detailed results
- Console output - Summary report

## Conclusion

This benchmarking suite provides a comprehensive way to evaluate Vectrill's streaming advantages in realistic scenarios. The focus is on use cases where streaming architecture provides fundamental benefits over traditional batch-oriented approaches.

For detailed use case information, see `STREAMING_USE_CASES.md`.
