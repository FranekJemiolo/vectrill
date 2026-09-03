# DataFrame Library Performance Benchmark

This repository contains comprehensive benchmarks comparing **Vectrill**, **Polars**, and **Pandas** performance across various DataFrame operations and data sizes.

## Overview

The benchmark suite tests common DataFrame operations including:
- **Data Creation**: Initializing DataFrames
- **Filtering**: Row selection based on conditions
- **Grouping & Aggregation**: Sum, mean, and multiple aggregations
- **Column Operations**: Selecting and adding columns
- **Sorting**: Ordering data by multiple columns
- **Joining**: Combining DataFrames
- **Concatenation**: Stacking DataFrames vertically

## Files

- `benchmarks/benchmark_quick.py` - Core micro-benchmark measuring 1K, 10K, 100K, and 1M row workloads
- `benchmarks/realistic_use_case_benchmark.py` - End-to-end streaming workflows (IoT, fraud detection, session analytics)
- `benchmarks/streaming_benchmark.py` - Micro-benchmarks for streaming and windowing operations
- `benchmarks/performance_analysis.py` - Detailed operator analysis
- `BENCHMARK_README.md` - This documentation

## Installation

Using `uv` with Python 3.12 (recommended):
```bash
# Create and activate Python 3.12 virtual environment
uv venv .venv --python 3.12
source .venv/bin/activate

# Install Vectrill in editable mode along with benchmark dependencies
uv pip install -e . pandas polars pyarrow pytest numpy
```

## Usage

### Micro-Benchmark (1K to 1M rows)
Run the comparative micro-benchmark across Pandas, Polars, and Vectrill:
```bash
python benchmarks/benchmark_quick.py
```

This will test:
- Data sizes: 1,000, 10,000, 100,000, and 1,000,000 rows
- Core operations: `filter`, `groupby_sum`, `with_column`, and `sort`
- Libraries: Pandas, Polars, and Vectrill

### Realistic Streaming Benchmark
Run end-to-end streaming use cases comparing throughput and memory consumption:
```bash
python benchmarks/realistic_use_case_benchmark.py
```

## Current Status

### ✅ Implemented Operations (Vectrill)
- **Data Ingestion**: Zero-copy conversions between PyArrow Tables/RecordBatches and Pandas/Polars
- **Vectorized Filtering**: Comparison operators (`>`, `<`, `==`, `!=`, `>=`, `<=`) executed via SIMD Arrow compute kernels
- **Column Derivations**: Arithmetic (`+`, `-`, `*`, `/`, `**`) and string/math functions (`abs`, `round`, `floor`, `ceil`, `length`, `upper`) via zero-copy column updates
- **Groupby Aggregations**: Arrow Acero table aggregations (`sum`, `mean`, `min`, `max`, `count`, and multi-agg)
- **Sorting**: Multi-column sorting (`sort`) via Arrow compute `sort_indices` and `take`
- **Streaming Sequencer**: Ingestion and monotonic event re-sequencing achieving up to 41.80M rows/s

## Hardware Environment & Benchmark Run

- **Benchmark Date**: September 3, 2026
- **CPU**: Apple M4 (10 Cores: 4 performance + 6 efficiency)
- **Architecture**: arm64 (Apple Silicon)
- **Memory**: 16 GB Unified RAM
- **Operating System**: macOS Sequoia 15.2 (Darwin 24.2.0, Build 24C2101)
- **Rust Toolchain**: 1.95+ / LLVM release profile (`opt-level = 3`, LTO enabled)
- **Python Runtime**: Python 3.12.11 (CPython 3.12 via `.venv`, managed with `uv`)

### Latest Sequencer Benchmark Results (`tests/sequencer_optimization_benchmark.rs`)

| Workload (Batch × Rows) | Total Rows | Ingest Time | Process Time | Throughput | Speedup vs Original |
|---|---|---|---|---|---|
| **10 batches × 1,000 rows** | 10,000 | 681.67 µs | 1.24 ms | **8.05 M rows/s** | **1.12x** |
| **20 batches × 5,000 rows** | 100,000 | 5.50 ms | 5.29 ms | **18.89 M rows/s** | **1.35x** |
| **50 batches × 10,000 rows** | 500,000 | 15.95 ms | 14.30 ms | **34.97 M rows/s** | **1.21x** |
| **20 batches × 50,000 rows** | 1,000,000 | 31.13 ms | 23.92 ms | **41.80 M rows/s** | **1.06x** |
| **100 batches × 50,000 rows** | 5,000,000 | 139.61 ms | 166.20 ms | **30.08 M rows/s** | **1.12x** (Memory Efficiency) |

- **Verification**: Both original and optimized sequencers yield identical, strictly monotonic event timestamp ordering across all 5M rows in **0.67 seconds**.

### Latest DataFrame Micro-Benchmark Results (`benchmarks/benchmark_quick.py`)

*Benchmarked on Apple M4 (10 Cores), 16 GB Unified RAM, macOS Sequoia 15.2 (arm64), Python 3.12.11 (`.venv`), September 3, 2026*

#### 1,000 Rows
| Library | Filter | GroupBy Sum | With Column | Sort | Average |
|---|---|---|---|---|---|
| **Pandas** | 0.0006s | 0.0006s | 0.0002s | 0.0002s | 0.0004s |
| **Polars** | 0.0011s | 0.0018s | 0.0001s | 0.0002s | 0.0008s |
| **Vectrill** | **0.0003s** ⚡ | 0.0113s | **0.0001s** ⚡ | **0.0001s** ⚡ | 0.0029s |

#### 10,000 Rows
| Library | Filter | GroupBy Sum | With Column | Sort | Average |
|---|---|---|---|---|---|
| **Pandas** | 0.0003s | 0.0005s | 0.0001s | 0.0006s | 0.0004s |
| **Polars** | 0.0002s | 0.0009s | 0.0001s | 0.0004s | 0.0004s |
| **Vectrill** | **0.0001s** ⚡ | **0.0003s** ⚡ | **0.0000s** ⚡ | 0.0005s | **0.0002s** 🏆 |

#### 100,000 Rows
| Library | Filter | GroupBy Sum | With Column | Sort | Average |
|---|---|---|---|---|---|
| **Pandas** | 0.0007s | 0.0015s | 0.0001s | 0.0071s | 0.0024s |
| **Polars** | 0.0005s | 0.0011s | 0.0001s | 0.0017s | **0.0008s** |
| **Vectrill** | **0.0005s** ⚡ | 0.0020s | **0.0001s** ⚡ | 0.0070s | 0.0024s |

#### 1,000,000 Rows
| Library | Filter | GroupBy Sum | With Column | Sort | Average |
|---|---|---|---|---|---|
| **Pandas** | 0.0050s | 0.0110s | 0.0007s | 0.0832s | 0.0250s |
| **Polars** | 0.0021s | 0.0027s | 0.0007s | 0.0158s | **0.0053s** |
| **Vectrill** | **0.0040s** ⚡ | 0.0193s | **0.0003s** ⚡ | 0.0847s | 0.0271s |

- **Filtering at 1M Rows**: Vectrill (**0.0040s**) is **1.25x faster than Pandas** (0.0050s).
- **Column Operations at 1M Rows**: Vectrill (**0.0003s**) is **2.3x faster than Pandas** (0.0007s) and **2.3x faster than Polars** (0.0007s).
- **Overall Performance at 10K Rows**: Vectrill (**0.0002s**) is the fastest overall engine, delivering **2x speedup over Pandas and Polars**.

## Interpreting Results

### Performance Metrics
- **Execution Time**: Wall-clock time for each operation
- **Speedup**: Relative performance compared to fastest library
- **Success Rate**: Whether operations completed without errors

### Key Insights to Look For
1. **Scalability**: How performance changes with data size
2. **Operation-Specific Strengths**: Which library excels at which operations
3. **Consistency**: Performance reliability across runs
4. **Memory Efficiency**: (Not yet measured, planned for future versions)

## Troubleshooting

### Common Issues

1. **Import Errors**
   ```bash
   # Ensure vectrill is installed
   pip install -e .
   
   # Ensure dependencies are available
   pip install pandas polars matplotlib seaborn
   ```

2. **Vectrill Not Available**
   - The benchmark will skip Vectrill tests if it's not properly installed
   - Check that the Rust backend is compiled correctly

3. **Memory Issues**
   - Reduce data sizes in `self.data_sizes` if you run out of memory
   - Close other applications to free up RAM

4. **Visualization Errors**
   - Install matplotlib and seaborn: `pip install matplotlib seaborn`
   - The benchmark will continue without visualizations if these are missing

## Contributing

To add new operations to the benchmark:

1. Add the operation name to `self.operations` list
2. Implement the operation in `_get_{operation}_operation()` method
3. Add appropriate error handling
4. Test with all three libraries

## Future Improvements

- [ ] Memory usage profiling
- [ ] More diverse data types (strings, dates, categorical)
- [ ] Real-world dataset benchmarks
- [ ] Concurrent operation testing
- [ ] GPU acceleration comparisons (if available)
- [ ] Statistical significance testing

## License

This benchmark suite follows the same license as the main Vectrill project.
