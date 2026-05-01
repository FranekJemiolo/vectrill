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

- `benchmark_comparison.py` - Main comprehensive benchmark script
- `benchmark_quick.py` - Quick test with smaller datasets
- `benchmark_test.py` - Basic functionality test
- `benchmark_requirements.txt` - Additional dependencies for visualizations
- `BENCHMARK_README.md` - This documentation

## Installation

1. Install the main vectrill package with its dependencies:
```bash
cd vectrill
pip install -e .
```

2. Install benchmark-specific dependencies:
```bash
pip install -r benchmark_requirements.txt
```

## Usage

### Quick Test
Run a quick test to verify everything works:
```bash
python benchmark_quick.py
```

### Full Benchmark
Run the complete benchmark suite:
```bash
python benchmark_comparison.py
```

This will:
- Test all three libraries (pandas, polars, vectrill)
- Use data sizes: 1K, 10K, 100K, 1M rows
- Test all operations listed above
- Generate:
  - `benchmark_results.json` - Detailed results
  - `benchmark_visualizations.png` - Performance charts
  - Console report with summary statistics

### Custom Benchmark
You can modify the benchmark by editing `benchmark_comparison.py`:
- Change data sizes in `self.data_sizes`
- Add/remove operations in `self.operations`
- Modify test data generation in `generate_test_data()`

## Results

The benchmark generates several outputs:

### Console Report
Real-time progress and final summary showing:
- Execution times for each operation
- Speedup comparisons between libraries
- Overall performance statistics

### JSON Results
`benchmark_results.json` contains:
- Detailed timing data for each test
- Library version information
- Error messages for failed tests
- Structured data for further analysis

### Visualizations
`benchmark_visualizations.png` includes:
- Performance comparison by data size
- Performance comparison by operation type
- Scalability plots (time vs data size)
- Heatmap of operation performance

## Current Status

### ✅ Implemented Operations
- Data creation
- Filtering (`value > 0`)
- Groupby aggregations (sum, mean, multiple aggregations)
- Column selection
- Adding computed columns
- Basic sorting (pandas, polars only)
- Basic joining (pandas, polars only)
- Concatenation (pandas, polars only)

### 🚧 Limited Implementation (Vectrill)
Some operations are not yet fully implemented in Vectrill:
- Sorting operations
- Join operations  
- Concatenation operations

These operations will show as "not implemented" in the results.

### 📊 Expected Results
Based on typical DataFrame library characteristics:
- **Polars**: Usually fastest for large datasets, especially with aggregations
- **Pandas**: Good all-around performance, very mature ecosystem
- **Vectrill**: Designed for high-performance with Rust backend

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
