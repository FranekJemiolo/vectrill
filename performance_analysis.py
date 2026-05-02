#!/usr/bin/env python3
import pandas as pd
import numpy as np

# Analyze benchmark results
print("Performance Analysis: Vectrill vs Pandas vs Polars")
print("=" * 60)

# Read benchmark results
try:
    df = pd.read_csv('benchmark_results.csv')
    print(f"Loaded {len(df)} benchmark results")
    print("\nRaw Data:")
    print(df[['operation', 'data_size', 'vectrill_time', 'pandas_time', 'polars_time', 'speedup_vs_pandas', 'speedup_vs_polars']])
    
    # Identify significantly slower functions
    print("\n" + "=" * 60)
    print("PERFORMANCE ANALYSIS")
    print("=" * 60)
    
    # Functions significantly slower than pandas (speedup < 0.5)
    slower_than_pandas = df[df['speedup_vs_pandas'] < 0.5]
    if not slower_than_pandas.empty:
        print("\n🔴 SIGNIFICANTLY SLOWER THAN PANDAS (speedup < 0.5x):")
        for _, row in slower_than_pandas.iterrows():
            print(f"  • {row['operation']} ({row['data_size']} rows): {row['speedup_vs_pandas']:.3f}x slower")
    else:
        print("\n✅ No functions significantly slower than pandas")
    
    # Functions significantly slower than polars (speedup < 0.5)
    slower_than_polars = df[df['speedup_vs_polars'] < 0.5]
    if not slower_than_polars.empty:
        print("\n🔴 SIGNIFICANTLY SLOWER THAN POLARS (speedup < 0.5x):")
        for _, row in slower_than_polars.iterrows():
            print(f"  • {row['operation']} ({row['data_size']} rows): {row['speedup_vs_polars']:.3f}x slower")
    else:
        print("\n✅ No functions significantly slower than polars")
    
    # Functions with good performance (speedup > 0.8x compared to pandas)
    good_vs_pandas = df[df['speedup_vs_pandas'] >= 0.8]
    if not good_vs_pandas.empty:
        print("\n✅ GOOD PERFORMANCE VS PANDAS (speedup >= 0.8x):")
        for _, row in good_vs_pandas.iterrows():
            print(f"  • {row['operation']} ({row['data_size']} rows): {row['speedup_vs_pandas']:.3f}x")
    
    # Functions with excellent performance (speedup > 1.0x compared to pandas)
    excellent_vs_pandas = df[df['speedup_vs_pandas'] > 1.0]
    if not excellent_vs_pandas.empty:
        print("\n🚀 EXCELLENT PERFORMANCE VS PANDAS (speedup > 1.0x):")
        for _, row in excellent_vs_pandas.iterrows():
            print(f"  • {row['operation']} ({row['data_size']} rows): {row['speedup_vs_pandas']:.3f}x faster")
    
    # Performance summary by operation
    print("\n" + "=" * 60)
    print("PERFORMANCE SUMMARY BY OPERATION")
    print("=" * 60)
    
    for operation in df['operation'].unique():
        op_data = df[df['operation'] == operation].sort_values('data_size')
        print(f"\n{operation.upper()}:")
        for _, row in op_data.iterrows():
            pandas_speedup = row['speedup_vs_pandas']
            if pandas_speedup >= 0.8:
                status = "✅"
            elif pandas_speedup >= 0.5:
                status = "⚠️"
            else:
                status = "🔴"
            print(f"  {status} {row['data_size']:>6} rows: {pandas_speedup:.3f}x vs pandas")
    
    # Recommendations
    print("\n" + "=" * 60)
    print("RECOMMENDATIONS")
    print("=" * 60)
    
    if not slower_than_pandas.empty:
        print("\n🔧 OPTIMIZATION NEEDED:")
        for operation in slower_than_pandas['operation'].unique():
            print(f"  • Optimize {operation} implementation")
            print(f"    - Current: {slower_than_pandas[slower_than_pandas['operation'] == operation]['speedup_vs_pandas'].mean():.3f}x vs pandas")
            print(f"    - Target: >0.8x vs pandas")
    
    print("\n📈 PERFORMANCE TARGETS:")
    print("  • All operations should achieve >0.8x speedup vs pandas")
    print("  • Critical operations should achieve >1.0x speedup vs pandas")
    print("  • Window functions should be optimized for large datasets")
    
except FileNotFoundError:
    print("❌ Benchmark results file not found. Run benchmarks first.")
except Exception as e:
    print(f"❌ Error analyzing benchmark results: {e}")
