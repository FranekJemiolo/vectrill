#!/usr/bin/env python3
"""
Comprehensive benchmark for Vectrill, pandas, and polars
Tests only functions that are available and working across all libraries
"""

import time
import sys
import os
import numpy as np
import pandas as pd
import polars as pl
from typing import Dict, List, Tuple, Any
import json

# Add vectrill to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'python'))

try:
    from vectrill.dataframe import VectrillDataFrame, col, functions, window
    VECTRILL_AVAILABLE = True
    print("✓ Vectrill available")
except ImportError as e:
    print(f"✗ Vectrill not available - {e}")
    VECTRILL_AVAILABLE = False

class ComprehensiveBenchmark:
    """Comprehensive benchmark for all available functions"""
    
    def __init__(self):
        self.results = []
        self.data_sizes = [1000, 10000, 100000]
        self.libraries = ['pandas', 'polars']
        if VECTRILL_AVAILABLE:
            self.libraries.append('vectrill')
    
    def generate_data(self, size: int) -> Dict[str, Any]:
        """Generate test data for benchmarking"""
        np.random.seed(42)
        return {
            'id': range(size),
            'group': np.random.choice(['A', 'B', 'C', 'D', 'E'], size),
            'value1': np.random.randn(size) * 100,
            'value2': np.random.randint(1, 1000, size),
            'category': np.random.choice(['X', 'Y', 'Z'], size),
            'flag': np.random.choice([True, False], size),
            'timestamp': pd.date_range('2023-01-01', periods=size, freq='1min')
        }
    
    def benchmark_filter(self, data: Dict[str, Any], library: str) -> Tuple[float, bool]:
        """Benchmark filter operation"""
        try:
            start_time = time.time()
            
            if library == 'pandas':
                df = pd.DataFrame(data)
                result = df[df['value1'] > 0]
                len(result)
                
            elif library == 'polars':
                df = pl.DataFrame(data)
                result = df.filter(pl.col('value1') > 0)
                len(result)
                
            elif library == 'vectrill':
                df = VectrillDataFrame(pd.DataFrame(data))
                result = df.filter(col('value1') > 0)
                len(result)
                
            return time.time() - start_time, True
            
        except Exception as e:
            print(f"  {library} filter error: {e}")
            return 0.0, False
    
    def benchmark_groupby(self, data: Dict[str, Any], library: str) -> Tuple[float, bool]:
        """Benchmark groupby aggregation"""
        try:
            start_time = time.time()
            
            if library == 'pandas':
                df = pd.DataFrame(data)
                result = df.groupby('group')['value1'].sum()
                len(result)
                
            elif library == 'polars':
                df = pl.DataFrame(data)
                result = df.group_by('group').agg(pl.col('value1').sum())
                len(result)
                
            elif library == 'vectrill':
                df = VectrillDataFrame(pd.DataFrame(data))
                result = df.groupby('group').agg({'value1': 'sum'})
                len(result)
                
            return time.time() - start_time, True
            
        except Exception as e:
            print(f"  {library} groupby error: {e}")
            return 0.0, False
    
    def benchmark_with_column(self, data: Dict[str, Any], library: str) -> Tuple[float, bool]:
        """Benchmark adding a new column"""
        try:
            start_time = time.time()
            
            if library == 'pandas':
                df = pd.DataFrame(data)
                result = df.assign(new_col=df['value1'] * 2)
                len(result)
                
            elif library == 'polars':
                df = pl.DataFrame(data)
                result = df.with_columns((pl.col('value1') * 2).alias('new_col'))
                len(result)
                
            elif library == 'vectrill':
                df = VectrillDataFrame(pd.DataFrame(data))
                result = df.with_column(col('value1') * 2, 'new_col')
                len(result)
                
            return time.time() - start_time, True
            
        except Exception as e:
            print(f"  {library} with_column error: {e}")
            return 0.0, False
    
    def benchmark_sort(self, data: Dict[str, Any], library: str) -> Tuple[float, bool]:
        """Benchmark sort operation"""
        try:
            start_time = time.time()
            
            if library == 'pandas':
                df = pd.DataFrame(data)
                result = df.sort_values('value1')
                len(result)
                
            elif library == 'polars':
                df = pl.DataFrame(data)
                result = df.sort('value1')
                len(result)
                
            elif library == 'vectrill':
                df = VectrillDataFrame(pd.DataFrame(data))
                result = df.sort('value1')
                len(result)
                
            return time.time() - start_time, True
            
        except Exception as e:
            print(f"  {library} sort error: {e}")
            return 0.0, False
    
    def benchmark_window_function(self, data: Dict[str, Any], library: str) -> Tuple[float, bool]:
        """Benchmark window function (mean)"""
        try:
            start_time = time.time()
            
            if library == 'pandas':
                df = pd.DataFrame(data)
                df = df.sort_values('timestamp')
                result = df.groupby('group')['value1'].transform('mean')
                len(result)
                
            elif library == 'polars':
                df = pl.DataFrame(data)
                result = df.sort('timestamp').with_columns(
                    pl.col('value1').mean().over('group')
                )
                len(result)
                
            elif library == 'vectrill':
                df = VectrillDataFrame(pd.DataFrame(data))
                result = df.sort('timestamp').with_columns([
                    functions.mean(col('value1')).over(window.partition_by('group')).alias('mean_value')
                ])
                len(result)
                
            return time.time() - start_time, True
            
        except Exception as e:
            print(f"  {library} window_function error: {e}")
            return 0.0, False
    
    def benchmark_rolling_function(self, data: Dict[str, Any], library: str) -> Tuple[float, bool]:
        """Benchmark rolling mean function"""
        try:
            start_time = time.time()
            
            if library == 'pandas':
                df = pd.DataFrame(data)
                df = df.sort_values('timestamp')
                result = df['value1'].rolling(window=10, min_periods=1).mean()
                len(result)
                
            elif library == 'polars':
                df = pl.DataFrame(data)
                result = df.sort('timestamp').with_columns(
                    pl.col('value1').rolling_mean(window_size=10, min_periods=1)
                )
                len(result)
                
            elif library == 'vectrill':
                df = VectrillDataFrame(pd.DataFrame(data))
                result = df.sort('timestamp').with_columns([
                    functions.rolling_mean(col('value1'), 10).alias('rolling_mean')
                ])
                len(result)
                
            return time.time() - start_time, True
            
        except Exception as e:
            print(f"  {library} rolling_function error: {e}")
            return 0.0, False
    
    def benchmark_complex_expression(self, data: Dict[str, Any], library: str) -> Tuple[float, bool]:
        """Benchmark complex arithmetic expression"""
        try:
            start_time = time.time()
            
            if library == 'pandas':
                df = pd.DataFrame(data)
                result = np.sqrt(df['value1']**2 + df['value2']**2)
                len(result)
                
            elif library == 'polars':
                df = pl.DataFrame(data)
                result = df.with_columns(
                    (pl.col('value1').pow(2) + pl.col('value2').pow(2)).sqrt().alias('magnitude')
                )
                len(result)
                
            elif library == 'vectrill':
                df = VectrillDataFrame(pd.DataFrame(data))
                result = df.with_column(
                    functions.sqrt(functions.pow(col('value1'), 2) + functions.pow(col('value2'), 2)),
                    'magnitude'
                )
                len(result)
                
            return time.time() - start_time, True
            
        except Exception as e:
            print(f"  {library} complex_expression error: {e}")
            return 0.0, False
    
    def run_benchmark(self):
        """Run comprehensive benchmark"""
        print("Running comprehensive benchmark...")
        print(f"Libraries: {', '.join(self.libraries)}")
        print(f"Data sizes: {', '.join(map(str, self.data_sizes))}")
        print()
        
        operations = [
            ('filter', self.benchmark_filter),
            ('groupby', self.benchmark_groupby),
            ('with_column', self.benchmark_with_column),
            ('sort', self.benchmark_sort),
            ('window_function', self.benchmark_window_function),
            ('rolling_function', self.benchmark_rolling_function),
            ('complex_expression', self.benchmark_complex_expression),
        ]
        
        for size in self.data_sizes:
            print(f"Testing with {size:,} rows:")
            data = self.generate_data(size)
            
            for library in self.libraries:
                print(f"  {library}:")
                library_results = {}
                
                for op_name, op_func in operations:
                    time_taken, success = op_func(data, library)
                    library_results[op_name] = {
                        'time': time_taken,
                        'success': success
                    }
                    if success:
                        print(f"    {op_name}: {time_taken:.4f}s")
                
                self.results.append({
                    'library': library,
                    'data_size': size,
                    'results': library_results
                })
            
            print()
    
    def generate_summary(self):
        """Generate comprehensive summary"""
        print("\n" + "="*60)
        print("COMPREHENSIVE BENCHMARK SUMMARY")
        print("="*60)
        
        # Calculate averages and performance ratios
        summary_data = {}
        
        for size in self.data_sizes:
            summary_data[size] = {}
            
            for op in ['filter', 'groupby', 'with_column', 'sort', 'window_function', 'rolling_function', 'complex_expression']:
                summary_data[size][op] = {}
                
                for library in self.libraries:
                    # Find the result for this library, size, and operation
                    result = None
                    for r in self.results:
                        if r['library'] == library and r['data_size'] == size:
                            result = r['results'].get(op, {})
                            break
                    
                    if result and result.get('success', False):
                        summary_data[size][op][library] = result['time']
        
        # Create performance table
        print("\nPerformance Comparison (seconds):")
        print("-" * 80)
        
        for size in self.data_sizes:
            print(f"\nData Size: {size:,} rows")
            print(f"{'Operation':<20} {'Pandas':<10} {'Polars':<10} {'Vectrill':<10}")
            print("-" * 50)
            
            for op in ['filter', 'groupby', 'with_column', 'sort', 'window_function', 'rolling_function', 'complex_expression']:
                pandas_time = summary_data[size][op].get('pandas', 0.0)
                polars_time = summary_data[size][op].get('polars', 0.0)
                vectrill_time = summary_data[size][op].get('vectrill', 0.0)
                
                print(f"{op:<20} {pandas_time:<10.4f} {polars_time:<10.4f} {vectrill_time:<10.4f}")
        
        # Calculate performance ratios
        print("\nPerformance Ratios (Vectrill vs Pandas):")
        print("-" * 50)
        
        for size in self.data_sizes:
            print(f"\nData Size: {size:,} rows")
            print(f"{'Operation':<20} {'Ratio':<10}")
            print("-" * 30)
            
            for op in ['filter', 'groupby', 'with_column', 'sort', 'window_function', 'rolling_function', 'complex_expression']:
                pandas_time = summary_data[size][op].get('pandas', 0.0)
                vectrill_time = summary_data[size][op].get('vectrill', 0.0)
                
                if pandas_time > 0 and vectrill_time > 0:
                    ratio = vectrill_time / pandas_time
                    print(f"{op:<20} {ratio:<10.2f}x")
        
        # Save detailed results
        results_file = os.path.join(os.path.dirname(__file__), '..', '..', 'comprehensive_benchmark_results.json')
        with open(results_file, 'w') as f:
            json.dump(self.results, f, indent=2)
        
        print(f"\nDetailed results saved to: {results_file}")

if __name__ == "__main__":
    benchmark = ComprehensiveBenchmark()
    benchmark.run_benchmark()
    benchmark.generate_summary()
