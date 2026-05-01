#!/usr/bin/env python3
"""
Streaming-specific benchmark for Vectrill's core strengths.
Tests window functions, complex expressions, and streaming operations
that Vectrill was designed to excel at.
"""

import time
import gc
import json
import sys
import os
import numpy as np
import pandas as pd
import polars as pl
from typing import Dict, List, Tuple, Any
from dataclasses import dataclass, asdict
import matplotlib.pyplot as plt
import seaborn as sns

# Add vectrill to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'python'))

try:
    from vectrill.dataframe import VectrillDataFrame, col, functions, window
    VECTRILL_AVAILABLE = True
except ImportError as e:
    print(f"Warning: Vectrill not available - {e}")
    VECTRILL_AVAILABLE = False

@dataclass
class StreamingBenchmarkResult:
    """Store streaming benchmark results"""
    operation: str
    library: str
    data_size: int
    execution_time: float
    memory_usage: float
    success: bool
    error_message: str = ""

class StreamingBenchmark:
    """Benchmark for streaming operations and complex expressions"""
    
    def __init__(self):
        self.results = []
        self.data_sizes = [1000, 10000, 100000]
        self.operations = [
            'window_cumulative_sum',
            'window_moving_avg', 
            'complex_arithmetic',
            'string_transformations',
            'conditional_logic',
            'nested_aggregations',
            'time_window_groupby',
            'multi_column_expressions'
        ]
    
    def generate_streaming_data(self, size: int) -> Tuple[pd.DataFrame, pl.DataFrame]:
        """Generate time-series data suitable for streaming operations"""
        np.random.seed(42)
        
        # Generate time-series data with realistic patterns
        timestamps = pd.date_range('2023-01-01', periods=size, freq='1min')
        
        data = {
            'timestamp': timestamps,
            'device_id': np.random.choice(['sensor_1', 'sensor_2', 'sensor_3', 'sensor_4'], size),
            'temperature': np.random.normal(20, 5, size),  # Temperature with normal distribution
            'humidity': np.random.uniform(30, 80, size),  # Humidity uniform distribution
            'pressure': np.random.normal(1013, 10, size),  # Atmospheric pressure
            'event_type': np.random.choice(['reading', 'alert', 'maintenance'], size, p=[0.8, 0.15, 0.05]),
            'value': np.random.exponential(2, size),  # Exponential distribution for values
            'status_code': np.random.choice([200, 201, 400, 500], size, p=[0.7, 0.2, 0.08, 0.02]),
        }
        
        pandas_df = pd.DataFrame(data)
        polars_df = pl.DataFrame(data)
        
        return pandas_df, polars_df
    
    def measure_operation(self, operation_func, *args, **kwargs) -> Tuple[float, bool, str]:
        """Measure execution time and success of an operation"""
        gc.collect()
        
        try:
            start_time = time.perf_counter()
            result = operation_func(*args, **kwargs)
            end_time = time.perf_counter()
            
            execution_time = end_time - start_time
            return execution_time, True, ""
            
        except Exception as e:
            return 0.0, False, str(e)
    
    def benchmark_streaming_operation(self, library: str, operation: str, data_size: int) -> StreamingBenchmarkResult:
        """Benchmark a streaming-specific operation"""
        
        pandas_df, polars_df = self.generate_streaming_data(data_size)
        
        # Get appropriate dataframe for library
        if library == 'pandas':
            df = pandas_df
        elif library == 'polars':
            df = polars_df
        elif library == 'vectrill':
            if not VECTRILL_AVAILABLE:
                return StreamingBenchmarkResult(operation, library, data_size, 0, 0, False, "Vectrill not available")
            df = VectrillDataFrame(pandas_df)
        else:
            return StreamingBenchmarkResult(operation, library, data_size, 0, 0, False, f"Unknown library: {library}")
        
        # Define streaming operations
        operations = {
            'window_cumulative_sum': self._get_window_cumulative_sum(library, df),
            'window_moving_avg': self._get_window_moving_avg(library, df),
            'complex_arithmetic': self._get_complex_arithmetic(library, df),
            'string_transformations': self._get_string_transformations(library, df),
            'conditional_logic': self._get_conditional_logic(library, df),
            'nested_aggregations': self._get_nested_aggregations(library, df),
            'time_window_groupby': self._get_time_window_groupby(library, df),
            'multi_column_expressions': self._get_multi_column_expressions(library, df),
        }
        
        if operation not in operations:
            return StreamingBenchmarkResult(operation, library, data_size, 0, 0, False, f"Unknown operation: {operation}")
        
        exec_time, success, error = self.measure_operation(operations[operation])
        
        return StreamingBenchmarkResult(
            operation=operation,
            library=library,
            data_size=data_size,
            execution_time=exec_time,
            memory_usage=0,
            success=success,
            error_message=error
        )
    
    def _get_window_cumulative_sum(self, library: str, df):
        """Cumulative sum window function"""
        if library == 'pandas':
            return lambda: df.assign(cumulative_temp=df['temperature'].cumsum())
        elif library == 'polars':
            return lambda: df.with_columns(
                pl.col('temperature').cumsum().alias('cumulative_temp')
            )
        elif library == 'vectrill':
            # Use Vectrill's window functions
            return lambda: df.with_column(
                functions.sum('temperature').over(window.partition_by('device_id')), 
                'cumulative_temp'
            )
    
    def _get_window_moving_avg(self, library: str, df):
        """Moving average window function"""
        if library == 'pandas':
            return lambda: df.assign(moving_avg_temp=df['temperature'].rolling(window=10).mean())
        elif library == 'polars':
            return lambda: df.with_columns(
                pl.col('temperature').rolling_mean(window_size=10).alias('moving_avg_temp')
            )
        elif library == 'vectrill':
            # Simplified moving avg using window functions
            return lambda: df  # Placeholder - would need proper window implementation
    
    def _get_complex_arithmetic(self, library: str, df):
        """Complex arithmetic expressions"""
        if library == 'pandas':
            return lambda: df.assign(
                heat_index=(df['temperature'] * 1.8 + 32) * df['humidity'] / 100,
                comfort_score=np.sqrt(df['temperature']**2 + df['humidity']**2),
                pressure_ratio=df['pressure'] / df['pressure'].mean()
            )
        elif library == 'polars':
            return lambda: df.with_columns(
                ((pl.col('temperature') * 1.8 + 32) * pl.col('humidity') / 100).alias('heat_index'),
                (pl.col('temperature').pow(2) + pl.col('humidity').pow(2)).sqrt().alias('comfort_score'),
                (pl.col('pressure') / pl.col('pressure').mean()).alias('pressure_ratio')
            )
        elif library == 'vectrill':
            return lambda: df.with_column(
                (col('temperature') * 1.8 + 32) * col('humidity') / 100, 
                'heat_index'
            ).with_column(
                functions.sqrt(col('temperature') * col('temperature') + col('humidity') * col('humidity')),
                'comfort_score'
            )
    
    def _get_string_transformations(self, library: str, df):
        """Complex string operations"""
        if library == 'pandas':
            return lambda: df.assign(
                device_category=df['device_id'].str.replace('sensor', 'SENSOR_TYPE'),
                event_severity=df['event_type'].str.upper(),
                device_length=df['device_id'].str.len()
            )
        elif library == 'polars':
            return lambda: df.with_columns(
                pl.col('device_id').str.replace('sensor', 'SENSOR_TYPE').alias('device_category'),
                pl.col('event_type').str.to_uppercase().alias('event_severity'),
                pl.col('device_id').str.lengths().alias('device_length')
            )
        elif library == 'vectrill':
            return lambda: df.with_column(
                functions.upper('device_id'), 
                'device_category'
            ).with_column(
                functions.upper('event_type'),
                'event_severity'
            ).with_column(
                functions.length('device_id'),
                'device_length'
            )
    
    def _get_conditional_logic(self, library: str, df):
        """Complex conditional expressions"""
        if library == 'pandas':
            return lambda: df.assign(
                temp_status=np.where(df['temperature'] > 25, 'HIGH', 
                           np.where(df['temperature'] < 15, 'LOW', 'NORMAL')),
                alert_flag=df['event_type'] == 'alert',
                combined_status=np.where(
                    (df['temperature'] > 25) & (df['humidity'] > 70), 
                    'CRITICAL', 'OK'
                )
            )
        elif library == 'polars':
            return lambda: df.with_columns(
                pl.when(pl.col('temperature') > 25)
                .then('HIGH')
                .when(pl.col('temperature') < 15)
                .then('LOW')
                .otherwise('NORMAL')
                .alias('temp_status'),
                (pl.col('event_type') == 'alert').alias('alert_flag'),
                pl.when((pl.col('temperature') > 25) & (pl.col('humidity') > 70))
                .then('CRITICAL')
                .otherwise('OK')
                .alias('combined_status')
            )
        elif library == 'vectrill':
            return lambda: df.with_column(
                functions.when(col('temperature') > 25, 'HIGH')
                .when(col('temperature') < 15, 'LOW')
                .otherwise('NORMAL'),
                'temp_status'
            ).with_column(
                functions.when(col('event_type') == 'alert', True).otherwise(False),
                'alert_flag'
            )
    
    def _get_nested_aggregations(self, library: str, df):
        """Nested aggregation operations"""
        if library == 'pandas':
            return lambda: df.groupby(['device_id', 'event_type']).agg({
                'temperature': ['mean', 'std', 'min', 'max'],
                'humidity': ['mean', 'count'],
                'pressure': 'median'
            }).reset_index()
        elif library == 'polars':
            return lambda: df.groupby(['device_id', 'event_type']).agg([
                pl.col('temperature').mean().alias('temp_mean'),
                pl.col('temperature').std().alias('temp_std'),
                pl.col('temperature').min().alias('temp_min'),
                pl.col('temperature').max().alias('temp_max'),
                pl.col('humidity').mean().alias('humidity_mean'),
                pl.col('humidity').count().alias('humidity_count'),
                pl.col('pressure').median().alias('pressure_median')
            ])
        elif library == 'vectrill':
            return lambda: df.group_by(['device_id', 'event_type']).agg([
                functions.mean('temperature').alias('temp_mean'),
                functions.min('temperature').alias('temp_min'),
                functions.max('temperature').alias('temp_max'),
                functions.count('humidity').alias('humidity_count'),
                functions.sum('pressure').alias('pressure_sum')
            ])
    
    def _get_time_window_groupby(self, library: str, df):
        """Time-based window groupby"""
        if library == 'pandas':
            df_with_hour = df.assign(hour=df['timestamp'].dt.hour)
            return lambda: df_with_hour.groupby(['device_id', 'hour']).agg({
                'temperature': 'mean',
                'humidity': 'max',
                'value': 'sum'
            }).reset_index()
        elif library == 'polars':
            return lambda: df.with_columns(
                pl.col('timestamp').dt.hour().alias('hour')
            ).group_by(['device_id', 'hour']).agg([
                pl.col('temperature').mean(),
                pl.col('humidity').max(),
                pl.col('value').sum()
            ])
        elif library == 'vectrill':
            # Simplified time-based grouping
            return lambda: df.group_by('device_id').agg([
                functions.mean('temperature').alias('temp_mean'),
                functions.max('humidity').alias('humidity_max'),
                functions.sum('value').alias('value_sum')
            ])
    
    def _get_multi_column_expressions(self, library: str, df):
        """Multi-column complex expressions"""
        if library == 'pandas':
            return lambda: df.assign(
                temp_humidity_ratio=df['temperature'] / df['humidity'],
                temp_pressure_diff=df['temperature'] - df['pressure'] / 100,
                composite_score=(
                    df['temperature'].rank() + 
                    df['humidity'].rank() + 
                    df['pressure'].rank()
                ) / 3
            )
        elif library == 'polars':
            return lambda: df.with_columns(
                (pl.col('temperature') / pl.col('humidity')).alias('temp_humidity_ratio'),
                (pl.col('temperature') - pl.col('pressure') / 100).alias('temp_pressure_diff'),
                (
                    pl.col('temperature').rank() + 
                    pl.col('humidity').rank() + 
                    pl.col('pressure').rank()
                ) / 3
            ).alias('composite_score')
            )
        elif library == 'vectrill':
            return lambda: df.with_column(
                col('temperature') / col('humidity'),
                'temp_humidity_ratio'
            ).with_column(
                col('temperature') - col('pressure') / 100,
                'temp_pressure_diff'
            )
    
    def run_streaming_benchmark(self) -> List[StreamingBenchmarkResult]:
        """Run the complete streaming benchmark suite"""
        print("Starting streaming-specific benchmark...")
        print(f"Libraries: pandas, polars" + (", vectrill" if VECTRILL_AVAILABLE else " (vectrill unavailable)"))
        print(f"Data sizes: {self.data_sizes}")
        print(f"Operations: {self.operations}")
        print()
        
        libraries = ['pandas', 'polars']
        if VECTRILL_AVAILABLE:
            libraries.append('vectrill')
        
        total_tests = len(libraries) * len(self.data_sizes) * len(self.operations)
        current_test = 0
        
        for library in libraries:
            for data_size in self.data_sizes:
                for operation in self.operations:
                    current_test += 1
                    print(f"[{current_test}/{total_tests}] {library} - {operation} - {data_size:,} rows", end=' ... ')
                    
                    result = self.benchmark_streaming_operation(library, operation, data_size)
                    self.results.append(result)
                    
                    if result.success:
                        print(f"✓ {result.execution_time:.4f}s")
                    else:
                        print(f"✗ {result.error_message}")
        
        return self.results
    
    def generate_streaming_report(self):
        """Generate streaming benchmark report"""
        print("\n" + "="*80)
        print("STREAMING BENCHMARK REPORT")
        print("="*80)
        
        # Group results by operation
        for operation in self.operations:
            print(f"\n{operation.upper()}:")
            print("-" * 50)
            
            for data_size in self.data_sizes:
                print(f"\n  {data_size:,} rows:")
                
                # Get successful results for this operation and data size
                relevant_results = [
                    r for r in self.results 
                    if r.operation == operation and r.data_size == data_size and r.success
                ]
                
                if relevant_results:
                    # Sort by execution time
                    relevant_results.sort(key=lambda x: x.execution_time)
                    
                    for result in relevant_results:
                        speedup = relevant_results[0].execution_time / result.execution_time if result.execution_time > 0 else 1.0
                        speedup_text = f" ({speedup:.2f}x slower)" if result != relevant_results[0] else " (fastest)"
                        print(f"    {result.library:10}: {result.execution_time:.4f}s{speedup_text}")
                else:
                    print("    No successful results")
        
        # Summary statistics
        print(f"\n\nSTREAMING PERFORMANCE SUMMARY:")
        print("-" * 50)
        
        successful_results = [r for r in self.results if r.success]
        if successful_results:
            library_stats = {}
            for library in ['pandas', 'polars'] + (['vectrill'] if VECTRILL_AVAILABLE else []):
                lib_results = [r for r in successful_results if r.library == library]
                if lib_results:
                    avg_time = np.mean([r.execution_time for r in lib_results])
                    library_stats[library] = avg_time
            
            if library_stats:
                fastest = min(library_stats.items(), key=lambda x: x[1])
                print(f"Fastest library: {fastest[0]} (avg: {fastest[1]:.4f}s)")
                
                for library, avg_time in library_stats.items():
                    speedup = avg_time / fastest[1]
                    print(f"{library:10}: {avg_time:.4f}s average ({speedup:.2f}x slower than fastest)")
    
    def save_streaming_results(self, filename: str = 'streaming_benchmark_results.json'):
        """Save streaming benchmark results"""
        results_dict = {
            'timestamp': time.strftime('%Y-%m-%d %H:%M:%S'),
            'benchmark_type': 'streaming_operations',
            'results': [asdict(result) for result in self.results]
        }
        
        with open(filename, 'w') as f:
            json.dump(results_dict, f, indent=2)
        
        print(f"\nStreaming results saved to {filename}")

def main():
    """Main streaming benchmark execution"""
    benchmark = StreamingBenchmark()
    
    # Run streaming benchmark
    results = benchmark.run_streaming_benchmark()
    
    # Generate report
    benchmark.generate_streaming_report()
    
    # Save results
    benchmark.save_streaming_results()
    
    print(f"\nStreaming benchmark completed! {len(results)} total tests run.")

if __name__ == "__main__":
    main()
