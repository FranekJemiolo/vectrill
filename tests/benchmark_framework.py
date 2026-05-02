#!/usr/bin/env python3
"""
Comprehensive benchmarking framework for Vectrill, pandas, and polars.
This framework provides systematic performance testing and correctness verification.
"""

import sys
import os
import time
import gc
import numpy as np
import pandas as pd
import polars as pl
from typing import Dict, List, Tuple, Any, Callable
import warnings
warnings.filterwarnings('ignore')

# Add vectrill to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'python'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

try:
    import vectrill
    from vectrill.dataframe import col, functions, window
    VECTRILL_AVAILABLE = True
except ImportError:
    VECTRILL_AVAILABLE = False
    print("Warning: Vectrill not available")

class BenchmarkResult:
    """Container for benchmark results"""
    
    def __init__(self, operation: str, data_size: int):
        self.operation = operation
        self.data_size = data_size
        self.vectrill_time = None
        self.pandas_time = None
        self.polars_time = None
        self.vectrill_result = None
        self.pandas_result = None
        self.polars_result = None
        self.vectrill_correct = None
        self.polars_correct = None
        self.error_message = None
    
    def add_timing(self, library: str, execution_time: float):
        """Add timing result for a library"""
        setattr(self, f"{library}_time", execution_time)
    
    def add_result(self, library: str, result):
        """Add result for a library"""
        setattr(self, f"{library}_result", result)
    
    def add_correctness(self, library: str, is_correct: bool):
        """Add correctness result for a library"""
        setattr(self, f"{library}_correct", is_correct)
    
    def summary(self) -> Dict[str, Any]:
        """Get summary of benchmark results"""
        return {
            'operation': self.operation,
            'data_size': self.data_size,
            'vectrill_time': self.vectrill_time,
            'pandas_time': self.pandas_time,
            'polars_time': self.polars_time,
            'vectrill_correct': self.vectrill_correct,
            'polars_correct': self.polars_correct,
            'speedup_vs_pandas': self.pandas_time / self.vectrill_time if self.vectrill_time and self.pandas_time else None,
            'speedup_vs_polars': self.polars_time / self.vectrill_time if self.vectrill_time and self.polars_time else None
        }

class BenchmarkFramework:
    """Comprehensive benchmarking framework"""
    
    def __init__(self):
        self.results: List[BenchmarkResult] = []
        self.test_data_generators = {}
        self.operation_tests = {}
    
    def add_data_generator(self, name: str, generator: Callable[[int], pd.DataFrame]):
        """Add a test data generator"""
        self.test_data_generators[name] = generator
    
    def add_operation_test(self, name: str, test_func: Callable[[Any, Any], Any]):
        """Add an operation test"""
        self.operation_tests[name] = test_func
    
    def generate_test_data(self, generator_name: str, size: int) -> pd.DataFrame:
        """Generate test data"""
        if generator_name not in self.test_data_generators:
            raise ValueError(f"Unknown data generator: {generator_name}")
        return self.test_data_generators[generator_name](size)
    
    def measure_time(self, func: Callable, *args, **kwargs) -> Tuple[float, Any]:
        """Measure execution time of a function"""
        gc.collect()  # Force garbage collection
        start_time = time.perf_counter()
        try:
            result = func(*args, **kwargs)
            end_time = time.perf_counter()
            return end_time - start_time, result
        except Exception as e:
            end_time = time.perf_counter()
            return end_time - start_time, None
    
    def run_benchmark(self, operation_name: str, data_generator: str, data_size: int) -> BenchmarkResult:
        """Run a single benchmark"""
        if operation_name not in self.operation_tests:
            raise ValueError(f"Unknown operation: {operation_name}")
        
        result = BenchmarkResult(operation_name, data_size)
        
        # Generate test data
        test_data = self.generate_test_data(data_generator, data_size)
        
        # Test pandas
        try:
            pandas_time, pandas_result = self.measure_time(
                self.operation_tests[operation_name], test_data.copy(), 'pandas'
            )
            result.add_timing('pandas', pandas_time)
            result.add_result('pandas', pandas_result)
        except Exception as e:
            result.error_message = f"Pandas error: {str(e)}"
        
        # Test polars
        try:
            polars_time, polars_result = self.measure_time(
                self.operation_tests[operation_name], test_data.copy(), 'polars'
            )
            result.add_timing('polars', polars_time)
            result.add_result('polars', polars_result)
        except Exception as e:
            result.error_message = f"Polars error: {str(e)}"
        
        # Test vectrill
        if VECTRILL_AVAILABLE:
            try:
                vectrill_time, vectrill_result = self.measure_time(
                    self.operation_tests[operation_name], test_data.copy(), 'vectrill'
                )
                result.add_timing('vectrill', vectrill_time)
                result.add_result('vectrill', vectrill_result)
            except Exception as e:
                result.error_message = f"Vectrill error: {str(e)}"
        
        # Check correctness
        if result.pandas_result is not None and result.vectrill_result is not None:
            result.add_correctness('vectrill', self._compare_results(result.pandas_result, result.vectrill_result))
        
        if result.pandas_result is not None and result.polars_result is not None:
            result.add_correctness('polars', self._compare_results(result.pandas_result, result.polars_result))
        
        self.results.append(result)
        return result
    
    def _compare_results(self, expected: Any, actual: Any) -> bool:
        """Compare results for correctness"""
        try:
            if isinstance(expected, pd.DataFrame) and isinstance(actual, pd.DataFrame):
                return expected.equals(actual)
            elif isinstance(expected, pd.Series) and isinstance(actual, pd.Series):
                return expected.equals(actual)
            elif isinstance(expected, np.ndarray) and isinstance(actual, np.ndarray):
                return np.array_equal(expected, actual, equal_nan=True)
            else:
                return expected == actual
        except Exception:
            return False
    
    def run_benchmark_suite(self, operations: List[str], data_sizes: List[int], data_generator: str):
        """Run a suite of benchmarks"""
        results = []
        for operation in operations:
            for size in data_sizes:
                print(f"Running {operation} with {size} rows...")
                result = self.run_benchmark(operation, data_generator, size)
                results.append(result)
                print(f"  Pandas: {result.pandas_time:.4f}s")
                print(f"  Polars: {result.polars_time:.4f}s")
                if result.vectrill_time:
                    print(f"  Vectrill: {result.vectrill_time:.4f}s")
                    print(f"  Correct: {result.vectrill_correct}")
                print()
        return results
    
    def generate_report(self) -> pd.DataFrame:
        """Generate a comprehensive benchmark report"""
        summaries = [result.summary() for result in self.results]
        return pd.DataFrame(summaries)
    
    def identify_slow_operations(self, threshold_factor: float = 2.0) -> List[str]:
        """Identify operations that are significantly slower than pandas"""
        slow_ops = []
        for result in self.results:
            if (result.vectrill_time and result.pandas_time and 
                result.vectrill_time > result.pandas_time * threshold_factor):
                slow_ops.append(f"{result.operation} ({result.data_size} rows)")
        return slow_ops

# Data Generators
def generate_user_activity_data(size: int) -> pd.DataFrame:
    """Generate user activity data for testing"""
    np.random.seed(42)
    num_users = max(100, size // 100)
    
    data = []
    for i in range(size):
        user_id = np.random.randint(1, num_users + 1)
        timestamp = pd.Timestamp('2023-01-01') + pd.Timedelta(seconds=np.random.randint(0, 86400 * 30))
        amount = np.random.uniform(1, 1000)
        data.append({'user_id': user_id, 'timestamp': timestamp, 'amount': amount})
    
    return pd.DataFrame(data)

def generate_sensor_data(size: int) -> pd.DataFrame:
    """Generate IoT sensor data for testing"""
    np.random.seed(42)
    num_sensors = max(50, size // 200)
    
    data = []
    for i in range(size):
        sensor_id = np.random.randint(1, num_sensors + 1)
        timestamp = pd.Timestamp('2023-01-01') + pd.Timedelta(seconds=i)
        temperature = np.random.normal(25, 5)
        humidity = np.random.normal(60, 10)
        data.append({'sensor_id': sensor_id, 'timestamp': timestamp, 
                    'temperature': temperature, 'humidity': humidity})
    
    return pd.DataFrame(data)

def generate_time_series_data(size: int) -> pd.DataFrame:
    """Generate time series data for testing"""
    np.random.seed(42)
    
    timestamps = pd.date_range('2023-01-01', periods=size, freq='1min')
    values = np.random.randn(size).cumsum() + 100
    
    return pd.DataFrame({'timestamp': timestamps, 'value': values})

# Operation Tests
def test_filter_operation(data: pd.DataFrame, library: str) -> Any:
    """Test filter operation"""
    if library == 'pandas':
        return data[data['amount'] > 500]
    elif library == 'polars':
        df = pl.DataFrame(data)
        return df.filter(pl.col('amount') > 500).to_pandas()
    elif library == 'vectrill':
        df = vectrill.from_pandas(data)
        return df.filter({"op": ">", "col": "amount", "value": 500}).to_pandas()

def test_sort_operation(data: pd.DataFrame, library: str) -> Any:
    """Test sort operation"""
    if library == 'pandas':
        return data.sort_values(['user_id', 'timestamp'])
    elif library == 'polars':
        df = pl.DataFrame(data)
        return df.sort(['user_id', 'timestamp']).to_pandas()
    elif library == 'vectrill':
        df = vectrill.from_pandas(data)
        return df.sort(['user_id', 'timestamp']).to_pandas()

def test_aggregation_operation(data: pd.DataFrame, library: str) -> Any:
    """Test aggregation operation"""
    if library == 'pandas':
        return data.groupby('user_id')['amount'].sum().reset_index()
    elif library == 'polars':
        df = pl.DataFrame(data)
        return df.groupby('user_id').agg(pl.col('amount').sum()).to_pandas()
    elif library == 'vectrill':
        # This would need to be implemented in Vectrill
        raise NotImplementedError("Aggregation not yet implemented in Vectrill")

def test_window_operation(data: pd.DataFrame, library: str) -> Any:
    """Test window operation (lag function)"""
    if library == 'pandas':
        sorted_data = data.sort_values(['user_id', 'timestamp'])
        sorted_data['prev_amount'] = sorted_data.groupby('user_id')['amount'].shift(1)
        return sorted_data
    elif library == 'polars':
        df = pl.DataFrame(data)
        result = df.sort(['user_id', 'timestamp']).with_columns([
            pl.col('amount').shift(1).over('user_id').alias('prev_amount')
        ])
        return result.to_pandas()
    elif library == 'vectrill':
        df = vectrill.from_pandas(data)
        result = df.sort(['user_id', 'timestamp']).with_columns([
            functions.lag(col('amount'), 1).over(window.partition_by('user_id').order_by('timestamp')).alias('prev_amount')
        ])
        return result.to_pandas()

# Initialize the framework
framework = BenchmarkFramework()

# Add data generators
framework.add_data_generator('user_activity', generate_user_activity_data)
framework.add_data_generator('sensor', generate_sensor_data)
framework.add_data_generator('time_series', generate_time_series_data)

# Add operation tests
framework.add_operation_test('filter', test_filter_operation)
framework.add_operation_test('sort', test_sort_operation)
framework.add_operation_test('aggregation', test_aggregation_operation)
framework.add_operation_test('window', test_window_operation)

if __name__ == "__main__":
    print("Vectrill Benchmarking Framework")
    print("=" * 50)
    
    # Run basic benchmarks
    operations = ['filter', 'sort', 'window']
    data_sizes = [1000, 10000, 100000]
    
    print("Running basic benchmarks...")
    results = framework.run_benchmark_suite(operations, data_sizes, 'user_activity')
    
    # Generate report
    report = framework.generate_report()
    print("\nBenchmark Report:")
    print(report.to_string(index=False))
    
    # Identify slow operations
    slow_ops = framework.identify_slow_operations()
    if slow_ops:
        print(f"\nSlow operations (>{2}x slower than pandas):")
        for op in slow_ops:
            print(f"  - {op}")
    
    # Save results
    report.to_csv('benchmark_results.csv', index=False)
    print(f"\nResults saved to benchmark_results.csv")
