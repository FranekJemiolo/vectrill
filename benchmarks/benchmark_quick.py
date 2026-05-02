#!/usr/bin/env python3
"""
Quick benchmark test with smaller dataset to verify functionality
"""

import time
import sys
import os
import numpy as np
import pandas as pd
import polars as pl

# Add vectrill to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'python'))

try:
    from vectrill.dataframe import VectrillDataFrame, col, functions
    VECTRILL_AVAILABLE = True
    print("✓ Vectrill available")
except ImportError as e:
    print(f"✗ Vectrill not available - {e}")
    VECTRILL_AVAILABLE = False

def quick_benchmark():
    """Run quick benchmark with smaller dataset"""
    print("Running quick benchmark test...")
    
    # Smaller dataset for quick testing
    data_sizes = [1000, 10000]
    operations = ['filter', 'groupby_sum', 'with_column']
    libraries = ['pandas', 'polars']
    if VECTRILL_AVAILABLE:
        libraries.append('vectrill')
    
    results = []
    
    for data_size in data_sizes:
        print(f"\nTesting with {data_size:,} rows:")
        
        # Generate test data
        np.random.seed(42)
        data = {
            'id': range(data_size),
            'group': np.random.choice(['A', 'B', 'C', 'D'], data_size),
            'value1': np.random.randn(data_size) * 100,
            'value2': np.random.randint(1, 1000, data_size),
        }
        
        pandas_df = pd.DataFrame(data)
        polars_df = pl.DataFrame(data)
        
        for library in libraries:
            print(f"  {library}: ", end='')
            
            # Get appropriate dataframe
            if library == 'pandas':
                df = pandas_df
            elif library == 'polars':
                df = polars_df
            elif library == 'vectrill':
                df = VectrillDataFrame(pandas_df)
            
            library_results = []
            
            for operation in operations:
                try:
                    start_time = time.perf_counter()
                    
                    if operation == 'filter':
                        if library == 'pandas':
                            result = df[df['value1'] > 0]
                        elif library == 'polars':
                            result = df.filter(pl.col('value1') > 0)
                        elif library == 'vectrill':
                            result = df.filter(col('value1') > 0)
                    
                    elif operation == 'groupby_sum':
                        if library == 'pandas':
                            result = df.groupby('group')['value1'].sum().reset_index()
                        elif library == 'polars':
                            result = df.group_by('group').agg(pl.col('value1').sum())
                        elif library == 'vectrill':
                            result = df.group_by('group').agg(functions.sum('value1').alias('value1_sum'))
                    
                    elif operation == 'with_column':
                        if library == 'pandas':
                            result = df.assign(new_col=df['value1'] * 2)
                        elif library == 'polars':
                            result = df.with_columns((pl.col('value1') * 2).alias('new_col'))
                        elif library == 'vectrill':
                            result = df.with_column(col('value1') * 2, 'new_col')
                    
                    end_time = time.perf_counter()
                    exec_time = end_time - start_time
                    library_results.append(exec_time)
                    print(f"{operation}={exec_time:.4f}s ", end='')
                    
                except Exception as e:
                    print(f"{operation}=ERROR({str(e)[:20]}...) ", end='')
                    library_results.append(0)
            
            avg_time = sum(library_results) / len(library_results) if library_results else 0
            print(f"avg={avg_time:.4f}s")
    
    print("\n✓ Quick benchmark completed successfully!")

if __name__ == "__main__":
    quick_benchmark()
