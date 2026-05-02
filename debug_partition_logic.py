#!/usr/bin/env python3
import sys
sys.path.insert(0, 'tests/python')
sys.path.insert(0, 'python')

# Debug partition logic in window functions
import vectrill
from vectrill.dataframe import col, functions, window
import pandas as pd
import numpy as np

# Create simple test data
data = pd.DataFrame({
    'group': ['X', 'X', 'Y', 'Y'],
    'value': [1, 2, 3, 4],
    'date': pd.date_range('2023-01-01', periods=4)
})

print("Testing partition logic:")

try:
    # Create window expression
    cumsum_expr = functions.cumsum(col('value')).over(window.partition_by('group').order_by('date'))
    print(f"Window expression: {cumsum_expr}")
    print(f"Window expression type: {type(cumsum_expr)}")
    
    # Check window spec
    window_spec = cumsum_expr.window_spec
    print(f"Window spec: {window_spec}")
    print(f"Window spec type: {type(window_spec)}")
    
    # Check if window spec has partition columns
    if hasattr(window_spec, 'partition_columns'):
        print(f"Partition columns: {window_spec.partition_columns}")
    if hasattr(window_spec, 'order_columns'):
        print(f"Order columns: {window_spec.order_columns}")
    
    # Check if window spec has to_rust_spec method
    if hasattr(window_spec, 'to_rust_spec'):
        rust_spec = window_spec.to_rust_spec()
        print(f"Rust spec: {rust_spec}")
        print(f"Rust spec type: {type(rust_spec)}")
        
        # Check partition columns in rust spec
        if hasattr(rust_spec, '_partition_columns'):
            print(f"Rust spec partition columns: {rust_spec._partition_columns}")
        if hasattr(rust_spec, '_order_columns'):
            print(f"Rust spec order columns: {rust_spec._order_columns}")
    
    # Test the actual window function
    vectrill_df = vectrill.from_pandas(data)
    result_df = vectrill_df.with_columns([cumsum_expr.alias('cumsum_value')])
    print("Result:")
    print(result_df.to_pandas())
    
    # Compare with pandas
    pandas_result = data.copy()
    pandas_sorted = pandas_result.sort_values(['group', 'date'])
    pandas_sorted['cumsum_value'] = pandas_sorted.groupby('group')['value'].cumsum()
    pandas_result = pandas_sorted.sort_index()
    print("Pandas result:")
    print(pandas_result)
    
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()
