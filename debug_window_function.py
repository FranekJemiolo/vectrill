#!/usr/bin/env python3
import sys
sys.path.insert(0, 'tests/python')
sys.path.insert(0, 'python')

# Debug window function implementation
import vectrill
from vectrill.dataframe import col, functions, window
import pandas as pd
import numpy as np

# Create simple test data
data = pd.DataFrame({
    'user_id': [1, 1, 1, 2, 2, 2],
    'timestamp': [
        pd.Timestamp('2023-01-01 10:00:00'),
        pd.Timestamp('2023-01-01 10:01:00'),
        pd.Timestamp('2023-01-01 10:02:00'),
        pd.Timestamp('2023-01-01 10:00:30'),
        pd.Timestamp('2023-01-01 10:01:30'),
        pd.Timestamp('2023-01-01 10:02:30')
    ]
})

print("Original data:")
print(data)

# Pandas implementation
print("\n=== Pandas Implementation ===")
pandas_result = data.copy()
pandas_result = pandas_result.sort_values(['user_id', 'timestamp'])
pandas_result['prev_timestamp'] = pandas_result.groupby('user_id')['timestamp'].shift(1)
print("Pandas result:")
print(pandas_result)

# Vectrill implementation
print("\n=== Vectrill Implementation ===")
try:
    vectrill_df = vectrill.from_pandas(data)
    print("Created vectrill DataFrame")
    
    # Sort first
    vectrill_sorted = vectrill_df.sort(['user_id', 'timestamp'])
    print("After sorting:")
    print(vectrill_sorted.to_pandas())
    
    # Apply lag function
    vectrill_with_lag = vectrill_sorted.with_columns([
        functions.lag(col('timestamp'), 1).over(window.partition_by('user_id').order_by('timestamp')).alias('prev_timestamp')
    ])
    
    print("After lag:")
    print(vectrill_with_lag.to_pandas())
    
    # Compare
    pandas_sorted = pandas_result.sort_values(['user_id', 'timestamp']).reset_index(drop=True)
    vectrill_result = vectrill_with_lag.to_pandas().sort_values(['user_id', 'timestamp']).reset_index(drop=True)
    
    print("\n=== Comparison ===")
    print("Pandas prev_timestamp:")
    print(pandas_sorted['prev_timestamp'].tolist())
    print("Vectrill prev_timestamp:")
    print(vectrill_result['prev_timestamp'].tolist())
    
    # Check if they match
    match = pandas_sorted['prev_timestamp'].equals(vectrill_result['prev_timestamp'])
    print(f"Results match: {match}")

except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()
