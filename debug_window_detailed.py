#!/usr/bin/env python3
import sys
sys.path.insert(0, 'tests/python')
sys.path.insert(0, 'python')

# Debug window function implementation in detail
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
print("Original index:", data.index.tolist())

# Simulate what the window function should do step by step
print("\n=== Manual Window Function Simulation ===")
df_copy = data.copy()

# Add temporary row index
df_copy['_temp_row_idx'] = range(len(df_copy))
print("After adding temp index:")
print(df_copy)

# Sort by partition and order columns
sort_cols = ['user_id', 'timestamp']
df_sorted = df_copy.sort_values(sort_cols)
print("After sorting:")
print(df_sorted)
print("Sorted index:", df_sorted.index.tolist())

# Apply lag function
df_sorted['prev_timestamp'] = df_sorted.groupby('user_id')['timestamp'].shift(1)
print("After applying lag:")
print(df_sorted)

# Merge back to original order
df_result = df_copy.merge(df_sorted[['_temp_row_idx', 'prev_timestamp']], on='_temp_row_idx', how='left')
df_result = df_result.drop(columns=['_temp_row_idx'])
print("After merging back:")
print(df_result)

# Compare with pandas
print("\n=== Pandas Implementation ===")
pandas_result = data.copy()
pandas_result = pandas_result.sort_values(['user_id', 'timestamp'])
pandas_result['prev_timestamp'] = pandas_result.groupby('user_id')['timestamp'].shift(1)
print("Pandas result (sorted):")
print(pandas_result)

# Test vectrill
print("\n=== Vectrill Implementation ===")
try:
    vectrill_df = vectrill.from_pandas(data)
    vectrill_result = vectrill_df.sort(['user_id', 'timestamp']).with_columns([
        functions.lag(col('timestamp'), 1).over(window.partition_by('user_id').order_by('timestamp')).alias('prev_timestamp')
    ])
    print("Vectrill result:")
    print(vectrill_result.to_pandas())
    
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()
