#!/usr/bin/env python3
import sys
sys.path.insert(0, 'tests/python')
sys.path.insert(0, 'python')

# Debug window function fix in detail
import vectrill
from vectrill.dataframe import col, functions, window
import pandas as pd
import numpy as np

# Create simple test data
data = pd.DataFrame({
    'user_id': [1, 1, 1, 2, 2, 2],
    'amount': [100, 200, 300, 150, 250, 350],
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

# Pandas implementation
print("\n=== Pandas Implementation ===")
pandas_result = data.copy()
pandas_result = pandas_result.sort_values(['user_id', 'timestamp'])
pandas_result['prev_amount'] = pandas_result.groupby('user_id')['amount'].shift(1)
print("Pandas result (sorted):")
print(pandas_result[['user_id', 'timestamp', 'amount', 'prev_amount']])

# Test vectrill step by step
print("\n=== Vectrill Step-by-Step Debug ===")
try:
    vectrill_df = vectrill.from_pandas(data)
    print("1. Created vectrill DataFrame")
    
    # Sort first
    vectrill_sorted = vectrill_df.sort(['user_id', 'timestamp'])
    print("2. Sorted DataFrame:")
    print(vectrill_sorted.to_pandas())
    
    # Apply lag function
    vectrill_with_lag = vectrill_sorted.with_columns([
        functions.lag(col('amount'), 1).over(window.partition_by('user_id').order_by('timestamp')).alias('prev_amount')
    ])
    print("3. After lag function:")
    print(vectrill_with_lag.to_pandas())
    
    # Compare results
    pandas_sorted = pandas_result.sort_values(['user_id', 'timestamp']).reset_index(drop=True)
    vectrill_result = vectrill_with_lag.to_pandas().sort_values(['user_id', 'timestamp']).reset_index(drop=True)
    
    print("\n=== Detailed Comparison ===")
    print("Pandas prev_amount:")
    print(pandas_sorted['prev_amount'].tolist())
    print("Vectrill prev_amount:")
    print(vectrill_result['prev_amount'].tolist())
    
    # Check if they match
    match = pandas_sorted['prev_amount'].equals(vectrill_result['prev_amount'])
    print(f"Results match: {match}")
    
    if not match:
        # Find differences
        diff_rows = []
        for i in range(len(pandas_sorted)):
            pandas_val = pandas_sorted.iloc[i]['prev_amount']
            vectrill_val = vectrill_result.iloc[i]['prev_amount']
            
            if pd.isna(pandas_val) and pd.isna(vectrill_val):
                continue
            elif pandas_val != vectrill_val:
                diff_rows.append({
                    'row': i,
                    'user_id': pandas_sorted.iloc[i]['user_id'],
                    'pandas': pandas_val,
                    'vectrill': vectrill_val
                })
        
        if diff_rows:
            print("Differences found:")
            for diff in diff_rows:
                print(f"  Row {diff['row']} (user_id={diff['user_id']}): pandas={diff['pandas']}, vectrill={diff['vectrill']}")

except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()
