#!/usr/bin/env python3
import sys
sys.path.insert(0, 'tests/python')
sys.path.insert(0, 'python')

# Debug cumsum window function
import vectrill
from vectrill.dataframe import col, functions, window
import pandas as pd
import numpy as np

# Create time series data
dates = pd.date_range('2023-01-01', periods=10, freq='D')
data = pd.DataFrame({
    'date': dates,
    'value': np.random.randn(10).cumsum() + 100,
    'group': np.random.choice(['X', 'Y', 'Z'], 10)
})

print("Testing cumsum window function:")
print("Original data:")
print(data)

# Test pandas implementation
print("\n=== Pandas Implementation ===")
pandas_sorted = data.sort_values(['group', 'date'])
pandas_sorted['cumsum_value'] = pandas_sorted.groupby('group')['value'].cumsum()
pandas_result = pandas_sorted.sort_index()
print("Pandas result:")
print(pandas_result[['group', 'date', 'value', 'cumsum_value']])

# Test Vectrill implementation step by step
print("\n=== Vectrill Implementation ===")
try:
    vectrill_df = vectrill.from_pandas(data)
    print("1. Created vectrill DataFrame")
    
    # Sort first
    vectrill_sorted = vectrill_df.sort(['group', 'date'])
    print("2. Sorted DataFrame:")
    print(vectrill_sorted.to_pandas())
    
    # Apply cumsum function
    vectrill_with_cumsum = vectrill_sorted.with_columns([
        functions.cumsum(col('value')).over(window.partition_by('group').order_by('date')).alias('cumsum_value')
    ])
    print("3. After cumsum function:")
    print(vectrill_with_cumsum.to_pandas())
    
    # Compare results
    pandas_sorted_result = pandas_result.sort_values(['group', 'date']).reset_index(drop=True)
    vectrill_result = vectrill_with_cumsum.to_pandas().sort_values(['group', 'date']).reset_index(drop=True)
    
    print("\n=== Detailed Comparison ===")
    print("Pandas cumsum_value:")
    print(pandas_sorted_result['cumsum_value'].tolist())
    print("Vectrill cumsum_value:")
    print(vectrill_result['cumsum_value'].tolist())
    
    # Check if they match
    match = np.allclose(pandas_sorted_result['cumsum_value'], vectrill_result['cumsum_value'], equal_nan=True)
    print(f"Results match: {match}")
    
    if not match:
        # Find differences
        diff_rows = []
        for i in range(len(pandas_sorted_result)):
            pandas_val = pandas_sorted_result.iloc[i]['cumsum_value']
            vectrill_val = vectrill_result.iloc[i]['cumsum_value']
            
            if not np.isnan(pandas_val) and not np.isnan(vectrill_val):
                if abs(pandas_val - vectrill_val) > 1e-10:
                    diff_rows.append({
                        'row': i,
                        'group': pandas_sorted_result.iloc[i]['group'],
                        'pandas': pandas_val,
                        'vectrill': vectrill_val
                    })
            elif np.isnan(pandas_val) != np.isnan(vectrill_val):
                diff_rows.append({
                    'row': i,
                    'group': pandas_sorted_result.iloc[i]['group'],
                    'pandas': pandas_val,
                    'vectrill': vectrill_val
                })
        
        if diff_rows:
            print("Differences found:")
            for diff in diff_rows[:10]:  # Show first 10 differences
                print(f"  Row {diff['row']} (group={diff['group']}): pandas={diff['pandas']}, vectrill={diff['vectrill']}")

except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()
