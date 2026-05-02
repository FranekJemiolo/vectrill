#!/usr/bin/env python3
import sys
sys.path.insert(0, 'tests/python')
sys.path.insert(0, 'python')

# Debug cumsum window function in detail
import vectrill
from vectrill.dataframe import col, functions, window
import pandas as pd
import numpy as np

# Create time series data (same as in the test)
dates = pd.date_range('2023-01-01', periods=50, freq='D')
data = pd.DataFrame({
    'date': dates,
    'value': np.random.randn(50).cumsum() + 100,
    'group': np.random.choice(['X', 'Y', 'Z'], 50)
})

print("Testing cumsum window function (same as test):")

# Test pandas implementation step by step
print("\n=== Pandas Implementation Step by Step ===")
pandas_step1 = data.copy()
print("Step 1 - Original:")
print(pandas_step1.head(10))

pandas_step2 = pandas_step1.sort_values(['group', 'date'])
print("Step 2 - Sorted:")
print(pandas_step2.head(10))

pandas_step3 = pandas_step2.copy()
pandas_step3['cumsum_value'] = pandas_step3.groupby('group')['value'].cumsum()
print("Step 3 - After cumsum:")
print(pandas_step3.head(10))

pandas_result = pandas_step3.sort_index()
print("Step 4 - Restored original order:")
print(pandas_result.head(10))

# Test Vectrill implementation step by step
print("\n=== Vectrill Implementation Step by Step ===")
try:
    # Step 1: Create Vectrill DataFrame
    vectrill_df = vectrill.from_pandas(data)
    print("Step 1 - Created vectrill DataFrame")
    
    # Step 2: Sort and apply cumsum
    vectrill_with_cumsum = vectrill_df.sort(['group', 'date']).with_columns([
        functions.cumsum(col('value')).over(window.partition_by('group').order_by('date')).alias('cumsum_value')
    ])
    print("Step 2 - Sorted and applied cumsum:")
    print(vectrill_with_cumsum.to_pandas().head(10))
    
    # Step 3: Compare results
    pandas_sorted_result = pandas_result.sort_values(['group', 'date']).reset_index(drop=True)
    vectrill_result = vectrill_with_cumsum.to_pandas().sort_values(['group', 'date']).reset_index(drop=True)
    
    print("\n=== Detailed Comparison ===")
    print("Pandas cumsum_value (first 10):")
    print(pandas_sorted_result['cumsum_value'].head(10).tolist())
    print("Vectrill cumsum_value (first 10):")
    print(vectrill_result['cumsum_value'].head(10).tolist())
    
    # Check if they match
    match = np.allclose(pandas_sorted_result['cumsum_value'], vectrill_result['cumsum_value'], equal_nan=True)
    print(f"Results match: {match}")
    
    if not match:
        # Find differences in first 10 rows
        print("Differences in first 10 rows:")
        for i in range(min(10, len(pandas_sorted_result))):
            pandas_val = pandas_sorted_result.iloc[i]['cumsum_value']
            vectrill_val = vectrill_result.iloc[i]['cumsum_value']
            group = pandas_sorted_result.iloc[i]['group']
            
            if abs(pandas_val - vectrill_val) > 1e-10:
                print(f"  Row {i} (group={group}): pandas={pandas_val:.6f}, vectrill={vectrill_val:.6f}")

except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()
