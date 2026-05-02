#!/usr/bin/env python3
import sys
sys.path.insert(0, 'tests/python')
sys.path.insert(0, 'python')

# Debug lag window function in detail
import vectrill
from vectrill.dataframe import col, functions, window
import pandas as pd
import numpy as np

# Create simple test data
data = pd.DataFrame({
    'group': ['X', 'X', 'Y', 'Y'],
    'value': [100, 200, 300, 400],
    'date': pd.date_range('2023-01-01', periods=4)
})

print("Testing lag window function:")

# Test pandas implementation step by step
print("\n=== Pandas Implementation Step by Step ===")
pandas_step1 = data.copy()
print("Step 1 - Original:")
print(pandas_step1)

pandas_step2 = pandas_step1.sort_values(['group', 'date'])
print("Step 2 - Sorted:")
print(pandas_step2)

pandas_step3 = pandas_step2.copy()
pandas_step3['lag_value'] = pandas_step3.groupby('group')['value'].shift(1)
print("Step 3 - After lag:")
print(pandas_step3)

pandas_result = pandas_step3.sort_index()
print("Step 4 - Restored original order:")
print(pandas_result)

# Test Vectrill implementation step by step
print("\n=== Vectrill Implementation Step by Step ===")
try:
    # Step 1: Create Vectrill DataFrame
    vectrill_df = vectrill.from_pandas(data)
    print("Step 1 - Created vectrill DataFrame")
    
    # Step 2: Apply lag function
    vectrill_with_lag = vectrill_df.with_columns([
        functions.lag(col('value')).over(window.partition_by('group').order_by('date')).alias('lag_value')
    ])
    print("Step 2 - Applied lag function:")
    print(vectrill_with_lag.to_pandas())
    
    # Step 3: Compare results
    pandas_sorted_result = pandas_result.sort_values(['group', 'date']).reset_index(drop=True)
    vectrill_result = vectrill_with_lag.to_pandas().sort_values(['group', 'date']).reset_index(drop=True)
    
    print("\n=== Detailed Comparison ===")
    print("Pandas lag_value:")
    print(pandas_sorted_result['lag_value'].tolist())
    print("Vectrill lag_value:")
    print(vectrill_result['lag_value'].tolist())
    
    # Check if they match
    match = np.allclose(pandas_sorted_result['lag_value'], vectrill_result['lag_value'], equal_nan=True)
    print(f"Results match: {match}")
    
    if not match:
        # Find differences
        print("Differences:")
        for i in range(len(pandas_sorted_result)):
            pandas_val = pandas_sorted_result.iloc[i]['lag_value']
            vectrill_val = vectrill_result.iloc[i]['lag_value']
            group = pandas_sorted_result.iloc[i]['group']
            
            if (pd.isna(pandas_val) and not pd.isna(vectrill_val)) or (not pd.isna(pandas_val) and pd.isna(vectrill_val)):
                print(f"  Row {i} (group={group}): pandas={pandas_val}, vectrill={vectrill_val} (NaN mismatch)")
            elif not pd.isna(pandas_val) and not pd.isna(vectrill_val):
                if abs(pandas_val - vectrill_val) > 1e-10:
                    print(f"  Row {i} (group={group}): pandas={pandas_val}, vectrill={vectrill_val}")

except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()
