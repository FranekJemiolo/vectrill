#!/usr/bin/env python3
import sys
sys.path.insert(0, 'tests/python')
sys.path.insert(0, 'python')

# Debug deterministic lag test
import vectrill
from vectrill.dataframe import col, functions, window
import pandas as pd
import numpy as np

# Create deterministic time series data (same as in the test)
np.random.seed(42)  # Make deterministic
dates = pd.date_range('2023-01-01', periods=50, freq='D')
time_series_data = pd.DataFrame({
    'date': dates,
    'value': np.random.randn(50).cumsum() + 100,
    'group': np.random.choice(['X', 'Y', 'Z'], 50)
})

print("Testing deterministic lag function:")
print("Time series data:")
print(time_series_data.head(15))

# Test pandas implementation
print("\n=== Pandas Implementation ===")
pandas_sorted = time_series_data.sort_values(['group', 'date'])
pandas_sorted['lag_value'] = pandas_sorted.groupby('group')['value'].shift(1)
pandas_result = pandas_sorted.sort_index()
print("Pandas result:")
print(pandas_result[['group', 'date', 'value', 'lag_value']].head(15))

# Test Vectrill implementation
print("\n=== Vectrill Implementation ===")
try:
    vectrill_df = vectrill.from_pandas(time_series_data)
    vectrill_result = vectrill_df.sort(['group', 'date']).with_columns([
        functions.lag(col('value'), 1).over(window.partition_by('group').order_by('date')).alias('lag_value')
    ]).to_pandas()
    print("Vectrill result:")
    print(vectrill_result[['group', 'date', 'value', 'lag_value']].head(15))
    
    # Compare results
    print("\n=== Detailed Comparison ===")
    pandas_sorted_result = pandas_result.sort_values(['group', 'date']).reset_index(drop=True)
    vectrill_sorted_result = vectrill_result.sort_values(['group', 'date']).reset_index(drop=True)
    
    print("Pandas lag_value (first 15):")
    print(pandas_sorted_result['lag_value'].head(15).tolist())
    print("Vectrill lag_value (first 15):")
    print(vectrill_sorted_result['lag_value'].head(15).tolist())
    
    # Check if they match
    match = np.allclose(pandas_sorted_result['lag_value'], vectrill_sorted_result['lag_value'], equal_nan=True)
    print(f"Results match: {match}")
    
    if not match:
        # Find differences
        print("Differences:")
        for i in range(min(15, len(pandas_sorted_result))):
            pandas_val = pandas_sorted_result.iloc[i]['lag_value']
            vectrill_val = vectrill_sorted_result.iloc[i]['lag_value']
            group = pandas_sorted_result.iloc[i]['group']
            
            if (pd.isna(pandas_val) and not pd.isna(vectrill_val)) or (not pd.isna(pandas_val) and pd.isna(vectrill_val)):
                print(f"  Row {i} (group={group}): pandas={pandas_val}, vectrill={vectrill_val} (NaN mismatch)")
            elif not pd.isna(pandas_val) and not pd.isna(vectrill_val):
                if abs(pandas_val - vectrill_val) > 1e-10:
                    print(f"  Row {i} (group={group}): pandas={pandas_val:.6f}, vectrill={vectrill_val:.6f}")

except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()
