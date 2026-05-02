#!/usr/bin/env python3
import sys
sys.path.insert(0, 'tests/python')
sys.path.insert(0, 'python')

# Debug window function flow
import vectrill
from vectrill.dataframe import col, functions, window
import pandas as pd
import numpy as np

# Create the exact same data as the test
np.random.seed(42)  # Make deterministic
dates = pd.date_range('2023-01-01', periods=50, freq='D')
time_series_data = pd.DataFrame({
    'date': dates,
    'value': np.random.randn(50).cumsum() + 100,
    'group': np.random.choice(['X', 'Y', 'Z'], 50)
})

print("Debug window function flow:")
print("Original time_series_data shape:", time_series_data.shape)

# Test pandas implementation step by step
print("\n=== Pandas Implementation Step by Step ===")
pandas_step1 = time_series_data.sort_values(['group', 'date'])
print("Step 1 - Sorted dataframe shape:", pandas_step1.shape)
print("Step 1 - Sorted index:", pandas_step1.index.tolist()[:10])

pandas_step2 = pandas_step1.copy()
pandas_step2['lag_value'] = pandas_step2.groupby('group')['value'].shift(1)
print("Step 2 - After lag shape:", pandas_step2.shape)
print("Step 2 - After lag index:", pandas_step2.index.tolist()[:10])

pandas_result = pandas_step2.sort_index()
print("Step 3 - Restored order shape:", pandas_result.shape)
print("Step 3 - Restored order index:", pandas_result.index.tolist()[:10])
print("Step 3 - Lag values:", pandas_result['lag_value'].head(10).tolist())

# Test Vectrill implementation step by step
print("\n=== Vectrill Implementation Step by Step ===")
vectrill_step1 = vectrill.from_pandas(time_series_data)
print("Step 1 - From pandas shape:", vectrill_step1.to_pandas().shape)
print("Step 1 - From pandas index:", vectrill_step1.to_pandas().index.tolist()[:10])

vectrill_step2 = vectrill_step1.sort(['group', 'date'])
print("Step 2 - Sorted shape:", vectrill_step2.to_pandas().shape)
print("Step 2 - Sorted index:", vectrill_step2.to_pandas().index.tolist()[:10])

vectrill_step3 = vectrill_step2.with_columns([
    functions.lag(col('value'), 1).over(window.partition_by('group').order_by('date')).alias('lag_value')
])
print("Step 3 - After lag shape:", vectrill_step3.to_pandas().shape)
print("Step 3 - After lag index:", vectrill_step3.to_pandas().index.tolist()[:10])
print("Step 3 - Lag values:", vectrill_step3.to_pandas()['lag_value'].head(10).tolist())

# Compare the results
print("\n=== Comparison ===")
print("Pandas lag values:", pandas_result['lag_value'].head(10).tolist())
print("Vectrill lag values:", vectrill_step3.to_pandas()['lag_value'].head(10).tolist())

# Check if the dataframes are the same
print("Pandas result equals Vectrill result:", pandas_result.equals(vectrill_step3.to_pandas()))

# Check the specific differences
if not pandas_result.equals(vectrill_step3.to_pandas()):
    print("Differences found:")
    for i in range(min(10, len(pandas_result))):
        pandas_val = pandas_result.iloc[i]['lag_value']
        vectrill_val = vectrill_step3.to_pandas().iloc[i]['lag_value']
        
        if (pd.isna(pandas_val) and not pd.isna(vectrill_val)) or (not pd.isna(pandas_val) and pd.isna(vectrill_val)):
            print(f"  Row {i}: pandas={pandas_val}, vectrill={vectrill_val} (NaN mismatch)")
        elif not pd.isna(pandas_val) and not pd.isna(vectrill_val):
            if abs(pandas_val - vectrill_val) > 1e-10:
                print(f"  Row {i}: pandas={pandas_val:.6f}, vectrill={vectrill_val:.6f}")
