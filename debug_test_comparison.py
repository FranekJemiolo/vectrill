#!/usr/bin/env python3
import sys
sys.path.insert(0, 'tests/python')
sys.path.insert(0, 'python')

# Debug test comparison
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

print("Debug test comparison:")
print("Time series data shape:", time_series_data.shape)
print("Time series data groups:", time_series_data['group'].value_counts().sort_index())

# Test the exact same logic as the test
print("\n=== Exact test logic ===")

# Test pandas
pandas_sorted = time_series_data.sort_values(['group', 'date'])
pandas_sorted['lag_value'] = pandas_sorted.groupby('group')['value'].shift(1)
pandas_result = pandas_sorted.sort_index()

# Test Vectrill
vectrill_df = vectrill.from_pandas(time_series_data)
vectrill_result = vectrill_df.sort(['group', 'date']).with_columns([
    functions.lag(col('value'), 1).over(window.partition_by('group').order_by('date')).alias('lag_value')
]).to_pandas()

print("Pandas result shape:", pandas_result.shape)
print("Vectrill result shape:", vectrill_result.shape)

# Check if the data is the same
print("Dataframes identical:", pandas_result[['group', 'date', 'value']].equals(vectrill_result[['group', 'date', 'value']]))

# Check the lag values
print("Pandas lag_value (first 10):", pandas_result['lag_value'].head(10).tolist())
print("Vectrill lag_value (first 10):", vectrill_result['lag_value'].head(10).tolist())

# Check the group distribution in sorted order
print("Pandas sorted groups:", pandas_sorted['group'].head(10).tolist())
print("Vectrill sorted groups:", vectrill_result.sort_values(['group', 'date'])['group'].head(10).tolist())

# Check if the issue is in the sorting
print("Pandas sorted index:", pandas_sorted.head(10).index.tolist())
print("Vectrill sorted index:", vectrill_result.sort_values(['group', 'date']).head(10).index.tolist())
