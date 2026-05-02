#!/usr/bin/env python3
import sys
sys.path.insert(0, 'tests/python')
sys.path.insert(0, 'python')

# Debug sort operation
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

print("Debug sort operation:")
print("Original data:")
print(time_series_data.head(10))

# Test pandas sort
pandas_sorted = time_series_data.sort_values(['group', 'date'])
print("Pandas sorted:")
print(pandas_sorted.head(10))

# Test Vectrill sort
vectrill_df = vectrill.from_pandas(time_series_data)
vectrill_sorted = vectrill_df.sort(['group', 'date']).to_pandas()
print("Vectrill sorted:")
print(vectrill_sorted.head(10))

# Compare the sorted results
print("Comparison:")
print("Pandas groups (first 10):", pandas_sorted['group'].head(10).tolist())
print("Vectrill groups (first 10):", vectrill_sorted['group'].head(10).tolist())

print("Pandas index (first 10):", pandas_sorted.head(10).index.tolist())
print("Vectrill index (first 10):", vectrill_sorted.head(10).index.tolist())

# Check if they're identical
print("Sorted dataframes identical:", pandas_sorted.equals(vectrill_sorted))

# Check the lag function on sorted data
print("\n=== Lag function on sorted data ===")

# Pandas lag
pandas_sorted['lag_value'] = pandas_sorted.groupby('group')['value'].shift(1)
print("Pandas lag result:")
print(pandas_sorted[['group', 'date', 'value', 'lag_value']].head(10))

# Vectrill lag on sorted data
vectrill_with_lag = vectrill_sorted.copy()
# Since we can't directly apply lag to the sorted dataframe, let's check what happens
print("Vectrill sorted data structure:")
print(vectrill_sorted.head(10))
