#!/usr/bin/env python3
import sys
sys.path.insert(0, 'tests/python')
sys.path.insert(0, 'python')

# Debug window function index handling
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

print("Debug window function index handling:")
print("Original time_series_data:")
print(time_series_data.head(10))
print("Original index:", time_series_data.index.tolist()[:10])

# Test pandas implementation
print("\n=== Pandas Implementation ===")
pandas_sorted = time_series_data.sort_values(['group', 'date'])
print("Pandas sorted dataframe:")
print(pandas_sorted.head(10))
print("Pandas sorted index:", pandas_sorted.index.tolist()[:10])

pandas_sorted['lag_value'] = pandas_sorted.groupby('group')['value'].shift(1)
print("Pandas after lag:")
print(pandas_sorted.head(10))

pandas_result = pandas_sorted.sort_index()
print("Pandas result (restored order):")
print(pandas_result.head(10))
print("Pandas result index:", pandas_result.index.tolist()[:10])

# Test Vectrill implementation
print("\n=== Vectrill Implementation ===")
vectrill_df = vectrill.from_pandas(time_series_data)
print("Vectrill original dataframe:")
print(vectrill_df.to_pandas().head(10))
print("Vectrill original index:", vectrill_df.to_pandas().index.tolist()[:10])

vectrill_sorted = vectrill_df.sort(['group', 'date'])
print("Vectrill sorted dataframe:")
print(vectrill_sorted.to_pandas().head(10))
print("Vectrill sorted index:", vectrill_sorted.to_pandas().index.tolist()[:10])

# Check if the sorted dataframes are equivalent
print("\n=== Comparison ===")
print("Pandas sorted equals Vectrill sorted:", pandas_sorted.equals(vectrill_sorted.to_pandas()))
print("Pandas sorted index equals Vectrill sorted index:", pandas_sorted.index.equals(vectrill_sorted.to_pandas().index))

# Check the data content
print("Pandas sorted groups:", pandas_sorted['group'].head(10).tolist())
print("Vectrill sorted groups:", vectrill_sorted.to_pandas()['group'].head(10).tolist())

print("Pandas sorted values:", pandas_sorted['value'].head(10).tolist())
print("Vectrill sorted values:", vectrill_sorted.to_pandas()['value'].head(10).tolist())
