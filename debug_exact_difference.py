#!/usr/bin/env python3
import sys
sys.path.insert(0, 'tests/python')
sys.path.insert(0, 'python')

# Debug exact difference between pandas and Vectrill sorted dataframes
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

print("Debug exact difference between pandas and Vectrill sorted dataframes:")

# Test pandas implementation
pandas_sorted = time_series_data.sort_values(['group', 'date'])

# Test Vectrill implementation
vectrill_df = vectrill.from_pandas(time_series_data)
vectrill_sorted = vectrill_df.sort(['group', 'date']).to_pandas()

print("Pandas sorted shape:", pandas_sorted.shape)
print("Vectrill sorted shape:", vectrill_sorted.shape)

print("Pandas sorted columns:", pandas_sorted.columns.tolist())
print("Vectrill sorted columns:", vectrill_sorted.columns.tolist())

print("Pandas sorted dtypes:")
print(pandas_sorted.dtypes)
print("Vectrill sorted dtypes:")
print(vectrill_sorted.dtypes)

# Check each column for differences
for col in pandas_sorted.columns:
    print(f"\nColumn {col}:")
    pandas_col = pandas_sorted[col]
    vectrill_col = vectrill_sorted[col]
    
    print(f"  Pandas first 5: {pandas_col.head(5).tolist()}")
    print(f"  Vectrill first 5: {vectrill_col.head(5).tolist()}")
    
    if pd.api.types.is_numeric_dtype(pandas_col):
        pandas_close = np.allclose(pandas_col, vectrill_col, equal_nan=True)
        print(f"  Values close: {pandas_close}")
    else:
        pandas_equal = pandas_col.equals(vectrill_col)
        print(f"  Values equal: {pandas_equal}")

# Check the exact difference
print(f"\nExact difference check:")
print(f"Dataframes equal: {pandas_sorted.equals(vectrill_sorted)}")

# Find the specific differences
try:
    comparison = pandas_sorted.compare(vectrill_sorted)
    print("Comparison result:")
    print(comparison)
except Exception as e:
    print(f"Comparison failed: {e}")

# Check if the issue is in the index
print(f"\nIndex comparison:")
print(f"Pandas index: {pandas_sorted.index.tolist()[:10]}")
print(f"Vectrill index: {vectrill_sorted.index.tolist()[:10]}")
print(f"Index equal: {pandas_sorted.index.equals(vectrill_sorted.index)}")

# Check if the issue is in the data
print(f"\nData comparison:")
for col in pandas_sorted.columns:
    try:
        if pandas_sorted[col].equals(vectrill_sorted[col]):
            print(f"  {col}: equal")
        else:
            print(f"  {col}: NOT equal")
            # Find the first difference
            for i in range(len(pandas_sorted)):
                if pd.isna(pandas_sorted[col].iloc[i]) and pd.isna(vectrill_sorted[col].iloc[i]):
                    continue
                elif pandas_sorted[col].iloc[i] != vectrill_sorted[col].iloc[i]:
                    print(f"    First difference at index {i}: pandas={pandas_sorted[col].iloc[i]}, vectrill={vectrill_sorted[col].iloc[i]}")
                    break
    except Exception as e:
        print(f"  {col}: comparison failed - {e}")
