#!/usr/bin/env python3
import sys
sys.path.insert(0, 'tests/python')
sys.path.insert(0, 'python')

# Debug data structure differences
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

print("Debug data structure differences:")
print("Original time_series_data:")
print(time_series_data.head(10))
print("Original index:", time_series_data.index.tolist()[:10])

# Test pandas
pandas_sorted = time_series_data.sort_values(['group', 'date'])
pandas_sorted['lag_value'] = pandas_sorted.groupby('group')['value'].shift(1)
pandas_result = pandas_sorted.sort_index()

print("\nPandas result:")
print(pandas_result.head(10))
print("Pandas result index:", pandas_result.index.tolist()[:10])

# Test Vectrill
vectrill_df = vectrill.from_pandas(time_series_data)
vectrill_result = vectrill_df.sort(['group', 'date']).with_columns([
    functions.lag(col('value'), 1).over(window.partition_by('group').order_by('date')).alias('lag_value')
]).to_pandas()

print("\nVectrill result:")
print(vectrill_result.head(10))
print("Vectrill result index:", vectrill_result.index.tolist()[:10])

# Check the difference
print("\nData comparison:")
print("Original data shape:", time_series_data.shape)
print("Pandas result shape:", pandas_result.shape)
print("Vectrill result shape:", vectrill_result.shape)

print("Original data columns:", time_series_data.columns.tolist())
print("Pandas result columns:", pandas_result.columns.tolist())
print("Vectrill result columns:", vectrill_result.columns.tolist())

print("Original data dtypes:")
print(time_series_data.dtypes)
print("Pandas result dtypes:")
print(pandas_result.dtypes)
print("Vectrill result dtypes:")
print(vectrill_result.dtypes)

# Check if the original data is preserved
print("Original data equals pandas base data:", time_series_data.equals(pandas_result[['group', 'date', 'value']]))
print("Original data equals vectrill base data:", time_series_data.equals(vectrill_result[['group', 'date', 'value']]))

# Check the specific differences
for col in ['group', 'date', 'value']:
    orig_col = time_series_data[col]
    pandas_col = pandas_result[col]
    vectrill_col = vectrill_result[col]
    
    print(f"\nColumn {col}:")
    print(f"  Original first 5: {orig_col.head(5).tolist()}")
    print(f"  Pandas first 5: {pandas_col.head(5).tolist()}")
    print(f"  Vectrill first 5: {vectrill_col.head(5).tolist()}")
    
    if pd.api.types.is_numeric_dtype(orig_col):
        pandas_close = np.allclose(orig_col, pandas_col, equal_nan=True)
        vectrill_close = np.allclose(orig_col, vectrill_col, equal_nan=True)
        print(f"  Original vs Pandas close: {pandas_close}")
        print(f"  Original vs Vectrill close: {vectrill_close}")
    else:
        pandas_equal = orig_col.equals(pandas_col)
        vectrill_equal = orig_col.equals(vectrill_col)
        print(f"  Original vs Pandas equal: {pandas_equal}")
        print(f"  Original vs Vectrill equal: {vectrill_equal}")
