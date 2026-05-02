#!/usr/bin/env python3
import sys
sys.path.insert(0, 'tests/python')
sys.path.insert(0, 'python')

# Debug data difference
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

print("Debug data difference:")
print("Original time_series_data:")
print(time_series_data.head(10))

# Create Vectrill DataFrame
vectrill_df = vectrill.from_pandas(time_series_data)
vectrill_back = vectrill_df.to_pandas()

print("Vectrill converted back to pandas:")
print(vectrill_back.head(10))

# Check if they're identical
print("Dataframes identical:", time_series_data.equals(vectrill_back))

# Check differences
if not time_series_data.equals(vectrill_back):
    print("Differences:")
    print("Original dtypes:", time_series_data.dtypes)
    print("Vectrill dtypes:", vectrill_back.dtypes)
    
    for col in time_series_data.columns:
        original_col = time_series_data[col]
        vectrill_col = vectrill_back[col]
        
        if not original_col.equals(vectrill_col):
            print(f"Column {col} differs:")
            print(f"  Original first 5: {original_col.head(5).tolist()}")
            print(f"  Vectrill first 5: {vectrill_col.head(5).tolist()}")
            print(f"  Original dtype: {original_col.dtype}")
            print(f"  Vectrill dtype: {vectrill_col.dtype}")
            
            # Check if values are close
            if pd.api.types.is_numeric_dtype(original_col) and pd.api.types.is_numeric_dtype(vectrill_col):
                close = np.allclose(original_col, vectrill_col, equal_nan=True)
                print(f"  Values close: {close}")
