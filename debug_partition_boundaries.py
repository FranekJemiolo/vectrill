#!/usr/bin/env python3
import sys
sys.path.insert(0, 'tests/python')
sys.path.insert(0, 'python')

# Debug partition boundaries
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

print("Debug partition boundaries:")
print("Original data:")
print(time_series_data[['group', 'date', 'value']].head(15))

# Test pandas implementation
print("\n=== Pandas Implementation ===")
pandas_sorted = time_series_data.sort_values(['group', 'date'])
pandas_sorted['lag_value'] = pandas_sorted.groupby('group')['value'].shift(1)
pandas_result = pandas_sorted.sort_index()

print("Pandas sorted with lag:")
print(pandas_sorted[['group', 'date', 'value', 'lag_value']].head(15))

# Show the group transitions
print("Pandas group transitions:")
for i in range(len(pandas_sorted) - 1):
    if pandas_sorted.iloc[i]['group'] != pandas_sorted.iloc[i + 1]['group']:
        print(f"Transition at index {i}: {pandas_sorted.iloc[i]['group']} -> {pandas_sorted.iloc[i + 1]['group']}")
        print(f"  Row {i}: lag_value = {pandas_sorted.iloc[i]['lag_value']}")
        print(f"  Row {i+1}: lag_value = {pandas_sorted.iloc[i + 1]['lag_value']}")

# Test Vectrill implementation
print("\n=== Vectrill Implementation ===")
vectrill_df = vectrill.from_pandas(time_series_data)
vectrill_result = vectrill_df.sort(['group', 'date']).with_columns([
    functions.lag(col('value'), 1).over(window.partition_by('group').order_by('date')).alias('lag_value')
]).to_pandas()

print("Vectrill result:")
print(vectrill_result[['group', 'date', 'value', 'lag_value']].head(15))

# Show the group transitions in Vectrill
vectrill_sorted = vectrill_result.sort_values(['group', 'date'])
print("Vectrill group transitions:")
for i in range(len(vectrill_sorted) - 1):
    if vectrill_sorted.iloc[i]['group'] != vectrill_sorted.iloc[i + 1]['group']:
        print(f"Transition at index {i}: {vectrill_sorted.iloc[i]['group']} -> {vectrill_sorted.iloc[i + 1]['group']}")
        print(f"  Row {i}: lag_value = {vectrill_sorted.iloc[i]['lag_value']}")
        print(f"  Row {i+1}: lag_value = {vectrill_sorted.iloc[i + 1]['lag_value']}")

# Compare the first few rows of each group
print("\n=== Comparison by group ===")
for group in ['X', 'Y', 'Z']:
    print(f"\nGroup {group}:")
    pandas_group = pandas_sorted[pandas_sorted['group'] == group].head(3)
    vectrill_group = vectrill_sorted[vectrill_sorted['group'] == group].head(3)
    
    print(f"  Pandas lag_value: {pandas_group['lag_value'].tolist()}")
    print(f"  Vectrill lag_value: {vectrill_group['lag_value'].tolist()}")
