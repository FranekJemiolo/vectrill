#!/usr/bin/env python3
import sys
sys.path.insert(0, 'tests/python')
sys.path.insert(0, 'python')

# Test GroupBy functionality
import vectrill
from vectrill.dataframe import col, functions
import pandas as pd
import numpy as np

# Create test data
np.random.seed(42)
test_data = pd.DataFrame({
    'group': ['A', 'B', 'A', 'B', 'A', 'B'],
    'value': [1, 2, 3, 4, 5, 6]
})

print("Test GroupBy functionality:")
print("Original data:")
print(test_data)

# Test pandas groupby
print("\n=== Pandas GroupBy ===")
pandas_grouped = test_data.groupby('group')['value'].sum().reset_index()
print("Pandas sum by group:")
print(pandas_grouped)

# Test Vectrill groupby
print("\n=== Vectrill GroupBy ===")
vectrill_df = vectrill.from_pandas(test_data)
vectrill_grouped = vectrill_df.groupby('group').agg(functions.sum(col('value')).alias('sum_value'))
print("Vectrill sum by group:")
print(vectrill_grouped.to_pandas())

# Test multiple aggregations
print("\n=== Multiple Aggregations ===")
pandas_multi = test_data.groupby('group').agg({
    'value': ['sum', 'mean', 'count']
}).reset_index()
print("Pandas multiple aggregations:")
print(pandas_multi)

# Test Vectrill multiple aggregations
vectrill_multi = vectrill_df.groupby('group').agg([
    functions.sum(col('value')).alias('sum_value'),
    functions.mean(col('value')).alias('mean_value'),
    functions.count(col('value')).alias('count_value')
])
print("Vectrill multiple aggregations:")
print(vectrill_multi.to_pandas())
