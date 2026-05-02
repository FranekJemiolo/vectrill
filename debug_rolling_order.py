#!/usr/bin/env python3
"""Debug script for rolling function with order_by"""

import sys
sys.path.insert(0, 'python')
import vectrill
from vectrill.dataframe import col, functions, window
import pandas as pd
import numpy as np

# Create simple test data
np.random.seed(42)
data = []
for i in range(20):
    data.append({
        'timestamp': pd.Timestamp('2023-01-01') + pd.Timedelta(seconds=i * 10),
        'is_error': np.random.choice([0, 1], p=[0.9, 0.1])
    })

df = pd.DataFrame(data).sort_values('timestamp')
print('Original data:')
print(df.head(10))

# Test pandas implementation
pandas_result = df.copy()
pandas_result['error_rate'] = pandas_result['is_error'].rolling(window=10, min_periods=1).mean()

print('\nPandas result (first 10):')
print(pandas_result[['timestamp', 'is_error', 'error_rate']].head(10))

# Test vectrill implementation
vectrill_df = vectrill.from_pandas(df)
vectrill_result = vectrill_df.sort('timestamp').with_columns([
    functions.rolling_mean(col('is_error'), window_size=10).over(window.order_by('timestamp')).alias('error_rate')
])

vectrill_pd = vectrill_result.to_pandas()
print('\Vectrill result (first 10):')
print(vectrill_pd[['timestamp', 'is_error', 'error_rate']].head(10))

# Compare first few values
print('\nComparison of first 10 values:')
for i in range(10):
    pandas_val = pandas_result.iloc[i]['error_rate']
    vectrill_val = vectrill_pd.iloc[i]['error_rate']
    diff = abs(pandas_val - vectrill_val)
    print(f"Row {i}: pandas={pandas_val:.3f}, vectrill={vectrill_val:.3f}, diff={diff:.3f}")
