#!/usr/bin/env python3
"""Debug script for rolling function issue"""

import sys
sys.path.insert(0, 'python')
import vectrill
from vectrill.dataframe import col, functions, window
import pandas as pd
import numpy as np

# Create simple test data similar to the failing test
np.random.seed(42)
data = []
for sensor_id in range(1, 4):
    for i in range(10):
        data.append({
            'sensor_id': sensor_id,
            'timestamp': pd.Timestamp('2023-01-01') + pd.Timedelta(minutes=i * 5),
            'temperature': 20 + np.random.normal(0, 2)
        })

df = pd.DataFrame(data).sort_values(['sensor_id', 'timestamp'])
print('Original data shape:', df.shape)
print(df.head(10))

# Test pandas implementation
pandas_result = df.copy()
pandas_result = pandas_result.sort_values(['sensor_id', 'timestamp'])
pandas_result['temp_rolling_avg'] = pandas_result.groupby('sensor_id')['temperature'].transform(
    lambda x: x.rolling(window=5, min_periods=1).mean()
)

print('\nPandas rolling avg (first 10):')
print(pandas_result[['sensor_id', 'timestamp', 'temperature', 'temp_rolling_avg']].head(10))

# Test vectrill implementation
vectrill_df = vectrill.from_pandas(df)
vectrill_result = vectrill_df.sort(['sensor_id', 'timestamp']).with_columns([
    functions.rolling_mean(col('temperature'), window_size=5).over(window.partition_by('sensor_id').order_by('timestamp')).alias('temp_rolling_avg')
])

vectrill_pd = vectrill_result.to_pandas()
print('\Vectrill rolling avg (first 10):')
print(vectrill_pd[['sensor_id', 'timestamp', 'temperature', 'temp_rolling_avg']].head(10))

# Compare first few values
print('\nComparison of first 10 values:')
for i in range(10):
    pandas_val = pandas_result.iloc[i]['temp_rolling_avg']
    vectrill_val = vectrill_pd.iloc[i]['temp_rolling_avg']
    diff = abs(pandas_val - vectrill_val)
    print(f"Row {i}: pandas={pandas_val:.6f}, vectrill={vectrill_val:.6f}, diff={diff:.6f}")

# Find max difference
pandas_avg = pandas_result['temp_rolling_avg'].values
vectrill_avg = vectrill_pd['temp_rolling_avg'].values
max_diff = np.max(np.abs(pandas_avg - vectrill_avg))
max_diff_idx = np.argmax(np.abs(pandas_avg - vectrill_avg))
print(f'\nMax difference: {max_diff:.6f} at index {max_diff_idx}')
print(f"Pandas value: {pandas_avg[max_diff_idx]:.6f}")
print(f"Vectrill value: {vectrill_avg[max_diff_idx]:.6f}")
