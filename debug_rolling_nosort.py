#!/usr/bin/env python3
"""Debug script for rolling function issue - without sort"""

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

# Test pandas implementation
pandas_result = df.copy()
pandas_result = pandas_result.sort_values(['sensor_id', 'timestamp'])
pandas_result['temp_rolling_avg'] = pandas_result.groupby('sensor_id')['temperature'].transform(
    lambda x: x.rolling(window=5, min_periods=1).mean()
)

# Test vectrill implementation WITHOUT sort
vectrill_df = vectrill.from_pandas(df)
vectrill_result = vectrill_df.with_columns([
    functions.rolling_mean(col('temperature'), window_size=5).over(window.partition_by('sensor_id').order_by('timestamp')).alias('temp_rolling_avg')
])

vectrill_pd = vectrill_result.to_pandas()

# Check sensor 3 specifically
sensor_id = 3
pandas_sensor = pandas_result[pandas_result['sensor_id'] == sensor_id]
vectrill_sensor = vectrill_pd[vectrill_pd['sensor_id'] == sensor_id]

print(f'For sensor {sensor_id} (without sort):')
print('Pandas rolling values:')
print(pandas_sensor[['temperature', 'temp_rolling_avg']].values)
print('Vectrill rolling values:')
print(vectrill_sensor[['temperature', 'temp_rolling_avg']].values)

# Compare first few values
print('\nComparison of first 3 values for sensor 3:')
for i in range(3):
    pandas_val = pandas_sensor.iloc[i]['temp_rolling_avg']
    vectrill_val = vectrill_sensor.iloc[i]['temp_rolling_avg']
    diff = abs(pandas_val - vectrill_val)
    print(f"Row {i}: pandas={pandas_val:.6f}, vectrill={vectrill_val:.6f}, diff={diff:.6f}")
