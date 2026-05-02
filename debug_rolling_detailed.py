#!/usr/bin/env python3
"""Debug script for rolling function issue - detailed analysis"""

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
print('Data index:', df.index.tolist())

# Test pandas implementation
pandas_result = df.copy()
pandas_result = pandas_result.sort_values(['sensor_id', 'timestamp'])
pandas_result['temp_rolling_avg'] = pandas_result.groupby('sensor_id')['temperature'].transform(
    lambda x: x.rolling(window=5, min_periods=1).mean()
)

# Test vectrill implementation
vectrill_df = vectrill.from_pandas(df)
vectrill_result = vectrill_df.sort(['sensor_id', 'timestamp']).with_columns([
    functions.rolling_mean(col('temperature'), window_size=5).over(window.partition_by('sensor_id').order_by('timestamp')).alias('temp_rolling_avg')
])

vectrill_pd = vectrill_result.to_pandas()

# Check around the problematic index
problem_idx = 20
print(f'\nProblem area around index {problem_idx}:')
print('Pandas result around problem:')
print(pandas_result.iloc[problem_idx-2:problem_idx+3][['sensor_id', 'timestamp', 'temperature', 'temp_rolling_avg']])

print('\Vectrill result around problem:')
print(vectrill_pd.iloc[problem_idx-2:problem_idx+3][['sensor_id', 'timestamp', 'temperature', 'temp_rolling_avg']])

# Check if the issue is with ordering
print('\nChecking ordering consistency:')
print('Pandas sensor_id sequence:', pandas_result['sensor_id'].tolist())
print('Vectrill sensor_id sequence:', vectrill_pd['sensor_id'].tolist())

# Check if the issue is with the actual rolling calculation for a specific sensor
sensor_id = 3
pandas_sensor = pandas_result[pandas_result['sensor_id'] == sensor_id]
vectrill_sensor = vectrill_pd[vectrill_pd['sensor_id'] == sensor_id]

print(f'\nFor sensor {sensor_id}:')
print('Pandas rolling values:')
print(pandas_sensor[['temperature', 'temp_rolling_avg']].values)
print('Vectrill rolling values:')
print(vectrill_sensor[['temperature', 'temp_rolling_avg']].values)
