#!/usr/bin/env python3
"""Debug script for lag function issue"""

import sys
sys.path.insert(0, 'python')
import vectrill
from vectrill.dataframe import col, functions, window
import pandas as pd
import numpy as np

# Create simple test data similar to the failing test
np.random.seed(42)
data = []
for i in range(10):
    data.append({
        'timestamp': pd.Timestamp('2023-01-01') + pd.Timedelta(hours=i),
        'user_id': np.random.randint(1, 3),
        'amount': 100 + i * 10
    })

df = pd.DataFrame(data).sort_values(['user_id', 'timestamp'])
print('Original data:')
print(df)

# Test pandas implementation
pandas_result = df.copy()
pandas_result['prev_timestamp'] = pandas_result.groupby('user_id')['timestamp'].shift(1)
pandas_result['time_diff_seconds'] = (pandas_result['timestamp'] - pandas_result['prev_timestamp']).dt.total_seconds()
pandas_result['time_diff_seconds'] = pandas_result['time_diff_seconds'].fillna(0)

print('\nPandas result:')
print(pandas_result[['user_id', 'timestamp', 'prev_timestamp', 'time_diff_seconds']])

# Test vectrill implementation
vectrill_df = vectrill.from_pandas(df)
vectrill_result = vectrill_df.sort(['user_id', 'timestamp']).with_columns([
    functions.lag(col('timestamp'), 1).over(window.partition_by('user_id').order_by('timestamp')).alias('prev_timestamp')
])

vectrill_pd = vectrill_result.to_pandas()
print('\Vectrill result (after lag):')
print(vectrill_pd[['user_id', 'timestamp', 'prev_timestamp']])

# Test the time difference
vectrill_result2 = vectrill_result.with_columns([
    (col('timestamp') - col('prev_timestamp')).alias('time_diff_seconds')
])
vectrill_pd2 = vectrill_result2.to_pandas()
print('\Vectrill result (after time diff):')
print(vectrill_pd2[['user_id', 'timestamp', 'prev_timestamp', 'time_diff_seconds']])
