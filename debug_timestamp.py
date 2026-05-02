#!/usr/bin/env python3
import sys
sys.path.insert(0, 'python')
import pandas as pd
import numpy as np
from vectrill.dataframe import VectrillDataFrame, col, functions, window

# Create simple test data to understand the difference
data = pd.DataFrame({
    'timestamp': [pd.Timestamp('2023-01-01 10:00:00'), pd.Timestamp('2023-01-01 10:01:00'), pd.Timestamp('2023-01-01 10:02:00')],
    'session_id': [1, 1, 1]
})

# Pandas implementation
pandas_result = data.copy()
session_times = pandas_result.groupby('session_id')['timestamp'].agg(['min', 'max'])
session_times['duration_seconds'] = (session_times['max'] - session_times['min']).dt.total_seconds()
pandas_result = pandas_result.merge(session_times['duration_seconds'], left_on='session_id', right_index=True)

print('Pandas result:')
print(pandas_result[['timestamp', 'session_id', 'duration_seconds']])

# Vectrill implementation
vectrill_df = VectrillDataFrame(data)
vectrill_result = vectrill_df.with_columns([
    functions.max(col('timestamp')).over(window.partition_by('session_id')).alias('session_end'),
    functions.min(col('timestamp')).over(window.partition_by('session_id')).alias('session_start')
]).with_columns([
    (col('session_end') - col('session_start')).alias('duration_seconds')
])

print('\nVectrill result:')
print(vectrill_result.to_pandas()[['timestamp', 'session_id', 'duration_seconds']])
