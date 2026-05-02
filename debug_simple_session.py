#!/usr/bin/env python3
import sys
sys.path.insert(0, 'tests/python')
sys.path.insert(0, 'python')

# Debug session data generation with simple case
import vectrill
from vectrill.dataframe import col, functions, window
import pandas as pd
import numpy as np

# Create simple test data with known session duration
data = pd.DataFrame({
    'session_id': [1, 1, 1, 2, 2],
    'timestamp': [pd.Timestamp('2023-01-01 10:00:00'), pd.Timestamp('2023-01-01 10:00:05'), 
               pd.Timestamp('2023-01-01 10:00:10'), pd.Timestamp('2023-01-01 10:00:15'),
               pd.Timestamp('2023-01-01 10:00:00'), pd.Timestamp('2023-01-01 10:00:10')]
})

print('Test data:')
print(data)

# Calculate expected session duration manually
session_times = data.groupby('session_id')['timestamp'].agg(['min', 'max'])
expected_duration = (session_times['max'] - session_times['min']).dt.total_seconds()

print('\nExpected session durations:')
print(expected_duration.tolist())

# Test vectrill implementation
print('\nTesting vectrill implementation...')

try:
    vectrill_df = vectrill.from_pandas(data)
    
    # Calculate session_end and session_start using window functions
    result = vectrill_df.with_columns([
        functions.max(col('timestamp')).over(window.partition_by('session_id')).alias('session_end'),
        functions.min(col('timestamp')).over(window.partition_by('session_id')).alias('session_start'),
    ]).with_columns([
        (col('session_end') - col('session_start')).alias('duration_seconds')
    ])
    
    vectrill_result = result.to_pandas()
    
    print('Vectrill session_end:')
    print(vectrill_result['session_end'].tolist())
    print('Vectrill session_start:')
    print(vectrill_result['session_start'].tolist())
    print('Vectrill duration_seconds:')
    print(vectrill_result['duration_seconds'].tolist())
    
    print('\nDuration comparison:')
    print('Expected:', expected_duration.tolist())
    print('Actual:  ', vectrill_result['duration_seconds'].tolist())
    print('Match:', np.allclose(expected_duration, vectrill_result['duration_seconds'], rtol=1e-10))
    
except Exception as e:
    print('Error:', e)
    import traceback
    traceback.print_exc()
