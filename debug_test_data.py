#!/usr/bin/env python3
import sys
sys.path.insert(0, 'tests/python')
sys.path.insert(0, 'python')

# Debug test data generation
import vectrill
from vectrill.dataframe import col, functions, window
import pandas as pd
import numpy as np

# Generate test data exactly like in the test
np.random.seed(42)
data = []
for session_id in range(1, 6):  # Just generate a few sessions
    events_per_session = 6  # Fixed number instead of random
    session_start = pd.Timestamp('2023-01-01') + pd.Timedelta(days=np.random.randint(0, 30))
    
    for event_idx in range(events_per_session):
        data.append({
            'timestamp': session_start + pd.Timedelta(seconds=event_idx * 5),  # Events every 5 seconds
            'session_id': session_id,
            'user_id': np.random.randint(1, 50000),
            'event_type': 'page_view',
            'page_url': f'/page_{event_idx}',
            'revenue': 0.0,
            'device_type': 'web',
            'response_time_ms': 100
        })

session_data = pd.DataFrame(data)

print('Generated session data:')
print(session_data[['session_id', 'timestamp']])

# Calculate expected session durations
session_times = session_data.groupby('session_id')['timestamp'].agg(['min', 'max'])
expected_durations = (session_times['max'] - session_times['min']).dt.total_seconds()

print('\nExpected session durations:')
print(expected_durations.tolist())

# Test vectrill implementation
vectrill_df = vectrill.from_pandas(session_data)
result = vectrill_df.with_columns([
    functions.max(col('timestamp')).over(window.partition_by('session_id')).alias('session_end'),
    functions.min(col('timestamp')).over(window.partition_by('session_id')).alias('session_start'),
]).with_columns([
    (col('session_end') - col('session_start')).alias('duration_seconds')
])

vectrill_pd = result.to_pandas()
actual_durations = vectrill_pd['duration_seconds']

print('\nVectrill session durations:')
print(actual_durations.tolist())

print('\nDuration comparison:')
print('Expected:', expected_durations.tolist())
print('Actual:  ', actual_durations.tolist())
print('Match:', np.allclose(expected_durations, actual_durations, rtol=1e-10))
