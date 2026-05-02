#!/usr/bin/env python3
import sys
sys.path.insert(0, 'tests/python')
sys.path.insert(0, 'python')

# Debug timestamp calculation issue
import vectrill
from vectrill.dataframe import col, functions, window
import pandas as pd
import numpy as np

# Create simple test data to debug timestamp issue
data = pd.DataFrame({
    'session_id': [1, 1, 2, 2],
    'timestamp': [pd.Timestamp('2023-01-01 10:00:00'), pd.Timestamp('2023-01-01 10:00:05'), 
               pd.Timestamp('2023-01-01 10:00:10'), pd.Timestamp('2023-01-01 10:00:15')]
})

print('Test data:')
print(data)

# Test vectrill session duration calculation
print('\nTesting vectrill timestamp calculation...')

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
    
    # Manual calculation for comparison
    session_times = data.groupby('session_id')['timestamp'].agg(['min', 'max'])
    manual_duration = (session_times['max'] - session_times['min']).dt.total_seconds()
    
    print('\nManual session_end:')
    print(session_times['max'].tolist())
    print('Manual session_start:')
    print(session_times['min'].tolist())
    print('Manual duration_seconds:')
    print(manual_duration.tolist())
    
    print('\nDuration comparison:')
    print('Expected:', manual_duration.tolist())
    print('Actual:  ', vectrill_result['duration_seconds'].tolist())
    print('Match:', np.allclose(manual_duration, vectrill_result['duration_seconds'], rtol=1e-10))
    
except Exception as e:
    print('Error:', e)
    import traceback
    traceback.print_exc()
