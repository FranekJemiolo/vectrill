#!/usr/bin/env python3
import sys
sys.path.insert(0, 'tests/python')
sys.path.insert(0, 'python')

# Debug timestamp subtraction issue
import vectrill
from vectrill.dataframe import col, functions, window
import pandas as pd
import numpy as np

# Create simple test data with timestamps
data = pd.DataFrame({
    'session_start': [pd.Timestamp('2023-01-01 10:00:00'), pd.Timestamp('2023-01-01 10:01:00')],
    'session_end': [pd.Timestamp('2023-01-01 10:00:25'), pd.Timestamp('2023-01-01 10:01:10')],
    'session_id': [1, 2]
})

print('Test data:')
print(data)

# Test timestamp subtraction
print('\nTesting timestamp subtraction...')

try:
    vectrill_df = vectrill.from_pandas(data)
    result = vectrill_df.with_columns([
        (col('session_end') - col('session_start')).alias('duration_seconds')
    ])
    
    print('Vectrill result:')
    print(result.to_pandas())
    
    # Compare with pandas
    pandas_result = data.copy()
    pandas_result['duration_seconds'] = pandas_result['session_end'] - pandas_result['session_start']
    
    print('\nPandas result:')
    print(pandas_result)
    
    print('\nPandas duration_seconds type:', type(pandas_result['duration_seconds'].iloc[0]))
    print('Pandas duration_seconds values:', pandas_result['duration_seconds'].tolist())
    
except Exception as e:
    print('Error:', e)
    import traceback
    traceback.print_exc()
