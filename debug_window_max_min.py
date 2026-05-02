#!/usr/bin/env python3
import sys
sys.path.insert(0, 'tests/python')
sys.path.insert(0, 'python')

# Debug window max/min functions
import vectrill
from vectrill.dataframe import col, functions, window
import pandas as pd
import numpy as np

# Create simple test data
data = pd.DataFrame({
    'session_id': [1, 1, 2, 2],
    'timestamp': [pd.Timestamp('2023-01-01 10:00:00'), pd.Timestamp('2023-01-01 10:00:05'), 
               pd.Timestamp('2023-01-01 10:00:10'), pd.Timestamp('2023-01-01 10:00:15')]
})

print('Test data:')
print(data)

# Test window max/min functions
print('\nTesting window max/min functions...')

try:
    vectrill_df = vectrill.from_pandas(data)
    
    # Test max function
    result = vectrill_df.with_columns([
        functions.max(col('timestamp')).over(window.partition_by('session_id')).alias('session_end')
    ])
    
    print('Max function result:')
    print(result.to_pandas())
    
    # Test min function
    result2 = vectrill_df.with_columns([
        functions.min(col('timestamp')).over(window.partition_by('session_id')).alias('session_start')
    ])
    
    print('\nMin function result:')
    print(result2.to_pandas())
    
except Exception as e:
    print('Error:', e)
    import traceback
    traceback.print_exc()
