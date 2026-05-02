#!/usr/bin/env python3

# Debug simple test case
import vectrill
from vectrill.dataframe import col, functions, window
import pandas as pd
import numpy as np

# Create simple test data
data = pd.DataFrame({
    'session_id': [1, 1, 2, 2],
    'timestamp': [pd.Timestamp('2023-01-01 10:00:00'), pd.Timestamp('2023-01-01 10:00:05'), 
               pd.Timestamp('2023-01-01 10:00:10'), pd.Timestamp('2023-01-01 10:00:15')],
    'value': [10, 20, 5, 15]
})

print('Test data:')
print(data)

# Test vectrill implementation
print('\nTesting vectrill implementation...')

try:
    vectrill_df = vectrill.from_pandas(data)
    
    # Test simple window function
    result = vectrill_df.with_columns([
        functions.sum(col('value')).over(window.partition_by('session_id')).alias('total_value')
    ])
    
    print('Vectrill result:')
    print(result.to_pandas())
    
except Exception as e:
    print('Error:', e)
    import traceback
    traceback.print_exc()
