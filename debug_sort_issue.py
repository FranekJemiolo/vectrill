#!/usr/bin/env python3
import sys
sys.path.insert(0, 'tests/python')
sys.path.insert(0, 'python')

# Debug sort operation issue
import vectrill
from vectrill.dataframe import col, functions, window
import pandas as pd
import numpy as np

# Create simple test data
data = pd.DataFrame({
    'user_id': [1, 2, 1, 2],
    'timestamp': [pd.Timestamp('2023-01-01 10:00:00'), pd.Timestamp('2023-01-01 10:00:05'), 
               pd.Timestamp('2023-01-01 10:00:10'), pd.Timestamp('2023-01-01 10:00:15')]
})

print('Test data:')
print(data)

# Test vectrill sort operation
print('\nTesting vectrill sort operation...')

try:
    vectrill_df = vectrill.from_pandas(data)
    
    # Test sort operation
    result = vectrill_df.sort(['user_id', 'timestamp'])
    
    print('Sort result:')
    print(result.to_pandas())
    
except Exception as e:
    print('Error:', e)
    import traceback
    traceback.print_exc()
