#!/usr/bin/env python3
import sys
sys.path.insert(0, 'tests/python')
sys.path.insert(0, 'python')

# Debug sort operation comprehensively
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

# Test vectrill sort operation step by step
print('\nTesting vectrill sort operation...')

try:
    vectrill_df = vectrill.from_pandas(data)
    
    print('Step 1: Create vectrill DataFrame')
    print('Columns:', vectrill_df.to_pandas().columns.tolist())
    
    print('\nStep 2: Call sort operation')
    # This is where the issue might be occurring
    try:
        result = vectrill_df.sort(['user_id', 'timestamp'])
        print('Sort result columns:', result.to_pandas().columns.tolist())
        print('Sort result data:')
        print(result.to_pandas())
        
    except Exception as e:
        print('Sort error:', e)
        import traceback
        traceback.print_exc()
    
except Exception as e:
    print('General error:', e)
    import traceback
    traceback.print_exc()
