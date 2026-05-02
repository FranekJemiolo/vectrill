#!/usr/bin/env python3
import sys
sys.path.insert(0, 'tests/python')
sys.path.insert(0, 'python')

# Debug the WhenExpression issue
import vectrill
from vectrill.dataframe import col, functions, window
import pandas as pd
import numpy as np

# Create simple test data
data = pd.DataFrame({
    'service': ['auth', 'payment', 'auth', 'payment'],
    'level': ['INFO', 'ERROR', 'ERROR', 'INFO'],
    'response_time_ms': [100, 200, 150, 120]
})

print('Test data:')
print(data)

# Test the WhenExpression step by step
print('\nTesting WhenExpression...')

# First test the when expression alone
try:
    vectrill_df = vectrill.from_pandas(data)
    result = vectrill_df.with_columns([
        functions.when(col('level') == 'ERROR').then(1).otherwise(0).alias('is_error')
    ])
    print('WhenExpression result:')
    print(result.to_pandas())
except Exception as e:
    print('Error in WhenExpression:', e)
    import traceback
    traceback.print_exc()

# Test the sum of when expression
print('\nTesting sum(WhenExpression)...')
try:
    vectrill_df = vectrill.from_pandas(data)
    result = vectrill_df.with_columns([
        functions.sum(functions.when(col('level') == 'ERROR').then(1).otherwise(0)).alias('error_count')
    ])
    print('Sum(WhenExpression) result:')
    print(result.to_pandas())
except Exception as e:
    print('Error in sum(WhenExpression):', e)
    import traceback
    traceback.print_exc()

# Test the window version
print('\nTesting window version...')
try:
    vectrill_df = vectrill.from_pandas(data)
    result = vectrill_df.with_columns([
        functions.sum(functions.when(col('level') == 'ERROR').then(1).otherwise(0)).over(window.partition_by('service')).alias('error_count')
    ])
    print('Window version result:')
    print(result.to_pandas())
except Exception as e:
    print('Error in window version:', e)
    import traceback
    traceback.print_exc()
