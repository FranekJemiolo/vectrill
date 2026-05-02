#!/usr/bin/env python3
import sys
sys.path.insert(0, 'tests/python')
sys.path.insert(0, 'python')

# Debug the sum_when window function issue
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

# Test the sum_when window function step by step
print('\nTesting sum_when window function...')

try:
    vectrill_df = vectrill.from_pandas(data)
    
    # Create the expression
    when_expr = functions.when(col('level') == 'ERROR').then(1).otherwise(0)
    sum_expr = functions.sum(when_expr)
    window_expr = sum_expr.over(window.partition_by('service'))
    
    print('When expression type:', type(when_expr))
    print('When expression alias_name:', getattr(when_expr, 'alias_name', None))
    print('Sum expression type:', type(sum_expr))
    print('Sum expression name:', sum_expr.name)
    print('Window expression type:', type(window_expr))
    
    # Apply the window expression
    result = vectrill_df.with_columns([
        window_expr.alias('error_count')
    ])
    
    print('Window result:')
    print(result.to_pandas())
    
except Exception as e:
    print('Error:', e)
    import traceback
    traceback.print_exc()
