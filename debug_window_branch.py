#!/usr/bin/env python3
import sys
sys.path.insert(0, 'tests/python')
sys.path.insert(0, 'python')

# Debug which window function branch is being taken
import vectrill
from vectrill.dataframe import col, functions, window
import pandas as pd

# Create simple test data
data = pd.DataFrame({
    'service': ['auth', 'payment', 'auth', 'payment'],
    'level': ['INFO', 'ERROR', 'ERROR', 'INFO'],
    'response_time_ms': [100, 200, 150, 120]
})

print('Test data:')
print(data)

# Test the window function branch selection
print('\nTesting window function branch selection...')

try:
    # Create the window expression like in the test
    window_expr = functions.sum(functions.when(col('level') == 'ERROR').then(1).otherwise(0)).over(window.partition_by('service'))
    
    print('Window expression created')
    print('Window expression type:', type(window_expr))
    
    # Check the window spec
    if hasattr(window_expr, 'window_spec'):
        print('Window spec:', window_expr.window_spec)
        window_spec = window_expr.window_spec
        partition_cols = getattr(window_spec, '_partition_columns', [])
        order_cols = getattr(window_spec, '_order_columns', [])
        print('Partition cols:', partition_cols)
        print('Order cols:', order_cols)
        
        if partition_cols and order_cols:
            print('Would go to: partition_cols and order_cols branch')
        elif partition_cols:
            print('Would go to: partition_cols only branch (EXPECTED)')
        elif order_cols:
            print('Would go to: order_cols only branch')
        else:
            print('Would go to: no partition or order branch')
    else:
        print('No window_spec attribute found')
    
    # Test the actual vectrill implementation
    vectrill_df = vectrill.from_pandas(data)
    result = vectrill_df.with_columns([
        window_expr.alias('error_count')
    ])
    
    print('Window result:')
    print(result.to_pandas())
    
except Exception as e:
    print('Error:', e)
    import traceback
    traceback.print_exc()
