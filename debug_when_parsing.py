#!/usr/bin/env python3
import sys
sys.path.insert(0, 'tests/python')
sys.path.insert(0, 'python')

# Debug the when expression parsing
import vectrill
from vectrill.dataframe import col, functions, window
import pandas as pd

# Create simple test data
data = pd.DataFrame({
    'service': ['auth', 'payment'],
    'level': ['INFO', 'ERROR']
})

print('Test data:')
print(data)

# Test the when expression parsing
print('\nTesting when expression parsing...')

try:
    # Create the when expression
    when_expr = functions.when(col('level') == 'ERROR').then(1).otherwise(0)
    
    print('When expression conditions:', when_expr.conditions)
    print('When expression then values:', when_expr.then_values)
    print('When expression otherwise value:', when_expr.otherwise_value)
    
    # Check the condition
    condition = when_expr.conditions[0]
    print('Condition:', condition)
    print('Condition type:', type(condition))
    
    # Test the vectrill implementation directly
    vectrill_df = vectrill.from_pandas(data)
    result = vectrill_df.with_columns([
        when_expr.alias('is_error')
    ])
    
    print('When expression result:')
    print(result.to_pandas())
    
except Exception as e:
    print('Error:', e)
    import traceback
    traceback.print_exc()
