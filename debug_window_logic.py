#!/usr/bin/env python3
import sys
sys.path.insert(0, 'tests/python')
sys.path.insert(0, 'python')

# Debug the window function logic
import vectrill
from vectrill.dataframe import col, functions, window
import pandas as pd

# Create simple test data
data = pd.DataFrame({
    'service': ['auth', 'payment', 'auth', 'payment'],
    'level': ['INFO', 'ERROR', 'ERROR', 'INFO']
})

print('Test data:')
print(data)

# Test the window function logic manually
print('\nTesting window function logic manually...')

try:
    df = data.copy()
    
    # Manually implement the sum_when window logic
    when_expr = functions.when(col('level') == 'ERROR').then(1).otherwise(0)
    
    # Apply when expression logic
    else_val = when_expr.otherwise_value if when_expr.otherwise_value is not None else 0
    df['temp_error'] = else_val
    
    print('After setting else value:')
    print(df[['service', 'level', 'temp_error']])
    
    # Process conditions
    for i, (condition, then_val) in enumerate(zip(when_expr.conditions, when_expr.then_values)):
        if isinstance(condition, dict):
            op = condition.get("op")
            col_name = condition.get("col")
            value = condition.get("value")
            
            print(f'Processing condition: {op} {col_name} {value}')
            
            if col_name in df.columns and op:
                condition_result = None
                
                # Create condition based on operator
                if op == "==":
                    condition_result = df[col_name] == value
                elif op == "!=":
                    condition_result = df[col_name] != value
                elif op == "<":
                    condition_result = df[col_name] < value
                elif op == ">":
                    condition_result = df[col_name] > value
                else:
                    condition_result = pd.Series([True] * len(df))
                
                print(f'Condition result: {condition_result.tolist()}')
                
                if condition_result is not None:
                    # Apply condition
                    mask = condition_result & (df['temp_error'] == else_val)
                    print(f'Mask: {mask.tolist()}')
                    df.loc[mask, 'temp_error'] = then_val
    
    print('After applying when expression:')
    print(df[['service', 'level', 'temp_error']])
    
    # Now apply window sum
    df['error_count'] = df.groupby('service')['temp_error'].cumsum()
    
    print('Final result:')
    print(df[['service', 'level', 'error_count']])
    
except Exception as e:
    print('Error:', e)
    import traceback
    traceback.print_exc()
