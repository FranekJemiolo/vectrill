#!/usr/bin/env python3
import sys
sys.path.insert(0, 'tests/python')
sys.path.insert(0, 'python')

# Debug when function
import vectrill
from vectrill.dataframe import col, functions, window
import pandas as pd

# Create test data
data = pd.DataFrame({
    'category': ['A', 'B', 'C', 'A', 'B'],
    'score': [10, 20, 30, 40, 50]
})

print("Testing when function:")

try:
    # Test basic when function
    when_expr = functions.when(col('category') == 'A')
    print(f"when expression: {when_expr}")
    print(f"when expression type: {type(when_expr)}")
    
    # Test then method
    then_expr = when_expr.then(col('score') * 2)
    print(f"then expression: {then_expr}")
    print(f"then expression type: {type(then_expr)}")
    
    # Test otherwise method
    otherwise_expr = then_expr.otherwise(col('score'))
    print(f"otherwise expression: {otherwise_expr}")
    print(f"otherwise expression type: {type(otherwise_expr)}")
    
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()
