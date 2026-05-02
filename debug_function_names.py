#!/usr/bin/env python3
import sys
sys.path.insert(0, 'tests/python')
sys.path.insert(0, 'python')

# Debug function names
import vectrill
from vectrill.dataframe import col, functions, window
import pandas as pd

# Create test data
data = pd.DataFrame({
    'amount': [100, -200, 300]
})

print("Testing function name generation:")

try:
    # Test what the abs function actually creates
    abs_expr = functions.abs(col('amount'))
    print(f"abs expression: {abs_expr}")
    print(f"abs expression name: {abs_expr.name}")
    print(f"abs expression type: {type(abs_expr)}")
    
    # Test what the var function creates
    var_expr = functions.var(col('amount'))
    print(f"var expression: {var_expr}")
    print(f"var expression name: {var_expr.name}")
    
    # Test what the round function creates
    round_expr = functions.round(col('amount'), 0)
    print(f"round expression: {round_expr}")
    print(f"round expression name: {round_expr.name}")
    
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()
