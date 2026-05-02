#!/usr/bin/env python3
import sys
sys.path.insert(0, 'tests/python')
sys.path.insert(0, 'python')

# Debug when function handler
import vectrill
from vectrill.dataframe import col, functions, window
import pandas as pd
import numpy as np

# Create simple test data
data = pd.DataFrame({
    'category': ['A', 'B'],
    'score': [10, 20]
})

print("Testing when function handler:")

try:
    # Create a simple when expression
    when_expr = functions.when(col('category') == 'A').then(col('score') * 2).otherwise(col('score'))
    print(f"when expression: {when_expr}")
    print(f"when expression type: {type(when_expr)}")
    
    # Test the expression directly
    vectrill_df = vectrill.from_pandas(data)
    
    # Check if the expression is recognized as WhenExpression
    print(f"Is WhenExpression: {isinstance(when_expr, vectrill.dataframe.WhenExpression)}")
    
    # Test with_columns
    result_df = vectrill_df.with_columns([when_expr.alias('category_score')])
    print("Result:")
    print(result_df.to_pandas())
    
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()
