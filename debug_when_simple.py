#!/usr/bin/env python3
import sys
sys.path.insert(0, 'tests/python')
sys.path.insert(0, 'python')

# Debug when function with fresh import
import vectrill
from vectrill.dataframe import col, functions, window
import pandas as pd

# Create test data
data = pd.DataFrame({
    'category': ['A', 'B', 'C', 'A', 'B'],
    'score': [10, 20, 30, 40, 50]
})

print("Testing when function with fresh import:")

try:
    # Test basic when function
    print("Creating when expression...")
    when_expr = functions.when(col('category') == 'A')
    print(f"when expression created: {when_expr}")
    
    # Test then method
    print("Adding then clause...")
    then_expr = when_expr.then(col('score') * 2)
    print(f"then expression created: {then_expr}")
    
    # Test otherwise method
    print("Adding otherwise clause...")
    otherwise_expr = then_expr.otherwise(col('score'))
    print(f"otherwise expression created: {otherwise_expr}")
    
    # Test with_columns
    print("Testing with_columns...")
    vectrill_df = vectrill.from_pandas(data)
    result_df = vectrill_df.with_columns([
        otherwise_expr.alias('category_score')
    ])
    print("Result:")
    print(result_df.to_pandas())
    
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()
