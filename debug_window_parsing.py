#!/usr/bin/env python3
import sys
sys.path.insert(0, 'tests/python')
sys.path.insert(0, 'python')

# Debug window function parsing
import vectrill
from vectrill.dataframe import col, functions, window
import pandas as pd
import numpy as np

# Create simple test data
data = pd.DataFrame({
    'group': ['X', 'X', 'Y', 'Y'],
    'value': [1, 2, 3, 4],
    'date': pd.date_range('2023-01-01', periods=4)
})

print("Testing window function parsing:")

try:
    # Create cumsum window expression
    cumsum_expr = functions.cumsum(col('value')).over(window.partition_by('group').order_by('date'))
    print(f"Cumsum expression: {cumsum_expr}")
    print(f"Cumsum expression type: {type(cumsum_expr)}")
    
    if hasattr(cumsum_expr, 'name'):
        print(f"Cumsum expression name: {cumsum_expr.name}")
    
    # Test the expression directly
    vectrill_df = vectrill.from_pandas(data)
    result_df = vectrill_df.with_columns([cumsum_expr.alias('cumsum_value')])
    print("Result:")
    print(result_df.to_pandas())
    
    # Test pandas equivalent
    pandas_result = data.copy()
    pandas_sorted = pandas_result.sort_values(['group', 'date'])
    pandas_sorted['cumsum_value'] = pandas_sorted.groupby('group')['value'].cumsum()
    pandas_result = pandas_sorted.sort_index()
    print("Pandas result:")
    print(pandas_result)
    
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()
